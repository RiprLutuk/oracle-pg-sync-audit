from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from html import escape
from pathlib import Path
from typing import Any

from oracle_pg_sync.config import AppConfig
from oracle_pg_sync.db import oracle, postgres
from oracle_pg_sync.reports.writer_csv import write_csv


@dataclass
class QueryPerfOptions:
    query_file: Path
    database: str = "both"
    include_refactors: bool = True
    timeout_seconds: int = 300


@dataclass
class QueryVariant:
    name: str
    sql: str
    notes: list[str] = field(default_factory=list)


def run_query_perf_report(config: AppConfig, options: QueryPerfOptions, run_dir: Path) -> list[dict[str, Any]]:
    sql = _read_select_sql(options.query_file)
    variants = build_query_variants(sql) if options.include_refactors else [QueryVariant("original", sql, [])]
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_variant_sql(run_dir / "query_variants.sql", variants)

    rows: list[dict[str, Any]] = []
    if options.database in {"both", "postgres"}:
        rows.extend(_run_postgres_variants(config, variants, run_dir, options.timeout_seconds))
    if options.database in {"both", "oracle"}:
        rows.extend(_run_oracle_variants(config, variants, run_dir, options.timeout_seconds))

    rows = _add_comparison(rows)
    recommendations = recommendation_rows(rows, variants)
    write_csv(run_dir / "query_perf_summary.csv", rows)
    write_csv(run_dir / "query_perf_recommendations.csv", recommendations)
    write_query_perf_html(run_dir / "query_perf_report.html", rows, recommendations, variants)
    return rows


def build_query_variants(sql: str) -> list[QueryVariant]:
    variants: list[QueryVariant] = [QueryVariant("original", normalize_sql(sql), ["Query asli"])]
    explicit = rewrite_implicit_join(variants[0].sql)
    if explicit != variants[0].sql:
        variants.append(
            QueryVariant(
                "explicit_join",
                explicit,
                ["Mengubah comma/implicit join menjadi ANSI JOIN agar join predicate lebih jelas."],
            )
        )
    base_count = len(variants)
    for variant in list(variants[:base_count]):
        rewritten, changed = rewrite_not_in_to_not_exists(variant.sql)
        if not changed:
            continue
        variants.append(
            QueryVariant(
                f"{variant.name}_not_exists" if variant.name != "original" else "not_exists",
                rewritten,
                [
                    "Mengubah NOT IN menjadi NOT EXISTS/anti-join pattern.",
                    "Pastikan kolom subquery tidak mengandung NULL sebelum menganggap hasilnya ekuivalen 100%.",
                ],
            )
        )
    return _dedupe_variants(variants)


def normalize_sql(sql: str) -> str:
    cleaned = sql.strip()
    while cleaned.endswith(";"):
        cleaned = cleaned[:-1].rstrip()
    return cleaned


def rewrite_implicit_join(sql: str) -> str:
    match = re.search(r"(?is)\bfrom\s+(?P<from>.+?)\bwhere\s+(?P<where>.+)$", sql)
    if not match:
        return sql
    from_part = match.group("from").strip()
    if "," not in from_part:
        return sql
    tables = [part.strip() for part in from_part.split(",")]
    if len(tables) != 2:
        return sql
    left = _parse_table_alias(tables[0])
    right = _parse_table_alias(tables[1])
    if not left or not right:
        return sql
    left_table, left_alias = left
    right_table, right_alias = right
    where_body, tail = _split_tail_clause(match.group("where").strip())
    predicates = _split_and_predicates(where_body)
    join_predicate = ""
    rest: list[str] = []
    alias_pattern = rf"(?:{re.escape(left_alias)}|{re.escape(right_alias)})"
    join_re = re.compile(rf"(?is)^\s*{alias_pattern}\.\w+\s*=\s*{alias_pattern}\.\w+\s*$")
    for predicate in predicates:
        if not join_predicate and join_re.match(predicate):
            aliases = set(re.findall(r"\b(\w+)\.", predicate))
            if {left_alias, right_alias}.issubset(aliases):
                join_predicate = predicate.strip()
                continue
        rest.append(predicate.strip())
    if not join_predicate:
        return sql
    where_sql = ""
    if rest:
        where_sql = " WHERE " + " AND ".join(rest)
    new_from = f"FROM {left_table} {left_alias} JOIN {right_table} {right_alias} ON {join_predicate}"
    return sql[: match.start()].rstrip() + "\n" + new_from + where_sql + tail


def rewrite_not_in_to_not_exists(sql: str) -> tuple[str, bool]:
    counter = 0

    def repl(match: re.Match[str]) -> str:
        nonlocal counter
        counter += 1
        outer = match.group("outer")
        inner_col = match.group("inner_col")
        table = match.group("table")
        alias = match.group("alias") or f"perf_ref_{counter}"
        return f"NOT EXISTS (SELECT 1 FROM {table} {alias} WHERE {alias}.{inner_col} = {outer})"

    rewritten = re.sub(
        r"(?is)(?P<outer>(?:\b\w+\.)?\b\w+)\s+NOT\s+IN\s*"
        r"\(\s*SELECT\s+(?P<inner_col>\w+)\s+FROM\s+(?P<table>[A-Za-z_][\w.$\"]*)"
        r"(?:\s+(?P<alias>\w+))?\s*\)",
        repl,
        sql,
    )
    return rewritten, rewritten != sql


def recommendation_rows(rows: list[dict[str, Any]], variants: list[QueryVariant]) -> list[dict[str, Any]]:
    by_db: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("status") == "OK":
            by_db.setdefault(str(row.get("database")), []).append(row)
    recommendations: list[dict[str, Any]] = []
    variant_notes = {variant.name: " ".join(variant.notes) for variant in variants}
    for db, db_rows in by_db.items():
        original = next((row for row in db_rows if row.get("variant") == "original"), None)
        best = min(db_rows, key=lambda row: float(row.get("runtime_ms") or 10**18))
        original_ms = float(original.get("runtime_ms") or 0) if original else 0.0
        best_ms = float(best.get("runtime_ms") or 0)
        improvement = ((original_ms - best_ms) / original_ms * 100) if original_ms else 0.0
        recommendations.append(
            {
                "database": db,
                "recommended_variant": best.get("variant"),
                "runtime_ms": round(best_ms, 3),
                "original_runtime_ms": round(original_ms, 3) if original else "",
                "improvement_percent": round(improvement, 2),
                "recommendation": _recommendation_text(str(best.get("variant")), improvement),
                "notes": variant_notes.get(str(best.get("variant")), ""),
            }
        )
    return recommendations


def write_query_perf_html(
    path: Path,
    rows: list[dict[str, Any]],
    recommendations: list[dict[str, Any]],
    variants: list[QueryVariant],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Query Performance Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #18202a; }}
    table {{ border-collapse: collapse; width: 100%; margin: 16px 0 28px; }}
    th, td {{ border: 1px solid #d7dde5; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #eef3f8; }}
    code, pre {{ background: #f6f8fa; }}
    pre {{ padding: 12px; overflow: auto; border: 1px solid #d7dde5; }}
    .ok {{ color: #116329; font-weight: 700; }}
    .err {{ color: #b42318; font-weight: 700; }}
  </style>
</head>
<body>
  <h1>Query Performance Report</h1>
  <h2>Recommendations</h2>
  {_html_table(recommendations)}
  <h2>Benchmark Summary</h2>
  {_html_table(rows)}
  <h2>Query Variants</h2>
  {''.join(_variant_html(variant) for variant in variants)}
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def _run_postgres_variants(
    config: AppConfig,
    variants: list[QueryVariant],
    run_dir: Path,
    timeout_seconds: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with postgres.connect(config.postgres, autocommit=True) as con:
        with con.cursor() as cur:
            if config.postgres.schema:
                cur.execute(f"SET search_path TO {_pg_ident(config.postgres.schema)}")
            cur.execute("SELECT set_config('statement_timeout', %s, false)", (f"{int(timeout_seconds) * 1000}ms",))
            for variant in variants:
                statement = _count_wrapper(variant.sql, database="postgres")
                started = time.perf_counter()
                row: dict[str, Any] = {
                    "database": "postgres",
                    "variant": variant.name,
                    "status": "OK",
                    "runtime_ms": "",
                    "row_count": "",
                    "planning_ms": "",
                    "execution_ms": "",
                    "total_cost": "",
                    "plan_rows": "",
                    "plan_node": "",
                    "error": "",
                }
                try:
                    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON) {statement}")
                    plan_payload = cur.fetchone()[0]
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    parsed = _json_plan_payload(plan_payload)
                    plan_path = run_dir / f"postgres_plan_{variant.name}.json"
                    plan_path.write_text(json.dumps(parsed, indent=2, default=str), encoding="utf-8")
                    top = parsed[0] if isinstance(parsed, list) and parsed else {}
                    plan = top.get("Plan", {})
                    row.update(
                        {
                            "runtime_ms": round(elapsed_ms, 3),
                            "planning_ms": top.get("Planning Time", ""),
                            "execution_ms": top.get("Execution Time", ""),
                            "total_cost": plan.get("Total Cost", ""),
                            "plan_rows": plan.get("Plan Rows", ""),
                            "plan_node": plan.get("Node Type", ""),
                            "plan_file": plan_path.name,
                        }
                    )
                    count_started = time.perf_counter()
                    cur.execute(statement)
                    row_count = cur.fetchone()[0]
                    row["row_count"] = int(row_count)
                    row["count_runtime_ms"] = round((time.perf_counter() - count_started) * 1000, 3)
                except Exception as exc:
                    row.update({"status": _error_status(exc), "error": str(exc)})
                rows.append(row)
    return rows


def _run_oracle_variants(
    config: AppConfig,
    variants: list[QueryVariant],
    run_dir: Path,
    timeout_seconds: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with oracle.connect(config.oracle) as con:
        if hasattr(con, "call_timeout"):
            con.call_timeout = int(timeout_seconds) * 1000
        with con.cursor() as cur:
            if config.oracle.schema:
                cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {oracle.qident(config.oracle.schema.upper())}")
            try:
                cur.callproc("DBMS_SESSION.SET_SQL_TRACE", [False])
            except Exception:
                pass
            for variant in variants:
                statement = _count_wrapper(variant.sql, database="oracle")
                row: dict[str, Any] = {
                    "database": "oracle",
                    "variant": variant.name,
                    "status": "OK",
                    "runtime_ms": "",
                    "row_count": "",
                    "planning_ms": "",
                    "execution_ms": "",
                    "total_cost": "",
                    "plan_rows": "",
                    "plan_node": "",
                    "error": "",
                }
                try:
                    cur.execute(f"EXPLAIN PLAN FOR {statement}")
                    cur.execute("SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, NULL, 'BASIC +COST +BYTES +PREDICATE'))")
                    plan_text = "\n".join(str(item[0]) for item in cur.fetchall())
                    plan_path = run_dir / f"oracle_plan_{variant.name}.txt"
                    plan_path.write_text(plan_text, encoding="utf-8")
                    started = time.perf_counter()
                    cur.execute(statement)
                    row_count = cur.fetchone()[0]
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    row.update(
                        {
                            "runtime_ms": round(elapsed_ms, 3),
                            "execution_ms": round(elapsed_ms, 3),
                            "row_count": int(row_count),
                            "plan_file": plan_path.name,
                        }
                    )
                    if elapsed_ms > timeout_seconds * 1000:
                        row["status"] = "TIMEOUT_REVIEW"
                except Exception as exc:
                    row.update({"status": _error_status(exc), "error": str(exc)})
                rows.append(row)
    return rows


def _add_comparison(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    originals: dict[str, float] = {}
    for row in rows:
        if row.get("variant") == "original" and row.get("status") == "OK" and row.get("runtime_ms") != "":
            originals[str(row.get("database"))] = float(row["runtime_ms"])
    for row in rows:
        original = originals.get(str(row.get("database")))
        runtime = float(row.get("runtime_ms") or 0)
        if original and runtime:
            row["vs_original_percent"] = round((runtime - original) / original * 100, 2)
        else:
            row["vs_original_percent"] = ""
    return rows


def _read_select_sql(path: Path) -> str:
    sql = normalize_sql(path.read_text(encoding="utf-8"))
    if not re.match(r"(?is)^\s*(select|with)\b", sql):
        raise ValueError("query-perf hanya menerima SELECT/WITH query")
    if ";" in sql:
        raise ValueError("query-perf hanya menerima satu statement tanpa semicolon di tengah")
    return sql


def _count_wrapper(sql: str, *, database: str) -> str:
    body = _remove_top_level_order_by(normalize_sql(sql))
    alias = "perf_q"
    if database == "oracle":
        return f"SELECT COUNT(*) FROM ({body}) {alias}"
    return f"SELECT COUNT(*) FROM ({body}) AS {alias}"


def _remove_top_level_order_by(sql: str) -> str:
    match = re.search(r"(?is)\border\s+by\b", sql)
    if not match:
        return sql
    return sql[: match.start()].rstrip()


def _split_tail_clause(sql: str) -> tuple[str, str]:
    match = re.search(r"(?is)\b(order\s+by|group\s+by|having|limit|fetch\s+first)\b", sql)
    if not match:
        return sql.strip(), ""
    return sql[: match.start()].strip(), " " + sql[match.start() :].strip()


def _split_and_predicates(where_body: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?is)\s+AND\s+", where_body) if part.strip()]


def _parse_table_alias(value: str) -> tuple[str, str] | None:
    parts = value.split()
    if len(parts) == 1:
        table = parts[0]
        alias = table.split(".")[-1].strip('"')
        return table, alias
    if len(parts) == 2:
        return parts[0], parts[1]
    return None


def _dedupe_variants(variants: list[QueryVariant]) -> list[QueryVariant]:
    seen: set[str] = set()
    result: list[QueryVariant] = []
    for variant in variants:
        key = re.sub(r"\s+", " ", variant.sql).strip().lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(variant)
    return result


def _write_variant_sql(path: Path, variants: list[QueryVariant]) -> None:
    chunks: list[str] = []
    for variant in variants:
        chunks.append(f"-- variant: {variant.name}")
        for note in variant.notes:
            chunks.append(f"-- note: {note}")
        chunks.append(variant.sql.rstrip() + ";")
        chunks.append("")
    path.write_text("\n".join(chunks), encoding="utf-8")


def _json_plan_payload(payload: Any) -> Any:
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


def _pg_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _recommendation_text(variant: str, improvement: float) -> str:
    if variant == "original":
        return "Query asli masih paling cepat pada benchmark ini."
    if improvement > 5:
        return "Pertimbangkan variant ini; benchmark menunjukkan peningkatan yang jelas."
    return "Variant ini paling cepat, tapi selisihnya kecil. Review plan dan hasil row count dulu."


def _error_status(exc: Exception) -> str:
    text = str(exc).lower()
    if "timeout" in text or "timed out" in text:
        return "TIMEOUT"
    return "ERROR"


def _html_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    head = "".join(f"<th>{escape(field)}</th>" for field in fields)
    body = []
    for row in rows:
        cells = "".join(f"<td>{escape(str(row.get(field, '')))}</td>" for field in fields)
        body.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def _variant_html(variant: QueryVariant) -> str:
    notes = "".join(f"<li>{escape(note)}</li>" for note in variant.notes)
    return f"<h3>{escape(variant.name)}</h3><ul>{notes}</ul><pre>{escape(variant.sql)}</pre>"
