from __future__ import annotations

from pathlib import Path

from oracle_pg_sync.utils.naming import split_schema_table


def write_schema_suggestions(path: Path, column_diff_rows: list[dict], *, suggest_drop: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "-- AUTO-GENERATED SCHEMA SUGGESTIONS",
        "-- Review with DBA before executing in production.",
        "",
    ]
    current_table = ""
    for row in column_diff_rows:
        diff_type = row.get("diff_type")
        if diff_type == "missing_in_postgres":
            statement = _add_column_statement(row)
        elif diff_type == "extra_in_postgres" and suggest_drop:
            statement = _drop_column_statement(row)
        else:
            continue
        table_name = str(row.get("table_name") or "")
        if table_name != current_table:
            if current_table:
                lines.append("")
            lines.append(f"-- {table_name}")
            current_table = table_name
        lines.append(statement)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _add_column_statement(row: dict) -> str:
    table = split_schema_table(str(row["table_name"]))
    column_name = str(row["column_name"])
    pg_type = str(row.get("suggested_pg_type") or "TEXT")
    return f"ALTER TABLE {_table_sql(table.schema, table.table)} ADD COLUMN {_quote_ident(column_name)} {pg_type};"


def _drop_column_statement(row: dict) -> str:
    table = split_schema_table(str(row["table_name"]))
    column_name = str(row["column_name"])
    return f"ALTER TABLE {_table_sql(table.schema, table.table)} DROP COLUMN {_quote_ident(column_name)};"


def _table_sql(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
