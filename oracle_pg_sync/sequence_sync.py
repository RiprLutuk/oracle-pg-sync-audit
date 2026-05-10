from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from psycopg import sql

from oracle_pg_sync.config import AppConfig, TableConfig
from oracle_pg_sync.db import oracle, postgres
from oracle_pg_sync.utils.naming import split_schema_table


@dataclass
class PostgresSequenceCandidate:
    schema: str
    name: str
    table_schema: str
    table_name: str
    column_name: str
    dependency_kind: str

    @property
    def fqname(self) -> str:
        return f"{self.schema}.{self.name}"


def sync_postgres_sequences_from_oracle(
    config: AppConfig,
    tables: list[str],
    logger: logging.Logger,
    *,
    execute: bool = False,
    sequence_buffer: int = 0,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with oracle.connect(config.oracle) as ocon, postgres.connect(config.postgres, autocommit=True) as pcon:
        with ocon.cursor() as ocur, pcon.cursor() as pcur:
            oracle_sequences = _oracle_sequences(ocur, config.oracle.schema)
            for table_name in tables:
                table_cfg = config.table_config(table_name) or TableConfig(name=table_name)
                target = split_schema_table(table_cfg.name, config.postgres.schema)
                pg_schema = table_cfg.target_schema or target.schema
                pg_table = table_cfg.target_table or target.table
                oracle_schema = table_cfg.source_schema or config.oracle.schema
                oracle_table = table_cfg.source_table or target.table
                candidates = _postgres_sequence_candidates(pcur, pg_schema, pg_table)
                if not candidates:
                    rows.append(
                        _row(
                            table_name=f"{pg_schema}.{pg_table}",
                            status="SKIPPED",
                            message="no PostgreSQL serial/identity/name-match sequence found",
                        )
                    )
                    continue
                for candidate in candidates:
                    rows.append(
                        _sync_one_sequence(
                            pcur,
                            candidate,
                            oracle_sequences,
                            oracle_schema=oracle_schema,
                            oracle_table=oracle_table,
                            logger=logger,
                            execute=execute,
                            sequence_buffer=sequence_buffer,
                        )
                    )
    return rows


def _sync_one_sequence(
    pcur: Any,
    candidate: PostgresSequenceCandidate,
    oracle_sequences: dict[str, dict[str, Any]],
    *,
    oracle_schema: str,
    oracle_table: str,
    logger: logging.Logger,
    execute: bool,
    sequence_buffer: int,
) -> dict[str, Any]:
    oracle_seq = _match_oracle_sequence(candidate, oracle_sequences, oracle_table)
    table_fqname = f"{candidate.table_schema}.{candidate.table_name}"
    if not oracle_seq:
        return _row(
            table_name=table_fqname,
            postgres_sequence=candidate.fqname,
            postgres_column=candidate.column_name,
            dependency_kind=candidate.dependency_kind,
            status="SKIPPED",
            message="no matching Oracle sequence found",
        )

    pg_info = _postgres_sequence_info(pcur, candidate.schema, candidate.name)
    table_max = _table_max_value(pcur, candidate.table_schema, candidate.table_name, candidate.column_name)
    pg_increment = int(pg_info.get("increment_by") or 1)
    oracle_last = int(oracle_seq["last_number"])
    postgres_current_next = _postgres_current_next(pg_info, pg_increment)
    desired_next = _desired_next_value(
        oracle_last=oracle_last,
        table_max=table_max,
        postgres_current_next=postgres_current_next,
        increment=pg_increment,
        sequence_buffer=sequence_buffer,
    )
    status = "DRY_RUN"
    message = ""
    if execute:
        _set_postgres_sequence(pcur, candidate.schema, candidate.name, desired_next)
        status = "SET"
        message = f"set PostgreSQL nextval to {desired_next}"
        logger.info(
            "Sequence set %s oracle=%s.%s oracle_last=%s buffer=%s table_max=%s pg_current_next=%s set_to=%s",
            candidate.fqname,
            oracle_schema,
            oracle_seq["sequence_name"],
            oracle_last,
            sequence_buffer,
            table_max,
            postgres_current_next,
            desired_next,
        )

    return _row(
        table_name=table_fqname,
        postgres_sequence=candidate.fqname,
        postgres_column=candidate.column_name,
        dependency_kind=candidate.dependency_kind,
        oracle_sequence=f"{oracle_schema}.{oracle_seq['sequence_name']}",
        oracle_last_number=oracle_last,
        sequence_buffer=sequence_buffer,
        postgres_current_next=postgres_current_next,
        table_max_value=table_max,
        postgres_set_to=desired_next,
        status=status,
        message=message,
    )


def _oracle_sequences(cur: Any, schema: str) -> dict[str, dict[str, Any]]:
    cur.execute(
        """
        SELECT sequence_name, last_number, increment_by, min_value, max_value, cache_size
        FROM all_sequences
        WHERE sequence_owner = :owner
        """,
        {"owner": schema.upper()},
    )
    result: dict[str, dict[str, Any]] = {}
    for sequence_name, last_number, increment_by, min_value, max_value, cache_size in cur.fetchall():
        result[str(sequence_name).upper()] = {
            "sequence_name": str(sequence_name).upper(),
            "last_number": int(last_number),
            "increment_by": int(increment_by),
            "min_value": min_value,
            "max_value": max_value,
            "cache_size": cache_size,
        }
    return result


def _postgres_sequence_candidates(cur: Any, schema: str, table: str) -> list[PostgresSequenceCandidate]:
    candidates: list[PostgresSequenceCandidate] = []
    seen: set[tuple[str, str]] = set()
    cur.execute(
        """
        SELECT a.attname, pg_get_serial_sequence(format('%%I.%%I', n.nspname, c.relname), a.attname)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = %s
          AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (schema, table),
    )
    for column_name, sequence_fqname in cur.fetchall():
        if not sequence_fqname:
            continue
        seq_schema, seq_name = _split_pg_fqname(str(sequence_fqname), default_schema=schema)
        key = (seq_schema, seq_name)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            PostgresSequenceCandidate(
                schema=seq_schema,
                name=seq_name,
                table_schema=schema,
                table_name=table,
                column_name=str(column_name),
                dependency_kind="serial_or_identity",
            )
        )

    cur.execute(
        """
        SELECT schemaname, sequencename
        FROM pg_sequences
        WHERE schemaname = %s
          AND sequencename ILIKE '%%' || %s || '%%'
        ORDER BY schemaname, sequencename
        """,
        (schema, table),
    )
    for seq_schema, seq_name in cur.fetchall():
        key = (str(seq_schema), str(seq_name))
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            PostgresSequenceCandidate(
                schema=str(seq_schema),
                name=str(seq_name),
                table_schema=schema,
                table_name=table,
                column_name="",
                dependency_kind="name_match",
            )
        )
    return candidates


def _match_oracle_sequence(
    candidate: PostgresSequenceCandidate,
    oracle_sequences: dict[str, dict[str, Any]],
    oracle_table: str,
) -> dict[str, Any] | None:
    exact = oracle_sequences.get(candidate.name.upper())
    if exact:
        return exact
    table_token = oracle_table.upper()
    matches = [row for name, row in oracle_sequences.items() if table_token in name]
    if len(matches) == 1:
        return matches[0]
    return None


def _postgres_sequence_info(cur: Any, schema: str, sequence: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT schemaname, sequencename, increment_by, last_value
        FROM pg_sequences
        WHERE schemaname = %s AND sequencename = %s
        """,
        (schema, sequence),
    )
    row = cur.fetchone()
    if not row:
        return {"increment_by": 1, "last_value": None, "is_called": False}
    last_value, is_called = _sequence_last_value(cur, schema, sequence)
    return {
        "schema": row[0],
        "sequence": row[1],
        "increment_by": row[2],
        "last_value": last_value,
        "is_called": is_called,
    }


def _sequence_last_value(cur: Any, schema: str, sequence: str) -> tuple[int | None, bool]:
    cur.execute(sql.SQL("SELECT last_value, is_called FROM {}").format(postgres.table_ident(schema, sequence)))
    row = cur.fetchone()
    if not row:
        return None, False
    return (int(row[0]) if row[0] is not None else None, bool(row[1]))


def _postgres_current_next(info: dict[str, Any], increment: int) -> int | None:
    last_value = info.get("last_value")
    if last_value is None:
        return None
    return int(last_value) + increment if info.get("is_called") else int(last_value)


def _table_max_value(cur: Any, schema: str, table: str, column: str) -> int | None:
    if not column:
        return None
    cur.execute(
        sql.SQL("SELECT MAX({}) FROM {}").format(
            sql.Identifier(column),
            postgres.table_ident(schema, table),
        )
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _desired_next_value(
    *,
    oracle_last: int,
    table_max: int | None,
    postgres_current_next: int | None,
    increment: int,
    sequence_buffer: int = 0,
) -> int:
    candidates = [oracle_last + max(int(sequence_buffer or 0), 0)]
    if postgres_current_next is not None:
        candidates.append(postgres_current_next)
    if table_max is not None:
        step = abs(int(increment or 1))
        candidates.append(table_max + step)
    return max(candidates)


def _set_postgres_sequence(cur: Any, schema: str, sequence: str, next_value: int) -> None:
    cur.execute(
        "SELECT setval(%s::regclass, %s, false)",
        (_regclass_name(schema, sequence), int(next_value)),
    )


def _regclass_name(schema: str, sequence: str) -> str:
    safe_schema = schema.replace('"', '""')
    safe_sequence = sequence.replace('"', '""')
    return f'"{safe_schema}"."{safe_sequence}"'


def _split_pg_fqname(value: str, *, default_schema: str) -> tuple[str, str]:
    cleaned = value.replace('"', "")
    if "." not in cleaned:
        return default_schema, cleaned
    schema, name = cleaned.rsplit(".", 1)
    return schema, name


def _row(**values: Any) -> dict[str, Any]:
    fields = [
        "table_name",
        "postgres_sequence",
        "postgres_column",
        "dependency_kind",
        "oracle_sequence",
        "oracle_last_number",
        "sequence_buffer",
        "postgres_current_next",
        "table_max_value",
        "postgres_set_to",
        "status",
        "message",
    ]
    return {field: values.get(field, "") for field in fields}
