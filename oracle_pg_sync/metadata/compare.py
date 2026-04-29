from __future__ import annotations

from dataclasses import dataclass

from oracle_pg_sync.config import AppConfig
from oracle_pg_sync.metadata.oracle_metadata import OracleTableMetadata
from oracle_pg_sync.metadata.postgres_metadata import PostgresTableMetadata
from oracle_pg_sync.metadata.type_mapping import (
    ColumnMeta,
    oracle_type_label,
    pg_type_label,
    suggested_pg_type,
)
from oracle_pg_sync.schema.type_compat import (
    COMPATIBLE_EXACT,
    INCOMPATIBLE,
    SEVERITY_ERROR,
    SEVERITY_OK,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    assess_column_compatibility,
    assess_ordinal_difference,
)
from oracle_pg_sync.utils.naming import split_schema_table


@dataclass
class AuditResult:
    inventory_rows: list[dict]
    column_diff_rows: list[dict]
    type_mismatch_rows: list[dict]
    dependency_rows: list[dict]


def compare_table_metadata(
    *,
    table_name: str,
    config: AppConfig,
    oracle_meta: OracleTableMetadata,
    postgres_meta: PostgresTableMetadata,
) -> tuple[dict, list[dict], list[dict]]:
    table = split_schema_table(table_name, config.postgres.schema)
    rename_map = config.rename_columns.get(table.key, {})

    oracle_cols = _mapped_oracle_columns(oracle_meta.columns, rename_map)
    pg_cols = {c.normalized_name: c for c in postgres_meta.columns}

    missing_in_pg = sorted(set(oracle_cols) - set(pg_cols))
    extra_in_pg = sorted(set(pg_cols) - set(oracle_cols))
    common = sorted(set(oracle_cols) & set(pg_cols), key=lambda name: oracle_cols[name].ordinal)

    column_diff_rows: list[dict] = []
    type_mismatch_rows: list[dict] = []

    for name in missing_in_pg:
        col = oracle_cols[name]
        row = _diff_row(
            table_name=table.fqname,
            column_name=name,
            oracle_col=col,
            postgres_col=None,
            diff_type="missing_in_postgres",
            compatibility_status=INCOMPATIBLE,
            severity=SEVERITY_ERROR,
            reason="Column exists in Oracle but is missing in PostgreSQL.",
            suggested_action=f"Add PostgreSQL column as {suggested_pg_type(col)}.",
        )
        column_diff_rows.append(row)
    for name in extra_in_pg:
        col = pg_cols[name]
        column_diff_rows.append(
            _diff_row(
                table_name=table.fqname,
                column_name=name,
                oracle_col=None,
                postgres_col=col,
                diff_type="extra_in_postgres",
                compatibility_status=INCOMPATIBLE,
                severity=SEVERITY_ERROR,
                reason="Column exists in PostgreSQL but not in Oracle.",
                suggested_action="Drop or ignore the PostgreSQL-only column after DBA review.",
            )
        )

    for name in common:
        oracle_col = oracle_cols[name]
        pg_col = pg_cols[name]
        if oracle_col.ordinal != pg_col.ordinal:
            column_diff_rows.append(
                _diff_row(
                    table_name=table.fqname,
                    column_name=name,
                    oracle_col=oracle_col,
                    postgres_col=pg_col,
                    diff_type="ordinal_mismatch",
                    **assess_ordinal_difference(oracle_col, pg_col).__dict__,
                )
            )
        assessment = assess_column_compatibility(oracle_col, pg_col)
        if assessment.compatibility_status != COMPATIBLE_EXACT and assessment.severity != SEVERITY_OK:
            row = _diff_row(
                table_name=table.fqname,
                column_name=name,
                oracle_col=oracle_col,
                postgres_col=pg_col,
                diff_type="type_compatibility",
                **assessment.__dict__,
            )
            column_diff_rows.append(row)
            if assessment.compatibility_status == INCOMPATIBLE:
                type_mismatch_rows.append(row)

    row_count_match = (
        oracle_meta.row_count is not None
        and postgres_meta.row_count is not None
        and oracle_meta.row_count == postgres_meta.row_count
    )
    schema_error_count = sum(1 for row in column_diff_rows if str(row.get("severity")) == SEVERITY_ERROR)
    schema_warning_count = sum(1 for row in column_diff_rows if str(row.get("severity")) == SEVERITY_WARNING)
    schema_info_count = sum(1 for row in column_diff_rows if str(row.get("severity")) == SEVERITY_INFO)
    column_structure_match = schema_error_count == 0

    if not oracle_meta.exists or not postgres_meta.exists:
        status = "MISSING"
    elif schema_error_count:
        status = "MISMATCH"
    elif schema_warning_count or not row_count_match:
        status = "WARNING"
    else:
        status = "MATCH"

    inventory = {
        "table_name": table.fqname,
        "oracle_exists": oracle_meta.exists,
        "postgres_exists": postgres_meta.exists,
        "oracle_row_count": oracle_meta.row_count,
        "postgres_row_count": postgres_meta.row_count,
        "row_count_match": row_count_match,
        "oracle_column_count": len(oracle_meta.columns),
        "postgres_column_count": len(postgres_meta.columns),
        "column_structure_match": column_structure_match,
        "type_mismatch_count": len(type_mismatch_rows),
        "schema_diff_error_count": schema_error_count,
        "schema_diff_warning_count": schema_warning_count,
        "schema_diff_info_count": schema_info_count,
        "missing_columns_in_pg": ";".join(missing_in_pg),
        "extra_columns_in_pg": ";".join(extra_in_pg),
        "index_count_oracle": oracle_meta.object_counts.get("index_count_oracle", 0),
        "index_count_postgres": postgres_meta.object_counts.get("index_count_postgres", 0),
        "view_count_related_oracle": oracle_meta.object_counts.get("view_count_related_oracle", 0),
        "view_count_related_postgres": postgres_meta.object_counts.get("view_count_related_postgres", 0),
        "sequence_count_oracle": oracle_meta.object_counts.get("sequence_count_oracle", 0),
        "sequence_count_postgres": postgres_meta.object_counts.get("sequence_count_postgres", 0),
        "stored_procedure_count_related_oracle": oracle_meta.object_counts.get(
            "stored_procedure_count_related_oracle", 0
        ),
        "function_count_related_postgres": postgres_meta.object_counts.get("function_count_related_postgres", 0),
        "trigger_count_oracle": oracle_meta.object_counts.get("trigger_count_oracle", 0),
        "trigger_count_postgres": postgres_meta.object_counts.get("trigger_count_postgres", 0),
        "constraint_count_oracle": oracle_meta.object_counts.get("constraint_count_oracle", 0),
        "constraint_count_postgres": postgres_meta.object_counts.get("constraint_count_postgres", 0),
        "status": status,
    }
    return inventory, column_diff_rows, type_mismatch_rows


def _mapped_oracle_columns(columns: list[ColumnMeta], rename_map: dict[str, str]) -> dict[str, ColumnMeta]:
    result: dict[str, ColumnMeta] = {}
    for col in columns:
        mapped_name = rename_map.get(col.normalized_name, col.normalized_name)
        result[mapped_name] = ColumnMeta(
            name=mapped_name,
            ordinal=col.ordinal,
            data_type=col.data_type,
            char_length=col.char_length,
            data_length=col.data_length,
            numeric_precision=col.numeric_precision,
            numeric_scale=col.numeric_scale,
            nullable=col.nullable,
            default=col.default,
            udt_name=col.udt_name,
        )
    return result


def inventory_has_fatal_mismatch(inventory_row: dict) -> bool:
    return (
        not inventory_row.get("oracle_exists")
        or not inventory_row.get("postgres_exists")
        or int(inventory_row.get("schema_diff_error_count") or 0) > 0
    )


def _diff_row(
    *,
    table_name: str,
    column_name: str,
    oracle_col: ColumnMeta | None,
    postgres_col: ColumnMeta | None,
    diff_type: str,
    compatibility_status: str,
    severity: str,
    reason: str,
    suggested_action: str,
) -> dict:
    return {
        "table_name": table_name,
        "column_name": column_name,
        "oracle_type": oracle_type_label(oracle_col) if oracle_col else "",
        "postgres_type": pg_type_label(postgres_col) if postgres_col else "",
        "oracle_ordinal": oracle_col.ordinal if oracle_col else "",
        "postgres_ordinal": postgres_col.ordinal if postgres_col else "",
        "diff_type": diff_type,
        "compatibility_status": compatibility_status,
        "severity": severity,
        "reason": reason,
        "suggested_action": suggested_action,
        "suggested_pg_type": suggested_pg_type(oracle_col) if oracle_col else "",
    }
