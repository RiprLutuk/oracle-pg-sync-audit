from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


COMPATIBLE_EXACT = "compatible_exact"
COMPATIBLE = "compatible"
COMPATIBLE_WITH_WARNING = "compatible_with_warning"
INCOMPATIBLE = "incompatible"

SEVERITY_OK = "OK"
SEVERITY_INFO = "INFO"
SEVERITY_WARNING = "WARNING"
SEVERITY_ERROR = "ERROR"


@dataclass(frozen=True)
class CompatibilityAssessment:
    compatibility_status: str
    severity: str
    reason: str
    suggested_action: str = ""

    @property
    def is_compatible(self) -> bool:
        return self.compatibility_status != INCOMPATIBLE


def assess_column_compatibility(oracle: Any, postgres: Any) -> CompatibilityAssessment:
    odt = str(getattr(oracle, "data_type", "") or "").upper()
    pdt = _pg_type_label(postgres).upper()

    if odt in {"VARCHAR", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}:
        return _assess_character(oracle, postgres, pdt)
    if odt == "NUMBER":
        return _assess_number(oracle, postgres, pdt)
    if odt in {"FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"}:
        return _assess_float(odt, pdt)
    if odt == "DATE" or odt.startswith("TIMESTAMP"):
        return _assess_temporal(odt, pdt)
    if odt.startswith("INTERVAL"):
        return _exact_or_compatible(
            "INTERVAL" in pdt,
            reason="Oracle interval maps to PostgreSQL interval",
            exact="INTERVAL" in pdt and odt == pdt,
            suggested_action="No action required.",
            incompatible_reason=f"Oracle {_oracle_type_label(oracle)} requires PostgreSQL interval-compatible type.",
            incompatible_action="Use PostgreSQL interval.",
        )
    if odt == "BOOLEAN":
        return _exact_or_compatible(
            "BOOL" in pdt,
            reason="Boolean types are equivalent.",
            exact="BOOL" in pdt,
            suggested_action="No action required.",
            incompatible_reason=f"Oracle {odt} requires PostgreSQL boolean.",
            incompatible_action="Use PostgreSQL boolean.",
        )
    if odt in {"RAW", "BLOB", "LONG RAW"}:
        return _exact_or_compatible(
            "BYTEA" in pdt,
            reason="Binary LOB/raw type maps to PostgreSQL bytea.",
            exact="BYTEA" in pdt and odt in {"RAW", "BLOB", "LONG RAW"},
            suggested_action="No action required.",
            incompatible_reason=f"Oracle {_oracle_type_label(oracle)} requires PostgreSQL bytea.",
            incompatible_action="Use PostgreSQL bytea for binary payloads.",
        )
    if "CLOB" in odt or odt == "LONG":
        return _assess_text_lob(oracle, postgres, pdt)
    if odt in {"ROWID", "UROWID"}:
        if any(token in pdt for token in ("TEXT", "VARCHAR", "CHAR", "BPCHAR")):
            return CompatibilityAssessment(COMPATIBLE, SEVERITY_OK, "ROWID value is represented as text in PostgreSQL.", "No action required.")
        return CompatibilityAssessment(
            INCOMPATIBLE,
            SEVERITY_ERROR,
            f"Oracle {_oracle_type_label(oracle)} requires PostgreSQL text-compatible type.",
            "Use PostgreSQL text or varchar.",
        )
    if odt in {"JSON", "XMLTYPE"}:
        if odt == "JSON" and any(token in pdt for token in ("JSON", "JSONB")):
            return CompatibilityAssessment(COMPATIBLE, SEVERITY_OK, "JSON storage is compatible.", "No action required.")
        if any(token in pdt for token in ("TEXT", "VARCHAR", "JSON", "JSONB")):
            return CompatibilityAssessment(
                COMPATIBLE_WITH_WARNING,
                SEVERITY_INFO,
                f"Oracle {odt} is stored in PostgreSQL as {pdt.lower()}.",
                "Review application expectations for formatting and validation.",
            )
        return CompatibilityAssessment(
            INCOMPATIBLE,
            SEVERITY_ERROR,
            f"Oracle {_oracle_type_label(oracle)} requires PostgreSQL text/json-compatible storage.",
            "Use PostgreSQL json/jsonb/text.",
        )
    if odt == pdt:
        return CompatibilityAssessment(COMPATIBLE_EXACT, SEVERITY_OK, "Type labels match exactly.", "No action required.")
    return CompatibilityAssessment(
        INCOMPATIBLE,
        SEVERITY_ERROR,
        f"Oracle {_oracle_type_label(oracle)} is not compatible with PostgreSQL {_pg_type_label(postgres)}.",
        f"Change PostgreSQL column to {_suggested_pg_type(oracle)}.",
    )


def assess_ordinal_difference(oracle: Any, postgres: Any) -> CompatibilityAssessment:
    return CompatibilityAssessment(
        COMPATIBLE_WITH_WARNING,
        SEVERITY_INFO,
        "Column ordinal differs but names and compatible types still align.",
        "No action required unless column order is operationally important.",
    )


def _assess_character(oracle: Any, postgres: Any, pdt: str) -> CompatibilityAssessment:
    if not any(token in pdt for token in ("VARCHAR", "CHAR", "TEXT", "BPCHAR")):
        return CompatibilityAssessment(
            INCOMPATIBLE,
            SEVERITY_ERROR,
            f"Oracle {_oracle_type_label(oracle)} requires PostgreSQL character/text storage.",
            f"Change PostgreSQL column to {_suggested_pg_type(oracle)}.",
        )
    pg_len = _extract_length(pdt)
    ora_len = getattr(oracle, "char_length", None) or getattr(oracle, "data_length", None)
    if pg_len is not None and ora_len is not None and pg_len < ora_len:
        return CompatibilityAssessment(
            INCOMPATIBLE,
            SEVERITY_ERROR,
            f"PostgreSQL length {pg_len} is narrower than Oracle length {ora_len}.",
            f"Increase PostgreSQL length to at least {ora_len} or use text.",
        )
    oracle_kind = "char" if getattr(oracle, "data_type", "").upper() in {"CHAR", "NCHAR"} else "varchar"
    pg_kind = "char" if any(token in pdt for token in ("CHAR", "BPCHAR")) and "VARCHAR" not in pdt else ("text" if "TEXT" in pdt else "varchar")
    if pg_kind == "text":
        return CompatibilityAssessment(
            COMPATIBLE,
            SEVERITY_OK,
            "PostgreSQL text is wider than Oracle character storage.",
            "No action required.",
        )
    if ora_len is not None and pg_len is not None and ora_len == pg_len and oracle_kind == pg_kind:
        return CompatibilityAssessment(COMPATIBLE_EXACT, SEVERITY_OK, "Character type and length match.", "No action required.")
    if ora_len is not None and pg_len is not None and ora_len == pg_len:
        return CompatibilityAssessment(COMPATIBLE, SEVERITY_OK, "Character aliases differ but effective length matches.", "No action required.")
    return CompatibilityAssessment(COMPATIBLE, SEVERITY_OK, "PostgreSQL character column is equal or wider than Oracle.", "No action required.")


def _assess_number(oracle: Any, postgres: Any, pdt: str) -> CompatibilityAssessment:
    precision = getattr(oracle, "numeric_precision", None)
    scale = getattr(oracle, "numeric_scale", None)
    if any(token in pdt for token in ("NUMERIC", "DECIMAL")):
        pg_precision, pg_scale = _extract_precision_scale(pdt)
        if precision is None and pg_scale == 0:
            return CompatibilityAssessment(
                COMPATIBLE_WITH_WARNING,
                SEVERITY_WARNING,
                "Oracle NUMBER without fixed precision/scale may contain decimals, but PostgreSQL numeric scale 0 rounds fractional values.",
                "Use PostgreSQL numeric without scale unless the source column is proven integer-only.",
            )
        if pg_precision is not None and precision is not None and pg_precision < precision:
            return CompatibilityAssessment(
                INCOMPATIBLE,
                SEVERITY_ERROR,
                f"PostgreSQL precision {pg_precision} is narrower than Oracle precision {precision}.",
                f"Change PostgreSQL column to numeric({precision},{scale or 0}).",
            )
        if pg_scale is not None and scale is not None and pg_scale < scale:
            return CompatibilityAssessment(
                INCOMPATIBLE,
                SEVERITY_ERROR,
                f"PostgreSQL scale {pg_scale} is narrower than Oracle scale {scale}.",
                f"Change PostgreSQL column to numeric({precision or 38},{scale}).",
            )
        if precision is not None and scale is not None and pg_precision == precision and pg_scale == scale:
            return CompatibilityAssessment(COMPATIBLE_EXACT, SEVERITY_OK, "Numeric precision and scale match.", "No action required.")
        return CompatibilityAssessment(COMPATIBLE, SEVERITY_OK, "Oracle NUMBER and PostgreSQL numeric are compatible.", "No action required.")
    if any(token in pdt for token in ("SMALLINT", "INTEGER", "INT", "BIGINT")):
        if scale not in (None, 0):
            return CompatibilityAssessment(
                INCOMPATIBLE,
                SEVERITY_ERROR,
                f"Oracle NUMBER scale {scale} cannot be stored safely in integer type {_pg_type_label(postgres)}.",
                "Use PostgreSQL numeric with matching scale.",
            )
        if precision is None:
            return CompatibilityAssessment(
                COMPATIBLE,
                SEVERITY_OK,
                "Oracle NUMBER without precision is integer-compatible with PostgreSQL integer storage.",
                "No action required when profiled source values are integer-only and within range.",
            )
        integer_limits = {
            "SMALLINT": 4,
            "INT2": 4,
            "INTEGER": 9,
            "INT": 9,
            "INT4": 9,
            "BIGINT": 18,
            "INT8": 18,
        }
        pg_limit = integer_limits.get(pdt)
        if pg_limit is None:
            pg_limit = next((limit for token, limit in integer_limits.items() if token in pdt), None)
        if pg_limit is not None and precision > pg_limit:
            return CompatibilityAssessment(
                INCOMPATIBLE,
                SEVERITY_ERROR,
                f"Oracle NUMBER({precision},0) is too large for PostgreSQL {_pg_type_label(postgres)}.",
                "Use PostgreSQL bigint or numeric with sufficient precision.",
            )
        return CompatibilityAssessment(COMPATIBLE, SEVERITY_OK, "Oracle integer-like NUMBER fits PostgreSQL integer storage.", "No action required.")
    if any(token in pdt for token in ("DOUBLE", "REAL", "FLOAT")):
        return CompatibilityAssessment(
            COMPATIBLE_WITH_WARNING,
            SEVERITY_WARNING,
            "Floating-point storage may change precision compared with Oracle NUMBER.",
            "Use PostgreSQL numeric if exact precision is required.",
        )
    return CompatibilityAssessment(
        INCOMPATIBLE,
        SEVERITY_ERROR,
        f"Oracle {_oracle_type_label(oracle)} is not compatible with PostgreSQL {_pg_type_label(postgres)}.",
        f"Change PostgreSQL column to {_suggested_pg_type(oracle)}.",
    )


def _assess_float(oracle_type: str, pdt: str) -> CompatibilityAssessment:
    if any(token in pdt for token in ("DOUBLE", "REAL", "FLOAT")):
        return CompatibilityAssessment(COMPATIBLE, SEVERITY_OK, "Floating-point types are compatible.", "No action required.")
    if any(token in pdt for token in ("NUMERIC", "DECIMAL")):
        return CompatibilityAssessment(
            COMPATIBLE_WITH_WARNING,
            SEVERITY_INFO,
            f"Oracle {oracle_type} is stored in PostgreSQL as exact numeric.",
            "No action required unless binary floating-point semantics are required.",
        )
    return CompatibilityAssessment(
        INCOMPATIBLE,
        SEVERITY_ERROR,
        f"Oracle {oracle_type} requires PostgreSQL float/real/double precision or numeric.",
        "Use PostgreSQL real, double precision, or numeric.",
    )


def _assess_temporal(oracle_type: str, pdt: str) -> CompatibilityAssessment:
    if "TIMESTAMP" in pdt:
        if oracle_type.startswith("TIMESTAMP"):
            return CompatibilityAssessment(COMPATIBLE_EXACT, SEVERITY_OK, "Timestamp types align.", "No action required.")
        return CompatibilityAssessment(
            COMPATIBLE_WITH_WARNING,
            SEVERITY_INFO,
            "Oracle DATE stores date and time; PostgreSQL timestamp is compatible.",
            "No action required.",
        )
    if oracle_type.startswith("TIMESTAMP") and "DATE" in pdt:
        return CompatibilityAssessment(
            INCOMPATIBLE,
            SEVERITY_ERROR,
            "PostgreSQL date is narrower than Oracle timestamp.",
            "Use PostgreSQL timestamp.",
        )
    if oracle_type == "DATE" and "DATE" in pdt:
        return CompatibilityAssessment(
            INCOMPATIBLE,
            SEVERITY_ERROR,
            "PostgreSQL date loses Oracle DATE time-of-day values.",
            "Use PostgreSQL timestamp.",
        )
    if "TIME" in pdt:
        return CompatibilityAssessment(
            INCOMPATIBLE,
            SEVERITY_ERROR,
            "PostgreSQL time does not preserve the full Oracle date-time value.",
            "Use PostgreSQL timestamp.",
        )
    return CompatibilityAssessment(
        INCOMPATIBLE,
        SEVERITY_ERROR,
        f"Oracle {oracle_type} requires PostgreSQL timestamp-compatible storage.",
        "Use PostgreSQL timestamp.",
    )


def _assess_text_lob(oracle: Any, postgres: Any, pdt: str) -> CompatibilityAssessment:
    if "TEXT" in pdt:
        return CompatibilityAssessment(COMPATIBLE, SEVERITY_OK, "Text LOB maps to PostgreSQL text.", "No action required.")
    if "VARCHAR" in pdt:
        pg_len = _extract_length(pdt)
        if pg_len is None:
            return CompatibilityAssessment(
                COMPATIBLE_WITH_WARNING,
                SEVERITY_WARNING,
                f"Oracle {_oracle_type_label(oracle)} stored in varchar should be monitored for truncation risk.",
                "Prefer PostgreSQL text for CLOB/LONG columns.",
            )
        return CompatibilityAssessment(
            INCOMPATIBLE,
            SEVERITY_ERROR,
            f"PostgreSQL varchar({pg_len}) is narrower than Oracle {_oracle_type_label(oracle)}.",
            "Use PostgreSQL text for LOB columns.",
        )
    return CompatibilityAssessment(
        INCOMPATIBLE,
        SEVERITY_ERROR,
        f"Oracle {_oracle_type_label(oracle)} requires PostgreSQL text-compatible storage.",
        "Use PostgreSQL text.",
    )


def _exact_or_compatible(
    condition: bool,
    *,
    reason: str,
    exact: bool,
    suggested_action: str,
    incompatible_reason: str,
    incompatible_action: str,
) -> CompatibilityAssessment:
    if condition:
        return CompatibilityAssessment(COMPATIBLE_EXACT if exact else COMPATIBLE, SEVERITY_OK, reason, suggested_action)
    return CompatibilityAssessment(INCOMPATIBLE, SEVERITY_ERROR, incompatible_reason, incompatible_action)


def _oracle_type_label(column: Any) -> str:
    data_type = str(getattr(column, "data_type", "") or "").upper()
    if data_type in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}:
        length = getattr(column, "char_length", None) or getattr(column, "data_length", None)
        return f"{data_type}({length})" if length else data_type
    if data_type == "NUMBER":
        precision = getattr(column, "numeric_precision", None)
        scale = getattr(column, "numeric_scale", None)
        if precision is not None and scale is not None:
            return f"NUMBER({precision},{scale})"
        if precision is not None:
            return f"NUMBER({precision})"
    return data_type


def _pg_type_label(column: Any) -> str:
    data_type = str(getattr(column, "data_type", "") or "").lower()
    udt_name = str(getattr(column, "udt_name", "") or "").lower()
    if data_type in {"character varying", "varchar"}:
        length = getattr(column, "char_length", None)
        return f"varchar({length})" if length else "varchar"
    if data_type in {"character", "char"} or udt_name == "bpchar":
        length = getattr(column, "char_length", None)
        return f"char({length})" if length else "char"
    if data_type in {"numeric", "decimal"}:
        precision = getattr(column, "numeric_precision", None)
        scale = getattr(column, "numeric_scale", None)
        if precision is not None and scale is not None:
            return f"numeric({precision},{scale})"
        return "numeric"
    return udt_name or data_type


def _suggested_pg_type(oracle: Any) -> str:
    odt = str(getattr(oracle, "data_type", "") or "").upper()
    precision = getattr(oracle, "numeric_precision", None)
    scale = getattr(oracle, "numeric_scale", None)
    if odt in {"VARCHAR", "VARCHAR2", "NVARCHAR2"}:
        length = getattr(oracle, "char_length", None) or getattr(oracle, "data_length", None)
        return f"varchar({length})" if length else "varchar"
    if odt in {"CHAR", "NCHAR"}:
        length = getattr(oracle, "char_length", None) or getattr(oracle, "data_length", None)
        return f"char({length})" if length else "char"
    if odt == "NUMBER":
        if precision is None:
            return "numeric"
        if scale in (None, 0):
            if precision <= 4:
                return "smallint"
            if precision <= 9:
                return "integer"
            if precision <= 18:
                return "bigint"
            return f"numeric({precision},0)"
        return f"numeric({precision},{scale})"
    if odt == "DATE" or odt.startswith("TIMESTAMP"):
        return "timestamp"
    if odt.startswith("INTERVAL"):
        return "interval"
    if odt == "BOOLEAN":
        return "boolean"
    if odt in {"FLOAT", "BINARY_FLOAT"}:
        return "real"
    if odt == "BINARY_DOUBLE":
        return "double precision"
    if odt in {"RAW", "BLOB", "LONG RAW"}:
        return "bytea"
    if "CLOB" in odt or odt == "LONG":
        return "text"
    return "text"


def _extract_length(type_label: str) -> int | None:
    match = re.search(r"\((\d+)\)", type_label)
    return int(match.group(1)) if match else None


def _extract_precision_scale(type_label: str) -> tuple[int | None, int | None]:
    match = re.search(r"\((\d+)(?:,(\d+))?\)", type_label)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2) or 0)
