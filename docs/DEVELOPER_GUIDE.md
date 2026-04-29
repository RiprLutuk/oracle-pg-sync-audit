# Developer Guide

## Repository Layout

- `oracle_pg_sync/cli.py`: primary CLI
- `oracle_pg_sync/ops.py`: operator-friendly wrapper commands
- `oracle_pg_sync/sync/`: direction-specific sync implementations
- `oracle_pg_sync/db/`: Oracle and PostgreSQL database helpers
- `oracle_pg_sync/metadata/`: metadata fetch and compare logic
- `oracle_pg_sync/schema/type_compat.py`: smart schema diff compatibility engine
- `oracle_pg_sync/reports/`: CSV, Excel, HTML, and SQL writers
- `oracle_pg_sync/checkpoint.py`: SQLite checkpoint and watermark store
- `jobs/`: cron-safe shell wrappers
- `tests/`: unit and smoke coverage

## Coding Rules In This Project

- preserve existing CLI names
- keep dry-run as the default
- do not expose secrets in reports or manifests
- avoid `fetchall` in large-row data paths
- prefer batch/stream processing

## Test Commands

```bash
pytest -q
pytest -q tests/test_ops_smoke.py
python tests/integration_reverse_merge_container.py
```

The PostgreSQL-backed integration probe runs only when `RUN_CONTAINER_TESTS=1`.

## CI

The GitHub Actions workflow runs:

- CLI smoke checks
- `compileall`
- `ruff`
- `black --check`
- `bandit`
- `pip-audit`
- `pytest`
- config example validation

## Adding Diff Rules

Smart schema diff rules live in [`oracle_pg_sync/schema/type_compat.py`](../oracle_pg_sync/schema/type_compat.py).

When you add a rule:

1. classify compatibility status
2. set severity
3. provide a human-readable reason
4. provide a suggested action
5. update tests in `tests/test_compare.py` and `tests/test_type_mapping.py`

## Adding Reports

Keep the centralized workbook stable. If you add a new metric or sheet:

1. update `writer_excel.py`
2. update `writer_html.py` if it should be visible in HTML
3. update `docs/REPORT_REFERENCE.md`
4. add or update tests

## Shell Job Changes

Job logic is shared through [`jobs/common.sh`](../jobs/common.sh). Keep wrappers small and direction-aware. Any change to lock naming, retry, timeout, or logging should be covered by `tests/test_jobs.py`.
