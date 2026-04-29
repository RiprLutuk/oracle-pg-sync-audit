# Production Runbook

## 1. Prepare The Host

1. Install Python 3.11+.
2. Install Oracle client libraries if thick mode is required.
3. Create `.venv` and install `pip install -e ".[dev]"`.
4. Place `.env`, `config.yaml`, and `configs/tables.yaml` outside source control if possible.
5. Confirm the service account can reach both databases.

## 2. Validate Before First Execute

Run:

```bash
ops doctor --config config.yaml
ops audit --config config.yaml
ops analyze lob --config config.yaml
ops dependencies check --config config.yaml
```

Review:

- `report.html`
- `report.xlsx`
- schema diff `ERROR` rows
- dependency `broken_count`
- LOB-heavy recommendations

Do not start execute mode until the audit is clean or the exceptions are understood.

## 3. First Oracle -> PostgreSQL Execute

1. Dry-run:

```bash
ops sync --config config.yaml --direction oracle-to-postgres
```

2. Execute:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --go
```

3. Review:

- `sync_result.csv`
- `dependency_summary.csv`
- `validation_checksum.csv` if enabled

## 4. First PostgreSQL -> Oracle Execute

Prefer `upsert` with keys for reverse sync.

1. Dry-run:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --mode upsert
```

2. Execute:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --mode upsert --go
```

3. Confirm:

- `status` is `SUCCESS` or an expected `WARNING`
- no checksum mismatch
- no critical dependency failures after the run

## 5. Daily Operations

Recommended operator commands:

```bash
ops status --config config.yaml
ops report latest --config config.yaml
ops dependencies check --config config.yaml
```

Use the run folder as the unit of investigation. Do not mix files from different run directories.

## 6. Cron Deployment

Full refresh:

```bash
jobs/daily.sh oracle_to_pg
jobs/daily.sh pg_to_oracle
```

Incremental:

```bash
jobs/incremental.sh oracle_to_pg
jobs/incremental.sh pg_to_oracle --tables public.address --mode upsert --key-columns address_id --incremental-column last_update
```

Job wrapper guarantees:

- one lock file per profile and direction
- retry loop
- timeout
- optional alert hook
- log rotation
- old rotated log cleanup

## 7. Failure Recovery

### Sync Failed

1. Check `reports/run_<...>/logs.txt`.
2. Open `report.xlsx` and `report.html`.
3. If the failure is transient and checkpointable, run:

```bash
ops resume --config config.yaml
```

4. If the incremental watermark is wrong, inspect and reset:

```bash
ops watermarks --config config.yaml
ops reset-watermark public.address --config config.yaml
```

### Dependency Failure

1. Review `dependency_pre.csv`, `dependency_post.csv`, and `dependency_summary.csv`.
2. Run:

```bash
ops dependencies repair --config config.yaml
```

3. If repair still exits non-zero, keep the run failed and escalate to DBA review.

### Checksum Failure

Treat checksum mismatch as a hard validation failure. Do not mark the run successful until source/target row selection, keying, and LOB policy are verified.

## 8. Rollback Strategy

There is no global automatic rollback command. Use the database-native recovery path that matches the executed mode.

Recommended rollback planning:

- `truncate` / `delete`: ensure you have a source-of-truth reload path before execute
- `append`: identify inserted batch scope before cleanup
- `upsert`: keep key-based reconciliation queries ready
- `swap`: only enable with explicit DBA approval and storage review

Operationally:

1. capture the failing run ID
2. preserve the run directory
3. disable cron for the affected direction
4. repair data using database-native SQL or a controlled resync
5. rerun dry-run before re-enabling execute jobs

## 9. Post-Change Checklist

After changing config, table scope, or LOB policy:

1. `ops doctor --config config.yaml`
2. `ops audit --config config.yaml`
3. `ops analyze lob --config config.yaml`
4. dry-run the affected direction
5. only then return to `--go`
