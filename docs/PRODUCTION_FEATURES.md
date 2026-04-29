# Production Features

## Implemented

- smart schema diff with compatibility status and severity
- order-only differences marked as `INFO`
- dependency graph reporting
- PostgreSQL MV refresh after execute/repair
- Oracle invalid object recompilation loop
- reverse sync with Oracle `MERGE`
- checkpoint and resume support
- incremental watermark support in both directions
- LOB policy engine for `BLOB`, `CLOB`, `NCLOB`, `LONG`, `LONG RAW`
- centralized Excel and HTML reporting
- `ops` command set for operators
- daily and incremental job wrappers for both directions
- CI with lint, tests, and security scanning

## Guardrails

- dry-run is still the default
- config in reports is sanitized
- dependency failures can fail the run
- large-row paths use streaming or batched reads
- no secret values are intentionally written into manifests or reports

## Operator Commands

```bash
ops audit
ops sync
ops sync --go
ops resume
ops status
ops report latest
ops analyze lob
ops dependencies check
ops dependencies repair
ops doctor
```
