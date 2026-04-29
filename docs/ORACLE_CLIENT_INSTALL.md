# Oracle Client Install

Use this only when your environment requires Oracle thick mode. The tool can also run with a plain DSN in environments where the Python Oracle driver works without a local client.

## Linux Example

1. Download Oracle Instant Client from Oracle.
2. Extract it to a stable path such as `/opt/oracle/instantclient_23_9`.
3. Set:

```dotenv
ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_23_9
```

4. Keep `client_lib_dir: ${ORACLE_CLIENT_LIB_DIR}` in `config.yaml`.

## Runtime Behavior

At startup, the CLI checks `oracle.client_lib_dir`. If it is set and exists, the process re-execs with `LD_LIBRARY_PATH` including that directory so the Oracle driver can initialize cleanly.

## Verify

Run:

```bash
ops doctor --config config.yaml
```

If Oracle connectivity still fails, verify:

- library path exists on disk
- host architecture matches the client package
- Oracle network configuration and credentials are correct
