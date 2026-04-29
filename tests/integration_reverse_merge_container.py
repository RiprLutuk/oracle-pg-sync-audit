"""
Optional integration check for reverse sync plumbing with a real PostgreSQL
container and a fake Oracle MERGE target.

Run manually:

    RUN_CONTAINER_TESTS=1 python tests/integration_reverse_merge_container.py

The file is intentionally not named test_*.py so normal unittest discovery
stays fast and does not require Docker. In CI, set PGHOST/PGPORT/PGDATABASE/
PGUSER/PGPASSWORD to use an existing PostgreSQL service instead of launching
a Docker container.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid


def main() -> int:
    if os.getenv("RUN_CONTAINER_TESTS") != "1":
        print("Set RUN_CONTAINER_TESTS=1 to run this PostgreSQL-backed integration check.")
        return 0
    if os.getenv("PGHOST"):
        conn = _pg_conn_from_env()
        _wait_for_postgres(**conn)
        _run_reverse_merge_probe(**conn)
        print("integration reverse MERGE probe OK")
        return 0

    name = f"oracle-pg-sync-it-{uuid.uuid4().hex[:8]}"
    conn = {
        "host": "127.0.0.1",
        "port": _free_tcp_port(),
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
    }
    try:
        subprocess.check_call(
            [
                "docker",
                "run",
                "--rm",
                "-d",
                "--name",
                name,
                "-e",
                f"POSTGRES_PASSWORD={conn['password']}",
                "-p",
                f"{conn['port']}:5432",
                "postgres:16-alpine",
            ]
        )
        _wait_for_postgres(**conn)
        _run_reverse_merge_probe(**conn)
        print("integration reverse MERGE probe OK")
        return 0
    finally:
        subprocess.call(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _pg_conn_from_env() -> dict:
    return {
        "host": os.getenv("PGHOST", "127.0.0.1"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "postgres"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "postgres"),
    }


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_postgres(host: str, port: int, dbname: str, user: str, password: str) -> None:
    import psycopg

    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            with psycopg.connect(host=host, port=port, dbname=dbname, user=user, password=password):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError("PostgreSQL did not become ready")


def _run_reverse_merge_probe(host: str, port: int, dbname: str, user: str, password: str) -> None:
    import psycopg

    from oracle_pg_sync.db import oracle

    with psycopg.connect(host=host, port=port, dbname=dbname, user=user, password=password) as con:
        with con.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS sample")
            cur.execute("CREATE TABLE sample (id integer primary key, name text)")
            cur.execute("INSERT INTO sample VALUES (1, 'Alice')")
            cur.execute("SELECT id, name FROM sample ORDER BY id")
            rows = cur.fetchall()

    class FakeOracleCursor:
        def __init__(self):
            self.rows = []
            self.statements = []

        def execute(self, query, params=None):
            self.statements.append(str(query))
            if "ALL_TABLES" in query:
                self._fetchone = ("SAMPLE",)

        def fetchone(self):
            return getattr(self, "_fetchone", None)

        def executemany(self, statement, rows_arg):
            self.statements.append(str(statement))
            self.rows = rows_arg

    fake = FakeOracleCursor()
    oracle.truncate_table(fake, owner="APP", table="SAMPLE")
    oracle.insert_rows(
        fake,
        owner="APP",
        table="SAMPLE",
        oracle_columns=["ID", "NAME"],
        rows=rows,
    )
    oracle.merge_rows(
        fake,
        owner="APP",
        table="SAMPLE",
        oracle_columns=["ID", "NAME"],
        key_columns=["ID"],
        rows=rows,
    )
    joined = "\n".join(fake.statements)
    assert "TRUNCATE TABLE" in joined
    assert "INSERT INTO" in joined
    assert "MERGE INTO" in joined
    assert fake.rows == rows


if __name__ == "__main__":
    raise SystemExit(main())
