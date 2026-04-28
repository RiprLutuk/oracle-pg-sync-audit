import tempfile
import unittest
from pathlib import Path

from oracle_pg_sync.checkpoint import CheckpointStore
from oracle_pg_sync.config import AppConfig, IncrementalConfig, OracleConfig, PostgresConfig, TableConfig
from oracle_pg_sync.db import oracle
from oracle_pg_sync.sync.postgres_to_oracle import PostgresToOracleSync, _apply_checksum_summary


class PostgresToOracleSyncTest(unittest.TestCase):
    def test_reverse_incremental_where_uses_watermark(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CheckpointStore(Path(tmp) / "checkpoint.sqlite3")
            store.set_watermark(
                direction="postgres_to_oracle",
                table_name="public.sample",
                strategy="updated_at",
                column_name="updated_at",
                value="2026-01-01T00:00:00",
            )
            sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))

            where = sync._incremental_where(
                store,
                TableConfig(
                    name="public.sample",
                    incremental=IncrementalConfig(enabled=True, strategy="updated_at", column="updated_at", overlap_minutes=5),
                ),
                "public.sample",
                incremental=True,
                full_refresh=False,
            )

        self.assertIn('"updated_at" >= TIMESTAMP', where)
        self.assertIn("INTERVAL '5 minutes'", where)

    def test_reverse_checksum_summary_marks_mismatch(self):
        result = type(
            "Result",
            (),
            {
                "checksum_status": "",
                "checksum_source_rows": None,
                "checksum_target_rows": None,
                "checksum_source_hash": "",
                "checksum_target_hash": "",
            },
        )()

        _apply_checksum_summary(
            result,
            [
                {
                    "status": "MISMATCH",
                    "row_count_source": 2,
                    "row_count_target": 1,
                    "source_hash": "a",
                    "target_hash": "b",
                }
            ],
        )

        self.assertEqual(result.checksum_status, "MISMATCH")
        self.assertEqual(result.checksum_source_rows, 2)
        self.assertEqual(result.checksum_target_hash, "b")

    def test_oracle_merge_rows_uses_merge_and_bind_rows(self):
        class Cursor:
            def __init__(self):
                self.statement = ""
                self.rows = []

            def execute(self, query, params=None):
                if "ALL_TABLES" in query:
                    self._fetchone = ("SAMPLE",)

            def fetchone(self):
                return getattr(self, "_fetchone", None)

            def executemany(self, statement, rows):
                self.statement = statement
                self.rows = rows

        cur = Cursor()

        count = oracle.merge_rows(
            cur,
            owner="APP",
            table="SAMPLE",
            oracle_columns=["ID", "NAME"],
            key_columns=["ID"],
            rows=[(1, "Alice")],
        )

        self.assertEqual(count, 1)
        self.assertIn("MERGE INTO", cur.statement)
        self.assertIn('WHEN MATCHED THEN UPDATE SET t."NAME" = s."NAME"', cur.statement)
        self.assertEqual(cur.rows, [(1, "Alice")])


if __name__ == "__main__":
    unittest.main()
