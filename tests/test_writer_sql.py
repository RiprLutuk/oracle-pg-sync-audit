import tempfile
import unittest
from pathlib import Path

from oracle_pg_sync.reports.writer_sql import write_schema_suggestions


class WriterSqlTest(unittest.TestCase):
    def test_write_add_and_optional_drop_suggestions(self):
        rows = [
            {
                "table_name": "public.sample_customer",
                "diff_type": "missing_in_postgres",
                "column_name": "created_at",
                "suggested_pg_type": "TIMESTAMP",
            },
            {
                "table_name": "public.sample_customer",
                "diff_type": "extra_in_postgres",
                "column_name": "legacy_code",
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "schema_suggestions.sql"
            write_schema_suggestions(path, rows, suggest_drop=True)

            text = path.read_text(encoding="utf-8")

        self.assertIn('ADD COLUMN "created_at" TIMESTAMP;', text)
        self.assertIn('DROP COLUMN "legacy_code";', text)


if __name__ == "__main__":
    unittest.main()
