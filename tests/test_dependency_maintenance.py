import unittest

from oracle_pg_sync.db import oracle, postgres


class OracleDependencyMaintenanceTest(unittest.TestCase):
    def test_compile_invalid_objects_builds_safe_compile_statements(self):
        class Cursor:
            def __init__(self):
                self.statements = []

            def execute(self, statement, params=None):
                self.statements.append(str(statement))

            def fetchall(self):
                return [
                    ("VIEW", "V_SAMPLE", "INVALID"),
                    ("PACKAGE BODY", "PKG_SAMPLE", "INVALID"),
                ]

        cur = Cursor()

        rows = oracle.compile_invalid_objects(cur, "APP")

        self.assertEqual(rows[0]["compile_status"], "attempted")
        self.assertIn('ALTER VIEW "APP"."V_SAMPLE" COMPILE', cur.statements)
        self.assertIn('ALTER PACKAGE "APP"."PKG_SAMPLE" COMPILE BODY', cur.statements)


class PostgresDependencyMaintenanceTest(unittest.TestCase):
    def test_refresh_materialized_views_deduplicates_dependencies(self):
        class Cursor:
            def __init__(self):
                self.executed = []

            def execute(self, statement, params=None):
                self.executed.append(statement)

        cur = Cursor()

        rows = postgres.refresh_materialized_views(
            cur,
            [
                {"object_schema": "public", "object_name": "mv_sales", "object_type": "MATERIALIZED VIEW"},
                {"object_schema": "public", "object_name": "mv_sales", "object_type": "MATERIALIZED VIEW"},
                {"object_schema": "public", "object_name": "v_sales", "object_type": "VIEW"},
            ],
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(len(cur.executed), 1)
        self.assertEqual(rows[0]["maintenance_status"], "refreshed")


if __name__ == "__main__":
    unittest.main()
