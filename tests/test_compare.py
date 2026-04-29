import unittest

from oracle_pg_sync.config import AppConfig, OracleConfig, PostgresConfig
from oracle_pg_sync.metadata.compare import compare_table_metadata, inventory_has_fatal_mismatch
from oracle_pg_sync.metadata.oracle_metadata import OracleTableMetadata
from oracle_pg_sync.metadata.postgres_metadata import PostgresTableMetadata
from oracle_pg_sync.metadata.type_mapping import ColumnMeta


class CompareTest(unittest.TestCase):
    def test_rename_mapping_counts_as_same_column(self):
        config = AppConfig(
            oracle=OracleConfig(schema="APP"),
            postgres=PostgresConfig(schema="public"),
            rename_columns={"public.sample": {"freeze": "freezee"}},
        )
        oracle_meta = OracleTableMetadata(
            exists=True,
            row_count=10,
            columns=[
                ColumnMeta("ID", 1, "NUMBER", numeric_precision=9, numeric_scale=0),
                ColumnMeta("FREEZE", 2, "VARCHAR2", char_length=10),
            ],
            object_counts={},
        )
        pg_meta = PostgresTableMetadata(
            exists=True,
            row_count=10,
            columns=[
                ColumnMeta("id", 1, "integer", udt_name="int4"),
                ColumnMeta("freezee", 2, "varchar", char_length=10, udt_name="varchar"),
            ],
            object_counts={},
        )

        inventory, column_diff, type_mismatch = compare_table_metadata(
            table_name="public.sample",
            config=config,
            oracle_meta=oracle_meta,
            postgres_meta=pg_meta,
        )

        self.assertEqual(inventory["status"], "MATCH")
        self.assertEqual(column_diff, [])
        self.assertEqual(type_mismatch, [])

    def test_ordinal_only_difference_is_info_not_mismatch(self):
        config = AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public"))
        oracle_meta = OracleTableMetadata(
            exists=True,
            row_count=10,
            columns=[
                ColumnMeta("ID", 1, "NUMBER", numeric_precision=9, numeric_scale=0),
                ColumnMeta("NAME", 2, "VARCHAR2", char_length=50),
            ],
            object_counts={},
        )
        pg_meta = PostgresTableMetadata(
            exists=True,
            row_count=10,
            columns=[
                ColumnMeta("name", 1, "varchar", char_length=50, udt_name="varchar"),
                ColumnMeta("id", 2, "integer", udt_name="int4"),
            ],
            object_counts={},
        )

        inventory, column_diff, type_mismatch = compare_table_metadata(
            table_name="public.sample",
            config=config,
            oracle_meta=oracle_meta,
            postgres_meta=pg_meta,
        )

        self.assertEqual(inventory["status"], "MATCH")
        self.assertEqual(inventory["schema_diff_info_count"], 2)
        self.assertEqual(type_mismatch, [])
        self.assertFalse(inventory_has_fatal_mismatch(inventory))
        self.assertTrue(all(row["severity"] == "INFO" for row in column_diff))

    def test_compatible_aliases_do_not_raise_false_mismatch(self):
        config = AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public"))
        oracle_meta = OracleTableMetadata(
            exists=True,
            row_count=4,
            columns=[
                ColumnMeta("AMOUNT", 1, "NUMBER", numeric_precision=38, numeric_scale=0),
                ColumnMeta("CREATED_AT", 2, "DATE"),
            ],
            object_counts={},
        )
        pg_meta = PostgresTableMetadata(
            exists=True,
            row_count=4,
            columns=[
                ColumnMeta("amount", 1, "numeric", numeric_precision=38, numeric_scale=0, udt_name="numeric"),
                ColumnMeta("created_at", 2, "timestamp without time zone", udt_name="timestamp"),
            ],
            object_counts={},
        )

        inventory, column_diff, type_mismatch = compare_table_metadata(
            table_name="public.sample",
            config=config,
            oracle_meta=oracle_meta,
            postgres_meta=pg_meta,
        )

        self.assertEqual(inventory["status"], "MATCH")
        self.assertEqual(inventory["schema_diff_error_count"], 0)
        self.assertEqual(type_mismatch, [])
        self.assertEqual(len(column_diff), 1)
        self.assertEqual(column_diff[0]["compatibility_status"], "compatible_with_warning")
        self.assertEqual(column_diff[0]["severity"], "INFO")

    def test_narrower_postgres_type_is_error(self):
        config = AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public"))
        oracle_meta = OracleTableMetadata(
            exists=True,
            row_count=1,
            columns=[ColumnMeta("ID", 1, "NUMBER", numeric_precision=18, numeric_scale=0)],
            object_counts={},
        )
        pg_meta = PostgresTableMetadata(
            exists=True,
            row_count=1,
            columns=[ColumnMeta("id", 1, "integer", udt_name="int4")],
            object_counts={},
        )

        inventory, column_diff, type_mismatch = compare_table_metadata(
            table_name="public.sample",
            config=config,
            oracle_meta=oracle_meta,
            postgres_meta=pg_meta,
        )

        self.assertEqual(inventory["status"], "MISMATCH")
        self.assertTrue(inventory_has_fatal_mismatch(inventory))
        self.assertEqual(type_mismatch[0]["severity"], "ERROR")
        self.assertIn("too large", type_mismatch[0]["reason"])


if __name__ == "__main__":
    unittest.main()
