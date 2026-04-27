import unittest

try:
    import psycopg  # noqa: F401
except ModuleNotFoundError:
    psycopg = None

from oracle_pg_sync.config import AppConfig, OracleConfig, PostgresConfig, SyncConfig

if psycopg is not None:
    from oracle_pg_sync.sync.oracle_to_postgres import OracleToPostgresSync


@unittest.skipIf(psycopg is None, "psycopg is not installed")
class OracleToPostgresSyncTest(unittest.TestCase):
    def test_swap_execute_is_guarded_by_default(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(allow_swap=False),
            )
        )

        message = sync._swap_guard_message("public.sample_customer", 1024, force=False)

        self.assertIn("mode swap dinonaktifkan", message)

    def test_swap_max_size_accepts_force_bypass(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(allow_swap=True, max_swap_table_bytes=1024),
            )
        )

        message = sync._swap_guard_message("public.sample_customer", 2048, force=True)

        self.assertEqual(message, "")

    def test_swap_dry_run_mentions_estimated_storage(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(swap_space_multiplier=2.5),
            )
        )

        message = sync._dry_run_message("public.sample_customer", "swap", 3, 1024**3)

        self.assertIn("2.5 GiB", message)


if __name__ == "__main__":
    unittest.main()
