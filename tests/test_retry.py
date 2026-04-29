import unittest
from unittest.mock import patch

from oracle_pg_sync.utils.retry import connect_retry, is_transient_connect_error


class RetryTest(unittest.TestCase):
    def test_dns_resolution_error_is_transient(self):
        exc = RuntimeError("failed to resolve host 'db.example': [Errno -3] Temporary failure in name resolution")

        self.assertTrue(is_transient_connect_error(exc))

    def test_auth_error_is_not_transient(self):
        exc = RuntimeError("password authentication failed for user app")

        self.assertFalse(is_transient_connect_error(exc))

    def test_connect_retry_retries_transient_errors(self):
        calls = {"count": 0}

        def connect():
            calls["count"] += 1
            if calls["count"] < 3:
                raise RuntimeError("could not translate host name")
            return "ok"

        with patch("oracle_pg_sync.utils.retry.time.sleep") as sleep:
            result = connect_retry(connect, attempts=3, delay_seconds=0.1)

        self.assertEqual(result, "ok")
        self.assertEqual(calls["count"], 3)
        self.assertEqual(sleep.call_count, 2)

    def test_connect_retry_does_not_retry_non_transient_errors(self):
        calls = {"count": 0}

        def connect():
            calls["count"] += 1
            raise RuntimeError("password authentication failed")

        with patch("oracle_pg_sync.utils.retry.time.sleep") as sleep, self.assertRaisesRegex(RuntimeError, "password"):
            connect_retry(connect, attempts=3, delay_seconds=0.1)

        self.assertEqual(calls["count"], 1)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
