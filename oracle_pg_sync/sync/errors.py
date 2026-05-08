from __future__ import annotations


class OperationalSyncError(RuntimeError):
    """Expected operational failure that should fail the table without traceback noise."""

