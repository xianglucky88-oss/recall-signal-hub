from __future__ import annotations

from contextlib import contextmanager
import sqlite3


@contextmanager
def transaction(connection: sqlite3.Connection):
    """Serialize writers and rollback every partial domain write on failure."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
