from pathlib import Path
import sqlite3

BASE_DIR = Path(__file__).resolve().parent.parent

# We will create/use this SQLite warehouse.
DB_PATH = BASE_DIR / "data" / "network_analytics.db"


def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
