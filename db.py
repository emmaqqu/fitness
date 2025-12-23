
import sqlite3
from contextlib import contextmanager
import bcrypt

@contextmanager
def db_session(db_name):
    conn = sqlite3.connect(db_name)
    try:
        yield conn
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Database error: {e}")
        raise 
    finally:
        conn.close()
        print("Connection closed.")
