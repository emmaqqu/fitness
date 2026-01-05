
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

import sqlite3

def init_db():
    conn = sqlite3.connect("instance/app.sqlite")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        age INTEGER,
        sex TEXT,
        height INTEGER,
        weight INTEGER,
        activity_level TEXT,
        health_status TEXT,
        xp INTEGER DEFAULT 0,
        level INTEGER DEFAULT 1
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS goals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        goal_name TEXT,
        target_value INTEGER,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS activities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        activity_name TEXT,
        duration INTEGER,
        activity_date TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS calories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        calories INTEGER,
        log_date TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS hydration (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        water_amount INTEGER,
        log_date TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS friends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        friend_id INTEGER,
        status TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (friend_id) REFERENCES users(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS avatars (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        avatar_name TEXT,
        required_level INTEGER
    )
    """)

    conn.commit()
    conn.close()

# Login SELECT query
def check_login(username, password):
    conn = sqlite3.connect("instance/app.sqlite")
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM users WHERE username = ? AND password = ?",
        (username, password)
    )

    user = cursor.fetchone()
    conn.close()

    return user

# Registration INSERT query
def create_user(username, password):
    conn = sqlite3.connect("instance/app.sqlite")
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO users (username, password) VALUES (?, ?)",
        (username, password)
    )

    conn.commit()
    conn.close()

