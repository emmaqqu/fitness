from __future__ import annotations

import hashlib
import re
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from werkzeug.security import check_password_hash, generate_password_hash

DB_PATH = Path("instance/app.sqlite")
ACTION_LOG_PATH = Path("instance/actions.log")
ERROR_LOG_PATH = Path("instance/errors.log")
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
WORKOUT_DIFFICULTIES = {"Easy", "Standard", "Hard"}
PASCAL_ACRONYMS = {
    "api": "API",
    "id": "ID",
    "ip": "IP",
    "sso": "SSO",
    "ui": "UI",
    "uid": "UID",
    "url": "URL",
    "uuid": "UUID",
    "xp": "XP",
}

HEALTH_KB = [
    {
        "topic": "hydration",
        "summary": "Drink water regularly throughout the day and increase intake after exercise.",
        "tips": ["Keep a bottle nearby", "Drink before you feel thirsty", "Track litres daily"],
    },
    {
        "topic": "sleep",
        "summary": "Aim for a consistent sleep schedule with 7-9 hours each night.",
        "tips": ["Limit screens before bed", "Sleep and wake at fixed times", "Keep bedroom cool and dark"],
    },
    {
        "topic": "stress",
        "summary": "Use short breathing breaks, regular movement, and social support.",
        "tips": ["Box breathing for 2 minutes", "Take walking breaks", "Write down priorities"],
    },
    {
        "topic": "nutrition",
        "summary": "Build meals around protein, fiber, and minimally processed foods.",
        "tips": ["Prioritize whole foods", "Plan snacks", "Balance carbs, protein, and fats"],
    },
    {
        "topic": "workout recovery",
        "summary": "Alternate intensity, sleep enough, and hydrate to recover faster.",
        "tips": ["Include rest days", "Do light mobility", "Replenish fluids"],
    },
]

# Core utilities

def _now() -> datetime:
    return datetime.now()


def _now_str() -> str:
    return _now().strftime(DATETIME_FORMAT)


def _today_str() -> str:
    return _now().strftime(DATE_FORMAT)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, DATETIME_FORMAT)
    except ValueError:
        return None


def _hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing = {str(column["name"]).lower() for column in columns}
    if column_name.lower() in existing:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def _rename_column_if_needed(
    conn: sqlite3.Connection,
    table_name: str,
    old_name: str,
    new_name: str,
) -> None:
    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing = {str(column["name"]).lower() for column in columns}
    if new_name.lower() in existing or old_name.lower() not in existing:
        return
    conn.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")


def _migrate_legacy_schema(conn: sqlite3.Connection) -> None:
    rename_operations = [
        ("USER", "first_name", "FirstName"),
        ("USER", "last_name", "LastName"),
        ("USER", "phone_num", "PhoneNum"),
        ("USER", "date_joined", "DateJoined"),
        ("USER", "failed_login_attempts", "FailedLoginAttempts"),
        ("USER", "locked_until", "LockedUntil"),
        ("AVATAR", "avatar_id", "AvatarID"),
        ("AVATAR", "avatar_name", "AvatarName"),
        ("AVATAR", "unlock_level", "UnlockLevel"),
        ("PROFILE", "profile_id", "ProfileID"),
        ("PROFILE", "avatar_id", "AvatarID"),
        ("PROFILE", "ProfileAvatarKey", "AvatarID"),
        ("ACTIVITIES", "activity_id", "ActivityID"),
        ("ACTIVITIES", "duration_minutes", "DurationMinutes"),
        ("ACTIVITIES", "distance_km", "DistanceKm"),
        ("ACTIVITIES", "activity_date", "ActivityDate"),
        ("CALORIES", "log_id", "LogID"),
        ("CALORIES", "calorie_intake", "CalorieIntake"),
        ("CALORIES", "log_date", "LogDate"),
        ("HYDRATION", "entry_id", "EntryID"),
        ("HYDRATION", "hydration_intake", "HydrationIntake"),
        ("HYDRATION", "entry_date", "EntryDate"),
        ("GAME_MODES", "mode_id", "ModeID"),
        ("GAME_MODES", "mode_name", "ModeName"),
        ("GAME", "session_id", "SessionID"),
        ("GAME", "mode_id", "ModeID"),
        ("GAME", "GameModeKey", "ModeID"),
        ("GAME", "xp_earned", "XPEarned"),
        ("GAME", "Winner", "GameWinner"),
        ("GAME", "start_time", "StartTime"),
        ("GAME", "end_time", "EndTime"),
        ("GAME_PLAYERS", "players_id", "PlayersID"),
        ("GAME_PLAYERS", "session_id", "SessionID"),
        ("GAME_PLAYERS", "PlayerSessionKey", "SessionID"),
        ("FRIENDS", "friendship_id", "FriendshipID"),
        ("FRIENDS", "RequesterUsername", "RequesterUsername"),
        ("FRIENDS", "TargetUsername", "TargetUsername"),
        ("FRIENDS", "request_status", "RequestStatus"),
        ("FRIENDS", "created_at", "FriendshipCreatedAt"),
        ("COOP_INVITES", "from_username", "from_username"),
        ("COOP_INVITES", "to_username", "to_username"),
        ("COOP_INVITES", "Status", "InviteStatus"),
        ("COOP_INVITES", "created_at", "InviteCreatedAt"),
        ("COOP_INVITES", "responded_at", "InviteRespondedAt"),
        ("COOP_MATCHES", "turn_username", "turn_username"),
        ("COOP_MATCHES", "Status", "MatchStatus"),
        ("COOP_MATCHES", "Winner", "MatchWinner"),
        ("COOP_MATCHES", "created_at", "MatchCreatedAt"),
        ("COOP_MATCHES", "updated_at", "MatchUpdatedAt"),
        ("HEALTH", "health_id", "HealthID"),
        ("HEALTH", "weight_kg", "WeightKg"),
        ("HEALTH", "height_cm", "HeightCm"),
        ("HEALTH", "activity_level", "ActivityLevel"),
        ("HEALTH", "overall_health", "OverallHealth"),
        ("GOAL_TYPE", "goal_type_id", "GoalTypeID"),
        ("GOAL_TYPE", "goal_type_name", "GoalTypeName"),
        ("GOALS", "goal_id", "GoalID"),
        ("GOALS", "goal_type_id", "GoalTypeID"),
        ("GOALS", "GoalTypeKey", "GoalTypeID"),
        ("GOALS", "target_value", "TargetValue"),
        ("GOALS", "start_date", "StartDate"),
        ("GOALS", "end_date", "EndDate"),
        ("GOALS", "Status", "GoalStatus"),
        ("SSO_TOKENS", "created_at", "SsoCreatedAt"),
        ("SSO_TOKENS", "expires_at", "SsoExpiresAt"),
        ("SSO_TOKENS", "used_at", "SsoUsedAt"),
        ("SSO_TOKENS", "token_hash", "SsoTokenKey"),
        ("FRIEND_INVITE_LINKS", "created_at", "LinkCreatedAt"),
        ("FRIEND_INVITE_LINKS", "expires_at", "LinkExpiresAt"),
        ("FRIEND_INVITE_LINKS", "token_hash", "FriendInviteLinkKey"),
    ]

    for table_name, old_name, new_name in rename_operations:
        _rename_column_if_needed(conn, table_name, old_name, new_name)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND lower(name) = lower(?) LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


@contextmanager
def db_session():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def _snake_key(key: str) -> str:
    key = str(key).strip()
    if not key:
        return key
    converted = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", key)
    converted = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", converted)
    converted = converted.replace("-", "_").lower()
    while "__" in converted:
        converted = converted.replace("__", "_")
    return converted


def _pascal_key(key: str) -> str:
    snake_key = _snake_key(key)
    if not snake_key:
        return snake_key
    parts = [part for part in snake_key.split("_") if part]
    converted_parts = [PASCAL_ACRONYMS.get(part.lower(), part[:1].upper() + part[1:]) for part in parts]
    return "".join(converted_parts)


class NormalizedRecord(dict):
    def __init__(self, record: dict[str, Any]):
        super().__init__()
        self._alias_map: dict[str, str] = {}
        for key, value in record.items():
            self._set_with_aliases(str(key), value)

    def _register_alias(self, alias: str, canonical: str) -> None:
        cleaned = str(alias).strip()
        if not cleaned:
            return
        self._alias_map[cleaned] = canonical

    def _set_with_aliases(self, key: str, value: Any) -> None:
        canonical = _pascal_key(key)
        dict.__setitem__(self, canonical, value)

        snake = _snake_key(key)
        lower = key.lower()
        canonical_snake = _snake_key(canonical)
        canonical_lower = canonical.lower()

        self._register_alias(key, canonical)
        self._register_alias(lower, canonical)
        self._register_alias(snake, canonical)
        self._register_alias(canonical, canonical)
        self._register_alias(canonical_lower, canonical)
        self._register_alias(canonical_snake, canonical)

    def _resolve_key(self, key: Any) -> str:
        lookup = str(key)
        if dict.__contains__(self, lookup):
            return lookup

        direct = self._alias_map.get(lookup)
        if direct:
            return direct

        lower = lookup.lower()
        direct = self._alias_map.get(lower)
        if direct:
            return direct

        snake = _snake_key(lookup)
        direct = self._alias_map.get(snake)
        if direct:
            return direct

        pascal = _pascal_key(lookup)
        if dict.__contains__(self, pascal):
            return pascal

        return lookup

    def __getitem__(self, key: Any) -> Any:
        return dict.__getitem__(self, self._resolve_key(key))

    def get(self, key: Any, default: Any = None) -> Any:
        resolved = self._resolve_key(key)
        if dict.__contains__(self, resolved):
            return dict.get(self, resolved, default)
        return default

    def __contains__(self, key: object) -> bool:
        try:
            resolved = self._resolve_key(key)
        except Exception:
            return False
        return dict.__contains__(self, resolved)

    def __setitem__(self, key: Any, value: Any) -> None:
        self._set_with_aliases(str(key), value)


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    return NormalizedRecord(record)


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [_normalize_record(dict(row)) for row in rows]


# -------------------------
# Initialization and seeding
# -------------------------

def init_db() -> None:
    with db_session() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS USER (
                Username TEXT PRIMARY KEY,
                Email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                FirstName TEXT,
                LastName TEXT,
                PhoneNum TEXT,
                DateJoined TEXT NOT NULL,
                FailedLoginAttempts INTEGER NOT NULL DEFAULT 0,
                LockedUntil TEXT
            );

            CREATE TABLE IF NOT EXISTS AVATAR (
                AvatarID INTEGER PRIMARY KEY AUTOINCREMENT,
                AvatarName TEXT NOT NULL UNIQUE,
                UnlockLevel INTEGER NOT NULL,
                Image TEXT
            );

            CREATE TABLE IF NOT EXISTS PROFILE (
                ProfileID INTEGER PRIMARY KEY AUTOINCREMENT,
                Username TEXT NOT NULL UNIQUE,
                AvatarID INTEGER,
                Level INTEGER NOT NULL DEFAULT 1,
                XP INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (Username) REFERENCES USER(Username) ON DELETE CASCADE,
                FOREIGN KEY (AvatarID) REFERENCES AVATAR(AvatarID)
            );

            CREATE TABLE IF NOT EXISTS ACTIVITIES (
                ActivityID INTEGER PRIMARY KEY AUTOINCREMENT,
                Username TEXT NOT NULL,
                Type TEXT NOT NULL,
                DurationMinutes INTEGER NOT NULL,
                CaloriesBurnt INTEGER,
                DistanceKm REAL,
                ActivityDate TEXT NOT NULL,
                Source TEXT NOT NULL,
                FOREIGN KEY (Username) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS CALORIES (
                LogID INTEGER PRIMARY KEY AUTOINCREMENT,
                Username TEXT NOT NULL,
                CalorieIntake INTEGER NOT NULL,
                LogDate TEXT NOT NULL,
                FOREIGN KEY (Username) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS HYDRATION (
                EntryID INTEGER PRIMARY KEY AUTOINCREMENT,
                Username TEXT NOT NULL,
                HydrationIntake REAL NOT NULL,
                EntryDate TEXT NOT NULL,
                FOREIGN KEY (Username) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS GAME_MODES (
                ModeID INTEGER PRIMARY KEY AUTOINCREMENT,
                ModeName TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS GAME (
                SessionID INTEGER PRIMARY KEY AUTOINCREMENT,
                ModeID INTEGER NOT NULL,
                XPEarned INTEGER NOT NULL,
                GameWinner TEXT,
                StartTime TEXT NOT NULL,
                EndTime TEXT,
                FOREIGN KEY (ModeID) REFERENCES GAME_MODES(ModeID)
            );

            CREATE TABLE IF NOT EXISTS GAME_PLAYERS (
                PlayersID INTEGER PRIMARY KEY AUTOINCREMENT,
                SessionID INTEGER NOT NULL,
                Username TEXT NOT NULL,
                FOREIGN KEY (SessionID) REFERENCES GAME(SessionID) ON DELETE CASCADE,
                FOREIGN KEY (Username) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS FRIENDS (
                FriendshipID INTEGER PRIMARY KEY AUTOINCREMENT,
                RequesterUsername TEXT NOT NULL,
                TargetUsername TEXT NOT NULL,
                RequestStatus TEXT NOT NULL CHECK(RequestStatus IN ('Pending', 'Accepted', 'Rejected')),
                FriendshipCreatedAt TEXT NOT NULL,
                UNIQUE (RequesterUsername, TargetUsername),
                FOREIGN KEY (RequesterUsername) REFERENCES USER(Username) ON DELETE CASCADE,
                FOREIGN KEY (TargetUsername) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS COOP_INVITES (
                invite_id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_username TEXT NOT NULL,
                to_username TEXT NOT NULL,
                InviteStatus TEXT NOT NULL CHECK(InviteStatus IN ('Pending', 'Accepted', 'Declined', 'Cancelled')),
                InviteCreatedAt TEXT NOT NULL,
                InviteRespondedAt TEXT,
                FOREIGN KEY (from_username) REFERENCES USER(Username) ON DELETE CASCADE,
                FOREIGN KEY (to_username) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS COOP_MATCHES (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_one TEXT NOT NULL,
                player_two TEXT NOT NULL,
                turn_username TEXT NOT NULL,
                state_json TEXT NOT NULL,
                MatchStatus TEXT NOT NULL CHECK(MatchStatus IN ('Active', 'Finished', 'Abandoned')),
                MatchWinner TEXT,
                MatchCreatedAt TEXT NOT NULL,
                MatchUpdatedAt TEXT NOT NULL,
                FOREIGN KEY (player_one) REFERENCES USER(Username) ON DELETE CASCADE,
                FOREIGN KEY (player_two) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS SSO_TOKENS (
                SsoTokenKey TEXT PRIMARY KEY,
                Username TEXT NOT NULL,
                SsoCreatedAt TEXT NOT NULL,
                SsoExpiresAt TEXT NOT NULL,
                SsoUsedAt TEXT,
                FOREIGN KEY (Username) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS FRIEND_INVITE_LINKS (
                FriendInviteLinkKey TEXT PRIMARY KEY,
                public_token TEXT NOT NULL,
                inviter_username TEXT NOT NULL,
                LinkCreatedAt TEXT NOT NULL,
                LinkExpiresAt TEXT NOT NULL,
                use_count INTEGER NOT NULL DEFAULT 0,
                max_uses INTEGER NOT NULL DEFAULT 25,
                is_active INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (inviter_username) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS HEALTH (
                HealthID INTEGER PRIMARY KEY AUTOINCREMENT,
                Username TEXT NOT NULL UNIQUE,
                Age INTEGER,
                Sex TEXT,
                WeightKg REAL,
                HeightCm REAL,
                ActivityLevel TEXT,
                OverallHealth TEXT,
                Mood TEXT,
                FOREIGN KEY (Username) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS GOAL_TYPE (
                GoalTypeID INTEGER PRIMARY KEY AUTOINCREMENT,
                GoalTypeName TEXT NOT NULL UNIQUE,
                Unit TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS GOALS (
                GoalID INTEGER PRIMARY KEY AUTOINCREMENT,
                GoalTypeID INTEGER NOT NULL,
                Username TEXT NOT NULL,
                TargetValue REAL NOT NULL,
                StartDate TEXT NOT NULL,
                EndDate TEXT,
                GoalStatus TEXT NOT NULL CHECK(GoalStatus IN ('Active', 'On Track', 'Completed', 'Cancelled')) DEFAULT 'Active',
                FOREIGN KEY (GoalTypeID) REFERENCES GOAL_TYPE(GoalTypeID),
                FOREIGN KEY (Username) REFERENCES USER(Username) ON DELETE CASCADE
            );
            """
        )

        _migrate_legacy_schema(conn)

        if _table_exists(conn, "users"):
            conn.execute(
                """
                INSERT OR IGNORE INTO USER (
                    Username, Email, password_hash, FirstName, LastName, PhoneNum, DateJoined,
                    FailedLoginAttempts, LockedUntil
                )
                SELECT
                    username, email, password_hash, first_name, last_name, phone_num, date_joined,
                    failed_login_attempts, locked_until
                FROM users
                """
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO users (
                    username, email, password_hash, first_name, last_name, phone_num, date_joined,
                    failed_login_attempts, locked_until
                )
                SELECT
                    Username, Email, password_hash, FirstName, LastName, PhoneNum, DateJoined,
                    FailedLoginAttempts, LockedUntil
                FROM USER
                """
            )

        if _table_exists(conn, "game_sessions"):
            conn.execute(
                """
                INSERT OR IGNORE INTO GAME (SessionID, ModeID, XPEarned, GameWinner, StartTime, EndTime)
                SELECT session_id, mode_id, xp_earned, winner, start_time, end_time
                FROM game_sessions
                """
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO game_sessions (session_id, mode_id, xp_earned, winner, start_time, end_time)
                SELECT SessionID, ModeID, XPEarned, GameWinner, StartTime, EndTime
                FROM GAME
                """
            )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sso_tokens_Username ON SSO_TOKENS(Username)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_friend_links_inviter ON FRIEND_INVITE_LINKS(inviter_username, LinkCreatedAt)"
        )

        _ensure_column(conn, "activities", "difficulty", "TEXT NOT NULL DEFAULT 'Standard'")
        _ensure_column(conn, "FRIEND_INVITE_LINKS", "public_token", "TEXT")
        _ensure_column(conn, "health", "health_conditions", "TEXT")
        _ensure_column(conn, "health", "diet_profile", "TEXT")
        _ensure_column(conn, "health", "climate", "TEXT")

        avatar_rows = [
            ("Starter Sprite", 1, "avatar_starter.png"),
            ("Trail Runner", 2, "avatar_runner.png"),
            ("Hydro Hero", 3, "avatar_hydro.png"),
            ("Card Master", 5, "avatar_master.png"),
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO AVATAR (AvatarName, UnlockLevel, Image) VALUES (?, ?, ?)",
            avatar_rows,
        )

        goal_Type_rows = [
            ("Calories", "kcal"),
            ("Hydration", "litres"),
            ("Exercise", "minutes"),
            ("Distance", "km"),
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO GOAL_TYPE (GoalTypeName, Unit) VALUES (?, ?)",
            goal_Type_rows,
        )

        mode_rows = [("Solo",), ("Co-op",)]
        conn.executemany(
            "INSERT OR IGNORE INTO GAME_MODES (ModeName) VALUES (?)",
            mode_rows,
        )


# -------------------------
# Logging
# -------------------------

def log_action(Username: str | None, action: str) -> None:
    ACTION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    actor = Username or "ANONYMOUS"
    with ACTION_LOG_PATH.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{_now_str()} | {actor} | {action}\n")


def log_error(Username: str | None, error_text: str) -> None:
    ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    actor = Username or "ANONYMOUS"
    payload = error_text.strip().replace("\n", "\\n")
    with ERROR_LOG_PATH.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{_now_str()} | {actor} | {payload}\n")


# -------------------------
# Users and auth
# -------------------------

def create_user(
    Username: str,
    Email: str,
    Password: str,
    FirstName: str | None = None,
    LastName: str | None = None,
    PhoneNum: str | None = None,
) -> None:
    password_hash = generate_password_hash(Password, method="pbkdf2:sha256")
    DateJoined = _today_str()

    with db_session() as conn:
        conn.execute(
            """
            INSERT INTO USER (Username, Email, password_hash, FirstName, LastName, PhoneNum, DateJoined)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (Username, Email, password_hash, FirstName, LastName, PhoneNum, DateJoined),
        )

        if _table_exists(conn, "users"):
            conn.execute(
                """
                INSERT OR IGNORE INTO users (
                    username, email, password_hash, first_name, last_name, phone_num, date_joined
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (Username, Email, password_hash, FirstName, LastName, PhoneNum, DateJoined),
            )

        starter_avatar = conn.execute(
            "SELECT AvatarID FROM AVATAR ORDER BY UnlockLevel ASC, AvatarID ASC LIMIT 1"
        ).fetchone()
        AvatarID = starter_avatar["AvatarID"] if starter_avatar else None

        conn.execute(
            "INSERT INTO PROFILE (Username, AvatarID, Level, XP) VALUES (?, ?, 1, 0)",
            (Username, AvatarID),
        )
        conn.execute("INSERT INTO HEALTH (Username) VALUES (?)", (Username,))


def get_user_by_identity(identity: str) -> dict[str, Any] | None:
    lookup = identity.strip().lower()
    if not lookup:
        return None

    with db_session() as conn:
        by_Username = conn.execute(
            "SELECT * FROM USER WHERE lower(Username) = ?",
            (lookup,),
        ).fetchone()
        if by_Username:
            return _normalize_record(dict(by_Username))

        by_Email = conn.execute(
            "SELECT * FROM USER WHERE lower(Email) = ?",
            (lookup,),
        ).fetchone()
        return _normalize_record(dict(by_Email)) if by_Email else None


def authenticate_user(identity: str, Password: str) -> tuple[dict[str, Any] | None, str]:
    lookup = identity.strip().lower()
    if not lookup:
        return None, "Username or Email is required."

    with db_session() as conn:
        user_row = conn.execute(
            "SELECT * FROM USER WHERE lower(Username) = ?",
            (lookup,),
        ).fetchone()
        if not user_row:
            user_row = conn.execute(
                "SELECT * FROM USER WHERE lower(Email) = ?",
                (lookup,),
            ).fetchone()

        if not user_row:
            return None, "Account not found."

        LockedUntil = _parse_datetime(user_row["LockedUntil"])
        now = _now()
        if LockedUntil and now < LockedUntil:
            return None, f"Account locked until {LockedUntil.strftime(DATETIME_FORMAT)}."

        if check_password_hash(user_row["password_hash"], Password):
            conn.execute(
                "UPDATE USER SET FailedLoginAttempts = 0, LockedUntil = NULL WHERE Username = ?",
                (user_row["Username"],),
            )
            return _normalize_record(dict(user_row)), ""

        attempts = int(user_row["FailedLoginAttempts"]) + 1
        if attempts >= 5:
            lock_time = now + timedelta(minutes=15)
            conn.execute(
                "UPDATE USER SET FailedLoginAttempts = ?, LockedUntil = ? WHERE Username = ?",
                (attempts, lock_time.strftime(DATETIME_FORMAT), user_row["Username"]),
            )
            return None, f"Too many failed attempts. Account locked until {lock_time.strftime(DATETIME_FORMAT)}."

        conn.execute(
            "UPDATE USER SET FailedLoginAttempts = ? WHERE Username = ?",
            (attempts, user_row["Username"]),
        )
        remaining = 5 - attempts
        return None, f"Invalid credentials. {remaining} attempt(s) remaining before lockout."


def get_user(Username: str) -> dict[str, Any] | None:
    with db_session() as conn:
        row = conn.execute("SELECT * FROM USER WHERE Username = ?", (Username,)).fetchone()
        return _normalize_record(dict(row)) if row else None


def update_personal_info(
    Username: str,
    Email: str,
    FirstName: str | None,
    LastName: str | None,
    PhoneNum: str | None,
) -> None:
    with db_session() as conn:
        conn.execute(
            """
            UPDATE USER
            SET Email = ?, FirstName = ?, LastName = ?, PhoneNum = ?
            WHERE Username = ?
            """,
            (Email, FirstName, LastName, PhoneNum, Username),
        )


def create_sso_token(identity: str, ttl_minutes: int = 10) -> tuple[bool, str, str | None]:
    user = get_user_by_identity(identity)
    if not user:
        return False, "Account not found.", None
    username = str(user.get("Username") or user.get("username") or "").strip()
    if not username:
        return False, "Account not found.", None

    raw_token = secrets.token_urlsafe(32)
    token_hash = _hash_token(raw_token)
    now = _now()
    expires_at = (now + timedelta(minutes=max(1, ttl_minutes))).strftime(DATETIME_FORMAT)

    with db_session() as conn:
        conn.execute(
            "DELETE FROM SSO_TOKENS WHERE SsoExpiresAt < ? OR SsoUsedAt IS NOT NULL",
            (now.strftime(DATETIME_FORMAT),),
        )
        conn.execute(
            """
            INSERT INTO SSO_TOKENS (SsoTokenKey, Username, SsoCreatedAt, SsoExpiresAt, SsoUsedAt)
            VALUES (?, ?, ?, ?, NULL)
            """,
            (token_hash, username, now.strftime(DATETIME_FORMAT), expires_at),
        )

    return True, "SSO link generated.", raw_token


def consume_sso_token(raw_token: str) -> tuple[dict[str, Any] | None, str]:
    if not raw_token:
        return None, "SSO token missing."

    token_hash = _hash_token(raw_token)
    now = _now().strftime(DATETIME_FORMAT)

    with db_session() as conn:
        row = conn.execute(
            """
            SELECT SsoTokenKey, Username, SsoExpiresAt AS expires_at, SsoUsedAt AS used_at
            FROM SSO_TOKENS
            WHERE SsoTokenKey = ?
            LIMIT 1
            """,
            (token_hash,),
        ).fetchone()

        if not row:
            return None, "SSO token is invalid."
        if row["used_at"]:
            return None, "SSO token has already been used."
        if str(row["expires_at"]) < now:
            return None, "SSO token has expired."

        conn.execute(
            "UPDATE SSO_TOKENS SET SsoUsedAt = ? WHERE SsoTokenKey = ?",
            (now, token_hash),
        )
        user_row = conn.execute(
            "SELECT * FROM USER WHERE Username = ?",
            (row["Username"],),
        ).fetchone()
        if not user_row:
            return None, "User account no longer exists."

        conn.execute(
            "UPDATE USER SET FailedLoginAttempts = 0, LockedUntil = NULL WHERE Username = ?",
            (row["Username"],),
        )
        return _normalize_record(dict(user_row)), ""


# -------------------------
# Profile, XP, avatars
# -------------------------

def get_profile(Username: str) -> dict[str, Any] | None:
    with db_session() as conn:
        row = conn.execute(
            """
            SELECT
                p.ProfileID,
                p.Username,
                p.AvatarID,
                p.Level,
                p.XP,
                a.AvatarName,
                a.UnlockLevel,
                a.Image,
                u.FirstName,
                u.LastName,
                u.DateJoined
            FROM PROFILE p
            JOIN USER u ON u.Username = p.Username
            LEFT JOIN AVATAR a ON a.AvatarID = p.AvatarID
            WHERE p.Username = ?
            """,
            (Username,),
        ).fetchone()
        return _normalize_record(dict(row)) if row else None


def award_XP(Username: str, XP_delta: int) -> dict[str, Any]:
    with db_session() as conn:
        row = conn.execute(
            "SELECT XP, Level FROM PROFILE WHERE Username = ?",
            (Username,),
        ).fetchone()
        if not row:
            return _normalize_record({"XP": 0, "Level": 1, "LeveledUp": False})

        previous_Level = int(row["Level"])
        new_XP = int(row["XP"]) + max(0, int(XP_delta))
        new_Level = max(1, (new_XP // 100) + 1)

        conn.execute(
            "UPDATE PROFILE SET XP = ?, Level = ? WHERE Username = ?",
            (new_XP, new_Level, Username),
        )

        return _normalize_record(
            {
                "XP": new_XP,
                "Level": new_Level,
                "LeveledUp": new_Level > previous_Level,
            }
        )


def award_xp(username: str, xp_delta: int) -> dict[str, Any]:
    return _normalize_record(award_XP(username, xp_delta))


def list_avatars() -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            "SELECT * FROM AVATAR ORDER BY UnlockLevel, AvatarID"
        ).fetchall()
        return _rows_to_dicts(rows)


def set_avatar(Username: str, AvatarID: int) -> tuple[bool, str]:
    with db_session() as conn:
        avatar = conn.execute(
            "SELECT AvatarName, UnlockLevel FROM AVATAR WHERE AvatarID = ?",
            (AvatarID,),
        ).fetchone()
        if not avatar:
            return False, "Avatar not found."

        profile = conn.execute(
            "SELECT Level FROM PROFILE WHERE Username = ?",
            (Username,),
        ).fetchone()
        if not profile:
            return False, "Profile not found."

        if int(profile["Level"]) < int(avatar["UnlockLevel"]):
            return False, f"{avatar['AvatarName']} unlocks at Level {avatar['UnlockLevel']}."

        conn.execute(
            "UPDATE PROFILE SET AvatarID = ? WHERE Username = ?",
            (AvatarID, Username),
        )
        return True, f"Avatar changed to {avatar['AvatarName']}."


# -------------------------
# Goals and activities
# -------------------------

def list_goal_Types() -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            "SELECT * FROM GOAL_TYPE ORDER BY GoalTypeName"
        ).fetchall()
        return _rows_to_dicts(rows)


def list_goal_types() -> list[dict[str, Any]]:
    return list_goal_Types()


def add_goal(
    username: str,
    goal_type_id: int,
    target_value: float,
    start_date: str | None,
    end_date: str | None,
    status: str = "Active",
) -> None:
    start = start_date or _today_str()
    with db_session() as conn:
        conn.execute(
            """
            INSERT INTO GOALS (GoalTypeID, Username, TargetValue, StartDate, EndDate, GoalStatus)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (goal_type_id, username, target_value, start, end_date, status),
        )


def list_goals(Username: str) -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            """
            SELECT
                g.GoalID,
                g.GoalTypeID,
                g.Username,
                g.TargetValue,
                g.StartDate,
                g.EndDate,
                g.GoalStatus AS Status,
                gt.GoalTypeName,
                gt.Unit
            FROM GOALS g
            JOIN GOAL_TYPE gt ON gt.GoalTypeID = g.GoalTypeID
            WHERE g.Username = ?
            ORDER BY g.GoalID DESC
            """,
            (Username,),
        ).fetchall()
        return _rows_to_dicts(rows)


def update_goal_Status(Username: str, GoalID: int, Status: str) -> None:
    with db_session() as conn:
        conn.execute(
            "UPDATE GOALS SET GoalStatus = ? WHERE GoalID = ? AND Username = ?",
            (Status, GoalID, Username),
        )


def update_goal_status(username: str, goal_id: int, status: str) -> None:
    update_goal_Status(username, goal_id, status)


def add_activity(
    username: str,
    activity_type: str,
    duration_minutes: int,
    calories_burnt: int | None,
    distance_km: float | None,
    activity_date: str | None,
    source: str,
    difficulty: str | None = None,
) -> None:
    stamp = activity_date or _now_str()
    difficulty_value = str(difficulty or "Standard").strip().title()
    if difficulty_value not in WORKOUT_DIFFICULTIES:
        difficulty_value = "Standard"
    with db_session() as conn:
        conn.execute(
            """
            INSERT INTO ACTIVITIES (
                Username, Type, DurationMinutes, CaloriesBurnt, DistanceKm, ActivityDate, Source, difficulty
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (username, activity_type, duration_minutes, calories, distance_km, stamp, source, difficulty_value),
        )


def list_activities(Username: str, limit: int = 100) -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM ACTIVITIES
            WHERE Username = ?
            ORDER BY ActivityDate DESC, ActivityID DESC
            LIMIT ?
            """,
            (Username, limit),
        ).fetchall()
        return _rows_to_dicts(rows)


def workout_XP_value(DurationMinutes: int, difficulty: str) -> int:
    duration = max(1, int(DurationMinutes))
    difficulty_value = str(difficulty or "Standard").strip().title()
    multiplier = {"Easy": 0.9, "Standard": 1.0, "Hard": 1.2}.get(difficulty_value, 1.0)
    return max(10, int(round(duration * multiplier)))


def workout_xp_value(duration_minutes: int, difficulty: str) -> int:
    return workout_XP_value(duration_minutes, difficulty)


# -------------------------
# Calories and hydration
# -------------------------

def add_calorie_log(Username: str, CalorieIntake: int, LogDate: str | None) -> None:
    EntryDate = LogDate or _today_str()
    with db_session() as conn:
        conn.execute(
            "INSERT INTO CALORIES (Username, CalorieIntake, LogDate) VALUES (?, ?, ?)",
            (Username, CalorieIntake, EntryDate),
        )


def list_calorie_logs(Username: str, limit: int = 90) -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            "SELECT * FROM CALORIES WHERE Username = ? ORDER BY LogDate DESC, LogID DESC LIMIT ?",
            (Username, limit),
        ).fetchall()
        return _rows_to_dicts(rows)


def add_hydration_log(Username: str, HydrationIntake: float, EntryDate: str | None) -> None:
    date_value = EntryDate or _today_str()
    with db_session() as conn:
        conn.execute(
            "INSERT INTO HYDRATION (Username, HydrationIntake, EntryDate) VALUES (?, ?, ?)",
            (Username, HydrationIntake, date_value),
        )


def list_hydration_logs(Username: str, limit: int = 90) -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            "SELECT * FROM HYDRATION WHERE Username = ? ORDER BY EntryDate DESC, EntryID DESC LIMIT ?",
            (Username, limit),
        ).fetchall()
        return _rows_to_dicts(rows)


# -------------------------
# Health and recommendations
# -------------------------

def get_health(Username: str) -> dict[str, Any] | None:
    with db_session() as conn:
        row = conn.execute(
            "SELECT * FROM HEALTH WHERE Username = ?",
            (Username,),
        ).fetchone()
        return _normalize_record(dict(row)) if row else None


def update_health(
    username: str,
    age: int | None,
    sex: str | None,
    weight_kg: float | None,
    height_cm: float | None,
    activity_level: str | None,
    overall_health: str | None,
    health_conditions: str | None,
    diet_profile: str | None,
    climate: str | None,
    mood: str | None,
) -> None:
    with db_session() as conn:
        conn.execute(
            """
            INSERT INTO HEALTH (
                Username,
                Age,
                Sex,
                WeightKg,
                HeightCm,
                ActivityLevel,
                OverallHealth,
                health_conditions,
                diet_profile,
                climate,
                Mood
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(Username)
            DO UPDATE SET
                Age = excluded.Age,
                Sex = excluded.Sex,
                WeightKg = excluded.WeightKg,
                HeightCm = excluded.HeightCm,
                ActivityLevel = excluded.ActivityLevel,
                OverallHealth = excluded.OverallHealth,
                health_conditions = excluded.health_conditions,
                diet_profile = excluded.diet_profile,
                climate = excluded.climate,
                Mood = excluded.Mood
            """,
            (
                username,
                age,
                sex,
                weight_kg,
                height_cm,
                activity_level,
                overall_health,
                health_conditions,
                diet_profile,
                climate,
                mood,
            ),
        )


def update_Mood(Username: str, Mood: str) -> None:
    current = get_health(Username) or {}
    update_health(
        username=Username,
        age=current.get("Age"),
        sex=current.get("Sex"),
        weight_kg=current.get("WeightKg"),
        height_cm=current.get("HeightCm"),
        activity_level=current.get("ActivityLevel"),
        overall_health=current.get("OverallHealth"),
        health_conditions=current.get("health_conditions"),
        diet_profile=current.get("diet_profile"),
        climate=current.get("climate"),
        mood=Mood,
    )


def update_mood(username: str, mood: str) -> None:
    update_Mood(username, mood)


def calorie_recommendation(Username: str) -> dict[str, Any]:
    health = get_health(Username) or {}
    Age = health.get("Age")
    Sex = (health.get("Sex") or "").strip().lower()
    weight = health.get("WeightKg")
    height = health.get("HeightCm")
    ActivityLevel = (health.get("ActivityLevel") or "moderate").strip().lower()
    OverallHealth = (health.get("OverallHealth") or "").strip().lower()

    if not all([Age, weight, height]):
        return _normalize_record(
            {
                "Recommended": 2000,
                "Basis": (
                    "General baseline used. Add Age, Sex, height, weight, activity Level, and health notes "
                    "for personalized guidance."
                ),
            }
        )

    if Sex.startswith("m"):
        bmr = (10 * weight) + (6.25 * height) - (5 * Age) + 5
    elif Sex.startswith("f"):
        bmr = (10 * weight) + (6.25 * height) - (5 * Age) - 161
    else:
        bmr = (10 * weight) + (6.25 * height) - (5 * Age) - 78

    multipliers = {
        "sedentary": 1.2,
        "light": 1.375,
        "moderate": 1.55,
        "active": 1.725,
        "very active": 1.9,
    }

    multiplier = multipliers.get(ActivityLevel, 1.55)
    recommended = int(round(bmr * multiplier))

    health_adjustment = 0
    adjustment_note = "No additional health-based adjustment applied."
    if any(term in OverallHealth for term in ("weight loss", "lose weight", "fat loss", "obesity")):
        health_adjustment = -250
        adjustment_note = "Adjusted for a possible weight-loss focus in health notes."
    elif any(term in OverallHealth for term in ("muscle gain", "underweight", "bulking", "gain weight")):
        health_adjustment = 200
        adjustment_note = "Adjusted for a possible muscle-gain/weight-gain focus in health notes."

    recommended = max(1200, recommended + health_adjustment)
    return _normalize_record(
        {
            "Recommended": recommended,
            "Basis": (
                f"Estimated using Age, Sex, weight, height, activity Level, and overall health notes. "
                f"{adjustment_note}"
            ),
        }
    )


def hydration_recommendation(Username: str) -> dict[str, Any]:
    health = get_health(Username) or {}
    Age = health.get("Age")
    Sex = (health.get("Sex") or "").strip().lower()
    weight = health.get("WeightKg")
    ActivityLevel = (health.get("ActivityLevel") or "moderate").strip().lower()
    climate = (health.get("climate") or "temperate").strip().lower()

    if weight:
        baseline = weight * 0.033
    else:
        baseline = 2.6
        if Sex.startswith("f"):
            baseline = 2.2
        elif Sex.startswith("m"):
            baseline = 3.0

    if Age and int(Age) >= 55:
        baseline -= 0.15

    activity_extras = {
        "sedentary": 0.2,
        "light": 0.35,
        "moderate": 0.5,
        "active": 0.7,
        "very active": 0.9,
    }
    climate_extras = {
        "cold": 0.1,
        "temperate": 0.3,
        "humid": 0.55,
        "dry": 0.6,
        "hot": 0.75,
    }

    recommended = round(
        max(
            1.5,
            baseline + activity_extras.get(ActivityLevel, 0.5) + climate_extras.get(climate, 0.3),
        ),
        2,
    )
    return _normalize_record(
        {
            "Recommended": recommended,
            "Basis": "General hydration estimate based on Age, Sex, activity Level, body weight, and climate.",
        }
    )


def personalized_health_tips(Username: str) -> list[str]:
    health = get_health(Username) or {}
    goals = list_goals(Username)
    activities = list_activities(Username, limit=14)
    tips: list[str] = []

    ActivityLevel = str(health.get("ActivityLevel") or "").strip().lower()
    if ActivityLevel in {"", "sedentary"}:
        tips.append("Start with short daily movement blocks (10-15 min) and increase gradually.")
    elif ActivityLevel in {"active", "very active"}:
        tips.append("Schedule at least one recovery-focused session each week to support consistency.")

    climate = str(health.get("climate") or "").strip().lower()
    if climate in {"hot", "humid", "dry"}:
        tips.append("Hydrate before and after sessions; hotter or drier climates usually require extra fluid.")

    conditions = str(health.get("health_conditions") or "").strip()
    if conditions:
        tips.append("Use low-impact alternatives when needed and pace intensity around your listed conditions.")

    diet_profile = str(health.get("diet_profile") or "").strip().lower()
    if "high protein" in diet_profile:
        tips.append("Spread protein intake across meals to support muscular recovery.")
    elif diet_profile:
        tips.append("Keep meals consistent with your diet preferences and prioritize minimally processed foods.")

    goal_Types = {str(goal.get("GoalTypeName") or "").lower() for goal in goals if goal.get("Status") != "Cancelled"}
    if "hydration" in goal_Types:
        tips.append("Pair each meal with water to make hydration goals easier to sustain.")
    if "exercise" in goal_Types:
        tips.append("Use progressive overload: small weekly increases in duration or intensity are more sustainable.")
    if "Calories" in goal_Types:
        tips.append("Track calorie patterns over a full week rather than single-day fluctuations.")

    recent_minutes = sum(int(item.get("DurationMinutes") or 0) for item in activities[:7])
    if recent_minutes < 90:
        tips.append("Aim for at least 90 active minutes this week, then scale toward 150+.")
    elif recent_minutes > 300:
        tips.append("You are training at high volume; prioritize sleep and mobility to reduce injury risk.")

    if not tips:
        tips.append("Maintain a balanced routine of movement, hydration, nutrition, and sleep.")
    return tips[:6]


def _linear_search_health_topics(search_term: str) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for item in HEALTH_KB:
        haystack = f"{item['topic']} {item['summary']} {' '.join(item['tips'])}".lower()
        if search_term in haystack:
            matches.append(item)
    return matches


def _binary_search_health_topic_exact(search_term: str) -> list[dict[str, Any]]:
    indexed_topics = sorted(
        ((str(item["topic"]).lower(), item) for item in HEALTH_KB),
        key=lambda entry: entry[0],
    )
    low = 0
    high = len(indexed_topics) - 1

    while low <= high:
        mid = (low + high) // 2
        topic, item = indexed_topics[mid]
        if topic == search_term:
            return [item]
        if search_term < topic:
            high = mid - 1
        else:
            low = mid + 1

    return []


def search_health_topics(query: str) -> dict[str, Any]:
    search_term = query.strip().lower()
    if not search_term:
        return _normalize_record(
            {
                "Results": [],
                "Algorithm": "none",
                "Note": "Enter a topic to search.",
            }
        )

    binary_results = _binary_search_health_topic_exact(search_term)
    if binary_results:
        return _normalize_record(
            {
                "Results": binary_results,
                "Algorithm": "binary",
                "Note": "Binary search found an exact topic match.",
            }
        )

    return _normalize_record(
        {
            "Results": _linear_search_health_topics(search_term),
            "Algorithm": "linear",
            "Note": "No exact topic match found, so linear search was used for partial matches.",
        }
    )


# -------------------------
# Friends
# -------------------------

def search_users(search_term: str, current_Username: str) -> list[dict[str, Any]]:
    query = f"%{search_term.strip()}%"
    with db_session() as conn:
        rows = conn.execute(
            """
            SELECT Username, FirstName, LastName
            FROM USER
            WHERE Username != ? AND Username LIKE ?
            ORDER BY Username
            LIMIT 20
            """,
            (current_Username, query),
        ).fetchall()
        return _rows_to_dicts(rows)


def send_friend_request(RequesterUsername: str, TargetUsername: str) -> tuple[bool, str]:
    if RequesterUsername == TargetUsername:
        return False, "You cannot add yourself."

    with db_session() as conn:
        target = conn.execute(
            "SELECT Username FROM USER WHERE Username = ?",
            (TargetUsername,),
        ).fetchone()
        if not target:
            return False, "User not found."

        existing = conn.execute(
            """
            SELECT FriendshipID, RequesterUsername, TargetUsername, RequestStatus
            FROM FRIENDS
            WHERE (RequesterUsername = ? AND TargetUsername = ?)
               OR (RequesterUsername = ? AND TargetUsername = ?)
            """,
            (RequesterUsername, TargetUsername, TargetUsername, RequesterUsername),
        ).fetchone()

        if existing:
            Status = existing["RequestStatus"]
            if Status == "Accepted":
                return False, "You are already friends."
            if Status == "Pending":
                return False, "Friend request already pending."

        conn.execute(
            """
            INSERT OR REPLACE INTO FRIENDS (
                FriendshipID, RequesterUsername, TargetUsername, RequestStatus, FriendshipCreatedAt
            )
            VALUES (?, ?, ?, 'Pending', ?)
            """,
            (
                existing["FriendshipID"] if existing else None,
                RequesterUsername,
                TargetUsername,
                _now_str(),
            ),
        )

    return True, "Friend request sent."


def respond_friend_request(Username: str, FriendshipID: int, decision: str) -> tuple[bool, str]:
    Status = "Accepted" if decision == "accept" else "Rejected"

    with db_session() as conn:
        row = conn.execute(
            """
            SELECT FriendshipID
            FROM FRIENDS
            WHERE FriendshipID = ? AND TargetUsername = ? AND RequestStatus = 'Pending'
            """,
            (FriendshipID, Username),
        ).fetchone()
        if not row:
            return False, "Request not found or already handled."

        conn.execute(
            "UPDATE FRIENDS SET RequestStatus = ? WHERE FriendshipID = ?",
            (Status, FriendshipID),
        )

    return True, f"Friend request {Status.lower()}."


def get_friend_data(Username: str) -> dict[str, list[dict[str, Any]]]:
    with db_session() as conn:
        accepted_rows = conn.execute(
            """
            SELECT
                FriendshipID,
                CASE
                    WHEN RequesterUsername = ? THEN TargetUsername
                    ELSE RequesterUsername
                END AS FriendUsername
            FROM FRIENDS
            WHERE (RequesterUsername = ? OR TargetUsername = ?)
              AND RequestStatus = 'Accepted'
            ORDER BY FriendUsername
            """,
            (Username, Username, Username),
        ).fetchall()

        incoming_rows = conn.execute(
            """
            SELECT FriendshipID, RequesterUsername, FriendshipCreatedAt AS created_at
            FROM FRIENDS
            WHERE TargetUsername = ? AND RequestStatus = 'Pending'
            ORDER BY FriendshipCreatedAt DESC
            """,
            (Username,),
        ).fetchall()

        outgoing_rows = conn.execute(
            """
            SELECT FriendshipID, TargetUsername, FriendshipCreatedAt AS created_at
            FROM FRIENDS
            WHERE RequesterUsername = ? AND RequestStatus = 'Pending'
            ORDER BY FriendshipCreatedAt DESC
            """,
            (Username,),
        ).fetchall()

    return _normalize_record(
        {
            "Accepted": _rows_to_dicts(accepted_rows),
            "Incoming": _rows_to_dicts(incoming_rows),
            "Outgoing": _rows_to_dicts(outgoing_rows),
        }
    )


def create_friend_invite_link(username: str, ttl_days: int = 7, max_uses: int = 25) -> dict[str, Any]:
    now = _now()
    expires_at = (now + timedelta(days=max(1, ttl_days))).strftime(DATETIME_FORMAT)
    raw_token = secrets.token_urlsafe(24)
    token_hash = _hash_token(raw_token)

    with db_session() as conn:
        conn.execute(
            "DELETE FROM FRIEND_INVITE_LINKS WHERE is_active = 0 OR LinkExpiresAt < ?",
            (now.strftime(DATETIME_FORMAT),),
        )
        conn.execute(
            """
            INSERT INTO FRIEND_INVITE_LINKS (
                FriendInviteLinkKey, public_token, inviter_username, LinkCreatedAt, LinkExpiresAt, use_count, max_uses, is_active
            )
            VALUES (?, ?, ?, ?, ?, 0, ?, 1)
            """,
            (
                token_hash,
                raw_token,
                username,
                now.strftime(DATETIME_FORMAT),
                expires_at,
                max(1, int(max_uses)),
            ),
        )

    return _normalize_record(
        {
            "Token": raw_token,
            "ExpiresAt": expires_at,
            "MaxUses": max(1, int(max_uses)),
        }
    )


def list_friend_invite_links(Username: str, limit: int = 8) -> list[dict[str, Any]]:
    now = _now().strftime(DATETIME_FORMAT)
    with db_session() as conn:
        rows = conn.execute(
            """
            SELECT FriendInviteLinkKey AS token_hash, LinkCreatedAt AS created_at, LinkExpiresAt AS expires_at, use_count, max_uses, is_active
                 , public_token
            FROM FRIEND_INVITE_LINKS
            WHERE inviter_username = ?
              AND is_active = 1
              AND LinkExpiresAt >= ?
            ORDER BY LinkCreatedAt DESC
            LIMIT ?
            """,
            (Username, now, limit),
        ).fetchall()
        return _rows_to_dicts(rows)


def disable_friend_invite_link(Username: str, token_hash: str) -> tuple[bool, str]:
    with db_session() as conn:
        row = conn.execute(
            """
            SELECT FriendInviteLinkKey AS token_hash
            FROM FRIEND_INVITE_LINKS
            WHERE FriendInviteLinkKey = ?
              AND inviter_username = ?
            LIMIT 1
            """,
            (token_hash, Username),
        ).fetchone()
        if not row:
            return False, "Invite link not found."

        conn.execute(
            "UPDATE FRIEND_INVITE_LINKS SET is_active = 0 WHERE FriendInviteLinkKey = ?",
            (token_hash,),
        )
    return True, "Invite link disabled."


def accept_friend_invite_link(raw_token: str, Username: str) -> tuple[bool, str]:
    if not raw_token:
        return False, "Invite link token is missing."

    token_hash = _hash_token(raw_token)
    now = _now().strftime(DATETIME_FORMAT)

    with db_session() as conn:
        link = conn.execute(
            """
            SELECT FriendInviteLinkKey AS token_hash, inviter_username, LinkExpiresAt AS expires_at, use_count, max_uses, is_active
            FROM FRIEND_INVITE_LINKS
            WHERE FriendInviteLinkKey = ?
            LIMIT 1
            """,
            (token_hash,),
        ).fetchone()
        if not link:
            return False, "Invite link is invalid."
        if int(link["is_active"]) != 1:
            return False, "Invite link is inactive."
        if str(link["expires_at"]) < now:
            return False, "Invite link has expired."
        if int(link["use_count"]) >= int(link["max_uses"]):
            return False, "Invite link usage limit reached."

        inviter = str(link["inviter_username"])
        if inviter == Username:
            return False, "You cannot use your own invite link."

    ok, message = send_friend_request(Username, inviter)
    if not ok:
        return False, message

    with db_session() as conn:
        conn.execute(
            """
            UPDATE FRIEND_INVITE_LINKS
            SET use_count = use_count + 1,
                is_active = CASE WHEN use_count + 1 >= max_uses THEN 0 ELSE is_active END
            WHERE FriendInviteLinkKey = ?
            """,
            (token_hash,),
        )
    return True, f"Invite accepted. Friend request sent to {inviter}."


# -------------------------
# Co-op matchmaking
# -------------------------

def _are_friends(conn: sqlite3.Connection, user_a: str, user_b: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM FRIENDS
        WHERE RequestStatus = 'Accepted'
          AND (
                (RequesterUsername = ? AND TargetUsername = ?)
             OR (RequesterUsername = ? AND TargetUsername = ?)
          )
        LIMIT 1
        """,
        (user_a, user_b, user_b, user_a),
    ).fetchone()
    return bool(row)


def _has_active_coop_match(conn: sqlite3.Connection, user_a: str, user_b: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM COOP_MATCHES
        WHERE MatchStatus = 'Active'
          AND (
                (player_one = ? AND player_two = ?)
             OR (player_one = ? AND player_two = ?)
          )
        LIMIT 1
        """,
        (user_a, user_b, user_b, user_a),
    ).fetchone()
    return bool(row)


def list_coop_friends(Username: str) -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            """
            SELECT
                CASE
                    WHEN RequesterUsername = ? THEN TargetUsername
                    ELSE RequesterUsername
                END AS FriendUsername
            FROM FRIENDS
            WHERE RequestStatus = 'Accepted'
              AND (RequesterUsername = ? OR TargetUsername = ?)
            ORDER BY FriendUsername
            """,
            (Username, Username, Username),
        ).fetchall()
        return _rows_to_dicts(rows)


def create_coop_invite(from_username: str, to_username: str) -> tuple[bool, str]:
    if from_username == to_username:
        return False, "You cannot invite yourself."

    with db_session() as conn:
        target = conn.execute(
            "SELECT Username FROM USER WHERE Username = ?",
            (to_username,),
        ).fetchone()
        if not target:
            return False, "Friend account not found."

        if not _are_friends(conn, from_username, to_username):
            return False, "You can only invite accepted friends."

        if _has_active_coop_match(conn, from_username, to_username):
            return False, "You already have an active co-op match with this friend."

        existing_pending = conn.execute(
            """
            SELECT invite_id
            FROM COOP_INVITES
            WHERE InviteStatus = 'Pending'
              AND (
                    (from_username = ? AND to_username = ?)
                 OR (from_username = ? AND to_username = ?)
              )
            LIMIT 1
            """,
            (from_username, to_username, to_username, from_username),
        ).fetchone()
        if existing_pending:
            return False, "A pending co-op invite already exists."

        conn.execute(
            """
            INSERT INTO COOP_INVITES (from_username, to_username, InviteStatus, InviteCreatedAt)
            VALUES (?, ?, 'Pending', ?)
            """,
            (from_username, to_username, _now_str()),
        )

    return True, "Co-op invite sent."


def cancel_coop_invite(Username: str, invite_id: int) -> tuple[bool, str]:
    with db_session() as conn:
        invite = conn.execute(
            """
            SELECT invite_id
            FROM COOP_INVITES
            WHERE invite_id = ?
              AND from_username = ?
              AND InviteStatus = 'Pending'
            """,
            (invite_id, Username),
        ).fetchone()
        if not invite:
            return False, "Invite not found."

        conn.execute(
            """
            UPDATE COOP_INVITES
            SET InviteStatus = 'Cancelled', InviteRespondedAt = ?
            WHERE invite_id = ?
            """,
            (_now_str(), invite_id),
        )
    return True, "Invite cancelled."


def list_coop_invites(Username: str) -> dict[str, list[dict[str, Any]]]:
    with db_session() as conn:
        incoming_rows = conn.execute(
            """
            SELECT invite_id, from_username, InviteCreatedAt AS created_at
            FROM COOP_INVITES
            WHERE to_username = ?
              AND InviteStatus = 'Pending'
            ORDER BY InviteCreatedAt DESC
            """,
            (Username,),
        ).fetchall()

        outgoing_rows = conn.execute(
            """
            SELECT invite_id, to_username, InviteCreatedAt AS created_at
            FROM COOP_INVITES
            WHERE from_username = ?
              AND InviteStatus = 'Pending'
            ORDER BY InviteCreatedAt DESC
            """,
            (Username,),
        ).fetchall()

    return _normalize_record(
        {
            "Incoming": _rows_to_dicts(incoming_rows),
            "Outgoing": _rows_to_dicts(outgoing_rows),
        }
    )


def get_pending_coop_invite_for_user(Username: str, invite_id: int) -> dict[str, Any] | None:
    with db_session() as conn:
        row = conn.execute(
            """
            SELECT invite_id, from_username, to_username, InviteStatus AS Status, InviteCreatedAt AS created_at
            FROM COOP_INVITES
            WHERE invite_id = ?
              AND to_username = ?
              AND InviteStatus = 'Pending'
            LIMIT 1
            """,
            (invite_id, Username),
        ).fetchone()
        return _normalize_record(dict(row)) if row else None


def respond_coop_invite(
    username: str,
    invite_id: int,
    decision: str,
    initial_state_json: str | None = None,
    turn_username: str | None = None,
) -> tuple[bool, str, int | None]:
    with db_session() as conn:
        invite = conn.execute(
            """
            SELECT invite_id, from_username, to_username, InviteStatus AS Status
            FROM COOP_INVITES
            WHERE invite_id = ?
              AND to_username = ?
              AND InviteStatus = 'Pending'
            """,
            (invite_id, username),
        ).fetchone()
        if not invite:
            return False, "Invite not found.", None

        if decision != "accept":
            conn.execute(
                """
                UPDATE COOP_INVITES
                SET InviteStatus = 'Declined', InviteRespondedAt = ?
                WHERE invite_id = ?
                """,
                (_now_str(), invite_id),
            )
            return True, "Invite declined.", None

        from_username = str(invite["from_username"])
        to_username = str(invite["to_username"])

        if not _are_friends(conn, from_username, to_username):
            return False, "You are no longer accepted friends.", None

        if _has_active_coop_match(conn, from_username, to_username):
            return False, "There is already an active match with this friend.", None

        if not initial_state_json:
            return False, "Co-op state payload missing.", None

        current_time = _now_str()
        if not turn_username:
            turn_username = from_username

        conn.execute(
            """
            INSERT INTO COOP_MATCHES (
                player_one,
                player_two,
                turn_username,
                state_json,
                MatchStatus,
                MatchWinner,
                MatchCreatedAt,
                MatchUpdatedAt
            )
            VALUES (?, ?, ?, ?, 'Active', NULL, ?, ?)
            """,
            (
                from_username,
                to_username,
                turn_username,
                initial_state_json,
                current_time,
                current_time,
            ),
        )
        match_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

        conn.execute(
            """
            UPDATE COOP_INVITES
            SET InviteStatus = 'Accepted', InviteRespondedAt = ?
            WHERE invite_id = ?
            """,
            (current_time, invite_id),
        )

        conn.execute(
            """
            UPDATE COOP_INVITES
            SET InviteStatus = 'Cancelled', InviteRespondedAt = ?
            WHERE InviteStatus = 'Pending'
              AND (
                    (from_username = ? AND to_username = ?)
                 OR (from_username = ? AND to_username = ?)
              )
            """,
            (current_time, from_username, to_username, to_username, from_username),
        )

    return True, "Invite accepted.", match_id


def get_active_coop_match_for_user(Username: str) -> dict[str, Any] | None:
    with db_session() as conn:
        row = conn.execute(
            """
            SELECT
                match_id,
                player_one,
                player_two,
                turn_username,
                state_json,
                MatchStatus AS Status,
                MatchWinner AS Winner,
                MatchCreatedAt AS created_at,
                MatchUpdatedAt AS updated_at
            FROM COOP_MATCHES
            WHERE MatchStatus = 'Active'
              AND (player_one = ? OR player_two = ?)
            ORDER BY MatchUpdatedAt DESC, match_id DESC
            LIMIT 1
            """,
            (Username, Username),
        ).fetchone()
        return _normalize_record(dict(row)) if row else None


def get_coop_match_for_user(Username: str, match_id: int) -> dict[str, Any] | None:
    with db_session() as conn:
        row = conn.execute(
            """
            SELECT
                match_id,
                player_one,
                player_two,
                turn_username,
                state_json,
                MatchStatus AS Status,
                MatchWinner AS Winner,
                MatchCreatedAt AS created_at,
                MatchUpdatedAt AS updated_at
            FROM COOP_MATCHES
            WHERE match_id = ?
              AND (player_one = ? OR player_two = ?)
            LIMIT 1
            """,
            (match_id, Username, Username),
        ).fetchone()
        return _normalize_record(dict(row)) if row else None


def update_coop_match_state(
    match_id: int,
    state_json: str,
    turn_username: str | None,
    status: str = "Active",
    winner: str | None = None,
) -> None:
    with db_session() as conn:
        turn_value = turn_username or ""
        conn.execute(
            """
            UPDATE COOP_MATCHES
            SET state_json = ?,
                turn_username = ?,
                MatchStatus = ?,
                MatchWinner = ?,
                MatchUpdatedAt = ?
            WHERE match_id = ?
            """,
            (state_json, turn_value, status, winner, _now_str(), match_id),
        )


def abandon_coop_match(Username: str, match_id: int) -> tuple[bool, str]:
    with db_session() as conn:
        row = conn.execute(
            """
            SELECT match_id, player_one, player_two, MatchStatus AS Status
            FROM COOP_MATCHES
            WHERE match_id = ?
              AND (player_one = ? OR player_two = ?)
            LIMIT 1
            """,
            (match_id, Username, Username),
        ).fetchone()
        if not row:
            return False, "Match not found."
        if row["Status"] != "Active":
            return False, "Match is not active."

        player_one = str(row["player_one"])
        player_two = str(row["player_two"])
        Winner = player_two if Username == player_one else player_one

        conn.execute(
            """
            UPDATE COOP_MATCHES
            SET MatchStatus = 'Abandoned',
                MatchWinner = ?,
                MatchUpdatedAt = ?
            WHERE match_id = ?
            """,
            (Winner, _now_str(), match_id),
        )

    return True, "Match abandoned."


# -------------------------
# Game and progression
# -------------------------

def create_game_session(
    username: str,
    mode_name: str,
    xp_earned: int,
    winner: str | None,
) -> dict[str, Any]:
    start = _now_str()

    with db_session() as conn:
        mode = conn.execute(
            "SELECT ModeID FROM GAME_MODES WHERE ModeName = ?",
            (mode_name,),
        ).fetchone()
        if not mode:
            conn.execute("INSERT INTO GAME_MODES (ModeName) VALUES (?)", (mode_name,))
            mode = conn.execute(
                "SELECT ModeID FROM GAME_MODES WHERE ModeName = ?",
                (mode_name,),
            ).fetchone()

        conn.execute(
            """
            INSERT INTO GAME (ModeID, XPEarned, GameWinner, StartTime, EndTime)
            VALUES (?, ?, ?, ?, ?)
            """,
            (mode["ModeID"], xp_earned, winner, start, _now_str()),
        )
        SessionID = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

        if _table_exists(conn, "game_sessions"):
            conn.execute(
                """
                INSERT OR IGNORE INTO game_sessions (
                    session_id, mode_id, xp_earned, winner, start_time, end_time
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (SessionID, mode["ModeID"], xp_earned, winner, start, _now_str()),
            )

        conn.execute(
            "INSERT INTO GAME_PLAYERS (SessionID, Username) VALUES (?, ?)",
            (SessionID, username),
        )

    XP_state = award_xp(username, xp_earned)
    return _normalize_record({"SessionID": SessionID, **XP_state})


def get_progress_dataset(Username: str, days: int) -> dict[str, Any]:
    if days < 1:
        days = 1

    EndDate = _now().date()
    StartDate = EndDate - timedelta(days=days - 1)

    labels = []
    Calories_data = []
    hydration_data = []
    exercise_data = []

    with db_session() as conn:
        calorie_rows = conn.execute(
            """
            SELECT LogDate AS day, SUM(CalorieIntake) AS total
            FROM CALORIES
            WHERE Username = ? AND date(LogDate) BETWEEN date(?) AND date(?)
            GROUP BY LogDate
            """,
            (Username, StartDate.isoformat(), EndDate.isoformat()),
        ).fetchall()
        hydration_rows = conn.execute(
            """
            SELECT EntryDate AS day, SUM(HydrationIntake) AS total
            FROM HYDRATION
            WHERE Username = ? AND date(EntryDate) BETWEEN date(?) AND date(?)
            GROUP BY EntryDate
            """,
            (Username, StartDate.isoformat(), EndDate.isoformat()),
        ).fetchall()
        exercise_rows = conn.execute(
            """
            SELECT date(ActivityDate) AS day, SUM(DurationMinutes) AS total
            FROM ACTIVITIES
            WHERE Username = ? AND date(ActivityDate) BETWEEN date(?) AND date(?)
            GROUP BY date(ActivityDate)
            """,
            (Username, StartDate.isoformat(), EndDate.isoformat()),
        ).fetchall()

    calorie_map = {row["day"]: row["total"] for row in calorie_rows}
    hydration_map = {row["day"]: row["total"] for row in hydration_rows}
    exercise_map = {row["day"]: row["total"] for row in exercise_rows}

    current = StartDate
    while current <= EndDate:
        key = current.isoformat()
        labels.append(key)
        Calories_data.append(int(calorie_map.get(key, 0) or 0))
        hydration_data.append(round(float(hydration_map.get(key, 0.0) or 0.0), 2))
        exercise_data.append(int(exercise_map.get(key, 0) or 0))
        current += timedelta(days=1)

    return _normalize_record(
        {
            "Labels": labels,
            "Calories": Calories_data,
            "Hydration": hydration_data,
            "Exercise": exercise_data,
        }
    )


def get_home_summary(Username: str) -> dict[str, Any]:
    goals = list_goals(Username)
    activities = list_activities(Username, limit=5)
    profile = get_profile(Username) or {"XP": 0, "Level": 1, "AvatarName": "Starter Sprite"}

    active_goals = [g for g in goals if (g.get("Status") or g.get("status")) in ("Active", "On Track")]
    completed_goals = [g for g in goals if (g.get("Status") or g.get("status")) == "Completed"]

    today = _today_str()
    today_minutes = 0
    today_activities = 0
    for activity in activities:
        if str(activity["ActivityDate"]).startswith(today):
            today_minutes += int(activity["DurationMinutes"])
            today_activities += 1

    return _normalize_record(
        {
            "Profile": profile,
            "ActiveGoalCount": len(active_goals),
            "CompletedGoalCount": len(completed_goals),
            "TodayMinutes": today_minutes,
            "TodayActivities": today_activities,
            "RecentActivities": activities,
        }
    )
