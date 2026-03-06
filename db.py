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


def _compact_name(name: str) -> str:
    return "".join(ch for ch in str(name) if ch.isalnum()).lower()


def _snake_case(name: str) -> str:
    text = str(name).strip()
    if not text:
        return ""
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"[^A-Za-z0-9]+", "_", text)
    return text.strip("_").lower()


def _pascal_case(name: str) -> str:
    parts = [part for part in _snake_case(name).split("_") if part]
    if not parts:
        return ""
    acronyms = {"id", "xp"}
    return "".join(part.upper() if part in acronyms else part.capitalize() for part in parts)


def _quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _table_columns(conn: sqlite3.Connection, table_name: str) -> tuple[dict[str, str], dict[str, str]]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    exact = {}
    compact = {}
    for row in rows:
        name = str(row["name"])
        exact[name.lower()] = name
        compact[_compact_name(name)] = name
    return exact, compact


def _resolve_column_name(
    exact_columns: dict[str, str],
    compact_columns: dict[str, str],
    *candidates: str,
) -> str:
    for candidate in candidates:
        found = exact_columns.get(candidate.lower())
        if found:
            return found
    for candidate in candidates:
        found = compact_columns.get(_compact_name(candidate))
        if found:
            return found
    raise KeyError(f"Unable to resolve column name from candidates: {candidates}")


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND lower(name) = lower(?) LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _user_table_names(conn: sqlite3.Connection) -> list[str]:
    tables: list[str] = []
    seen: set[str] = set()
    for candidate in ("USER", "users"):
        if not _table_exists(conn, candidate):
            continue
        lowered = candidate.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        tables.append(candidate)
    return tables or ["USER"]


def _friend_link_columns(conn: sqlite3.Connection) -> dict[str, str]:
    exact_columns, compact_columns = _table_columns(conn, "FRIEND_INVITE_LINKS")
    return {
        "key": _resolve_column_name(
            exact_columns,
            compact_columns,
            "FriendInviteLinkKey",
        ),
        "public_token": _resolve_column_name(
            exact_columns,
            compact_columns,
            "PublicToken",
            "public_token",
        ),
        "inviter_username": _resolve_column_name(
            exact_columns,
            compact_columns,
            "InviterUsername",
            "inviter_username",
        ),
        "created_at": _resolve_column_name(
            exact_columns,
            compact_columns,
            "LinkCreatedAt",
            "link_created_at",
        ),
        "expires_at": _resolve_column_name(
            exact_columns,
            compact_columns,
            "LinkExpiresAt",
            "link_expires_at",
        ),
        "use_count": _resolve_column_name(
            exact_columns,
            compact_columns,
            "UseCount",
            "use_count",
        ),
        "max_uses": _resolve_column_name(
            exact_columns,
            compact_columns,
            "MaxUses",
            "max_uses",
        ),
        "is_active": _resolve_column_name(
            exact_columns,
            compact_columns,
            "IsActive",
            "is_active",
        ),
    }


def _coop_invite_columns(conn: sqlite3.Connection) -> dict[str, str]:
    exact_columns, compact_columns = _table_columns(conn, "COOP_INVITES")
    return {
        "id": _resolve_column_name(exact_columns, compact_columns, "InviteID", "invite_id"),
        "from_username": _resolve_column_name(
            exact_columns,
            compact_columns,
            "FromUsername",
            "from_username",
        ),
        "to_username": _resolve_column_name(
            exact_columns,
            compact_columns,
            "ToUsername",
            "to_username",
        ),
        "status": _resolve_column_name(
            exact_columns,
            compact_columns,
            "InviteStatus",
            "invite_status",
        ),
        "created_at": _resolve_column_name(
            exact_columns,
            compact_columns,
            "InviteCreatedAt",
            "invite_created_at",
        ),
        "responded_at": _resolve_column_name(
            exact_columns,
            compact_columns,
            "InviteRespondedAt",
            "invite_responded_at",
        ),
    }


def _coop_match_columns(conn: sqlite3.Connection) -> dict[str, str]:
    exact_columns, compact_columns = _table_columns(conn, "COOP_MATCHES")
    return {
        "id": _resolve_column_name(exact_columns, compact_columns, "MatchID", "match_id"),
        "player_one": _resolve_column_name(
            exact_columns,
            compact_columns,
            "PlayerOne",
            "player_one",
        ),
        "player_two": _resolve_column_name(
            exact_columns,
            compact_columns,
            "PlayerTwo",
            "player_two",
        ),
        "turn_username": _resolve_column_name(
            exact_columns,
            compact_columns,
            "TurnUsername",
            "turn_username",
        ),
        "state_json": _resolve_column_name(
            exact_columns,
            compact_columns,
            "StateJson",
            "state_json",
        ),
        "status": _resolve_column_name(
            exact_columns,
            compact_columns,
            "MatchStatus",
            "match_status",
        ),
        "winner": _resolve_column_name(
            exact_columns,
            compact_columns,
            "MatchWinner",
            "match_winner",
        ),
        "created_at": _resolve_column_name(
            exact_columns,
            compact_columns,
            "MatchCreatedAt",
            "match_created_at",
        ),
        "updated_at": _resolve_column_name(
            exact_columns,
            compact_columns,
            "MatchUpdatedAt",
            "match_updated_at",
        ),
    }


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

def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    aliases: dict[str, Any] = {}

    for key, value in normalized.items():
        snake_key = _snake_case(str(key))
        pascal_key = _pascal_case(str(key))

        if snake_key and snake_key not in normalized:
            aliases[snake_key] = value
        if pascal_key and pascal_key not in normalized:
            aliases[pascal_key] = value

    normalized.update(aliases)
    return normalized


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [_normalize_record(dict(row)) for row in rows]

# Initialization and seeding

def init_db() -> None:
    with db_session() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS USER (
                Username TEXT PRIMARY KEY,
                Email TEXT UNIQUE NOT NULL,
                PasswordHash TEXT NOT NULL,
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
                Difficulty TEXT NOT NULL DEFAULT 'Standard',
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
                InviteID INTEGER PRIMARY KEY AUTOINCREMENT,
                FromUsername TEXT NOT NULL,
                ToUsername TEXT NOT NULL,
                InviteStatus TEXT NOT NULL CHECK(InviteStatus IN ('Pending', 'Accepted', 'Declined', 'Cancelled')),
                InviteCreatedAt TEXT NOT NULL,
                InviteRespondedAt TEXT,
                FOREIGN KEY (FromUsername) REFERENCES USER(Username) ON DELETE CASCADE,
                FOREIGN KEY (ToUsername) REFERENCES USER(Username) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS COOP_MATCHES (
                MatchID INTEGER PRIMARY KEY AUTOINCREMENT,
                PlayerOne TEXT NOT NULL,
                PlayerTwo TEXT NOT NULL,
                TurnUsername TEXT NOT NULL,
                StateJson TEXT NOT NULL,
                MatchStatus TEXT NOT NULL CHECK(MatchStatus IN ('Active', 'Finished', 'Abandoned')),
                MatchWinner TEXT,
                MatchCreatedAt TEXT NOT NULL,
                MatchUpdatedAt TEXT NOT NULL,
                FOREIGN KEY (PlayerOne) REFERENCES USER(Username) ON DELETE CASCADE,
                FOREIGN KEY (PlayerTwo) REFERENCES USER(Username) ON DELETE CASCADE
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
                PublicToken TEXT NOT NULL,
                InviterUsername TEXT NOT NULL,
                LinkCreatedAt TEXT NOT NULL,
                LinkExpiresAt TEXT NOT NULL,
                UseCount INTEGER NOT NULL DEFAULT 0,
                MaxUses INTEGER NOT NULL DEFAULT 25,
                IsActive INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (InviterUsername) REFERENCES USER(Username) ON DELETE CASCADE
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
                HealthConditions TEXT,
                DietProfile TEXT,
                Climate TEXT,
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

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sso_tokens_Username ON SSO_TOKENS(Username)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_friend_links_inviter ON FRIEND_INVITE_LINKS(InviterUsername, LinkCreatedAt)"
        )
        # Keep additive migrations small and explicit for existing databases
        _ensure_column(conn, "ACTIVITIES", "Difficulty", "TEXT NOT NULL DEFAULT 'Standard'")
        _ensure_column(conn, "FRIEND_INVITE_LINKS", "PublicToken", "TEXT")
        _ensure_column(conn, "HEALTH", "HealthConditions", "TEXT")
        _ensure_column(conn, "HEALTH", "DietProfile", "TEXT")
        _ensure_column(conn, "HEALTH", "Climate", "TEXT")

        # Switched from PascalCase to snake_case without realizing so...
        # Legacy databases may have both public_token and PublicToken; keep the canonical field populated
        friend_link_columns = {str(column["name"]) for column in conn.execute("PRAGMA table_info(FRIEND_INVITE_LINKS)").fetchall()}
        if "PublicToken" in friend_link_columns and "public_token" in friend_link_columns:
            conn.execute(
                """
                UPDATE FRIEND_INVITE_LINKS
                SET PublicToken = COALESCE(PublicToken, public_token)
                """
            )

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

        goal_type_rows = [
            ("Calories", "kcal"),
            ("Hydration", "litres"),
            ("Exercise", "minutes"),
            ("Distance", "km"),
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO GOAL_TYPE (GoalTypeName, Unit) VALUES (?, ?)",
            goal_type_rows,
        )

        mode_rows = [("Solo",), ("Co-op",)]
        conn.executemany(
            "INSERT OR IGNORE INTO GAME_MODES (ModeName) VALUES (?)",
            mode_rows,
        )


# Logging

def log_action(username: str | None, action: str) -> None:
    ACTION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    actor = username or "ANONYMOUS"
    with ACTION_LOG_PATH.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{_now_str()} | {actor} | {action}\n")


def log_error(username: str | None, error_text: str) -> None:
    ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    actor = username or "ANONYMOUS"
    payload = error_text.strip().replace("\n", "\\n")
    with ERROR_LOG_PATH.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{_now_str()} | {actor} | {payload}\n")


# Users and authentication

def create_user(
    username: str,
    email: str,
    password: str,
    first_name: str | None = None,
    last_name: str | None = None,
    phone_num: str | None = None,
) -> None:
    password_hash = generate_password_hash(password, method="pbkdf2:sha256")
    date_joined = _today_str()

    with db_session() as conn:
        for user_table in _user_table_names(conn):
            exact_columns, compact_columns = _table_columns(conn, user_table)
            username_col = _resolve_column_name(exact_columns, compact_columns, "Username", "username")
            email_col = _resolve_column_name(exact_columns, compact_columns, "Email", "email")
            password_col = _resolve_column_name(
                exact_columns,
                compact_columns,
                "PasswordHash",
                "password_hash",
            )
            first_name_col = _resolve_column_name(
                exact_columns,
                compact_columns,
                "FirstName",
                "first_name",
            )
            last_name_col = _resolve_column_name(
                exact_columns,
                compact_columns,
                "LastName",
                "last_name",
            )
            phone_col = _resolve_column_name(exact_columns, compact_columns, "PhoneNum", "phone_num")
            joined_col = _resolve_column_name(exact_columns, compact_columns, "DateJoined", "date_joined")

            conn.execute(
                f"""
                INSERT INTO {_quote_identifier(user_table)} (
                    {_quote_identifier(username_col)},
                    {_quote_identifier(email_col)},
                    {_quote_identifier(password_col)},
                    {_quote_identifier(first_name_col)},
                    {_quote_identifier(last_name_col)},
                    {_quote_identifier(phone_col)},
                    {_quote_identifier(joined_col)}
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (username, email, password_hash, first_name, last_name, phone_num, date_joined),
            )

        starter_avatar_row = conn.execute(
            "SELECT AvatarID FROM AVATAR ORDER BY UnlockLevel ASC, AvatarID ASC LIMIT 1"
        ).fetchone()
        starter_avatar_id = starter_avatar_row["AvatarID"] if starter_avatar_row else None

        conn.execute(
            "INSERT INTO PROFILE (Username, AvatarID, Level, XP) VALUES (?, ?, 1, 0)",
            (username, starter_avatar_id),
        )
        conn.execute("INSERT INTO HEALTH (Username) VALUES (?)", (username,))


def get_user_by_identity(identity: str) -> dict[str, Any] | None:
    lookup = identity.strip().lower()
    if not lookup:
        return None

    with db_session() as conn:
        for user_table in _user_table_names(conn):
            exact_columns, compact_columns = _table_columns(conn, user_table)
            username_col = _resolve_column_name(exact_columns, compact_columns, "Username", "username")
            email_col = _resolve_column_name(exact_columns, compact_columns, "Email", "email")

            by_username = conn.execute(
                f"""
                SELECT * FROM {_quote_identifier(user_table)}
                WHERE lower({_quote_identifier(username_col)}) = ?
                """,
                (lookup,),
            ).fetchone()
            if by_username:
                return _normalize_record(dict(by_username))

            by_email = conn.execute(
                f"""
                SELECT * FROM {_quote_identifier(user_table)}
                WHERE lower({_quote_identifier(email_col)}) = ?
                """,
                (lookup,),
            ).fetchone()
            if by_email:
                return _normalize_record(dict(by_email))
        return None


def authenticate_user(identity: str, password: str) -> tuple[dict[str, Any] | None, str]:
    lookup = identity.strip().lower()
    if not lookup:
        return None, "Username or Email is required."

    with db_session() as conn:
        found: tuple[sqlite3.Row, str, str] | None = None
        for user_table in _user_table_names(conn):
            exact_columns, compact_columns = _table_columns(conn, user_table)
            username_col = _resolve_column_name(exact_columns, compact_columns, "Username", "username")
            email_col = _resolve_column_name(exact_columns, compact_columns, "Email", "email")
            failed_login_col = _resolve_column_name(
                exact_columns,
                compact_columns,
                "FailedLoginAttempts",
                "failed_login_attempts",
            )
            locked_until_col = _resolve_column_name(
                exact_columns,
                compact_columns,
                "LockedUntil",
                "locked_until",
            )

            user_row = conn.execute(
                f"""
                SELECT * FROM {_quote_identifier(user_table)}
                WHERE lower({_quote_identifier(username_col)}) = ?
                """,
                (lookup,),
            ).fetchone()
            if not user_row:
                user_row = conn.execute(
                    f"""
                    SELECT * FROM {_quote_identifier(user_table)}
                    WHERE lower({_quote_identifier(email_col)}) = ?
                    """,
                    (lookup,),
                ).fetchone()
            if user_row:
                found = (user_row, user_table, username_col)
                break

        if not found:
            return None, "Account not found."

        user_row, user_table, username_col = found
        exact_columns, compact_columns = _table_columns(conn, user_table)
        failed_login_col = _resolve_column_name(
            exact_columns,
            compact_columns,
            "FailedLoginAttempts",
            "failed_login_attempts",
        )
        locked_until_col = _resolve_column_name(
            exact_columns,
            compact_columns,
            "LockedUntil",
            "locked_until",
        )

        user = _normalize_record(dict(user_row))
        username_value = str(user.get("Username") or "").strip()
        locked_until = _parse_datetime(str(user.get("LockedUntil") or ""))
        now = _now()
        if locked_until and now < locked_until:
            return None, f"Account locked until {locked_until.strftime(DATETIME_FORMAT)}."

        password_hash = str(user.get("PasswordHash") or "")
        if password_hash and check_password_hash(password_hash, password):
            conn.execute(
                f"""
                UPDATE {_quote_identifier(user_table)}
                SET {_quote_identifier(failed_login_col)} = 0,
                    {_quote_identifier(locked_until_col)} = NULL
                WHERE {_quote_identifier(username_col)} = ?
                """,
                (username_value,),
            )
            return user, ""

        attempts = int(user.get("FailedLoginAttempts") or 0) + 1
        if attempts >= 5:
            lock_time = now + timedelta(minutes=15)
            conn.execute(
                f"""
                UPDATE {_quote_identifier(user_table)}
                SET {_quote_identifier(failed_login_col)} = ?,
                    {_quote_identifier(locked_until_col)} = ?
                WHERE {_quote_identifier(username_col)} = ?
                """,
                (attempts, lock_time.strftime(DATETIME_FORMAT), username_value),
            )
            return None, f"Too many failed attempts. Account locked until {lock_time.strftime(DATETIME_FORMAT)}."

        conn.execute(
            f"""
            UPDATE {_quote_identifier(user_table)}
            SET {_quote_identifier(failed_login_col)} = ?
            WHERE {_quote_identifier(username_col)} = ?
            """,
            (attempts, username_value),
        )
        remaining = 5 - attempts
        return None, f"Invalid credentials. {remaining} attempt(s) remaining before lockout."


def get_user(username: str) -> dict[str, Any] | None:
    with db_session() as conn:
        for user_table in _user_table_names(conn):
            exact_columns, compact_columns = _table_columns(conn, user_table)
            username_col = _resolve_column_name(exact_columns, compact_columns, "Username", "username")
            row = conn.execute(
                f"""
                SELECT * FROM {_quote_identifier(user_table)}
                WHERE {_quote_identifier(username_col)} = ?
                """,
                (username,),
            ).fetchone()
            if row:
                return _normalize_record(dict(row))
        return None


def update_personal_info(
    username: str,
    email: str,
    first_name: str | None,
    last_name: str | None,
    phone_num: str | None,
) -> None:
    with db_session() as conn:
        conn.execute(
            """
            UPDATE USER
            SET Email = ?, FirstName = ?, LastName = ?, PhoneNum = ?
            WHERE Username = ?
            """,
            (email, first_name, last_name, phone_num, username),
        )


def create_sso_token(identity: str, ttl_minutes: int = 10) -> tuple[bool, str, str | None]:
    user = get_user_by_identity(identity)
    if not user:
        return False, "Account not found.", None
    username = str(user.get("Username") or "").strip()
    if not username:
        return False, "Account not found.", None

    raw_token = secrets.token_urlsafe(32)
    token_hash = _hash_token(raw_token)
    now = _now()
    expires_at = (now + timedelta(minutes=max(1, ttl_minutes))).strftime(DATETIME_FORMAT)

    with db_session() as conn:
        # Remove stale and previously-used tokens before issuing a new one.
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
            SELECT SsoTokenKey, Username, SsoExpiresAt AS ExpiresAt, SsoUsedAt AS UsedAt
            FROM SSO_TOKENS
            WHERE SsoTokenKey = ?
            LIMIT 1
            """,
            (token_hash,),
        ).fetchone()

        if not row:
            return None, "SSO token is invalid."
        if row["UsedAt"]:
            return None, "SSO token has already been used."
        if str(row["ExpiresAt"]) < now:
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

# Profile, XP, avatars

def get_profile(username: str) -> dict[str, Any] | None:
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
            (username,),
        ).fetchone()
        return _normalize_record(dict(row)) if row else None


def award_xp(username: str, xp_delta: int) -> dict[str, Any]:
    with db_session() as conn:
        row = conn.execute(
            "SELECT XP, Level FROM PROFILE WHERE Username = ?",
            (username,),
        ).fetchone()
        if not row:
            return _normalize_record({"XP": 0, "Level": 1, "LeveledUp": False})

        previous_level = int(row["Level"])
        new_xp = int(row["XP"]) + max(0, int(xp_delta))
        new_level = max(1, (new_xp // 100) + 1)

        conn.execute(
            "UPDATE PROFILE SET XP = ?, Level = ? WHERE Username = ?",
            (new_xp, new_level, username),
        )

        return _normalize_record(
            {
                "XP": new_xp,
                "Level": new_level,
                "LeveledUp": new_level > previous_level,
            }
        )

def list_avatars() -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            "SELECT * FROM AVATAR ORDER BY UnlockLevel, AvatarID"
        ).fetchall()
        return _rows_to_dicts(rows)

def set_avatar(username: str, avatar_id: int) -> tuple[bool, str]:
    with db_session() as conn:
        avatar = conn.execute(
            "SELECT AvatarName, UnlockLevel FROM AVATAR WHERE AvatarID = ?",
            (avatar_id,),
        ).fetchone()
        if not avatar:
            return False, "Avatar not found."

        profile = conn.execute(
            "SELECT Level FROM PROFILE WHERE Username = ?",
            (username,),
        ).fetchone()
        if not profile:
            return False, "Profile not found."

        if int(profile["Level"]) < int(avatar["UnlockLevel"]):
            return False, f"{avatar['AvatarName']} unlocks at Level {avatar['UnlockLevel']}."

        conn.execute(
            "UPDATE PROFILE SET AvatarID = ? WHERE Username = ?",
            (avatar_id, username),
        )
        return True, f"Avatar changed to {avatar['AvatarName']}."

# Goals and activities

def list_goal_types() -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute("SELECT * FROM GOAL_TYPE ORDER BY GoalTypeName").fetchall()
        return _rows_to_dicts(rows)


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


def list_goals(username: str) -> list[dict[str, Any]]:
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
                g.GoalStatus,
                gt.GoalTypeName,
                gt.Unit
            FROM GOALS g
            JOIN GOAL_TYPE gt ON gt.GoalTypeID = g.GoalTypeID
            WHERE g.Username = ?
            ORDER BY g.GoalID DESC
            """,
            (username,),
        ).fetchall()
        return _rows_to_dicts(rows)


def update_goal_status(username: str, goal_id: int, status: str) -> None:
    with db_session() as conn:
        conn.execute(
            "UPDATE GOALS SET GoalStatus = ? WHERE GoalID = ? AND Username = ?",
            (status, goal_id, username),
        )


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
    logged_at = activity_date or _now_str()
    difficulty_value = str(difficulty or "Standard").strip().title()
    if difficulty_value not in WORKOUT_DIFFICULTIES:
        difficulty_value = "Standard"

    with db_session() as conn:
        conn.execute(
            """
            INSERT INTO ACTIVITIES (
                Username, Type, DurationMinutes, CaloriesBurnt, DistanceKm, ActivityDate, Source, Difficulty
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                username,
                activity_type,
                duration_minutes,
                calories_burnt,
                distance_km,
                logged_at,
                source,
                difficulty_value,
            ),
        )


def list_activities(username: str, limit: int = 100) -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM ACTIVITIES
            WHERE Username = ?
            ORDER BY ActivityDate DESC, ActivityID DESC
            LIMIT ?
            """,
            (username, limit),
        ).fetchall()
        return _rows_to_dicts(rows)


def workout_xp_value(duration_minutes: int, difficulty: str) -> int:
    duration = max(1, int(duration_minutes))
    difficulty_value = str(difficulty or "Standard").strip().title()
    multiplier = {"Easy": 0.9, "Standard": 1.0, "Hard": 1.2}.get(difficulty_value, 1.0)
    return max(10, int(round(duration * multiplier)))

# Calories and hydration

def add_calorie_log(username: str, calorie_intake: int, log_date: str | None) -> None:
    entry_date = log_date or _today_str()
    with db_session() as conn:
        conn.execute(
            "INSERT INTO CALORIES (Username, CalorieIntake, LogDate) VALUES (?, ?, ?)",
            (username, calorie_intake, entry_date),
        )


def list_calorie_logs(username: str, limit: int = 90) -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            "SELECT * FROM CALORIES WHERE Username = ? ORDER BY LogDate DESC, LogID DESC LIMIT ?",
            (username, limit),
        ).fetchall()
        return _rows_to_dicts(rows)


def add_hydration_log(username: str, hydration_intake: float, entry_date: str | None) -> None:
    date_value = entry_date or _today_str()
    with db_session() as conn:
        conn.execute(
            "INSERT INTO HYDRATION (Username, HydrationIntake, EntryDate) VALUES (?, ?, ?)",
            (username, hydration_intake, date_value),
        )


def list_hydration_logs(username: str, limit: int = 90) -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            "SELECT * FROM HYDRATION WHERE Username = ? ORDER BY EntryDate DESC, EntryID DESC LIMIT ?",
            (username, limit),
        ).fetchall()
        return _rows_to_dicts(rows)

# Health and recommendations

def get_health(username: str) -> dict[str, Any] | None:
    with db_session() as conn:
        row = conn.execute(
            "SELECT * FROM HEALTH WHERE Username = ?",
            (username,),
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
                HealthConditions,
                DietProfile,
                Climate,
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
                HealthConditions = excluded.HealthConditions,
                DietProfile = excluded.DietProfile,
                Climate = excluded.Climate,
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


def update_mood(username: str, mood: str) -> None:
    current = get_health(username) or {}
    update_health(
        username=username,
        age=current.get("Age"),
        sex=current.get("Sex"),
        weight_kg=current.get("WeightKg"),
        height_cm=current.get("HeightCm"),
        activity_level=current.get("ActivityLevel"),
        overall_health=current.get("OverallHealth"),
        health_conditions=current.get("HealthConditions"),
        diet_profile=current.get("DietProfile"),
        climate=current.get("Climate"),
        mood=mood,
    )


def calorie_recommendation(username: str) -> dict[str, Any]:
    health = get_health(username) or {}
    age = health.get("Age")
    sex = (health.get("Sex") or "").strip().lower()
    weight = health.get("WeightKg")
    height = health.get("HeightCm")
    activity_level = (health.get("ActivityLevel") or "moderate").strip().lower()
    overall_health = (health.get("OverallHealth") or "").strip().lower()

    if not all([age, weight, height]):
        return _normalize_record(
            {
                "Recommended": 2000,
                "Basis": (
                    "General baseline used. Add Age, Sex, height, weight, activity Level, and health notes "
                    "for personalized guidance."
                ),
            }
        )

    if sex.startswith("m"):
        bmr = (10 * weight) + (6.25 * height) - (5 * age) + 5
    elif sex.startswith("f"):
        bmr = (10 * weight) + (6.25 * height) - (5 * age) - 161
    else:
        bmr = (10 * weight) + (6.25 * height) - (5 * age) - 78

    multipliers = {
        "sedentary": 1.2,
        "light": 1.375,
        "moderate": 1.55,
        "active": 1.725,
        "very active": 1.9,
    }

    multiplier = multipliers.get(activity_level, 1.55)
    recommended = int(round(bmr * multiplier))

    health_adjustment = 0
    adjustment_note = "No additional health-based adjustment applied."
    if any(term in overall_health for term in ("weight loss", "lose weight", "fat loss", "obesity")):
        health_adjustment = -250
        adjustment_note = "Adjusted for a possible weight-loss focus in health notes."
    elif any(term in overall_health for term in ("muscle gain", "underweight", "bulking", "gain weight")):
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


def hydration_recommendation(username: str) -> dict[str, Any]:
    health = get_health(username) or {}
    age = health.get("Age")
    sex = (health.get("Sex") or "").strip().lower()
    weight = health.get("WeightKg")
    activity_level = (health.get("ActivityLevel") or "moderate").strip().lower()
    climate = (health.get("Climate") or "temperate").strip().lower()

    if weight:
        baseline = weight * 0.033
    else:
        baseline = 2.6
        if sex.startswith("f"):
            baseline = 2.2
        elif sex.startswith("m"):
            baseline = 3.0

    if age and int(age) >= 55:
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
            baseline + activity_extras.get(activity_level, 0.5) + climate_extras.get(climate, 0.3),
        ),
        2,
    )
    return _normalize_record(
        {
            "Recommended": recommended,
            "Basis": "General hydration estimate based on Age, Sex, activity Level, body weight, and climate.",
        }
    )


def personalized_health_tips(username: str) -> list[str]:
    health = get_health(username) or {}
    goals = list_goals(username)
    activities = list_activities(username, limit=14)
    tips: list[str] = []

    activity_level = str(health.get("ActivityLevel") or "").strip().lower()
    if activity_level in {"", "sedentary"}:
        tips.append("Start with short daily movement blocks (10-15 min) and increase gradually.")
    elif activity_level in {"active", "very active"}:
        tips.append("Schedule at least one recovery-focused session each week to support consistency.")

    climate = str(health.get("Climate") or "").strip().lower()
    if climate in {"hot", "humid", "dry"}:
        tips.append("Hydrate before and after sessions; hotter or drier climates usually require extra fluid.")

    conditions = str(health.get("HealthConditions") or "").strip()
    if conditions:
        tips.append("Use low-impact alternatives when needed and pace intensity around your listed conditions.")

    diet_profile = str(health.get("DietProfile") or "").strip().lower()
    if "high protein" in diet_profile:
        tips.append("Spread protein intake across meals to support muscular recovery.")
    elif diet_profile:
        tips.append("Keep meals consistent with your diet preferences and prioritize minimally processed foods.")

    active_goal_types = {
        str(goal.get("GoalTypeName") or "").lower()
        for goal in goals
        if goal.get("GoalStatus") != "Cancelled"
    }
    if "hydration" in active_goal_types:
        tips.append("Pair each meal with water to make hydration goals easier to sustain.")
    if "exercise" in active_goal_types:
        tips.append("Use progressive overload: small weekly increases in duration or intensity are more sustainable.")
    if "calories" in active_goal_types:
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

# Friends

def search_users(search_term: str, current_username: str) -> list[dict[str, Any]]:
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
            (current_username, query),
        ).fetchall()
        return _rows_to_dicts(rows)


def send_friend_request(requester_username: str, target_username: str) -> tuple[bool, str]:
    if requester_username == target_username:
        return False, "You cannot add yourself."

    with db_session() as conn:
        target = conn.execute(
            "SELECT Username FROM USER WHERE Username = ?",
            (target_username,),
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
            (requester_username, target_username, target_username, requester_username),
        ).fetchone()

        if existing:
            request_status = existing["RequestStatus"]
            if request_status == "Accepted":
                return False, "You are already friends."
            if request_status == "Pending":
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
                requester_username,
                target_username,
                _now_str(),
            ),
        )

    return True, "Friend request sent."


def respond_friend_request(username: str, friendship_id: int, decision: str) -> tuple[bool, str]:
    status = "Accepted" if decision == "accept" else "Rejected"

    with db_session() as conn:
        row = conn.execute(
            """
            SELECT FriendshipID
            FROM FRIENDS
            WHERE FriendshipID = ? AND TargetUsername = ? AND RequestStatus = 'Pending'
            """,
            (friendship_id, username),
        ).fetchone()
        if not row:
            return False, "Request not found or already handled."

        conn.execute(
            "UPDATE FRIENDS SET RequestStatus = ? WHERE FriendshipID = ?",
            (status, friendship_id),
        )

    return True, f"Friend request {status.lower()}."


def get_friend_data(username: str) -> dict[str, list[dict[str, Any]]]:
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
            (username, username, username),
        ).fetchall()

        incoming_rows = conn.execute(
            """
            SELECT FriendshipID, RequesterUsername, FriendshipCreatedAt
            FROM FRIENDS
            WHERE TargetUsername = ? AND RequestStatus = 'Pending'
            ORDER BY FriendshipCreatedAt DESC
            """,
            (username,),
        ).fetchall()

        outgoing_rows = conn.execute(
            """
            SELECT FriendshipID, TargetUsername, FriendshipCreatedAt
            FROM FRIENDS
            WHERE RequesterUsername = ? AND RequestStatus = 'Pending'
            ORDER BY FriendshipCreatedAt DESC
            """,
            (username,),
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
        columns = _friend_link_columns(conn)
        conn.execute(
            f"""
            DELETE FROM FRIEND_INVITE_LINKS
            WHERE {_quote_identifier(columns["is_active"])} = 0
               OR {_quote_identifier(columns["expires_at"])} < ?
            """,
            (now.strftime(DATETIME_FORMAT),),
        )
        conn.execute(
            f"""
            INSERT INTO FRIEND_INVITE_LINKS (
                {_quote_identifier(columns["key"])},
                {_quote_identifier(columns["public_token"])},
                {_quote_identifier(columns["inviter_username"])},
                {_quote_identifier(columns["created_at"])},
                {_quote_identifier(columns["expires_at"])},
                {_quote_identifier(columns["use_count"])},
                {_quote_identifier(columns["max_uses"])},
                {_quote_identifier(columns["is_active"])}
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


def list_friend_invite_links(username: str, limit: int = 8) -> list[dict[str, Any]]:
    now = _now().strftime(DATETIME_FORMAT)
    with db_session() as conn:
        columns = _friend_link_columns(conn)
        rows = conn.execute(
            f"""
            SELECT
                {_quote_identifier(columns["key"])},
                {_quote_identifier(columns["created_at"])},
                {_quote_identifier(columns["expires_at"])},
                {_quote_identifier(columns["use_count"])},
                {_quote_identifier(columns["max_uses"])},
                {_quote_identifier(columns["is_active"])},
                {_quote_identifier(columns["public_token"])}
            FROM FRIEND_INVITE_LINKS
            WHERE {_quote_identifier(columns["inviter_username"])} = ?
              AND {_quote_identifier(columns["is_active"])} = 1
              AND {_quote_identifier(columns["expires_at"])} >= ?
            ORDER BY {_quote_identifier(columns["created_at"])} DESC
            LIMIT ?
            """,
            (username, now, limit),
        ).fetchall()
        return _rows_to_dicts(rows)


def disable_friend_invite_link(username: str, token_hash: str) -> tuple[bool, str]:
    with db_session() as conn:
        columns = _friend_link_columns(conn)
        row = conn.execute(
            f"""
            SELECT {_quote_identifier(columns["key"])}
            FROM FRIEND_INVITE_LINKS
            WHERE {_quote_identifier(columns["key"])} = ?
              AND {_quote_identifier(columns["inviter_username"])} = ?
            LIMIT 1
            """,
            (token_hash, username),
        ).fetchone()
        if not row:
            return False, "Invite link not found."

        conn.execute(
            f"""
            UPDATE FRIEND_INVITE_LINKS
            SET {_quote_identifier(columns["is_active"])} = 0
            WHERE {_quote_identifier(columns["key"])} = ?
            """,
            (token_hash,),
        )
    return True, "Invite link disabled."


def accept_friend_invite_link(raw_token: str, username: str) -> tuple[bool, str]:
    if not raw_token:
        return False, "Invite link token is missing."

    token_hash = _hash_token(raw_token)
    now = _now().strftime(DATETIME_FORMAT)

    with db_session() as conn:
        columns = _friend_link_columns(conn)
        link = conn.execute(
            f"""
            SELECT
                {_quote_identifier(columns["key"])},
                {_quote_identifier(columns["inviter_username"])},
                {_quote_identifier(columns["expires_at"])},
                {_quote_identifier(columns["use_count"])},
                {_quote_identifier(columns["max_uses"])},
                {_quote_identifier(columns["is_active"])}
            FROM FRIEND_INVITE_LINKS
            WHERE {_quote_identifier(columns["key"])} = ?
            LIMIT 1
            """,
            (token_hash,),
        ).fetchone()
        if not link:
            return False, "Invite link is invalid."
        link_data = _normalize_record(dict(link))
        if int(link_data["IsActive"]) != 1:
            return False, "Invite link is inactive."
        if str(link_data["LinkExpiresAt"]) < now:
            return False, "Invite link has expired."
        if int(link_data["UseCount"]) >= int(link_data["MaxUses"]):
            return False, "Invite link usage limit reached."

        inviter = str(link_data["InviterUsername"])
        if inviter == username:
            return False, "You cannot use your own invite link."

    ok, message = send_friend_request(username, inviter)
    if not ok:
        return False, message

    with db_session() as conn:
        columns = _friend_link_columns(conn)
        conn.execute(
            f"""
            UPDATE FRIEND_INVITE_LINKS
            SET {_quote_identifier(columns["use_count"])} = {_quote_identifier(columns["use_count"])} + 1,
                {_quote_identifier(columns["is_active"])} = CASE
                    WHEN {_quote_identifier(columns["use_count"])} + 1 >= {_quote_identifier(columns["max_uses"])}
                    THEN 0
                    ELSE {_quote_identifier(columns["is_active"])}
                END
            WHERE {_quote_identifier(columns["key"])} = ?
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
    columns = _coop_match_columns(conn)
    row = conn.execute(
        f"""
        SELECT 1
        FROM COOP_MATCHES
        WHERE {_quote_identifier(columns["status"])} = 'Active'
          AND (
                ({_quote_identifier(columns["player_one"])} = ? AND {_quote_identifier(columns["player_two"])} = ?)
             OR ({_quote_identifier(columns["player_one"])} = ? AND {_quote_identifier(columns["player_two"])} = ?)
          )
        LIMIT 1
        """,
        (user_a, user_b, user_b, user_a),
    ).fetchone()
    return bool(row)


def list_coop_friends(username: str) -> list[dict[str, Any]]:
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
            (username, username, username),
        ).fetchall()
        return _rows_to_dicts(rows)


def create_coop_invite(from_username: str, to_username: str) -> tuple[bool, str]:
    if from_username == to_username:
        return False, "You cannot invite yourself."

    target = get_user(to_username)
    if not target:
        return False, "Friend account not found."

    with db_session() as conn:
        invite_columns = _coop_invite_columns(conn)

        if not _are_friends(conn, from_username, to_username):
            return False, "You can only invite accepted friends."

        if _has_active_coop_match(conn, from_username, to_username):
            return False, "You already have an active co-op match with this friend."

        existing_pending = conn.execute(
            f"""
            SELECT {_quote_identifier(invite_columns["id"])}
            FROM COOP_INVITES
            WHERE {_quote_identifier(invite_columns["status"])} = 'Pending'
              AND (
                    ({_quote_identifier(invite_columns["from_username"])} = ? AND {_quote_identifier(invite_columns["to_username"])} = ?)
                 OR ({_quote_identifier(invite_columns["from_username"])} = ? AND {_quote_identifier(invite_columns["to_username"])} = ?)
              )
            LIMIT 1
            """,
            (from_username, to_username, to_username, from_username),
        ).fetchone()
        if existing_pending:
            return False, "A pending co-op invite already exists."

        conn.execute(
            f"""
            INSERT INTO COOP_INVITES (
                {_quote_identifier(invite_columns["from_username"])},
                {_quote_identifier(invite_columns["to_username"])},
                {_quote_identifier(invite_columns["status"])},
                {_quote_identifier(invite_columns["created_at"])}
            )
            VALUES (?, ?, 'Pending', ?)
            """,
            (from_username, to_username, _now_str()),
        )

    return True, "Co-op invite sent."


def cancel_coop_invite(username: str, invite_id: int) -> tuple[bool, str]:
    with db_session() as conn:
        columns = _coop_invite_columns(conn)
        invite = conn.execute(
            f"""
            SELECT {_quote_identifier(columns["id"])}
            FROM COOP_INVITES
            WHERE {_quote_identifier(columns["id"])} = ?
              AND {_quote_identifier(columns["from_username"])} = ?
              AND {_quote_identifier(columns["status"])} = 'Pending'
            """,
            (invite_id, username),
        ).fetchone()
        if not invite:
            return False, "Invite not found."

        conn.execute(
            f"""
            UPDATE COOP_INVITES
            SET {_quote_identifier(columns["status"])} = 'Cancelled',
                {_quote_identifier(columns["responded_at"])} = ?
            WHERE {_quote_identifier(columns["id"])} = ?
            """,
            (_now_str(), invite_id),
        )
    return True, "Invite cancelled."


def list_coop_invites(username: str) -> dict[str, list[dict[str, Any]]]:
    with db_session() as conn:
        columns = _coop_invite_columns(conn)
        incoming_rows = conn.execute(
            f"""
            SELECT
                {_quote_identifier(columns["id"])},
                {_quote_identifier(columns["from_username"])},
                {_quote_identifier(columns["created_at"])}
            FROM COOP_INVITES
            WHERE {_quote_identifier(columns["to_username"])} = ?
              AND {_quote_identifier(columns["status"])} = 'Pending'
            ORDER BY {_quote_identifier(columns["created_at"])} DESC
            """,
            (username,),
        ).fetchall()

        outgoing_rows = conn.execute(
            f"""
            SELECT
                {_quote_identifier(columns["id"])},
                {_quote_identifier(columns["to_username"])},
                {_quote_identifier(columns["created_at"])}
            FROM COOP_INVITES
            WHERE {_quote_identifier(columns["from_username"])} = ?
              AND {_quote_identifier(columns["status"])} = 'Pending'
            ORDER BY {_quote_identifier(columns["created_at"])} DESC
            """,
            (username,),
        ).fetchall()

    return _normalize_record(
        {
            "Incoming": _rows_to_dicts(incoming_rows),
            "Outgoing": _rows_to_dicts(outgoing_rows),
        }
    )


def get_pending_coop_invite_for_user(username: str, invite_id: int) -> dict[str, Any] | None:
    with db_session() as conn:
        columns = _coop_invite_columns(conn)
        row = conn.execute(
            f"""
            SELECT
                {_quote_identifier(columns["id"])},
                {_quote_identifier(columns["from_username"])},
                {_quote_identifier(columns["to_username"])},
                {_quote_identifier(columns["status"])},
                {_quote_identifier(columns["created_at"])}
            FROM COOP_INVITES
            WHERE {_quote_identifier(columns["id"])} = ?
              AND {_quote_identifier(columns["to_username"])} = ?
              AND {_quote_identifier(columns["status"])} = 'Pending'
            LIMIT 1
            """,
            (invite_id, username),
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
        invite_columns = _coop_invite_columns(conn)
        match_columns = _coop_match_columns(conn)
        invite = conn.execute(
            f"""
            SELECT
                {_quote_identifier(invite_columns["id"])},
                {_quote_identifier(invite_columns["from_username"])},
                {_quote_identifier(invite_columns["to_username"])},
                {_quote_identifier(invite_columns["status"])}
            FROM COOP_INVITES
            WHERE {_quote_identifier(invite_columns["id"])} = ?
              AND {_quote_identifier(invite_columns["to_username"])} = ?
              AND {_quote_identifier(invite_columns["status"])} = 'Pending'
            """,
            (invite_id, username),
        ).fetchone()
        if not invite:
            return False, "Invite not found.", None

        invite_data = _normalize_record(dict(invite))
        if decision != "accept":
            conn.execute(
                f"""
                UPDATE COOP_INVITES
                SET {_quote_identifier(invite_columns["status"])} = 'Declined',
                    {_quote_identifier(invite_columns["responded_at"])} = ?
                WHERE {_quote_identifier(invite_columns["id"])} = ?
                """,
                (_now_str(), invite_id),
            )
            return True, "Invite declined.", None

        from_username = str(invite_data["FromUsername"])
        to_username = str(invite_data["ToUsername"])

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
            f"""
            INSERT INTO COOP_MATCHES (
                {_quote_identifier(match_columns["player_one"])},
                {_quote_identifier(match_columns["player_two"])},
                {_quote_identifier(match_columns["turn_username"])},
                {_quote_identifier(match_columns["state_json"])},
                {_quote_identifier(match_columns["status"])},
                {_quote_identifier(match_columns["winner"])},
                {_quote_identifier(match_columns["created_at"])},
                {_quote_identifier(match_columns["updated_at"])}
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
            f"""
            UPDATE COOP_INVITES
            SET {_quote_identifier(invite_columns["status"])} = 'Accepted',
                {_quote_identifier(invite_columns["responded_at"])} = ?
            WHERE {_quote_identifier(invite_columns["id"])} = ?
            """,
            (current_time, invite_id),
        )

        # Once a match starts, close any other pending invite between the same pair.
        conn.execute(
            f"""
            UPDATE COOP_INVITES
            SET {_quote_identifier(invite_columns["status"])} = 'Cancelled',
                {_quote_identifier(invite_columns["responded_at"])} = ?
            WHERE {_quote_identifier(invite_columns["status"])} = 'Pending'
              AND (
                    ({_quote_identifier(invite_columns["from_username"])} = ? AND {_quote_identifier(invite_columns["to_username"])} = ?)
                 OR ({_quote_identifier(invite_columns["from_username"])} = ? AND {_quote_identifier(invite_columns["to_username"])} = ?)
              )
            """,
            (current_time, from_username, to_username, to_username, from_username),
        )

    return True, "Invite accepted.", match_id


def get_active_coop_match_for_user(username: str) -> dict[str, Any] | None:
    with db_session() as conn:
        columns = _coop_match_columns(conn)
        row = conn.execute(
            f"""
            SELECT *
            FROM COOP_MATCHES
            WHERE {_quote_identifier(columns["status"])} = 'Active'
              AND (
                    {_quote_identifier(columns["player_one"])} = ?
                 OR {_quote_identifier(columns["player_two"])} = ?
              )
            ORDER BY {_quote_identifier(columns["updated_at"])} DESC,
                     {_quote_identifier(columns["id"])} DESC
            LIMIT 1
            """,
            (username, username),
        ).fetchone()
        return _normalize_record(dict(row)) if row else None


def get_coop_match_for_user(username: str, match_id: int) -> dict[str, Any] | None:
    with db_session() as conn:
        columns = _coop_match_columns(conn)
        row = conn.execute(
            f"""
            SELECT *
            FROM COOP_MATCHES
            WHERE {_quote_identifier(columns["id"])} = ?
              AND (
                    {_quote_identifier(columns["player_one"])} = ?
                 OR {_quote_identifier(columns["player_two"])} = ?
              )
            LIMIT 1
            """,
            (match_id, username, username),
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
        columns = _coop_match_columns(conn)
        turn_value = turn_username or ""
        conn.execute(
            f"""
            UPDATE COOP_MATCHES
            SET {_quote_identifier(columns["state_json"])} = ?,
                {_quote_identifier(columns["turn_username"])} = ?,
                {_quote_identifier(columns["status"])} = ?,
                {_quote_identifier(columns["winner"])} = ?,
                {_quote_identifier(columns["updated_at"])} = ?
            WHERE {_quote_identifier(columns["id"])} = ?
            """,
            (state_json, turn_value, status, winner, _now_str(), match_id),
        )


def abandon_coop_match(username: str, match_id: int) -> tuple[bool, str]:
    with db_session() as conn:
        columns = _coop_match_columns(conn)
        row = conn.execute(
            f"""
            SELECT *
            FROM COOP_MATCHES
            WHERE {_quote_identifier(columns["id"])} = ?
              AND (
                    {_quote_identifier(columns["player_one"])} = ?
                 OR {_quote_identifier(columns["player_two"])} = ?
              )
            LIMIT 1
            """,
            (match_id, username, username),
        ).fetchone()
        if not row:
            return False, "Match not found."
        row_data = _normalize_record(dict(row))
        if row_data["MatchStatus"] != "Active":
            return False, "Match is not active."

        player_one = str(row_data["PlayerOne"])
        player_two = str(row_data["PlayerTwo"])
        winner = player_two if username == player_one else player_one

        conn.execute(
            f"""
            UPDATE COOP_MATCHES
            SET {_quote_identifier(columns["status"])} = 'Abandoned',
                {_quote_identifier(columns["winner"])} = ?,
                {_quote_identifier(columns["updated_at"])} = ?
            WHERE {_quote_identifier(columns["id"])} = ?
            """,
            (winner, _now_str(), match_id),
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
        session_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

        conn.execute(
            "INSERT INTO GAME_PLAYERS (SessionID, Username) VALUES (?, ?)",
            (session_id, username),
        )

    xp_state = award_xp(username, xp_earned)
    return _normalize_record({"SessionID": session_id, **xp_state})


def get_progress_dataset(username: str, days: int) -> dict[str, Any]:
    if days < 1:
        days = 1

    end_date = _now().date()
    start_date = end_date - timedelta(days=days - 1)

    labels = []
    calories_data = []
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
            (username, start_date.isoformat(), end_date.isoformat()),
        ).fetchall()
        hydration_rows = conn.execute(
            """
            SELECT EntryDate AS day, SUM(HydrationIntake) AS total
            FROM HYDRATION
            WHERE Username = ? AND date(EntryDate) BETWEEN date(?) AND date(?)
            GROUP BY EntryDate
            """,
            (username, start_date.isoformat(), end_date.isoformat()),
        ).fetchall()
        exercise_rows = conn.execute(
            """
            SELECT date(ActivityDate) AS day, SUM(DurationMinutes) AS total
            FROM ACTIVITIES
            WHERE Username = ? AND date(ActivityDate) BETWEEN date(?) AND date(?)
            GROUP BY date(ActivityDate)
            """,
            (username, start_date.isoformat(), end_date.isoformat()),
        ).fetchall()

    calorie_map = {row["day"]: row["total"] for row in calorie_rows}
    hydration_map = {row["day"]: row["total"] for row in hydration_rows}
    exercise_map = {row["day"]: row["total"] for row in exercise_rows}

    current = start_date
    # Make one row per day so charts stay stable even when no logs were entered
    while current <= end_date:
        key = current.isoformat()
        labels.append(key)
        calories_data.append(int(calorie_map.get(key, 0) or 0))
        hydration_data.append(round(float(hydration_map.get(key, 0.0) or 0.0), 2))
        exercise_data.append(int(exercise_map.get(key, 0) or 0))
        current += timedelta(days=1)

    return _normalize_record(
        {
            "Labels": labels,
            "Calories": calories_data,
            "Hydration": hydration_data,
            "Exercise": exercise_data,
        }
    )


def get_home_summary(username: str) -> dict[str, Any]:
    goals = list_goals(username)
    activities = list_activities(username, limit=5)
    profile = get_profile(username) or {"XP": 0, "Level": 1, "AvatarName": "Starter Sprite"}

    active_goals = [goal for goal in goals if goal.get("GoalStatus") in ("Active", "On Track")]
    completed_goals = [goal for goal in goals if goal.get("GoalStatus") == "Completed"]

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
