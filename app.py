from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
import random
import traceback

from flask import Flask, flash, redirect, render_template, request, session, url_for
from werkzeug.exceptions import HTTPException

from db import (
    add_activity,
    add_calorie_log,
    add_goal,
    add_hydration_log,
    award_xp,
    authenticate_user,
    calorie_recommendation,
    create_game_session,
    create_user,
    create_coop_invite,
    cancel_coop_invite,
    consume_sso_token,
    create_friend_invite_link,
    get_friend_data,
    get_health,
    get_home_summary,
    create_sso_token,
    disable_friend_invite_link,
    get_profile,
    get_progress_dataset,
    get_user,
    get_active_coop_match_for_user,
    get_coop_match_for_user,
    get_pending_coop_invite_for_user,
    hydration_recommendation,
    init_db,
    list_coop_friends,
    list_coop_invites,
    list_activities,
    list_avatars,
    list_calorie_logs,
    list_friend_invite_links,
    list_goal_types,
    list_goals,
    list_hydration_logs,
    log_action,
    log_error,
    personalized_health_tips,
    respond_coop_invite,
    abandon_coop_match,
    update_coop_match_state,
    respond_friend_request,
    search_health_topics,
    search_users,
    send_friend_request,
    set_avatar,
    accept_friend_invite_link,
    update_goal_status,
    update_health,
    update_mood,
    update_personal_info,
    workout_xp_value,
)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "fitquest-dev-secret")
app.config.setdefault("SESSION_COOKIE_HTTPONLY", True)
app.config.setdefault("SESSION_COOKIE_SAMESITE", "Lax")
init_db()

PUBLIC_ENDPOINTS = {"login", "register", "sso_start", "sso_verify", "static"}
ALLOWED_GOAL_STATUSES = {"Active", "On Track", "Completed", "Cancelled"}
WORKOUT_DIFFICULTIES = ("Easy", "Standard", "Hard")
DEFAULT_ACTIVITY_DURATIONS = {
    "running": 30,
    "walking": 35,
    "cycling": 40,
    "swimming": 30,
    "yoga": 25,
    "strength training": 45,
    "stretching": 20,
}
PROGRESS_PERIODS = {
    "weekly": {"days": 7, "label": "Weekly"},
    "monthly": {"days": 30, "label": "Monthly"},
    "yearly": {"days": 365, "label": "Yearly"},
}


def _username() -> str:
    return session["username"]


def _post_login_redirect_target() -> str:
    next_url = request.form.get("next", "").strip() or request.args.get("next", "").strip()
    if next_url.startswith("/") and not next_url.startswith("//"):
        return next_url
    return url_for("home")


def _optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    return int(value)


def _optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    return float(value)


def _clock_to_minutes(clock_value: str | None) -> int | None:
    if not clock_value:
        return None
    try:
        parsed = datetime.strptime(clock_value.strip(), "%H:%M")
    except ValueError:
        return None
    return parsed.hour * 60 + parsed.minute


def _derive_activity_duration(
    activity_type: str,
    duration_raw: str | None,
    start_clock: str | None,
    end_clock: str | None,
) -> tuple[int, str]:
    explicit_duration = _optional_int(duration_raw)
    if explicit_duration and explicit_duration > 0:
        return explicit_duration, "manual"

    start_minutes = _clock_to_minutes(start_clock)
    end_minutes = _clock_to_minutes(end_clock)
    if start_minutes is not None and end_minutes is not None:
        duration = end_minutes - start_minutes
        if duration <= 0:
            duration += 24 * 60
        return max(1, duration), "auto_time_range"

    activity_key = activity_type.strip().lower()
    estimated = DEFAULT_ACTIVITY_DURATIONS.get(activity_key, 20)
    return estimated, "auto_estimate"


GAME_MODES = {"Solo": "AI Bot", "Co-op": "Training Partner"}
GAME_DIFFICULTIES = {
    "Easy": {"opponent_hand_size": 4, "xp_multiplier": 0.9},
    "Standard": {"opponent_hand_size": 5, "xp_multiplier": 1.0},
    "Hard": {"opponent_hand_size": 6, "xp_multiplier": 1.15},
}
GAME_COLORS = {
    "R": {"label": "red", "css": "red"},
    "B": {"label": "blue", "css": "blue"},
    "Y": {"label": "yellow", "css": "yellow"},
    "G": {"label": "green", "css": "green"},
    "W": {"label": "wild", "css": "wild"},
}
GAME_EXERCISES = {
    "JJ": {
        "name": "Jumping Jacks",
        "minutes": 8,
        "calories": 70,
        "effect": "none",
        "bonus_xp": 0,
        "description": "Stand tall, jump feet wide while raising arms overhead, then return to start.",
    },
    "PU": {
        "name": "Push-ups",
        "minutes": 6,
        "calories": 60,
        "effect": "none",
        "bonus_xp": 0,
        "description": "Keep a straight body line, lower your chest, then press back up with control.",
    },
    "SQ": {
        "name": "Squats",
        "minutes": 10,
        "calories": 90,
        "effect": "none",
        "bonus_xp": 0,
        "description": "Lower hips back and down with chest up, then drive through heels to stand.",
    },
    "MC": {
        "name": "Mountain Climbers",
        "minutes": 7,
        "calories": 80,
        "effect": "none",
        "bonus_xp": 0,
        "description": "From a plank, alternate driving knees toward chest at a steady controlled pace.",
    },
    "LU": {
        "name": "Lunges",
        "minutes": 9,
        "calories": 85,
        "effect": "none",
        "bonus_xp": 0,
        "description": "Step forward, bend both knees to about 90 degrees, then push back and switch.",
    },
    "AR": {
        "name": "Adrenaline Rush",
        "minutes": 5,
        "calories": 45,
        "effect": "extra_turn",
        "bonus_xp": 6,
        "description": "Perform a short, high-energy burst workout and keep intensity high for the set.",
    },
    "SB": {
        "name": "Sabotage Sprint",
        "minutes": 4,
        "calories": 38,
        "effect": "opponent_draw",
        "bonus_xp": 5,
        "description": "Quick sprint intervals with short recovery to spike heart rate and speed.",
    },
    "XP": {
        "name": "XP Surge",
        "minutes": 6,
        "calories": 55,
        "effect": "xp_boost",
        "bonus_xp": 12,
        "description": "Tempo rounds with strong form to maximize effort and bonus XP gain.",
    },
    "WD": {
        "name": "Wild Pulse",
        "minutes": 6,
        "calories": 50,
        "effect": "wild",
        "bonus_xp": 8,
        "description": "Choose any preferred movement and maintain a consistent moderate intensity.",
    },
}
GAME_STANDARD_CODES = ["JJ", "PU", "SQ", "MC", "LU"]
GAME_POWER_CODES = ["AR", "SB", "XP"]
GAME_QUESTS = [
    {"kind": "cards", "title": "Card Sprinter", "text": "Play 6 cards in one match", "target": 6, "bonus_xp": 20},
    {"kind": "calories", "title": "Burn Builder", "text": "Burn 320 calories in one match", "target": 320, "bonus_xp": 24},
    {"kind": "combo", "title": "Combo Starter", "text": "Reach a combo streak of 3", "target": 3, "bonus_xp": 18},
]

DEFAULT_AVATAR_VISUAL = {
    "asset": "avatars/starter_sprite.svg",
    "theme_class": "avatar-theme-starter",
    "tagline": "A balanced starting look for new players.",
}
AVATAR_VISUALS = {
    "Starter Sprite": {
        "asset": "avatars/starter_sprite.svg",
        "theme_class": "avatar-theme-starter",
        "tagline": "A balanced starting look for new players.",
    },
    "Trail Runner": {
        "asset": "avatars/trail_runner.svg",
        "theme_class": "avatar-theme-runner",
        "tagline": "Built for speed and cardio-focused sessions.",
    },
    "Hydro Hero": {
        "asset": "avatars/hydro_hero.svg",
        "theme_class": "avatar-theme-hydro",
        "tagline": "A hydration-focused champion of consistency.",
    },
    "Card Master": {
        "asset": "avatars/card_master.svg",
        "theme_class": "avatar-theme-card",
        "tagline": "For advanced players who dominate card strategy.",
    },
}


def _decorate_avatar_item(item: dict | None) -> dict | None:
    if not item:
        return None
    avatar_name = str(item.get("AvatarName") or "")
    visual = AVATAR_VISUALS.get(avatar_name, DEFAULT_AVATAR_VISUAL)
    # Attach presentation-only fields used by the templates.
    decorated = item
    decorated["art_file"] = visual["asset"]
    decorated["theme_class"] = visual["theme_class"]
    decorated["tagline"] = visual["tagline"]
    return decorated


def _encode_card(color_code: str, exercise_code: str) -> str:
    exercise = GAME_EXERCISES[exercise_code]
    effect = exercise.get("effect", "none")
    bonus_xp = int(exercise.get("bonus_xp", 0))
    return (
        f"{color_code}|{exercise_code}|{exercise['minutes']}|{exercise['calories']}"
        f"|{effect}|{bonus_xp}"
    )


def _decode_card(token: str) -> dict:
    color_code = "W"
    exercise_code = "WD"
    minutes = 0
    calories = 0
    effect = "none"
    bonus_xp = 0

    parts = token.split("|")
    if len(parts) >= 2:
        color_code = parts[0]
        exercise_code = parts[1]
    if len(parts) >= 3:
        try:
            minutes = int(parts[2])
        except ValueError:
            minutes = 0
    if len(parts) >= 4:
        try:
            calories = int(parts[3])
        except ValueError:
            calories = 0
    if len(parts) >= 5:
        effect = parts[4] or "none"
    if len(parts) >= 6:
        try:
            bonus_xp = int(parts[5])
        except ValueError:
            bonus_xp = 0

    color_info = GAME_COLORS.get(color_code, GAME_COLORS["W"])
    exercise = GAME_EXERCISES.get(exercise_code, GAME_EXERCISES["WD"])
    minutes = minutes or int(exercise["minutes"])
    calories = calories or int(exercise["calories"])
    effect = effect or str(exercise.get("effect", "none"))
    bonus_xp = bonus_xp or int(exercise.get("bonus_xp", 0))

    effect_labels = {
        "none": "Standard",
        "extra_turn": "Extra Turn",
        "opponent_draw": "Opponent Draw +1",
        "xp_boost": "XP Boost",
        "wild": "Wild Card",
    }
    effect_details = {
        "none": "Normal card with no extra gameplay effect.",
        "extra_turn": "After playing this card, you keep the turn and can play again.",
        "opponent_draw": "Forces the opponent to draw one extra card.",
        "xp_boost": "Adds bonus XP to your end-of-match reward.",
        "wild": "Can be played on any discard color or exercise.",
    }
    effect_label = effect_labels.get(effect, effect)

    return {
        "token": token,
        "color_code": color_code,
        "color_name": color_info["label"],
        "css_class": color_info["css"],
        "exercise_code": exercise_code,
        "exercise_name": exercise["name"],
        "minutes": minutes,
        "calories": calories,
        "effect": effect,
        "bonus_xp": bonus_xp,
        "effect_label": effect_label,
        "effect_description": effect_details.get(effect, ""),
        "is_power": effect != "none",
        "exercise_description": str(exercise.get("description", "")),
    }


def _card_score(card: dict) -> float:
    score = float(card["minutes"]) + (float(card["calories"]) / 12.0) + float(card["bonus_xp"])
    if card["effect"] == "extra_turn":
        score += 7
    elif card["effect"] == "opponent_draw":
        score += 5
    elif card["effect"] == "xp_boost":
        score += 4
    elif card["effect"] == "wild":
        score += 3
    return score


def _build_game_deck() -> list[str]:
    deck: list[str] = []
    for _ in range(2):
        for color in ("R", "B", "Y", "G"):
            for code in GAME_STANDARD_CODES:
                deck.append(_encode_card(color, code))
    for color in ("R", "B", "Y", "G"):
        for code in GAME_POWER_CODES:
            deck.append(_encode_card(color, code))
    for _ in range(4):
        deck.append(_encode_card("W", "WD"))
    random.shuffle(deck)
    return deck


def _build_side_quest() -> dict:
    template = random.choice(GAME_QUESTS)
    return {
        "kind": template["kind"],
        "title": template["title"],
        "text": template["text"],
        "target": template["target"],
        "bonus_xp": template["bonus_xp"],
        "progress": 0,
        "completed": False,
    }


def _replenish_deck(state: dict) -> None:
    if state["deck"]:
        return
    if len(state["discard"]) <= 1:
        return

    top_card = state["discard"][-1]
    recycled = state["discard"][:-1]
    random.shuffle(recycled)
    state["deck"] = recycled
    state["discard"] = [top_card]


def _draw_to_hand(state: dict, hand_key: str) -> str | None:
    _replenish_deck(state)
    if not state["deck"]:
        return None
    token = state["deck"].pop()
    state[hand_key].append(token)
    return token


def _is_playable_card(card_token: str, top_token: str) -> bool:
    card = _decode_card(card_token)
    top = _decode_card(top_token)

    if card["color_code"] == "W":
        return True
    if top["color_code"] == "W":
        return True
    return (
        card["color_code"] == top["color_code"]
        or card["exercise_code"] == top["exercise_code"]
    )


def _push_game_event(state: dict, message: str) -> None:
    events = state.setdefault("event_log", [])
    events.insert(0, message)
    del events[12:]


def _quest_progress(state: dict, quest: dict) -> int:
    kind = quest["kind"]
    if kind == "cards":
        return int(state.get("player_cards_played", 0))
    if kind == "calories":
        return int(state.get("player_total_calories", 0))
    if kind == "combo":
        return int(state.get("best_combo", 0))
    return 0


def _update_side_quest(state: dict) -> str | None:
    quest = state.get("quest")
    if not quest:
        return None

    progress_value = _quest_progress(state, quest)
    quest["progress"] = min(progress_value, int(quest["target"]))

    if not quest.get("completed") and progress_value >= int(quest["target"]):
        quest["completed"] = True
        bonus_xp = int(quest["bonus_xp"])
        state["bonus_xp"] += bonus_xp
        return f"Side quest complete: {quest['title']} (+{bonus_xp} XP bonus)."

    return None


def _new_game_state(mode: str, difficulty: str) -> dict:
    difficulty_cfg = GAME_DIFFICULTIES.get(difficulty, GAME_DIFFICULTIES["Standard"])
    deck = _build_game_deck()
    player_hand = [deck.pop() for _ in range(5)]
    opponent_hand = [deck.pop() for _ in range(int(difficulty_cfg["opponent_hand_size"]))]

    top_discard = deck.pop()
    while _decode_card(top_discard)["color_code"] == "W" and deck:
        deck.insert(0, top_discard)
        random.shuffle(deck)
        top_discard = deck.pop()

    opponent_name = GAME_MODES.get(mode, "Opponent")
    state = {
        "mode": mode,
        "difficulty": difficulty,
        "deck": deck,
        "discard": [top_discard],
        "player_hand": player_hand,
        "opponent_hand": opponent_hand,
        "turn": "player",
        "status": "active",
        "winner": None,
        "message": "Game started. Play a matching card or draw one.",
        "opponent_action": "",
        "player_cards_played": 0,
        "player_total_minutes": 0,
        "player_total_calories": 0,
        "turn_count": 0,
        "draw_count": 0,
        "combo_streak": 0,
        "best_combo": 0,
        "bonus_xp": 0,
        "last_player_action": "",
        "quest": _build_side_quest(),
        "badges": [],
        "event_log": [],
        "xp_earned": 0,
        "session_result": None,
        "opponent_name": opponent_name,
    }
    _push_game_event(
        state,
        (
            f"New {difficulty} {mode} match started against {opponent_name}. "
            "Complete your side quest for bonus XP."
        ),
    )
    return state


def _reset_game(mode: str, difficulty: str | None = None) -> dict:
    if mode not in GAME_MODES:
        mode = "Solo"
    if difficulty is None:
        difficulty = session.get("game_difficulty", "Standard")
    if difficulty not in GAME_DIFFICULTIES:
        difficulty = "Standard"

    session["game_mode"] = mode
    session["game_difficulty"] = difficulty
    state = _new_game_state(mode, difficulty)
    session["game_state"] = state
    return state


def _get_or_create_game_state() -> dict:
    current_mode = session.get("game_mode", "Solo")
    current_difficulty = session.get("game_difficulty", "Standard")
    state = session.get("game_state")

    if not state:
        return _reset_game(current_mode, current_difficulty)

    required_keys = {"mode", "difficulty", "deck", "discard", "player_hand", "opponent_hand", "quest", "event_log"}
    if any(key not in state for key in required_keys):
        return _reset_game(current_mode, current_difficulty)

    if state.get("mode") not in GAME_MODES:
        return _reset_game(current_mode, current_difficulty)

    if state.get("difficulty") not in GAME_DIFFICULTIES:
        return _reset_game(current_mode, current_difficulty)

    if state.get("mode") != current_mode or state.get("difficulty") != current_difficulty:
        return _reset_game(current_mode, current_difficulty)

    state["opponent_name"] = GAME_MODES.get(state["mode"], "Opponent")
    return state


def _record_game_activity(username: str, card: dict, difficulty: str = "Standard") -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    add_activity(
        username=username,
        activity_type=card["exercise_name"],
        duration_minutes=card["minutes"],
        calories_burnt=card["calories"],
        distance_km=None,
        activity_date=timestamp,
        source="Game",
        difficulty=difficulty,
    )


def _award_combo_bonus(state: dict) -> int:
    streak = int(state.get("combo_streak", 0))
    if streak <= 1:
        return 0
    bonus = min(12, streak * 2)
    state["bonus_xp"] += bonus
    return bonus


def _apply_card_effect(state: dict, actor: str, card: dict) -> tuple[bool, list[str]]:
    effect = card.get("effect", "none")
    messages: list[str] = []
    keep_turn = False

    actor_name = "You" if actor == "player" else state["opponent_name"]

    if effect == "extra_turn":
        keep_turn = True
        messages.append(f"{actor_name} triggered Extra Turn.")
    elif effect == "opponent_draw":
        target_hand = "opponent_hand" if actor == "player" else "player_hand"
        drawn_count = 0
        for _ in range(1):
            drawn = _draw_to_hand(state, target_hand)
            if drawn:
                drawn_count += 1
        if drawn_count > 0:
            if actor == "player":
                messages.append(f"Sabotage Sprint: opponent drew {drawn_count} card.")
            else:
                messages.append(f"{state['opponent_name']} forced you to draw {drawn_count} card.")
    elif effect == "xp_boost":
        if actor == "player":
            bonus = max(8, int(card.get("bonus_xp", 0)))
            state["bonus_xp"] += bonus
            messages.append(f"XP Surge activated: +{bonus} bonus XP.")
    elif effect == "wild":
        if actor == "player":
            bonus = max(4, int(card.get("bonus_xp", 0) // 2))
            state["bonus_xp"] += bonus
            messages.append(f"Wild Pulse: +{bonus} bonus XP.")

    return keep_turn, messages


def _finish_game(state: dict, username: str, winner: str) -> None:
    if state.get("status") != "active":
        return

    cards_played = int(state.get("player_cards_played", 0))
    base_xp = max(20, cards_played * 10 + (25 if winner == username else 8))
    bonus_xp = int(state.get("bonus_xp", 0))
    difficulty = state.get("difficulty", "Standard")
    difficulty_cfg = GAME_DIFFICULTIES.get(difficulty, GAME_DIFFICULTIES["Standard"])

    xp_earned = int(round((base_xp + bonus_xp) * float(difficulty_cfg["xp_multiplier"])))
    xp_earned = max(15, xp_earned)

    session_result = create_game_session(
        username=username,
        mode_name=state["mode"],
        xp_earned=xp_earned,
        winner=winner,
    )

    state["status"] = "finished"
    state["turn"] = "none"
    state["winner"] = winner
    state["xp_earned"] = xp_earned
    state["session_result"] = session_result

    badges: list[str] = []
    if winner == username:
        badges.append("Match Winner")
    if int(state.get("best_combo", 0)) >= 3:
        badges.append("Combo Master")
    if int(state.get("draw_count", 0)) == 0 and int(state.get("player_cards_played", 0)) >= 4:
        badges.append("No-Draw Warrior")
    if state.get("quest", {}).get("completed"):
        badges.append("Quest Complete")
    if difficulty == "Hard" and winner == username:
        badges.append("Hard Mode Hero")
    state["badges"] = badges

    if winner == username:
        state["message"] = f"You won the match! +{xp_earned} XP earned."
    else:
        state["message"] = f"{winner} won the match. You still earned +{xp_earned} XP."
    _push_game_event(state, state["message"])
    if badges:
        _push_game_event(state, f"Badges unlocked: {', '.join(badges)}")


def _select_ai_card_index(playable_indexes: list[int], state: dict) -> int:
    difficulty = state.get("difficulty", "Standard")
    if difficulty == "Easy":
        return random.choice(playable_indexes)

    scored = []
    for index in playable_indexes:
        card = _decode_card(state["opponent_hand"][index])
        scored.append((index, _card_score(card)))

    scored.sort(key=lambda item: item[1], reverse=True)
    if difficulty == "Hard":
        return scored[0][0]

    top_choices = [item[0] for item in scored[: min(2, len(scored))]]
    return random.choice(top_choices)


def _opponent_turn(state: dict, username: str) -> None:
    if state.get("status") != "active":
        return

    state["turn"] = "opponent"
    opponent_name = state["opponent_name"]
    action_parts: list[str] = []

    chain_guard = 0
    while state.get("status") == "active" and chain_guard < 4:
        chain_guard += 1
        top_token = state["discard"][-1]
        playable_indexes = [
            idx
            for idx, token in enumerate(state["opponent_hand"])
            if _is_playable_card(token, top_token)
        ]

        keep_turn = False
        if playable_indexes:
            index = _select_ai_card_index(playable_indexes, state)
            token = state["opponent_hand"].pop(index)
            card = _decode_card(token)
            state["discard"].append(token)
            action_parts.append(f"{opponent_name} played {card['exercise_name']} ({card['color_name']}).")
            keep_turn, effect_messages = _apply_card_effect(state, actor="opponent", card=card)
            action_parts.extend(effect_messages)
        else:
            drawn = _draw_to_hand(state, "opponent_hand")
            if not drawn:
                action_parts.append(f"{opponent_name} could not draw.")
                keep_turn = False
            else:
                drawn_card = _decode_card(drawn)
                action_parts.append(f"{opponent_name} drew a card.")
                should_play_drawn = _is_playable_card(drawn, top_token)
                if should_play_drawn and state.get("difficulty") in ("Standard", "Hard"):
                    played = state["opponent_hand"].pop()
                    played_card = _decode_card(played)
                    state["discard"].append(played)
                    action_parts.append(
                        f"{opponent_name} drew and played {played_card['exercise_name']} ({played_card['color_name']})."
                    )
                    keep_turn, effect_messages = _apply_card_effect(state, actor="opponent", card=played_card)
                    action_parts.extend(effect_messages)
                else:
                    keep_turn = False

        if not state["opponent_hand"]:
            _finish_game(state, username, opponent_name)
            break

        if not keep_turn:
            break

    state["opponent_action"] = " ".join(action_parts) if action_parts else f"{opponent_name} passed."
    _push_game_event(state, state["opponent_action"])

    if state.get("status") == "active":
        state["turn"] = "player"


def _play_player_card(state: dict, username: str, index: int) -> tuple[bool, str]:
    if state.get("status") != "active":
        return False, "The game is finished. Start a new game."

    if state.get("turn") != "player":
        return False, "Wait for your turn."

    if index < 0 or index >= len(state["player_hand"]):
        return False, "Invalid card selection."

    token = state["player_hand"][index]
    top_token = state["discard"][-1]
    if not _is_playable_card(token, top_token):
        return False, "That card does not match the current discard."

    played = state["player_hand"].pop(index)
    card = _decode_card(played)
    state["discard"].append(played)
    state["turn_count"] += 1
    state["player_cards_played"] += 1
    state["player_total_minutes"] += card["minutes"]
    state["player_total_calories"] += card["calories"]
    state["last_player_action"] = "play"
    state["combo_streak"] = int(state.get("combo_streak", 0)) + 1
    state["best_combo"] = max(int(state.get("best_combo", 0)), int(state["combo_streak"]))

    combo_bonus = _award_combo_bonus(state)
    keep_turn, effect_messages = _apply_card_effect(state, actor="player", card=card)
    quest_message = _update_side_quest(state)

    message = f"You played {card['exercise_name']} ({card['color_name']})."
    if combo_bonus > 0:
        message += f" Combo x{state['combo_streak']} (+{combo_bonus} XP)."
    if quest_message:
        message += f" {quest_message}"
    if effect_messages:
        message += f" {' '.join(effect_messages)}"

    state["message"] = message
    _push_game_event(state, state["message"])
    _record_game_activity(username, card, str(state.get("difficulty", "Standard")))

    if not state["player_hand"]:
        _finish_game(state, username, username)
        return True, state["message"]

    if keep_turn:
        state["turn"] = "player"
    else:
        _opponent_turn(state, username)
    return True, state["message"]


def _draw_player_card(state: dict, username: str) -> str:
    if state.get("status") != "active":
        return "The game is finished. Start a new game."

    if state.get("turn") != "player":
        return "Wait for your turn."

    drawn = _draw_to_hand(state, "player_hand")
    if not drawn:
        return "Deck is empty. You cannot draw."

    state["turn_count"] += 1
    state["draw_count"] += 1
    state["combo_streak"] = 0
    state["last_player_action"] = "draw"
    card = _decode_card(drawn)
    top_token = state["discard"][-1]
    if _is_playable_card(drawn, top_token):
        state["message"] = (
            f"You drew {card['exercise_name']} ({card['color_name']}) and can still play this turn."
        )
        _push_game_event(state, state["message"])
        state["turn"] = "player"
        return state["message"]

    state["message"] = f"You drew {card['exercise_name']} ({card['color_name']}). Turn passed."
    _push_game_event(state, state["message"])
    _opponent_turn(state, username)
    return state["message"]


def _game_template_context(state: dict) -> dict:
    top_token = state["discard"][-1]
    discard = _decode_card(top_token)
    player_cards = []
    for index, token in enumerate(state["player_hand"]):
        card = _decode_card(token)
        card["index"] = index
        card["playable"] = (
            state["status"] == "active"
            and state["turn"] == "player"
            and _is_playable_card(token, top_token)
        )
        player_cards.append(card)

    return {
        "is_coop_lobby": False,
        "is_coop_match": False,
        "match_id": None,
        "mode": state["mode"],
        "status": state["status"],
        "turn": state["turn"],
        "message": state.get("message", ""),
        "opponent_action": state.get("opponent_action", ""),
        "opponent_name": state.get("opponent_name", "Opponent"),
        "difficulty": state.get("difficulty", "Standard"),
        "difficulties": list(GAME_DIFFICULTIES.keys()),
        "opponent_count": len(state["opponent_hand"]),
        "player_count": len(state["player_hand"]),
        "player_cards": player_cards,
        "discard": discard,
        "deck_count": len(state["deck"]),
        "player_cards_played": state.get("player_cards_played", 0),
        "player_total_minutes": state.get("player_total_minutes", 0),
        "player_total_calories": state.get("player_total_calories", 0),
        "draw_count": state.get("draw_count", 0),
        "combo_streak": state.get("combo_streak", 0),
        "best_combo": state.get("best_combo", 0),
        "bonus_xp": state.get("bonus_xp", 0),
        "turn_count": state.get("turn_count", 0),
        "quest": state.get("quest", {}),
        "quest_progress_pct": int(
            (int(state.get("quest", {}).get("progress", 0)) / max(1, int(state.get("quest", {}).get("target", 1))))
            * 100
        ),
        "badges": state.get("badges", []),
        "event_log": state.get("event_log", []),
        "winner": state.get("winner"),
        "xp_earned": state.get("xp_earned", 0),
        "session_result": state.get("session_result"),
    }


def _new_coop_player_state(hand: list[str]) -> dict:
    return {
        "hand": hand,
        "cards_played": 0,
        "total_minutes": 0,
        "total_calories": 0,
        "draw_count": 0,
        "combo_streak": 0,
        "best_combo": 0,
        "bonus_xp": 0,
        "quest": _build_side_quest(),
        "badges": [],
    }


def _new_coop_match_state(player_one: str, player_two: str, difficulty: str) -> dict:
    difficulty_cfg = GAME_DIFFICULTIES.get(difficulty, GAME_DIFFICULTIES["Standard"])
    deck = _build_game_deck()
    player_one_hand = [deck.pop() for _ in range(5)]
    player_two_hand = [deck.pop() for _ in range(5)]

    top_discard = deck.pop()
    while _decode_card(top_discard)["color_code"] == "W" and deck:
        deck.insert(0, top_discard)
        random.shuffle(deck)
        top_discard = deck.pop()

    state = {
        "mode": "Co-op",
        "difficulty": difficulty,
        "status": "active",
        "winner": None,
        "turn_username": player_one,
        "deck": deck,
        "discard": [top_discard],
        "turn_count": 0,
        "message": f"Co-op match started. {player_one} plays first.",
        "players": {
            player_one: _new_coop_player_state(player_one_hand),
            player_two: _new_coop_player_state(player_two_hand),
        },
        "event_log": [],
        "xp_earned_map": {},
        "session_results": {},
        "rewards_persisted": False,
        "difficulty_multiplier": float(difficulty_cfg["xp_multiplier"]),
    }
    _push_game_event(
        state,
        f"New {difficulty} Co-op match: {player_one} vs {player_two}. Complete your side quest for bonus XP.",
    )
    return state


def _other_coop_username(state: dict, username: str) -> str | None:
    players = list(state.get("players", {}).keys())
    for player_name in players:
        if player_name != username:
            return player_name
    return None


def _draw_for_coop_player(state: dict, username: str) -> str | None:
    _replenish_deck(state)
    if not state["deck"]:
        return None
    token = state["deck"].pop()
    state["players"][username]["hand"].append(token)
    return token


def _coop_quest_progress(player_state: dict, kind: str) -> int:
    if kind == "cards":
        return int(player_state.get("cards_played", 0))
    if kind == "calories":
        return int(player_state.get("total_calories", 0))
    if kind == "combo":
        return int(player_state.get("best_combo", 0))
    return 0


def _update_coop_side_quest(player_state: dict) -> str | None:
    quest = player_state.get("quest")
    if not quest:
        return None

    progress_value = _coop_quest_progress(player_state, quest["kind"])
    quest["progress"] = min(progress_value, int(quest["target"]))

    if not quest.get("completed") and progress_value >= int(quest["target"]):
        quest["completed"] = True
        bonus_xp = int(quest["bonus_xp"])
        player_state["bonus_xp"] += bonus_xp
        return f"Side quest complete: {quest['title']} (+{bonus_xp} XP bonus)."
    return None


def _apply_coop_card_effect(state: dict, actor_username: str, card: dict) -> tuple[bool, list[str]]:
    effect = card.get("effect", "none")
    messages: list[str] = []
    keep_turn = False

    actor_state = state["players"][actor_username]
    opponent_username = _other_coop_username(state, actor_username)

    if effect == "extra_turn":
        keep_turn = True
        messages.append("Extra Turn activated.")
    elif effect == "opponent_draw" and opponent_username:
        drawn = _draw_for_coop_player(state, opponent_username)
        if drawn:
            messages.append(f"{opponent_username} drew 1 penalty card.")
    elif effect == "xp_boost":
        bonus = max(8, int(card.get("bonus_xp", 0)))
        actor_state["bonus_xp"] += bonus
        messages.append(f"XP Surge activated (+{bonus} XP).")
    elif effect == "wild":
        bonus = max(4, int(card.get("bonus_xp", 0) // 2))
        actor_state["bonus_xp"] += bonus
        messages.append(f"Wild Pulse bonus (+{bonus} XP).")

    return keep_turn, messages


def _finish_coop_state(state: dict, winner: str) -> None:
    if state.get("status") != "active":
        return

    difficulty = state.get("difficulty", "Standard")
    multiplier = float(GAME_DIFFICULTIES.get(difficulty, GAME_DIFFICULTIES["Standard"])["xp_multiplier"])

    xp_map: dict[str, int] = {}
    for player_name, player_state in state["players"].items():
        cards_played = int(player_state.get("cards_played", 0))
        base_xp = max(15, cards_played * 9 + (28 if player_name == winner else 14))
        total_xp = int(round((base_xp + int(player_state.get("bonus_xp", 0))) * multiplier))
        xp_map[player_name] = max(10, total_xp)

        badges: list[str] = []
        if player_name == winner:
            badges.append("Match Winner")
        if int(player_state.get("best_combo", 0)) >= 3:
            badges.append("Combo Master")
        if int(player_state.get("draw_count", 0)) == 0 and cards_played >= 4:
            badges.append("No-Draw Warrior")
        if player_state.get("quest", {}).get("completed"):
            badges.append("Quest Complete")
        if difficulty == "Hard" and player_name == winner:
            badges.append("Hard Mode Hero")
        player_state["badges"] = badges

    state["status"] = "finished"
    state["winner"] = winner
    state["turn_username"] = ""
    state["xp_earned_map"] = xp_map
    state["message"] = f"{winner} won the co-op match."
    _push_game_event(state, state["message"])


def _persist_coop_rewards(state: dict) -> None:
    if state.get("rewards_persisted"):
        return

    session_results: dict[str, dict] = {}
    winner = state.get("winner")
    for player_name, xp_value in state.get("xp_earned_map", {}).items():
        session_results[player_name] = create_game_session(
            username=player_name,
            mode_name="Co-op",
            xp_earned=int(xp_value),
            winner=winner,
        )
    state["session_results"] = session_results
    state["rewards_persisted"] = True

    badges_summary = []
    for player_name, player_state in state["players"].items():
        for badge in player_state.get("badges", []):
            badges_summary.append(f"{player_name}: {badge}")
    if badges_summary:
        _push_game_event(state, f"Badges unlocked: {', '.join(badges_summary)}")


def _coop_play_card(state: dict, username: str, index: int) -> tuple[bool, str]:
    if state.get("status") != "active":
        return False, "Match is already finished."

    if state.get("turn_username") != username:
        return False, "Wait for your turn."

    player_state = state["players"].get(username)
    if not player_state:
        return False, "You are not part of this match."

    hand = player_state["hand"]
    if index < 0 or index >= len(hand):
        return False, "Invalid card selection."

    token = hand[index]
    top_token = state["discard"][-1]
    if not _is_playable_card(token, top_token):
        return False, "That card does not match the discard card."

    played_token = hand.pop(index)
    card = _decode_card(played_token)
    state["discard"].append(played_token)
    state["turn_count"] += 1
    player_state["cards_played"] += 1
    player_state["total_minutes"] += card["minutes"]
    player_state["total_calories"] += card["calories"]
    player_state["combo_streak"] = int(player_state.get("combo_streak", 0)) + 1
    player_state["best_combo"] = max(int(player_state.get("best_combo", 0)), int(player_state["combo_streak"]))

    combo_bonus = 0
    if int(player_state["combo_streak"]) > 1:
        combo_bonus = min(12, int(player_state["combo_streak"]) * 2)
        player_state["bonus_xp"] += combo_bonus

    keep_turn, effect_messages = _apply_coop_card_effect(state, username, card)
    quest_message = _update_coop_side_quest(player_state)
    _record_game_activity(username, card, str(state.get("difficulty", "Standard")))

    message = f"{username} played {card['exercise_name']} ({card['color_name']})."
    if combo_bonus > 0:
        message += f" Combo x{player_state['combo_streak']} (+{combo_bonus} XP)."
    if quest_message:
        message += f" {quest_message}"
    if effect_messages:
        message += f" {' '.join(effect_messages)}"
    state["message"] = message
    _push_game_event(state, message)

    if not hand:
        _finish_coop_state(state, username)
        return True, state["message"]

    if keep_turn:
        state["turn_username"] = username
    else:
        opponent = _other_coop_username(state, username)
        state["turn_username"] = opponent or username
    return True, state["message"]


def _coop_draw_card(state: dict, username: str) -> str:
    if state.get("status") != "active":
        return "Match is already finished."

    if state.get("turn_username") != username:
        return "Wait for your turn."

    player_state = state["players"].get(username)
    if not player_state:
        return "You are not part of this match."

    drawn = _draw_for_coop_player(state, username)
    if not drawn:
        return "Deck is empty. Cannot draw."

    player_state["draw_count"] += 1
    player_state["combo_streak"] = 0
    state["turn_count"] += 1

    card = _decode_card(drawn)
    top_token = state["discard"][-1]
    if _is_playable_card(drawn, top_token):
        state["message"] = f"{username} drew {card['exercise_name']} and can still play."
        _push_game_event(state, state["message"])
        state["turn_username"] = username
        return state["message"]

    opponent = _other_coop_username(state, username)
    state["turn_username"] = opponent or username
    state["message"] = f"{username} drew {card['exercise_name']} and passed the turn."
    _push_game_event(state, state["message"])
    return state["message"]


def _build_coop_match_context(username: str, match_id: int, state: dict) -> dict:
    player_state = state["players"][username]
    opponent_username = _other_coop_username(state, username) or "Friend"
    opponent_state = state["players"].get(opponent_username, {"hand": [], "cards_played": 0})
    top_token = state["discard"][-1]
    discard = _decode_card(top_token)

    player_cards = []
    for index, token in enumerate(player_state["hand"]):
        card = _decode_card(token)
        card["index"] = index
        card["playable"] = (
            state["status"] == "active"
            and state.get("turn_username") == username
            and _is_playable_card(token, top_token)
        )
        player_cards.append(card)

    quest = player_state.get("quest", {})
    quest_progress = int(quest.get("progress", 0))
    quest_target = max(1, int(quest.get("target", 1)))
    quest_progress_pct = int((quest_progress / quest_target) * 100)

    relative_turn = "none"
    if state["status"] == "active":
        relative_turn = "player" if state.get("turn_username") == username else "opponent"

    return {
        "is_coop_lobby": False,
        "is_coop_match": True,
        "match_id": match_id,
        "mode": "Co-op",
        "difficulty": state.get("difficulty", "Standard"),
        "difficulties": list(GAME_DIFFICULTIES.keys()),
        "status": state.get("status", "active"),
        "turn": relative_turn,
        "turn_username": state.get("turn_username", ""),
        "message": state.get("message", ""),
        "opponent_action": "",
        "opponent_name": opponent_username,
        "opponent_count": len(opponent_state.get("hand", [])),
        "player_count": len(player_state["hand"]),
        "player_cards": player_cards,
        "discard": discard,
        "deck_count": len(state.get("deck", [])),
        "turn_count": int(state.get("turn_count", 0)),
        "player_cards_played": int(player_state.get("cards_played", 0)),
        "player_total_minutes": int(player_state.get("total_minutes", 0)),
        "player_total_calories": int(player_state.get("total_calories", 0)),
        "draw_count": int(player_state.get("draw_count", 0)),
        "combo_streak": int(player_state.get("combo_streak", 0)),
        "best_combo": int(player_state.get("best_combo", 0)),
        "bonus_xp": int(player_state.get("bonus_xp", 0)),
        "quest": quest,
        "quest_progress_pct": quest_progress_pct,
        "badges": player_state.get("badges", []),
        "event_log": state.get("event_log", []),
        "winner": state.get("winner"),
        "xp_earned": int(state.get("xp_earned_map", {}).get(username, 0)),
        "session_result": state.get("session_results", {}).get(username),
    }


def _coop_lobby_context(username: str) -> dict:
    invites = list_coop_invites(username)
    return {
        "is_coop_lobby": True,
        "is_coop_match": False,
        "match_id": None,
        "mode": "Co-op",
        "difficulty": session.get("game_difficulty", "Standard"),
        "difficulties": list(GAME_DIFFICULTIES.keys()),
        "status": "lobby",
        "turn": "none",
        "turn_username": "",
        "message": "Invite an accepted friend to start a shared co-op match.",
        "opponent_action": "",
        "opponent_name": "",
        "opponent_count": 0,
        "player_count": 0,
        "player_cards": [],
        "discard": _decode_card(_encode_card("W", "WD")),
        "deck_count": 0,
        "turn_count": 0,
        "player_cards_played": 0,
        "player_total_minutes": 0,
        "player_total_calories": 0,
        "draw_count": 0,
        "combo_streak": 0,
        "best_combo": 0,
        "bonus_xp": 0,
        "quest": {"title": "-", "text": "-", "target": 1, "progress": 0, "bonus_xp": 0, "completed": False},
        "quest_progress_pct": 0,
        "badges": [],
        "event_log": [],
        "winner": None,
        "xp_earned": 0,
        "session_result": None,
        "coop_friends": list_coop_friends(username),
        "incoming_invites": invites["Incoming"],
        "outgoing_invites": invites["Outgoing"],
    }


def _build_exercise_goal_suggestions(
    activities: list[dict],
    goal_types: list[dict],
) -> list[dict]:
    goal_type_map = {
        str(goal_type["GoalTypeName"]).strip().lower(): goal_type
        for goal_type in goal_types
    }
    suggestions: list[dict] = []

    if activities:
        active_days = {
            str(item["ActivityDate"])[:10]
            for item in activities
            if item.get("ActivityDate")
        }
        day_count = max(1, len(active_days))
        total_minutes = sum(int(item.get("DurationMinutes") or 0) for item in activities)
        total_calories = sum(int(item.get("CaloriesBurnt") or 0) for item in activities)
        total_distance = sum(float(item.get("DistanceKm") or 0.0) for item in activities)
    else:
        day_count = 1
        total_minutes = 0
        total_calories = 0
        total_distance = 0.0

    avg_daily_minutes = total_minutes / day_count
    avg_daily_calories = total_calories / day_count
    avg_daily_distance = total_distance / day_count if total_distance > 0 else 0.0

    def _round_to_step(value: float, step: int) -> int:
        raw = int(round(value))
        if raw <= 0:
            return step
        return int(round(raw / step) * step)

    exercise_type = goal_type_map.get("exercise")
    if exercise_type:
        weekly_minutes_target = _round_to_step(avg_daily_minutes * 7, 5) if total_minutes > 0 else 120
        suggestions.append(
            {
                "goal_type_id": int(exercise_type["GoalTypeID"]),
                "goal_type_name": exercise_type["GoalTypeName"],
                "unit": exercise_type["Unit"],
                "target_value": float(weekly_minutes_target),
                "target_display": str(weekly_minutes_target),
                "title": "Weekly Exercise Goal",
                "reason": "Based on your logged exercise minutes trend.",
            }
        )

    calories_type = goal_type_map.get("calories")
    if calories_type:
        weekly_calorie_target = _round_to_step(avg_daily_calories * 7, 50) if total_calories > 0 else 1500
        suggestions.append(
            {
                "goal_type_id": int(calories_type["GoalTypeID"]),
                "goal_type_name": calories_type["GoalTypeName"],
                "unit": calories_type["Unit"],
                "target_value": float(weekly_calorie_target),
                "target_display": str(weekly_calorie_target),
                "title": "Weekly Calories Burn Goal",
                "reason": "Derived from calories burned in your activity table.",
            }
        )

    distance_type = goal_type_map.get("distance")
    if distance_type:
        if total_distance > 0:
            weekly_distance_target = round(avg_daily_distance * 7, 1)
        else:
            weekly_distance_target = 10.0
        suggestions.append(
            {
                "goal_type_id": int(distance_type["GoalTypeID"]),
                "goal_type_name": distance_type["GoalTypeName"],
                "unit": distance_type["Unit"],
                "target_value": float(weekly_distance_target),
                "target_display": f"{weekly_distance_target:.1f}",
                "title": "Weekly Distance Goal",
                "reason": "Calculated from distance values in recent activities.",
            }
        )

    return suggestions


def _build_category_exercise_suggestions(
    goals: list[dict],
    activities: list[dict],
) -> list[dict]:
    active_goal_names = {
        str(goal.get("GoalTypeName") or "").strip().lower()
        for goal in goals
        if goal.get("GoalStatus") in ("Active", "On Track")
    }
    recent_minutes = sum(int(item.get("DurationMinutes") or 0) for item in activities[:14])
    base_duration = 15 if recent_minutes < 180 else 25

    focus_tags: set[str] = set()
    if "calories" in active_goal_names:
        focus_tags.update({"body composition", "cardiovascular endurance"})
    if "distance" in active_goal_names:
        focus_tags.update({"cardiovascular endurance", "muscular endurance"})
    if "exercise" in active_goal_names:
        focus_tags.update({"muscular strength", "muscular endurance", "flexibility"})
    if not focus_tags:
        focus_tags = {
            "body composition",
            "muscular endurance",
            "muscular strength",
            "cardiovascular endurance",
            "flexibility",
        }

    catalog = [
        {
            "category": "Body Composition",
            "exercise": "Circuit Intervals (bodyweight)",
            "duration": base_duration + 5,
            "reason": "Supports calorie-burn and body-composition targets.",
        },
        {
            "category": "Muscular Endurance",
            "exercise": "High-rep squat and push-up rounds",
            "duration": base_duration,
            "reason": "Builds fatigue resistance across repeated sets.",
        },
        {
            "category": "Muscular Strength",
            "exercise": "Progressive resistance training",
            "duration": base_duration + 10,
            "reason": "Improves force output and strength progression.",
        },
        {
            "category": "Cardiovascular Endurance",
            "exercise": "Tempo run or brisk cycling",
            "duration": base_duration + 10,
            "reason": "Raises aerobic capacity and stamina.",
        },
        {
            "category": "Flexibility",
            "exercise": "Mobility and stretch flow",
            "duration": max(10, base_duration - 5),
            "reason": "Improves movement quality and recovery.",
        },
    ]

    suggestions: list[dict] = []
    for item in catalog:
        if item["category"].lower() in focus_tags:
            suggestions.append(item)
    return suggestions


def _build_adaptive_challenges(activities: list[dict]) -> list[dict]:
    recent = activities[:21]
    total_minutes = sum(int(item.get("DurationMinutes") or 0) for item in recent)
    sessions = max(1, len(recent))
    avg_session = max(10, int(round(total_minutes / sessions)))
    weekly_target = max(90, int(round((total_minutes / 3) / 5) * 5))

    return [
        {
            "title": "Weekly Minutes Ramp",
            "description": f"Complete {weekly_target} total minutes this week.",
            "why": "Scaled to your recent logged training volume.",
        },
        {
            "title": "Consistency Streak",
            "description": "Log activity on 4 separate days this week.",
            "why": "Encourages routine consistency over single long sessions.",
        },
        {
            "title": "Session Builder",
            "description": f"Complete 3 sessions of at least {avg_session} minutes.",
            "why": "Adaptive target based on your average session length.",
        },
    ]


def _resolve_progress_period(period_value: str | None) -> tuple[str, int, str]:
    period_key = str(period_value or "").strip().lower()
    if period_key not in PROGRESS_PERIODS:
        period_key = "weekly"
    period_config = PROGRESS_PERIODS[period_key]
    return period_key, int(period_config["days"]), str(period_config["label"])


def _build_period_trend(rows: list[dict], period_key: str) -> list[dict]:
    if not rows:
        return []

    trend_points: list[dict] = []
    if period_key == "weekly":
        for row in rows:
            date_key = str(row.get("date_key") or "")
            try:
                parsed = datetime.strptime(date_key, "%Y-%m-%d")
                label = parsed.strftime("%a")
                title = parsed.strftime("%b %d, %Y")
            except ValueError:
                label = str(row.get("label") or "")
                title = label
            trend_points.append(
                {
                    "label": label,
                    "title": title,
                    "value": int(row.get("exercise") or 0),
                }
            )
    elif period_key == "monthly":
        for start in range(0, len(rows), 7):
            chunk = rows[start : start + 7]
            if not chunk:
                continue
            trend_points.append(
                {
                    "label": f"W{len(trend_points) + 1}",
                    "title": f"{chunk[0]['label']} - {chunk[-1]['label']}",
                    "value": sum(int(item.get("exercise") or 0) for item in chunk),
                }
            )
    else:
        monthly_totals: dict[str, dict[str, str | int]] = {}
        month_order: list[str] = []
        for row in rows:
            date_key = str(row.get("date_key") or "")
            try:
                parsed = datetime.strptime(date_key, "%Y-%m-%d")
            except ValueError:
                continue

            month_key = parsed.strftime("%Y-%m")
            if month_key not in monthly_totals:
                monthly_totals[month_key] = {
                    "label": parsed.strftime("%b"),
                    "title": parsed.strftime("%B %Y"),
                    "value": 0,
                }
                month_order.append(month_key)
            monthly_totals[month_key]["value"] = int(monthly_totals[month_key]["value"]) + int(
                row.get("exercise") or 0
            )

        for month_key in month_order[-12:]:
            month_item = monthly_totals[month_key]
            trend_points.append(
                {
                    "label": str(month_item["label"]),
                    "title": str(month_item["title"]),
                    "value": int(month_item["value"]),
                }
            )

    max_trend = max((item["value"] for item in trend_points), default=1)
    for item in trend_points:
        item["pct"] = int((item["value"] / max_trend) * 100) if max_trend else 0
    return trend_points


def _progress_context(period_key: str) -> dict:
    """Prepare server-rendered progress data for a selected time period."""
    period_key, days, period_label = _resolve_progress_period(period_key)
    dataset = get_progress_dataset(_username(), days)
    labels = dataset["Labels"]
    calories = dataset["Calories"]
    hydration = dataset["Hydration"]
    exercise = dataset["Exercise"]

    max_calories = max(calories) if calories else 1
    max_hydration = max(hydration) if hydration else 1
    max_exercise = max(exercise) if exercise else 1

    rows = []
    for label, cal, hyd, ex in zip(labels, calories, hydration, exercise):
        display_date = label
        try:
            display_date = datetime.strptime(label, "%Y-%m-%d").strftime("%b %d, %Y")
        except ValueError:
            pass

        rows.append(
            {
                "date_key": label,
                "label": display_date,
                "calories": cal,
                "hydration": hyd,
                "exercise": ex,
                "calories_pct": int((cal / max_calories) * 100) if max_calories else 0,
                "hydration_pct": int((hyd / max_hydration) * 100) if max_hydration else 0,
                "exercise_pct": int((ex / max_exercise) * 100) if max_exercise else 0,
            }
        )

    summary = {
        "days": days,
        "total_calories": sum(calories),
        "total_hydration": round(sum(hydration), 2),
        "total_exercise": sum(exercise),
    }

    goals = list_goals(_username())
    completed_goals = [goal for goal in goals if goal["GoalStatus"] == "Completed"]
    completion_pct = int(round((len(completed_goals) / len(goals)) * 100)) if goals else 0

    trend_points = _build_period_trend(rows, period_key)

    avg_minutes = int(round(summary["total_exercise"] / max(days, 1)))
    exercise_status = [
        {
            "label": "Body Composition",
            "value": min(100, max(8, avg_minutes * 2)),
        },
        {
            "label": "Muscular Endurance",
            "value": min(100, max(12, avg_minutes * 3)),
        },
        {
            "label": "Muscular Strength",
            "value": min(100, max(18, avg_minutes * 4)),
        },
        {
            "label": "Cardiovascular Endurance",
            "value": min(100, max(24, avg_minutes * 5)),
        },
        {
            "label": "Flexibility",
            "value": min(100, max(10, avg_minutes * 3)),
        },
    ]

    return {
        "period": period_label,
        "period_key": period_key,
        "period_label": period_label,
        "rows": rows,
        "summary": summary,
        "completion_pct": completion_pct,
        "trend_points": trend_points,
        "exercise_status": exercise_status,
    }


@app.context_processor
def inject_user() -> dict:
    return {"current_user": session.get("username")}


@app.before_request
def enforce_login():
    if request.endpoint is None:
        return None
    if request.endpoint in PUBLIC_ENDPOINTS or request.path.startswith("/static/"):
        return None
    if "username" not in session:
        next_target = request.full_path if request.query_string else request.path
        return redirect(url_for("login", next=next_target))
    return None


@app.route("/")
def index():
    if "username" in session:
        return redirect(url_for("home"))
    return redirect(url_for("login"))


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        first_name = request.form.get("first_name", "").strip() or None
        last_name = request.form.get("last_name", "").strip() or None
        phone_num = request.form.get("phone_num", "").strip() or None

        if not username or not email or not password:
            flash("Username, email, and password are required.", "danger")
            return render_template("register.html")
        if len(password) < 8:
            flash("Password must be at least 8 characters.", "danger")
            return render_template("register.html")

        try:
            create_user(username, email, password, first_name, last_name, phone_num)
            log_action(username, "Registered account")
            flash("Registration complete. You can now log in.", "success")
            return redirect(url_for("login"))
        except sqlite3.IntegrityError:
            flash("Username or email already exists.", "danger")

    return render_template("register.html")


@app.route("/sso/start", methods=["POST"])
def sso_start():
    if "username" in session:
        return redirect(url_for("home"))

    identity = request.form.get("sso_identity", "").strip()
    next_target = request.form.get("next", "").strip()
    if not identity:
        flash("Enter your username or email for SSO.", "danger")
        return redirect(url_for("login", next=next_target))

    ok, message, raw_token = create_sso_token(identity)
    if not ok or not raw_token:
        flash(message, "danger")
        return redirect(url_for("login", next=next_target))

    sso_url = url_for("sso_verify", token=raw_token, next=next_target, _external=True)
    return render_template("sso_link.html", sso_url=sso_url, identity=identity, next_target=next_target)


@app.route("/sso/verify")
def sso_verify():
    if "username" in session:
        return redirect(url_for("home"))

    token = request.args.get("token", "").strip()
    user, error_message = consume_sso_token(token)
    if not user:
        flash(error_message, "danger")
        return redirect(url_for("login"))

    session.clear()
    session["username"] = user["Username"]
    log_action(user["Username"], "Successful SSO login")
    flash("SSO login successful.", "success")
    return redirect(_post_login_redirect_target())


@app.route("/login", methods=["GET", "POST"])
def login():
    if "username" in session:
        return redirect(url_for("home"))

    if request.method == "POST":
        identity = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if not identity or not password:
            flash("Enter both username and password.", "danger")
            return render_template("login.html", next_target=request.form.get("next", "").strip())

        user, error_message = authenticate_user(identity, password)
        if user:
            session.clear()
            session["username"] = user["Username"]
            log_action(user["Username"], "Successful login")
            flash("Login successful.", "success")
            return redirect(_post_login_redirect_target())

        log_action(identity or None, f"Failed login: {error_message}")
        flash(error_message, "danger")

    return render_template("login.html", next_target=request.args.get("next", "").strip())


@app.route("/logout")
def logout():
    username = session.get("username")
    session.clear()
    log_action(username, "Logged out")
    flash("You have been logged out.", "info")
    return redirect(url_for("login"))


@app.route("/home")
def home():
    username = _username()
    summary = get_home_summary(username)
    goals = list_goals(username)
    recent_for_challenges = list_activities(username, 30)
    active_goal_preview = [goal for goal in goals if goal["GoalStatus"] in ("Active", "On Track")][:2]
    goal_preview = active_goal_preview if active_goal_preview else goals[:2]

    return render_template(
        "home.html",
        summary=summary,
        goal_preview=goal_preview,
        activity_preview=summary["RecentActivities"][:2],
        adaptive_challenges=_build_adaptive_challenges(recent_for_challenges),
    )


@app.route("/modifygoals", methods=["GET", "POST"])
def modify_goals():
    username = _username()
    goal_types = list_goal_types()
    valid_goal_type_ids = {int(item["GoalTypeID"]) for item in goal_types}

    if request.method == "POST":
        action = request.form.get("action", "add")

        if action == "status":
            try:
                goal_id = int(request.form.get("goal_id", "0"))
            except ValueError:
                goal_id = 0
            status = request.form.get("status", "").strip()
            if goal_id <= 0 or status not in ALLOWED_GOAL_STATUSES:
                flash("Enter a valid goal update.", "danger")
                return redirect(url_for("modify_goals"))
            update_goal_status(username, goal_id, status)
            log_action(username, f"Updated goal {goal_id} to status {status}")
            flash("Goal status updated.", "success")
        else:
            try:
                goal_type_id = int(request.form.get("goal_type_id", "0"))
                target_value = float(request.form.get("target_value", "0"))
            except ValueError:
                flash("Enter a valid goal type and target value.", "danger")
                return redirect(url_for("modify_goals"))

            start_date = request.form.get("start_date", "").strip() or None
            end_date = request.form.get("end_date", "").strip() or None
            if goal_type_id not in valid_goal_type_ids or target_value <= 0:
                flash("Enter a valid goal type and target value.", "danger")
                return redirect(url_for("modify_goals"))
            if start_date and end_date and end_date < start_date:
                flash("End date must be on or after start date.", "danger")
                return redirect(url_for("modify_goals"))

            add_goal(username, goal_type_id, target_value, start_date, end_date)
            log_action(username, "Added a goal")
            flash("Goal added.", "success")

        return redirect(url_for("modify_goals"))

    return render_template(
        "modify_goals.html",
        goal_types=goal_types,
        goals=list_goals(username),
        today=datetime.now().date().isoformat(),
    )


@app.route("/modifyactivities", methods=["GET", "POST"])
def modify_activities():
    username = _username()

    if request.method == "POST":
        try:
            activity_type = request.form.get("type", "").strip()
            duration_minutes, duration_source = _derive_activity_duration(
                activity_type=activity_type,
                duration_raw=request.form.get("duration_minutes"),
                start_clock=request.form.get("start_time"),
                end_clock=request.form.get("end_time"),
            )
            calories = _optional_int(request.form.get("calories"))
            distance_km = _optional_float(request.form.get("distance_km"))
            activity_date = request.form.get("activity_date")
            difficulty = request.form.get("difficulty", "Standard").strip().title()
            if difficulty not in WORKOUT_DIFFICULTIES:
                difficulty = "Standard"
            source = "Manual"

            if not activity_type or duration_minutes <= 0:
                raise ValueError

            if activity_date:
                start_clock = request.form.get("start_time", "").strip()
                if _clock_to_minutes(start_clock) is not None:
                    activity_date = f"{activity_date} {start_clock}:00"
                else:
                    activity_date = f"{activity_date} 00:00:00"

            add_activity(
                username=username,
                activity_type=activity_type,
                duration_minutes=duration_minutes,
                calories_burnt=calories,
                distance_km=distance_km,
                activity_date=activity_date,
                source=source,
                difficulty=difficulty,
            )
            xp_earned = workout_xp_value(duration_minutes, difficulty)
            xp_state = award_xp(username, xp_earned)
            log_action(
                username,
                (
                    f"Added activity: {activity_type} ({duration_minutes} min, {difficulty}, "
                    f"duration_source={duration_source}, xp={xp_earned})"
                ),
            )
            level_suffix = ""
            if xp_state.get("LeveledUp"):
                level_suffix = f" Level up! You are now level {xp_state['Level']}."
            flash(
                f"Activity logged ({duration_minutes} min). +{xp_earned} XP earned for {difficulty} difficulty.{level_suffix}",
                "success",
            )
        except ValueError:
            flash("Enter a valid activity and duration.", "danger")

        return redirect(url_for("modify_activities"))

    activities = list_activities(username)
    return render_template(
        "modify_activities.html",
        activities=activities,
        today=datetime.now().date().isoformat(),
    )


@app.route("/game", methods=["GET", "POST"])
def game():
    username = _username()

    if request.method == "POST":
        action = request.form.get("action", "")

        if action == "set_mode":
            requested_mode = request.form.get("mode", "Solo")
            mode = requested_mode if requested_mode in GAME_MODES else "Solo"
            difficulty = session.get("game_difficulty", "Standard")
            session["game_mode"] = mode
            if mode == "Solo":
                _reset_game("Solo", difficulty)
            else:
                session.pop("game_state", None)
            log_action(username, f"Started {mode} game")
            return redirect(url_for("game"))

        if action == "set_difficulty":
            requested_difficulty = request.form.get("difficulty", "Standard")
            difficulty = requested_difficulty if requested_difficulty in GAME_DIFFICULTIES else "Standard"
            mode = session.get("game_mode", "Solo")
            session["game_difficulty"] = difficulty
            if mode == "Solo":
                _reset_game("Solo", difficulty)
                log_action(username, f"Started {difficulty} Solo game")
            else:
                log_action(username, f"Set Co-op difficulty to {difficulty}")
            return redirect(url_for("game"))

        mode = session.get("game_mode", "Solo")
        if mode == "Co-op":
            if action == "send_invite":
                target_username = request.form.get("target_username", "").strip()
                ok, message = create_coop_invite(username, target_username)
                log_action(username, f"Co-op invite to {target_username}: {message}")
                flash(message, "success" if ok else "danger")
                return redirect(url_for("game"))

            if action == "cancel_invite":
                try:
                    invite_id = int(request.form.get("invite_id", "0"))
                except ValueError:
                    invite_id = 0
                ok, message = cancel_coop_invite(username, invite_id)
                log_action(username, f"Cancel co-op invite {invite_id}: {message}")
                flash(message, "success" if ok else "danger")
                return redirect(url_for("game"))

            if action == "respond_invite":
                try:
                    invite_id = int(request.form.get("invite_id", "0"))
                except ValueError:
                    invite_id = 0
                decision = request.form.get("decision", "decline")

                if decision == "accept":
                    invite = get_pending_coop_invite_for_user(username, invite_id)
                    if not invite:
                        flash("Invite not found.", "danger")
                        return redirect(url_for("game"))

                    difficulty = session.get("game_difficulty", "Standard")
                    initial_state = _new_coop_match_state(
                        player_one=invite["FromUsername"],
                        player_two=username,
                        difficulty=difficulty,
                    )
                    ok, message, _match_id = respond_coop_invite(
                        username=username,
                        invite_id=invite_id,
                        decision="accept",
                        initial_state_json=json.dumps(initial_state),
                        turn_username=invite["FromUsername"],
                    )
                else:
                    ok, message, _match_id = respond_coop_invite(
                        username=username,
                        invite_id=invite_id,
                        decision="decline",
                        initial_state_json=None,
                        turn_username=None,
                    )

                log_action(username, f"Responded to co-op invite {invite_id}: {decision}")
                flash(message, "success" if ok else "danger")
                return redirect(url_for("game"))

            if action == "leave_match":
                try:
                    match_id = int(request.form.get("match_id", "0"))
                except ValueError:
                    match_id = 0
                ok, message = abandon_coop_match(username, match_id)
                log_action(username, f"Leave co-op match {match_id}: {message}")
                flash(message, "success" if ok else "danger")
                return redirect(url_for("game"))

            if action in {"play_card", "draw_card"}:
                try:
                    match_id = int(request.form.get("match_id", "0"))
                except ValueError:
                    match_id = 0

                match_row = get_coop_match_for_user(username, match_id)
                if not match_row or match_row.get("MatchStatus") != "Active":
                    flash("Active co-op match not found.", "danger")
                    return redirect(url_for("game"))

                try:
                    coop_state = json.loads(match_row["StateJson"])
                except json.JSONDecodeError:
                    flash("Co-op match state is corrupted.", "danger")
                    return redirect(url_for("game"))

                if action == "play_card":
                    try:
                        card_index = int(request.form.get("card_index", "-1"))
                    except ValueError:
                        card_index = -1
                    ok, message = _coop_play_card(coop_state, username, card_index)
                else:
                    message = _coop_draw_card(coop_state, username)
                    ok = True

                if not ok:
                    flash(message, "danger")
                else:
                    log_action(username, f"Co-op action {action} in match {match_id}")

                if coop_state.get("status") == "finished":
                    _persist_coop_rewards(coop_state)
                    db_status = "Finished"
                else:
                    db_status = "Active"

                update_coop_match_state(
                    match_id=match_id,
                    state_json=json.dumps(coop_state),
                    turn_username=coop_state.get("turn_username"),
                    status=db_status,
                    winner=coop_state.get("winner"),
                )
                return redirect(url_for("game"))

            if action == "new_game":
                active_match = get_active_coop_match_for_user(username)
                if not active_match:
                    flash("No active co-op match to reset.", "danger")
                    return redirect(url_for("game"))

                try:
                    current_state = json.loads(active_match["StateJson"])
                except json.JSONDecodeError:
                    flash("Could not reset co-op match state.", "danger")
                    return redirect(url_for("game"))

                new_state = _new_coop_match_state(
                    player_one=active_match["PlayerOne"],
                    player_two=active_match["PlayerTwo"],
                    difficulty=current_state.get("difficulty", session.get("game_difficulty", "Standard")),
                )
                update_coop_match_state(
                    match_id=int(active_match["MatchID"]),
                    state_json=json.dumps(new_state),
                    turn_username=new_state.get("turn_username"),
                    status="Active",
                    winner=None,
                )
                log_action(username, f"Reset co-op match {active_match['MatchID']}")
                return redirect(url_for("game"))

            return redirect(url_for("game"))

        state = _get_or_create_game_state()

        if action == "new_game":
            state = _reset_game(state["mode"], state.get("difficulty", "Standard"))
            log_action(username, f"Reset {state['difficulty']} {state['mode']} game")
        elif action == "play_card":
            try:
                card_index = int(request.form.get("card_index", "-1"))
            except ValueError:
                card_index = -1
            ok, message = _play_player_card(state, username, card_index)
            if ok:
                log_action(username, f"Played card index {card_index} ({state['mode']})")
            else:
                state["message"] = message
        elif action == "draw_card":
            state["message"] = _draw_player_card(state, username)
            log_action(username, f"Drew card ({state['mode']})")

        session["game_state"] = state
        return redirect(url_for("game"))

    mode = session.get("game_mode", "Solo")
    if mode == "Co-op":
        active_match = get_active_coop_match_for_user(username)
        if active_match:
            try:
                state = json.loads(active_match["StateJson"])
            except json.JSONDecodeError:
                flash("Could not load co-op match state.", "danger")
                return render_template("game.html", game=_coop_lobby_context(username))

            return render_template(
                "game.html",
                game=_build_coop_match_context(username, int(active_match["MatchID"]), state),
            )

        return render_template("game.html", game=_coop_lobby_context(username))

    state = _get_or_create_game_state()
    return render_template("game.html", game=_game_template_context(state))


@app.route("/sologame")
def solo_game():
    _reset_game("Solo", session.get("game_difficulty", "Standard"))
    log_action(_username(), "Opened solo game")
    return redirect(url_for("game"))


@app.route("/coopgame")
def coop_game():
    session["game_mode"] = "Co-op"
    if session.get("game_difficulty") not in GAME_DIFFICULTIES:
        session["game_difficulty"] = "Standard"
    session.pop("game_state", None)
    log_action(_username(), "Opened co-op game")
    return redirect(url_for("game"))


@app.route("/help")
def help_support_alias():
    return redirect(url_for("search"))


@app.route("/search", methods=["GET", "POST"])
def search():
    username = _username()
    query = request.args.get("q", "").strip()

    if request.method == "POST":
        mood = request.form.get("mood", "").strip()
        if mood:
            if len(mood) > 80:
                flash("Mood is too long. Keep it under 80 characters.", "danger")
            else:
                update_mood(username, mood)
                log_action(username, f"Updated mood to: {mood}")
                flash("Mood saved.", "success")
        return redirect(url_for("search", q=query))

    if query:
        search_payload = search_health_topics(query)
    else:
        search_payload = {"Results": [], "Algorithm": "none", "Note": "Enter a topic to search."}

    results = search_payload["Results"]
    health = get_health(username)
    return render_template(
        "search.html",
        query=query,
        search_meta=search_payload,
        results=results,
        health=health,
    )


@app.route("/progress")
def progress():
    return render_template("progress.html", **_progress_context(period_key=request.args.get("period")))


@app.route("/viewprogressweek")
def view_progress_week():
    return redirect(url_for("progress", period="weekly"))


@app.route("/viewprogressmonth")
def view_progress_month():
    return redirect(url_for("progress", period="monthly"))


@app.route("/viewprogressyear")
def view_progress_year():
    return redirect(url_for("progress", period="yearly"))


@app.route("/calories", methods=["GET", "POST"])
def calories():
    username = _username()

    if request.method == "POST":
        try:
            intake = int(request.form.get("calorie_intake", "0"))
            log_date = request.form.get("log_date") or None
            if intake <= 0:
                raise ValueError
            add_calorie_log(username, intake, log_date)
            log_action(username, f"Logged calories: {intake}")
            flash("Calorie intake logged.", "success")
        except ValueError:
            flash("Enter a valid calorie amount.", "danger")
        return redirect(url_for("calories"))

    return render_template(
        "calories.html",
        logs=list_calorie_logs(username),
        recommendation=calorie_recommendation(username),
        today=datetime.now().date().isoformat(),
    )


@app.route("/hydration", methods=["GET", "POST"])
def hydration():
    username = _username()

    if request.method == "POST":
        try:
            intake = float(request.form.get("hydration_intake", "0"))
            entry_date = request.form.get("entry_date") or None
            if intake <= 0:
                raise ValueError
            add_hydration_log(username, intake, entry_date)
            log_action(username, f"Logged hydration: {intake}L")
            flash("Hydration intake logged.", "success")
        except ValueError:
            flash("Enter a valid hydration amount.", "danger")
        return redirect(url_for("hydration"))

    return render_template(
        "hydration.html",
        logs=list_hydration_logs(username),
        recommendation=hydration_recommendation(username),
        today=datetime.now().date().isoformat(),
    )


@app.route("/exercise", methods=["GET", "POST"])
def exercise():
    username = _username()
    goal_types = list_goal_types()
    valid_goal_type_ids = {int(item["GoalTypeID"]) for item in goal_types}

    if request.method == "POST":
        action = request.form.get("action", "")
        if action == "add_goal_from_exercise":
            try:
                goal_type_id = int(request.form.get("goal_type_id", "0"))
                target_value = float(request.form.get("target_value", "0"))
                if goal_type_id not in valid_goal_type_ids or target_value <= 0:
                    raise ValueError

                add_goal(
                    username=username,
                    goal_type_id=goal_type_id,
                    target_value=target_value,
                    start_date=datetime.now().date().isoformat(),
                    end_date=None,
                )
                log_action(
                    username,
                    f"Added suggested goal from exercise data (type={goal_type_id}, target={target_value})",
                )
                flash("Goal added from exercise data.", "success")
            except ValueError:
                flash("Unable to add that goal. Refresh and try again.", "danger")

        return redirect(url_for("exercise"))

    activities = list_activities(username, 200)
    goals = list_goals(username)
    suggestions = _build_exercise_goal_suggestions(activities, goal_types)
    active_goal_type_ids = {
        int(goal["GoalTypeID"])
        for goal in goals
        if goal.get("GoalStatus") in ("Active", "On Track")
    }
    for item in suggestions:
        item["already_active"] = int(item["goal_type_id"]) in active_goal_type_ids

    return render_template(
        "exercise.html",
        activities=activities,
        goal_suggestions=suggestions,
        category_suggestions=_build_category_exercise_suggestions(goals, activities),
    )


@app.route("/friends", methods=["GET", "POST"])
def friends():
    username = _username()
    query = request.args.get("q", "").strip()

    if request.method == "POST":
        action = request.form.get("action")
        if action == "send":
            target_username = request.form.get("target_username", "").strip()
            ok, message = send_friend_request(username, target_username)
            log_action(username, f"Friend request to {target_username}: {message}")
            flash(message, "success" if ok else "danger")
        elif action == "create_link":
            payload = create_friend_invite_link(username=username)
            invite_url = url_for("accept_friend_invite", token=payload["Token"], _external=True)
            session["latest_friend_invite_url"] = invite_url
            log_action(username, "Created friend invite link")
            flash("Invite link created.", "success")
        elif action == "disable_link":
            token_hash = request.form.get("friend_invite_link_key", "").strip()
            ok, message = disable_friend_invite_link(username, token_hash)
            log_action(username, f"Disable friend invite link: {message}")
            flash(message, "success" if ok else "danger")
        elif action == "respond":
            try:
                friendship_id = int(request.form.get("friendship_id", "0"))
            except ValueError:
                friendship_id = 0
            decision = request.form.get("decision", "reject").strip().lower()
            if friendship_id <= 0 or decision not in {"accept", "reject"}:
                flash("Invalid friend request action.", "danger")
                return redirect(url_for("friends", q=query))
            ok, message = respond_friend_request(username, friendship_id, decision)
            log_action(username, f"Friend request {friendship_id} response: {decision}")
            flash(message, "success" if ok else "danger")

        return redirect(url_for("friends", q=query))

    results = search_users(query, username) if query else []
    friend_data = get_friend_data(username)
    invite_links = list_friend_invite_links(username)
    for item in invite_links:
        public_token = str(item.get("PublicToken") or "").strip()
        item["url"] = url_for("accept_friend_invite", token=public_token, _external=True) if public_token else ""
    return render_template(
        "friends.html",
        query=query,
        results=results,
        friend_data=friend_data,
        invite_links=invite_links,
        latest_invite_url=session.pop("latest_friend_invite_url", ""),
    )


@app.route("/friends/invite/<token>")
def accept_friend_invite(token: str):
    username = _username()
    ok, message = accept_friend_invite_link(token, username)
    log_action(username, f"Invite link use result: {message}")
    flash(message, "success" if ok else "danger")
    return redirect(url_for("friends"))


@app.route("/editavatar", methods=["GET", "POST"])
def edit_avatar():
    username = _username()

    if request.method == "POST":
        try:
            avatar_id = int(request.form.get("avatar_id", "0"))
        except ValueError:
            avatar_id = 0
        if avatar_id <= 0:
            flash("Please choose a valid avatar.", "danger")
            return redirect(url_for("edit_avatar"))
        ok, message = set_avatar(username, avatar_id)
        log_action(username, f"Avatar change attempt {avatar_id}: {message}")
        flash(message, "success" if ok else "danger")
        return redirect(url_for("edit_avatar"))

    profile_data = _decorate_avatar_item(get_profile(username))
    avatars = [_decorate_avatar_item(item) for item in list_avatars()]
    return render_template(
        "edit_avatar.html",
        avatars=avatars,
        profile=profile_data,
    )


@app.route("/personal", methods=["GET", "POST"])
def personal():
    username = _username()

    if request.method == "POST":
        email = request.form.get("email", "").strip()
        first_name = request.form.get("first_name", "").strip() or None
        last_name = request.form.get("last_name", "").strip() or None
        phone_num = request.form.get("phone_num", "").strip() or None

        if not email:
            flash("Email is required.", "danger")
        else:
            try:
                update_personal_info(username, email, first_name, last_name, phone_num)
                log_action(username, "Updated personal information")
                flash("Personal details updated.", "success")
                return redirect(url_for("personal"))
            except sqlite3.IntegrityError:
                flash("Email already exists.", "danger")

    return render_template("personal.html", user=get_user(username))


@app.route("/edithealth", methods=["GET", "POST"])
def edit_health():
    username = _username()

    if request.method == "POST":
        try:
            update_health(
                username=username,
                age=_optional_int(request.form.get("age")),
                sex=request.form.get("sex", "").strip() or None,
                weight_kg=_optional_float(request.form.get("weight_kg")),
                height_cm=_optional_float(request.form.get("height_cm")),
                activity_level=request.form.get("activity_level", "").strip() or None,
                overall_health=request.form.get("overall_health", "").strip() or None,
                health_conditions=request.form.get("health_conditions", "").strip() or None,
                diet_profile=request.form.get("diet_profile", "").strip() or None,
                climate=request.form.get("climate", "").strip() or None,
                mood=request.form.get("mood", "").strip() or None,
            )
            log_action(username, "Updated health status")
            flash("Health status saved.", "success")
            return redirect(url_for("edit_health"))
        except ValueError:
            flash("Invalid number in health form.", "danger")

    return render_template(
        "edit_health.html",
        health=get_health(username),
        activity_levels=["sedentary", "light", "moderate", "active", "very active"],
        climate_options=["cold", "temperate", "humid", "dry", "hot"],
    )


@app.route("/profile")
def profile():
    username = _username()
    profile_data = _decorate_avatar_item(get_profile(username))
    friend_data = get_friend_data(username)

    return render_template(
        "profile.html",
        profile=profile_data,
        health=get_health(username),
        goals=list_goals(username)[:5],
        activities=list_activities(username, 5),
        friend_data=friend_data,
        personalized_tips=personalized_health_tips(username),
    )


@app.errorhandler(Exception)
def handle_unexpected_error(error: Exception):
    if isinstance(error, HTTPException):
        return error

    log_error(session.get("username"), traceback.format_exc())
    flash("An unexpected error occurred. The issue was logged to instance/errors.log.", "danger")
    if "username" in session:
        return redirect(url_for("home"))
    return redirect(url_for("login"))


if __name__ == "__main__":
    app.run(debug=True)
