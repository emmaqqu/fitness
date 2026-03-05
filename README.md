# FitQuest (Flask + SQLite)

PDF-aligned implementation of the IA2 design using **Python Flask** and **SQLite3** with **Bootstrap templates**.

## Features Implemented

- Authentication with registration and login lockout after 5 failed attempts
- Full route set from the design:
  - `/login` (default root), `/home`, `/modifygoals`, `/modifyactivities`
  - `/game`, `/sologame`, `/coopgame`
  - `/search`, `/viewprogressweek`, `/viewprogressmonth`, `/viewprogressyear`
  - `/calories`, `/hydration`, `/exercise`
  - `/friends`, `/editavatar`, `/personal`, `/edithealth`, `/profile`, `/logout`
- SQLite schema covering users, profiles, avatars, goals, activities, calories, hydration, game sessions, friends, and health
- Action logging to `instance/actions.log`
- Progress views (week/month/year) rendered server-side in Bootstrap tables
- XP + level progression and avatar unlock checks

## Skill Assumptions

- No custom JavaScript knowledge required
- Python knowledge expected: variables, loops, lists, strings, dictionaries, functions
- All business logic is in Python (`app.py`, `db.py`) and templates are plain Jinja + Bootstrap

## Setup

```bash
./venv/bin/pip install -r requirements.txt
```

## Run

```bash
./venv/bin/python run.py
```

Open `http://127.0.0.1:5000`.

Optional environment variables:

```bash
HOST=0.0.0.0 PORT=5000 DEBUG=true ./venv/bin/python run.py
```
