import os
import re
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List

import requests
from dotenv import load_dotenv


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
COOLDOWN_SECONDS = int(os.getenv("SWIPE_COOLDOWN_SECONDS", "15"))

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY in .env")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}


def now_utc_iso() -> str:
    """Return the current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(iso_ts: str) -> datetime:
    """
    Safely parse ISO timestamps from Supabase across Raspberry Pi Python versions.

    Handles examples like:
    - 2026-03-21T20:51:40.00172+00:00
    - 2026-03-21T20:51:40+00:00
    - 2026-03-21T20:51:40.00172Z
    - 2026-03-21T20:51:40Z
    """
    ts = iso_ts.strip()

    if ts.endswith("Z"):
        ts = ts[:-1] + "+0000"
    elif len(ts) >= 6 and ts[-6] in ["+", "-"] and ts[-3] == ":":
        # Convert timezone from +00:00 to +0000 for stricter Python builds.
        ts = ts[:-3] + ts[-2:]

    formats = [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue

    raise ValueError(f"Invalid ISO timestamp: {iso_ts}")


def seconds_since_iso(iso_ts: str) -> float:
    """Return elapsed seconds between now (UTC) and the provided ISO timestamp."""
    dt = parse_iso_datetime(iso_ts)
    return (datetime.now(timezone.utc) - dt).total_seconds()


def extract_card_id(raw_swipe: str) -> str:
    """
    Pull the most useful card identifier from a raw magstripe swipe string.

    Current strategy:
    1. Strip whitespace
    2. Remove common sentinels
    3. Look for digit groups of length 4+
    4. Return the last digit group if found
    5. Otherwise return the cleaned raw string
    """
    cleaned = raw_swipe.strip()
    cleaned = cleaned.replace("%", "").replace("?", "").strip()

    digit_groups = re.findall(r"\d{4,}", cleaned)
    if digit_groups:
        return digit_groups[-1]

    return cleaned


def supabase_get(table: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    response = requests.get(url, headers=HEADERS, params=params, timeout=15)
    response.raise_for_status()
    return response.json()


def supabase_insert(table: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = HEADERS.copy()
    headers["Prefer"] = "return=representation"
    response = requests.post(url, headers=headers, json=payload, timeout=15)
    response.raise_for_status()
    return response.json()


def find_person_by_card(card_id: str) -> Optional[Dict[str, Any]]:
    rows = supabase_get(
        "people",
        params={
            "select": "*",
            "card_id": f"eq.{card_id}",
            "limit": 1,
        },
    )
    return rows[0] if rows else None


def get_last_swipe(card_id: str) -> Optional[Dict[str, Any]]:
    rows = supabase_get(
        "swipe_events",
        params={
            "select": "id,card_id,event_type,swiped_at",
            "card_id": f"eq.{card_id}",
            "order": "swiped_at.desc",
            "limit": 1,
        },
    )
    return rows[0] if rows else None


def get_last_swipe_today(card_id: str) -> Optional[Dict[str, Any]]:
    """
    Find the most recent swipe for this card since today's UTC midnight.
    """
    now = datetime.now(timezone.utc)
    today_midnight_utc = now.replace(hour=0, minute=0, second=0, microsecond=0)

    rows = supabase_get(
        "swipe_events",
        params={
            "select": "id,card_id,event_type,swiped_at",
            "card_id": f"eq.{card_id}",
            "swiped_at": f"gte.{today_midnight_utc.isoformat()}",
            "order": "swiped_at.desc",
            "limit": 1,
        },
    )
    return rows[0] if rows else None


def determine_event_type(card_id: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Alternate IN/OUT based on the latest swipe today.
    If there is no swipe today, default to IN.
    """
    last_today = get_last_swipe_today(card_id)

    if not last_today:
        return "IN", None

    last_type = last_today.get("event_type", "IN")
    if last_type == "IN":
        return "OUT", last_today
    return "IN", last_today


def should_ignore_duplicate(card_id: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Ignore repeated swipes from the same card within the cooldown window.
    """
    last_swipe = get_last_swipe(card_id)
    if not last_swipe:
        return False, None

    elapsed = seconds_since_iso(last_swipe["swiped_at"])
    if elapsed < COOLDOWN_SECONDS:
        return True, last_swipe

    return False, last_swipe


def log_swipe(raw_swipe: str) -> None:
    raw_swipe = raw_swipe.strip()
    if not raw_swipe:
        print("Empty swipe ignored.")
        return

    card_id = extract_card_id(raw_swipe)
    print(f"\nRaw swipe: {raw_swipe}")
    print(f"Parsed card_id: {card_id}")

    duplicate, last_swipe = should_ignore_duplicate(card_id)
    if duplicate and last_swipe is not None:
        elapsed = seconds_since_iso(last_swipe["swiped_at"])
        print(
            f"Ignored duplicate swipe. "
            f"Last swipe was {elapsed:.1f}s ago, under {COOLDOWN_SECONDS}s cooldown."
        )
        return

    person = find_person_by_card(card_id)
    event_type, last_today = determine_event_type(card_id)

    payload = {
        "person_id": person["id"] if person else None,
        "card_id": card_id,
        "event_type": event_type,
        "swiped_at": now_utc_iso(),
        "raw_swipe": raw_swipe,
        "source": "minimag",
    }

    inserted = supabase_insert("swipe_events", payload)
    row = inserted[0] if inserted else payload

    if person:
        print(
            f"Logged swipe for {person.get('full_name') or 'Unknown Name'} "
            f"({card_id}) -> {row['event_type']}"
        )
    else:
        print(f"Logged swipe for unknown card ({card_id}) -> {row['event_type']}")

    if last_today:
        print(
            f"Previous swipe today was {last_today['event_type']} "
            f"at {last_today['swiped_at']}"
        )
    else:
        print("No previous swipe today. Defaulted to IN.")


def main() -> None:
    print("MiniMag Swipe App")
    print("Focus this terminal, then swipe a card.")
    print("If your reader acts like a keyboard wedge, the swipe should appear as text.")
    print("Press Ctrl+C to quit.\n")

    while True:
        try:
            raw = input("Swipe now: ")
            log_swipe(raw)
        except KeyboardInterrupt:
            print("\nExiting.")
            break
        except requests.HTTPError as e:
            print(f"HTTP error: {e}")
            try:
                print("Response:", e.response.text)
            except Exception:
                pass
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            print("-" * 50)


if __name__ == "__main__":
    main()
