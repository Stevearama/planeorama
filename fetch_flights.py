#!/usr/bin/env python3
"""
Planeorama — Daily flight data collector
========================================
Fetches departure data from the OpenSky Network API and stores per-airport,
per-year CSVs in Google Drive. Run daily via GitHub Actions.

Daily flow
----------
Phase 1 — Forward fill
    Ensure every airport has data up to yesterday. Runs first, always.

Phase 2 — Backfill
    Use remaining credits to push history back toward BACKFILL_TO_DATE.
    Credits are divided equally across airports that still need work, so
    all airports stay at a comparable depth. Airports with the least
    history are processed first within each run.

State persistence
-----------------
frontiers.json      — oldest/latest date fetched per airport (on Drive)
aircraft_cache.json — icao24 → aircraft metadata (on Drive)
airlines.csv        — OpenFlights airline name table (on Drive)
"""

import csv
import io
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# ── User-editable config ───────────────────────────────────────────────────────

AIRPORTS = [
    "KATL",   # Atlanta Hartsfield-Jackson
    "KLAX",   # Los Angeles International
    "KORD",   # Chicago O'Hare
    "KDFW",   # Dallas / Fort Worth
    "KDEN",   # Denver International
    "KJFK",   # New York JFK
    "KSFO",   # San Francisco International
    "KLAS",   # Las Vegas Harry Reid
    "KSEA",   # Seattle-Tacoma
    "KMIA",   # Miami International
]

# Oldest date to backfill to. Lower this variable over time to extend history.
BACKFILL_TO_DATE = date(2024, 1, 1)

# Stop backfilling when this many flight-endpoint credits remain.
# 500 covers ~16 forward-fill windows, so tomorrow's Phase 1 will always have room.
CREDIT_BUFFER = 500

# Window size for each API call. 1–2 days = 30 credits; 3+ days costs 4× more.
# Do not change this above 2.
QUERY_DAYS = 2

# Courtesy pause between flight-data API calls (seconds).
CALL_DELAY = 0.5

# Pause between aircraft metadata calls on cache misses (seconds).
AIRCRAFT_CALL_DELAY = 0.2

# Upload the aircraft cache to Drive every N fetch windows.
CACHE_FLUSH_EVERY = 15

# ── API endpoints ──────────────────────────────────────────────────────────────

OPENSKY_BASE     = "https://opensky-network.org/api"
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
OPENFLIGHTS_AIRLINES_URL = (
    "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"
)

# Drive filenames for shared state
FRONTIERS_FILENAME      = "frontiers.json"
AIRCRAFT_CACHE_FILENAME = "aircraft_cache.json"
AIRLINES_FILENAME       = "airlines.csv"

# ── CSV output columns ─────────────────────────────────────────────────────────

FIELDS = [
    "date",
    "departure_airport",
    "icao24",
    "callsign",
    "carrier_icao",
    "carrier_name",
    "departure_time_utc",
    "destination_airport",
    "registration",
    "manufacturer",
    "model",
    "typecode",
]

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ── OAuth2 token manager ───────────────────────────────────────────────────────

class TokenManager:
    """Fetches and auto-refreshes an OpenSky OAuth2 client-credentials token."""

    def __init__(self, client_id: str, client_secret: str):
        self._id      = client_id
        self._secret  = client_secret
        self._token   = ""
        self._expires = 0.0

    def header(self) -> dict:
        return {"Authorization": f"Bearer {self._get()}"}

    def _get(self) -> str:
        if time.time() >= self._expires:
            self._refresh()
        return self._token

    def _refresh(self):
        r = requests.post(
            OPENSKY_TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     self._id,
                "client_secret": self._secret,
            },
            timeout=20,
        )
        r.raise_for_status()
        body          = r.json()
        self._token   = body["access_token"]
        self._expires = time.time() + body.get("expires_in", 1800) - 60
        log.info("OAuth2 token refreshed.")

# ── Airline lookup ─────────────────────────────────────────────────────────────

def load_airlines(drive) -> dict:
    """Returns {ICAO_3_letter: full_airline_name}. Cached on Drive."""
    raw = drive.download(AIRLINES_FILENAME)
    if not raw:
        log.info("Downloading OpenFlights airlines database...")
        r = requests.get(OPENFLIGHTS_AIRLINES_URL, timeout=30)
        r.raise_for_status()
        raw = r.content
        drive.upload(AIRLINES_FILENAME, raw, mime="text/plain")

    result = {}
    for row in csv.reader(io.StringIO(raw.decode("utf-8", errors="replace"))):
        if len(row) < 5:
            continue
        icao = row[4].strip().strip('"')
        name = row[1].strip().strip('"')
        if icao and icao not in (r"\N", "N/A", ""):
            result[icao] = name

    log.info("Airlines loaded: %d entries", len(result))
    return result


def carrier_from_callsign(callsign: str, airlines: dict) -> tuple:
    """Returns (icao_3letter, full_name) from the first 3 chars of a callsign."""
    cs = (callsign or "").strip()
    if len(cs) < 3:
        return ("", "")
    code = cs[:3].upper()
    return (code, airlines.get(code, ""))

# ── Aircraft metadata cache ────────────────────────────────────────────────────

def load_aircraft_cache(drive) -> dict:
    data = drive.download_json(AIRCRAFT_CACHE_FILENAME)
    cache = data if isinstance(data, dict) else {}
    log.info("Aircraft cache: %d entries", len(cache))
    return cache


def lookup_aircraft(icao24: str, cache: dict, tokens: TokenManager) -> dict:
    """Returns aircraft metadata dict; uses in-memory cache, falls back to API."""
    key = icao24.lower().strip()
    if not key or key in cache:
        return cache.get(key, {})

    time.sleep(AIRCRAFT_CALL_DELAY)
    try:
        r = requests.get(
            f"{OPENSKY_BASE}/metadata/aircraft/icao/{key}",
            headers=tokens.header(),
            timeout=15,
        )
        if r.status_code == 200:
            cache[key] = r.json()
        elif r.status_code == 429:
            wait = int(r.headers.get("X-Rate-Limit-Retry-After-Seconds", 60))
            log.warning("Aircraft lookup rate-limited — waiting %ds", wait)
            time.sleep(wait)
            return lookup_aircraft(icao24, cache, tokens)
        else:
            cache[key] = {}
    except requests.RequestException as exc:
        log.warning("Aircraft lookup failed for %s: %s", key, exc)
        cache[key] = {}

    return cache.get(key, {})

# ── OpenSky departures ─────────────────────────────────────────────────────────

def fetch_departures(
    airport: str,
    w_start: date,
    w_end: date,
    tokens: TokenManager,
) -> tuple:
    """
    Returns (flights, credits_remaining).
    Queries [w_start 00:00 UTC, w_end 00:00 UTC).
    credits_remaining is None when the header is absent (e.g. on 404).
    """
    begin_ts = int(datetime(w_start.year, w_start.month, w_start.day, tzinfo=timezone.utc).timestamp())
    end_ts   = int(datetime(w_end.year,   w_end.month,   w_end.day,   tzinfo=timezone.utc).timestamp())

    for attempt in range(3):
        try:
            time.sleep(CALL_DELAY)
            r = requests.get(
                f"{OPENSKY_BASE}/flights/departure",
                params={"airport": airport, "begin": begin_ts, "end": end_ts},
                headers=tokens.header(),
                timeout=30,
            )
            raw_remaining = r.headers.get("X-Rate-Limit-Remaining")
            credits = int(raw_remaining) if raw_remaining else None

            if r.status_code == 200:
                return (r.json() or [], credits)
            elif r.status_code == 404:
                return ([], credits)
            elif r.status_code == 429:
                wait = int(r.headers.get("X-Rate-Limit-Retry-After-Seconds", 60))
                log.warning("Rate limited — waiting %ds (attempt %d)", wait, attempt + 1)
                time.sleep(wait)
            elif r.status_code == 401:
                log.info("Token expired mid-fetch, retrying...")
            else:
                log.warning("HTTP %d for %s %s→%s", r.status_code, airport, w_start, w_end)
                return ([], None)
        except requests.RequestException as exc:
            log.warning("Request error attempt %d: %s", attempt + 1, exc)
            time.sleep(5 * (attempt + 1))

    log.error("All retries failed for %s %s→%s", airport, w_start, w_end)
    return ([], None)

# ── Row builder ────────────────────────────────────────────────────────────────

def build_row(flight: dict, aircraft: dict, carrier_icao: str, carrier_name: str) -> dict:
    ts = flight.get("firstSeen") or flight.get("lastSeen")
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
    return {
        "date":                dt.strftime("%Y-%m-%d")          if dt else "",
        "departure_airport":   flight.get("estDepartureAirport", ""),
        "icao24":              flight.get("icao24", ""),
        "callsign":            (flight.get("callsign") or "").strip(),
        "carrier_icao":        carrier_icao,
        "carrier_name":        carrier_name,
        "departure_time_utc":  dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "",
        "destination_airport": flight.get("estArrivalAirport", ""),
        "registration":        aircraft.get("registration", ""),
        "manufacturer":        aircraft.get("manufacturername", ""),
        "model":               aircraft.get("model", ""),
        "typecode":            aircraft.get("typecode", ""),
    }

# ── CSV helpers ────────────────────────────────────────────────────────────────

def rows_to_csv_bytes(rows: list) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def csv_bytes_to_rows(data: bytes) -> list:
    if not data:
        return []
    return list(csv.DictReader(io.StringIO(data.decode("utf-8", errors="replace"))))

# ── Frontier helpers ───────────────────────────────────────────────────────────

def get_earliest(frontiers: dict, airport: str) -> date | None:
    val = frontiers.get(airport, {}).get("earliest")
    return date.fromisoformat(val) if val else None


def get_latest(frontiers: dict, airport: str) -> date | None:
    val = frontiers.get(airport, {}).get("latest")
    return date.fromisoformat(val) if val else None


def update_frontier(frontiers: dict, airport: str, earliest: date = None, latest: date = None):
    if airport not in frontiers:
        frontiers[airport] = {}
    if earliest is not None:
        frontiers[airport]["earliest"] = earliest.isoformat()
    if latest is not None:
        frontiers[airport]["latest"] = latest.isoformat()


def date_windows(start: date, end: date):
    """Yields (w_start, w_end) pairs covering [start, end) in QUERY_DAYS steps."""
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=QUERY_DAYS), end)
        yield cur, nxt
        cur = nxt

# ── Core fetch-and-store logic ─────────────────────────────────────────────────

def fetch_windows(
    airport: str,
    windows: list,
    tokens: TokenManager,
    airlines: dict,
    aircraft_cache: dict,
    drive,
    cache_flush_counter: list,
) -> tuple:
    """
    Fetches a list of (w_start, w_end) windows for one airport.
    Downloads the relevant per-year CSVs from Drive, appends new rows, re-uploads.
    Returns (new_row_count, credits_remaining).
    """
    # Group windows by year so we only download/upload each year file once.
    by_year: dict = defaultdict(list)
    for w_start, w_end in windows:
        # A window can span a year boundary; assign it to the start year.
        by_year[w_start.year].append((w_start, w_end))

    total_new = 0
    credits   = None

    for year, year_windows in sorted(by_year.items()):
        filename       = f"{airport}_{year}.csv"
        existing_bytes = drive.download(filename)
        existing_rows  = csv_bytes_to_rows(existing_bytes)
        new_rows       = []

        for w_start, w_end in year_windows:
            flights, credits = fetch_departures(airport, w_start, w_end, tokens)
            for fl in flights:
                icao24   = (fl.get("icao24") or "").strip()
                aircraft = lookup_aircraft(icao24, aircraft_cache, tokens) if icao24 else {}
                ci, cn   = carrier_from_callsign(fl.get("callsign", ""), airlines)
                new_rows.append(build_row(fl, aircraft, ci, cn))

            log.info(
                "  %s %s→%s: %d flights  (credits remaining: %s)",
                airport, w_start, w_end, len(flights), credits,
            )
            cache_flush_counter[0] += 1
            if cache_flush_counter[0] >= CACHE_FLUSH_EVERY:
                drive.upload_json(AIRCRAFT_CACHE_FILENAME, aircraft_cache)
                cache_flush_counter[0] = 0

        if new_rows:
            all_rows = existing_rows + new_rows
            drive.upload(filename, rows_to_csv_bytes(all_rows))
            total_new += len(new_rows)

    return total_new, credits

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    client_id     = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise SystemExit("OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET must be set.")

    from drive_client import DriveClient
    drive = DriveClient()

    log.info("=== Planeorama Flight Collector ===")
    log.info("Airports       : %s", ", ".join(AIRPORTS))
    log.info("Backfill target: %s", BACKFILL_TO_DATE)
    log.info("Credit buffer  : %d", CREDIT_BUFFER)

    tokens         = TokenManager(client_id, client_secret)
    airlines       = load_airlines(drive)
    aircraft_cache = load_aircraft_cache(drive)
    frontiers      = drive.download_json(FRONTIERS_FILENAME) or {}

    yesterday             = date.today() - timedelta(days=1)
    credits               = None
    cache_flush_counter   = [0]   # mutable so fetch_windows can update it
    total_rows_written    = 0

    # ── Phase 1: Forward fill ──────────────────────────────────────────────────
    log.info("--- Phase 1: Forward fill (ensuring data through %s) ---", yesterday)

    for airport in AIRPORTS:
        latest = get_latest(frontiers, airport)

        if latest is not None and latest >= yesterday:
            log.info("%s already current (latest: %s)", airport, latest)
            continue

        start = (latest + timedelta(days=1)) if latest else yesterday
        windows = list(date_windows(start, yesterday + timedelta(days=1)))

        log.info("%s: filling %s → %s (%d windows)", airport, start, yesterday, len(windows))
        n, credits = fetch_windows(airport, windows, tokens, airlines, aircraft_cache, drive, cache_flush_counter)
        total_rows_written += n

        update_frontier(frontiers, airport,
                        earliest=get_earliest(frontiers, airport) or start,
                        latest=yesterday)
        drive.upload_json(FRONTIERS_FILENAME, frontiers)

    # ── Phase 2: Backfill ──────────────────────────────────────────────────────
    log.info("--- Phase 2: Backfill (target: %s) ---", BACKFILL_TO_DATE)

    # Airports that still need backfilling
    needs_backfill = [
        a for a in AIRPORTS
        if (get_earliest(frontiers, a) or yesterday) > BACKFILL_TO_DATE
    ]

    if not needs_backfill:
        log.info("All airports fully backfilled to %s.", BACKFILL_TO_DATE)
    else:
        # Estimate available credits (use header value if we have it, else assume full budget)
        available = max(0, (credits if credits is not None else 4000) - CREDIT_BUFFER)
        # Divide equally; each 2-day window costs 30 credits
        windows_per_airport = max(1, (available // 30) // len(needs_backfill))

        log.info(
            "%d airports need backfill — ~%d windows each (%d credits available)",
            len(needs_backfill), windows_per_airport, available,
        )

        # Sort: most recent earliest first (= least history = highest priority)
        needs_backfill.sort(
            key=lambda a: get_earliest(frontiers, a) or yesterday,
            reverse=True,
        )

        for airport in needs_backfill:
            if credits is not None and credits <= CREDIT_BUFFER:
                log.info("Credit buffer reached (%d remaining). Stopping.", credits)
                break

            current_earliest = get_earliest(frontiers, airport) or yesterday
            if current_earliest <= BACKFILL_TO_DATE:
                continue

            # Build the list of windows going backward from current_earliest
            windows = []
            cursor  = current_earliest
            for _ in range(windows_per_airport):
                if cursor <= BACKFILL_TO_DATE:
                    break
                w_end   = cursor
                w_start = max(cursor - timedelta(days=QUERY_DAYS), BACKFILL_TO_DATE)
                windows.append((w_start, w_end))
                cursor = w_start

            if not windows:
                continue

            log.info(
                "%s: backfilling %s → %s (%d windows)",
                airport, windows[-1][0], windows[0][1], len(windows),
            )
            n, credits = fetch_windows(airport, windows, tokens, airlines, aircraft_cache, drive, cache_flush_counter)
            total_rows_written += n

            new_earliest = windows[-1][0]
            update_frontier(frontiers, airport, earliest=new_earliest)
            drive.upload_json(FRONTIERS_FILENAME, frontiers)

    # Final cache flush
    drive.upload_json(AIRCRAFT_CACHE_FILENAME, aircraft_cache)

    log.info("=== Run complete — %d rows written ===", total_rows_written)


if __name__ == "__main__":
    main()
