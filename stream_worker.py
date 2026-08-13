import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

STREAM_URL = "https://stream.companieshouse.gov.uk/companies"
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "screened_companies.db"))
TARGET_SIC_CODES = {
    "62012",
    "63110",
    "64209",
    "64301",
    "64999",
    "72110",
}


def get_connection():
    connection = sqlite3.connect(DATABASE_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def initialise_database():
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS screened_companies (
                company_number TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                incorporation_date TEXT,
                company_status TEXT,
                sic_codes TEXT,
                company_url TEXT NOT NULL,
                screened_at TEXT NOT NULL,
                shortlisted INTEGER NOT NULL DEFAULT 0,
                published_at TEXT
            )
            """
        )

        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(screened_companies)"
            ).fetchall()
        }

        if "shortlisted" not in columns:
            connection.execute(
                "ALTER TABLE screened_companies "
                "ADD COLUMN shortlisted INTEGER NOT NULL DEFAULT 0"
            )

        if "published_at" not in columns:
            connection.execute(
                "ALTER TABLE screened_companies ADD COLUMN published_at TEXT"
            )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS stream_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                timepoint INTEGER
            )
            """
        )
        connection.commit()


def get_timepoint():
    with get_connection() as connection:
        row = connection.execute(
            "SELECT timepoint FROM stream_state WHERE id = 1"
        ).fetchone()
    return row["timepoint"] if row and row["timepoint"] else None


def save_event(company, event):
    company_number = company.get("company_number")
    sic_codes = set(company.get("sic_codes") or [])
    incorporation_date = company.get("date_of_creation")

    if not company_number:
        return False

    if not sic_codes.intersection(TARGET_SIC_CODES):
        return False

    stream_start_date = os.getenv("STREAM_START_DATE", "")
    if stream_start_date and (
        not incorporation_date or incorporation_date < stream_start_date
    ):
        return False

    now = datetime.now(timezone.utc).isoformat()
    company_name = company.get("company_name") or "Unnamed company"
    published_at = event.get("published_at")
    company_url = (
        "https://find-and-update.company-information.service.gov.uk/company/"
        f"{company_number}"
    )

    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO screened_companies (
                company_number,
                company_name,
                incorporation_date,
                company_status,
                sic_codes,
                company_url,
                screened_at,
                shortlisted,
                published_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
            ON CONFLICT(company_number) DO UPDATE SET
                company_name = excluded.company_name,
                incorporation_date = excluded.incorporation_date,
                company_status = excluded.company_status,
                sic_codes = excluded.sic_codes,
                company_url = excluded.company_url,
                published_at = COALESCE(
                    excluded.published_at,
                    screened_companies.published_at
                )
            """,
            (
                company_number,
                company_name,
                incorporation_date or "",
                company.get("company_status", ""),
                ", ".join(sorted(sic_codes)),
                company_url,
                now,
                published_at,
            ),
        )

        if event.get("timepoint") is not None:
            connection.execute(
                """
                INSERT INTO stream_state (id, timepoint)
                VALUES (1, ?)
                ON CONFLICT(id) DO UPDATE SET timepoint = excluded.timepoint
                """,
                (int(event["timepoint"]),),
            )

        connection.commit()

    return True


def run_worker(api_key):
    initialise_database()
    reconnect_delay = 5

    while True:
        timepoint = get_timepoint()
        params = {"timepoint": timepoint} if timepoint else {}

        try:
            with requests.get(
                STREAM_URL,
                params=params,
                auth=(api_key, ""),
                stream=True,
                timeout=(30, 300),
            ) as response:
                response.raise_for_status()
                reconnect_delay = 5

                for raw_line in response.iter_lines(decode_unicode=True):
                    if not raw_line:
                        continue

                    event = json.loads(raw_line)
                    event_data = event.get("event") or {}
                    company = event.get("data") or {}
                    save_event(company, event_data)

        except (
            requests.RequestException,
            json.JSONDecodeError,
            OSError,
        ) as error:
            print(
                f"Stream disconnected: {error}. "
                f"Reconnecting in {reconnect_delay}s.",
                flush=True,
            )
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 300)


if __name__ == "__main__":
    streaming_api_key = os.getenv("COMPANIES_HOUSE_STREAMING_API_KEY")

    if not streaming_api_key:
        raise RuntimeError(
            "Set COMPANIES_HOUSE_STREAMING_API_KEY before starting the worker."
        )

    run_worker(streaming_api_key)
