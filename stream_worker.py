import json
import os
import time
from datetime import datetime, timezone

import psycopg
import requests
from psycopg.rows import dict_row

STREAM_URL = "https://stream.companieshouse.gov.uk/companies"
TARGET_SIC_CODES = {
    "62012",
    "63110",
    "64209",
    "64301",
    "64999",
    "72110",
}


def get_connection(database_url):
    return psycopg.connect(
        database_url,
        row_factory=dict_row,
        connect_timeout=30,
        sslmode="require",
    )


def ensure_worker_status_table(connection):
    connection.execute(
        "CREATE TABLE IF NOT EXISTS worker_status ("
        "id INTEGER PRIMARY KEY CHECK (id = 1), "
        "status TEXT NOT NULL, "
        "last_connected_at TIMESTAMPTZ, "
        "last_event_at TIMESTAMPTZ, "
        "last_error TEXT, "
        "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
        ")"
    )


def update_worker_status(connection, status, error=None, event_received=False):
    connection.execute(
        "INSERT INTO worker_status ("
        "id, status, last_connected_at, last_event_at, last_error, updated_at"
        ") VALUES (1, %s, "
        "CASE WHEN %s = 'connected' THEN NOW() ELSE NULL END, "
        "CASE WHEN %s THEN NOW() ELSE NULL END, %s, NOW()) "
        "ON CONFLICT (id) DO UPDATE SET "
        "status = EXCLUDED.status, "
        "last_connected_at = CASE "
        "WHEN EXCLUDED.status = 'connected' THEN NOW() "
        "ELSE worker_status.last_connected_at END, "
        "last_event_at = CASE "
        "WHEN %s THEN NOW() ELSE worker_status.last_event_at END, "
        "last_error = EXCLUDED.last_error, "
        "updated_at = NOW()",
        (status, status, event_received, error, event_received),
    )


def get_timepoint(connection):
    row = connection.execute(
        "SELECT timepoint FROM stream_state WHERE id = 1"
    ).fetchone()
    return row["timepoint"] if row else None


def save_timepoint(connection, timepoint):
    if timepoint is None:
        return
    connection.execute(
        "INSERT INTO stream_state (id, timepoint, updated_at) "
        "VALUES (1, %s, NOW()) "
        "ON CONFLICT (id) DO UPDATE SET "
        "timepoint = EXCLUDED.timepoint, updated_at = NOW()",
        (int(timepoint),),
    )


def extract_metadata(event):
    metadata = event.get("event") or {}
    return (
        metadata.get("timepoint", event.get("timepoint")),
        metadata.get("published_at", event.get("published_at")),
    )


def save_matching_company(
    connection,
    company,
    published_at,
    received_at,
    start_date,
    test_all_sic_codes,
):
    company_number = company.get("company_number")
    sic_codes = {
        str(code).strip() for code in (company.get("sic_codes") or [])
    }
    incorporation_date = company.get("date_of_creation")

    if not company_number:
        return False
    if not test_all_sic_codes and not sic_codes.intersection(TARGET_SIC_CODES):
        return False
    if start_date and (not incorporation_date or incorporation_date < start_date):
        return False

    company_name = company.get("company_name") or "Unnamed company"
    company_url = (
        "https://find-and-update.company-information.service.gov.uk/company/"
        f"{company_number}"
    )

    connection.execute(
        "INSERT INTO screened_companies ("
        "company_number, company_name, incorporation_date, company_status, "
        "sic_codes, company_url, screened_at, shortlisted, published_at, received_at"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE, %s, %s) "
        "ON CONFLICT (company_number) DO UPDATE SET "
        "company_name = EXCLUDED.company_name, "
        "incorporation_date = EXCLUDED.incorporation_date, "
        "company_status = EXCLUDED.company_status, "
        "sic_codes = EXCLUDED.sic_codes, "
        "company_url = EXCLUDED.company_url, "
        "published_at = COALESCE(EXCLUDED.published_at, "
        "screened_companies.published_at), "
        "received_at = EXCLUDED.received_at",
        (
            company_number,
            company_name,
            incorporation_date,
            company.get("company_status", ""),
            ", ".join(sorted(sic_codes)),
            company_url,
            received_at,
            published_at,
            received_at,
        ),
    )
    return True


def stream_worker(database_url, api_key, start_date, test_all_sic_codes):
    reconnect_delay = 5
    status_interval_seconds = 30
    last_status_update = 0.0
    session = requests.Session()
    session.auth = (api_key, "")
    session.headers.update({"Accept": "application/json"})

    print(
        "Render worker starting. "
        f"SIC mode={'ALL' if test_all_sic_codes else 'REFINED'}. "
        f"Start date={start_date or 'not set'}",
        flush=True,
    )

    while True:
        connection = None
        try:
            connection = get_connection(database_url)
            ensure_worker_status_table(connection)
            update_worker_status(connection, "connecting")
            connection.commit()

            timepoint = get_timepoint(connection)
            params = {"timepoint": timepoint} if timepoint else {}
            print(
                f"Connecting to Companies House stream from timepoint={timepoint}",
                flush=True,
            )

            with session.get(
                STREAM_URL,
                params=params,
                stream=True,
                timeout=(30, 300),
            ) as response:
                response.raise_for_status()
                reconnect_delay = 5
                update_worker_status(connection, "connected")
                connection.commit()
                print("Companies House stream connected.", flush=True)

                for raw_line in response.iter_lines(decode_unicode=True):
                    if not raw_line:
                        continue

                    received_at = datetime.now(timezone.utc)
                    event = json.loads(raw_line)
                    company = event.get("data") or {}
                    event_timepoint, published_at = extract_metadata(event)
                    matched = save_matching_company(
                        connection,
                        company,
                        published_at,
                        received_at,
                        start_date,
                        test_all_sic_codes,
                    )
                    save_timepoint(connection, event_timepoint)

                    now = time.monotonic()
                    if now - last_status_update >= status_interval_seconds:
                        update_worker_status(
                            connection,
                            "connected",
                            event_received=True,
                        )
                        last_status_update = now

                    connection.commit()

                    if matched:
                        print(
                            f"Matched {company.get('company_number')} - "
                            f"{company.get('company_name', 'Unnamed company')}",
                            flush=True,
                        )

        except (
            requests.RequestException,
            json.JSONDecodeError,
            psycopg.Error,
            OSError,
        ) as error:
            if connection is not None:
                try:
                    update_worker_status(
                        connection,
                        "reconnecting",
                        error=str(error),
                    )
                    connection.commit()
                except psycopg.Error:
                    pass

            print(
                f"Worker disconnected: {error}. "
                f"Reconnecting in {reconnect_delay} seconds.",
                flush=True,
            )
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 300)

        finally:
            if connection is not None:
                connection.close()


if __name__ == "__main__":
    database_url = os.environ["DATABASE_URL"]
    api_key = os.environ["COMPANIES_HOUSE_STREAMING_API_KEY"]
    start_date = os.environ.get("STREAM_START_DATE", "")
    test_all_sic_codes = (
        os.environ.get("TEST_ALL_SIC_CODES", "false").lower() == "true"
    )
    stream_worker(
        database_url,
        api_key,
        start_date,
        test_all_sic_codes,
    )
