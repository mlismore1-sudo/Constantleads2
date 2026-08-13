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

TEST_ALL_SIC_CODES = (
    os.getenv("TEST_ALL_SIC_CODES", "false").lower() == "true"
)


def get_connection():
    return psycopg.connect(
        os.environ["DATABASE_URL"],
        row_factory=dict_row,
        connect_timeout=30,
        sslmode="require",
    )


def get_timepoint(connection):
    row = connection.execute(
        "SELECT timepoint FROM stream_state WHERE id = 1"
    ).fetchone()
    return row["timepoint"] if row else None


def extract_metadata(event):
    metadata = event.get("event") or {}
    timepoint = metadata.get("timepoint", event.get("timepoint"))
    published_at = metadata.get(
        "published_at",
        event.get("published_at"),
    )
    return timepoint, published_at


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


def save_matching_company(connection, company, published_at, received_at):
    company_number = company.get("company_number")
    sic_codes = {
        str(code).strip()
        for code in (company.get("sic_codes") or [])
    }
    incorporation_date = company.get("date_of_creation")
    stream_start_date = os.environ.get("STREAM_START_DATE", "")

    if not company_number:
        return False

    if not TEST_ALL_SIC_CODES and not sic_codes.intersection(
        TARGET_SIC_CODES
    ):
        return False

    if stream_start_date and (
        not incorporation_date or incorporation_date < stream_start_date
    ):
        return False

    company_name = company.get("company_name") or "Unnamed company"
    company_url = (
        "https://find-and-update.company-information.service.gov.uk/company/"
        f"{company_number}"
    )

    connection.execute(
        "INSERT INTO screened_companies ("
        "company_number, company_name, incorporation_date, "
        "company_status, sic_codes, company_url, screened_at, "
        "shortlisted, published_at, received_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE, %s, %s) "
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


def run_worker(api_key):
    reconnect_delay = 5
    matched_total = 0
    session = requests.Session()
    session.auth = (api_key, "")
    session.headers.update({"Accept": "application/json"})

    print(
        "Worker starting. "
        f"SIC mode={'ALL' if TEST_ALL_SIC_CODES else 'REFINED'}. "
        f"Start date={os.environ.get('STREAM_START_DATE', 'not set')}",
        flush=True,
    )

    while True:
        connection = None

        try:
            connection = get_connection()
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
                    )

                    # Always checkpoint every event, even when it is not a match.
                    save_timepoint(connection, event_timepoint)
                    connection.commit()

                    if matched:
                        matched_total += 1
                        print(
                            f"Matched {company.get('company_number')} - "
                            f"{company.get('company_name', 'Unnamed company')} "
                            f"(total matched: {matched_total})",
                            flush=True,
                        )

        except (
            requests.RequestException,
            json.JSONDecodeError,
            psycopg.Error,
            OSError,
        ) as error:
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
    streaming_api_key = os.environ.get("COMPANIES_HOUSE_STREAMING_API_KEY")

    if not streaming_api_key:
        raise RuntimeError(
            "Set COMPANIES_HOUSE_STREAMING_API_KEY before starting the worker."
        )

    if not os.environ.get("DATABASE_URL"):
        raise RuntimeError("Set DATABASE_URL before starting the worker.")

    if not os.environ.get("STREAM_START_DATE"):
        raise RuntimeError("Set STREAM_START_DATE before starting the worker.")

    run_worker(streaming_api_key)
