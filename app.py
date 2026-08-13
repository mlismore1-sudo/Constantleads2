import json
import os
import sqlite3
import threading
import time
from datetime import date, datetime, timezone

import pandas as pd
import psycopg
import requests
import streamlit as st
from psycopg.rows import dict_row
from streamlit_autorefresh import st_autorefresh

APP_VERSION = "2026-08-13-single-streamlit-app"
STREAM_URL = "https://stream.companieshouse.gov.uk/companies"
DISPLAY_LIMIT = 250
REFRESH_INTERVAL_MS = 1000
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


def ensure_shortlist_column(dataframe):
    dataframe = dataframe.copy()

    if "Shortlist" not in dataframe.columns:
        dataframe["Shortlist"] = False

    dataframe["Shortlist"] = (
        dataframe["Shortlist"].fillna(False).astype(bool)
    )
    return dataframe


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
        str(code).strip()
        for code in (company.get("sic_codes") or [])
    }
    incorporation_date = company.get("date_of_creation")

    if not company_number:
        return False

    if not test_all_sic_codes and not sic_codes.intersection(
        TARGET_SIC_CODES
    ):
        return False

    if start_date and (
        not incorporation_date or incorporation_date < start_date
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


def stream_worker(
    database_url,
    api_key,
    start_date,
    test_all_sic_codes,
):
    reconnect_delay = 5
    session = requests.Session()
    session.auth = (api_key, "")
    session.headers.update({"Accept": "application/json"})

    print(
        "Background worker starting. "
        f"SIC mode={'ALL' if test_all_sic_codes else 'REFINED'}. "
        f"Start date={start_date or 'not set'}",
        flush=True,
    )

    while True:
        connection = None

        try:
            connection = get_connection(database_url)
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
                        start_date,
                        test_all_sic_codes,
                    )

                    # Checkpoint every event, including non-matching events.
                    save_timepoint(connection, event_timepoint)
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
            print(
                f"Background worker disconnected: {error}. "
                f"Reconnecting in {reconnect_delay} seconds.",
                flush=True,
            )
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 300)

        finally:
            if connection is not None:
                connection.close()


def start_background_worker(database_url, api_key, start_date, test_all_sic_codes):
    worker = threading.Thread(
        target=stream_worker,
        args=(database_url, api_key, start_date, test_all_sic_codes),
        daemon=True,
        name="companies-house-stream-worker",
    )
    worker.start()
    return worker


@st.cache_resource
def start_worker_once(database_url, api_key, start_date, test_all_sic_codes):
    return start_background_worker(
        database_url,
        api_key,
        start_date,
        test_all_sic_codes,
    )


def get_history(database_url, start_date, end_date):
    query = (
        "SELECT company_name AS \"Company name\", "
        "company_number AS \"Company number\", "
        "incorporation_date AS \"Incorporation date\", "
        "company_status AS \"Status\", "
        "sic_codes AS \"SIC codes\", "
        "company_url AS \"Companies House page\", "
        "received_at AS \"Received by worker\", "
        "published_at AS \"Published by Companies House\", "
        "shortlisted AS \"Shortlist\" "
        "FROM screened_companies "
        "WHERE incorporation_date >= %s "
        "AND incorporation_date <= %s "
        "ORDER BY received_at DESC, company_name ASC LIMIT %s"
    )

    with get_connection(database_url) as connection:
        history = pd.read_sql_query(
            query,
            connection,
            params=(
                start_date.isoformat(),
                end_date.isoformat(),
                DISPLAY_LIMIT,
            ),
        )

    return ensure_shortlist_column(history)


def get_counts(database_url, start_date, end_date):
    with get_connection(database_url) as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE shortlisted = TRUE) AS shortlisted "
            "FROM screened_companies "
            "WHERE incorporation_date >= %s AND incorporation_date <= %s",
            (start_date.isoformat(), end_date.isoformat()),
        ).fetchone()

        status = connection.execute(
            "SELECT MAX(received_at) AS last_received, "
            "MAX(published_at) AS last_published, "
            "COUNT(*) AS all_time_total "
            "FROM screened_companies"
        ).fetchone()

    return row, status


def update_changed_shortlist(database_url, previous_history, edited_history):
    previous_history = ensure_shortlist_column(previous_history)
    edited_history = ensure_shortlist_column(edited_history)

    previous = previous_history.set_index("Company number")["Shortlist"]
    current = edited_history.set_index("Company number")["Shortlist"]
    previous = previous.reindex(current.index).fillna(False).astype(bool)
    current = current.fillna(False).astype(bool)
    changed_numbers = current.index[previous.ne(current)]

    if len(changed_numbers) == 0:
        return

    updates = [
        (bool(current.loc[number]), number)
        for number in changed_numbers
    ]

    with get_connection(database_url) as connection:
        connection.executemany(
            "UPDATE screened_companies SET shortlisted = %s "
            "WHERE company_number = %s",
            updates,
        )
        connection.commit()


def get_shortlist(database_url, start_date, end_date):
    query = (
        "SELECT company_name AS \"Company name\", "
        "company_number AS \"Company number\", "
        "incorporation_date AS \"Incorporation date\", "
        "company_status AS \"Status\", "
        "sic_codes AS \"SIC codes\", "
        "company_url AS \"Companies House page\", "
        "received_at AS \"Received by worker\", "
        "published_at AS \"Published by Companies House\" "
        "FROM screened_companies "
        "WHERE incorporation_date >= %s "
        "AND incorporation_date <= %s "
        "AND shortlisted = TRUE ORDER BY received_at DESC"
    )

    with get_connection(database_url) as connection:
        return pd.read_sql_query(
            query,
            connection,
            params=(start_date.isoformat(), end_date.isoformat()),
        )


st.set_page_config(
    page_title="Live Companies House Screener",
    page_icon="⚡",
    layout="wide",
)

required_secrets = [
    "DATABASE_URL",
    "COMPANIES_HOUSE_STREAMING_API_KEY",
]
missing_secrets = [
    key for key in required_secrets if key not in st.secrets
]

if missing_secrets:
    st.error(
        "Add these missing values to Streamlit Secrets: "
        + ", ".join(missing_secrets)
    )
    st.stop()

database_url = st.secrets["DATABASE_URL"]
api_key = st.secrets["COMPANIES_HOUSE_STREAMING_API_KEY"]
start_date_secret = str(st.secrets.get("STREAM_START_DATE", date.today()))
test_all_sic_codes = str(
    st.secrets.get("TEST_ALL_SIC_CODES", "false")
).lower() == "true"

start_worker_once(
    database_url,
    api_key,
    start_date_secret,
    test_all_sic_codes,
)

st_autorefresh(
    interval=REFRESH_INTERVAL_MS,
    debounce=True,
    key="single_app_refresh",
)

st.title("Live Companies House Screener")
st.caption(f"Application version: {APP_VERSION}")
st.caption(
    "The Companies House worker starts automatically inside Streamlit."
)

col1, col2 = st.columns(2)
start_date = col1.date_input("From incorporation date", value=date.today())
end_date = col2.date_input("To incorporation date", value=date.today())

if start_date > end_date:
    st.error("The start date must be on or before the end date.")
    st.stop()

try:
    history = get_history(database_url, start_date, end_date)
    counts, status = get_counts(database_url, start_date, end_date)
except Exception as error:
    st.error(f"Could not read Supabase: {error}")
    st.stop()

matching_count = int(counts["total"] or 0)
shortlist_count = int(counts["shortlisted"] or 0)

col1, col2, col3 = st.columns(3)
col1.metric("Matching companies", matching_count)
col2.metric("Displayed interactively", len(history))
col3.metric("Shortlisted", shortlist_count)

with st.expander("Connection diagnostics"):
    st.write(f"All-time records in Supabase: {status['all_time_total'] or 0}")
    st.write(f"Latest received event: {status['last_received'] or 'None'}")
    st.write(f"Latest published event: {status['last_published'] or 'None'}")
    st.write(f"SIC mode: {'ALL' if test_all_sic_codes else 'REFINED'}")
    st.write(f"Worker start date: {start_date_secret}")

if matching_count > DISPLAY_LIMIT:
    st.info(
        f"Showing the {DISPLAY_LIMIT} newest records for speed. "
        f"{matching_count - DISPLAY_LIMIT} older records remain stored."
    )

st.subheader("Live company results")

if history.empty:
    st.info("No matching companies have been received for this date range yet.")
    st.stop()

editor_columns = [
    "Shortlist",
    "Company name",
    "Company number",
    "Incorporation date",
    "Status",
    "SIC codes",
    "Companies House page",
    "Received by worker",
    "Published by Companies House",
]

editable_history = ensure_shortlist_column(history[editor_columns])

edited_history = st.data_editor(
    editable_history,
    use_container_width=True,
    hide_index=True,
    key="single_app_shortlist_editor",
    disabled=[column for column in editor_columns if column != "Shortlist"],
    column_config={
        "Shortlist": st.column_config.CheckboxColumn(
            "Shortlist",
            help="Select this company for the downloadable shortlist.",
            default=False,
            pinned=True,
        ),
        "Companies House page": st.column_config.LinkColumn(
            "Companies House page",
            display_text="Open company page",
        ),
    },
)

edited_history = ensure_shortlist_column(edited_history)
update_changed_shortlist(database_url, editable_history, edited_history)

shortlist = get_shortlist(database_url, start_date, end_date)
st.subheader("Shortlist")
st.write(f"{len(shortlist)} company(ies) selected in this date range.")

if shortlist.empty:
    st.info("Select at least one company above to create a shortlist.")
else:
    st.download_button(
        "Download shortlist as CSV",
        data=shortlist.to_csv(index=False).encode("utf-8"),
        file_name="companies_house_shortlist.csv",
        mime="text/csv",
        type="primary",
    )
    st.dataframe(
        shortlist,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Companies House page": st.column_config.LinkColumn(
                "Companies House page",
                display_text="Open company page",
            )
        },
    )
