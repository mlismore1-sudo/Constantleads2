import os
import sqlite3
import threading
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from stream_worker import run_worker

DATABASE_PATH = Path("screened_companies.db")
DISPLAY_LIMIT = 250
REFRESH_INTERVAL_MS = 1000


def get_connection():
    connection = sqlite3.connect(
        DATABASE_PATH,
        timeout=30,
        check_same_thread=False,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    return connection


def initialise_database():
    schema = (
        "CREATE TABLE IF NOT EXISTS screened_companies ("
        "company_number TEXT PRIMARY KEY,"
        "company_name TEXT NOT NULL,"
        "incorporation_date TEXT,"
        "company_status TEXT,"
        "sic_codes TEXT,"
        "company_url TEXT NOT NULL,"
        "screened_at TEXT NOT NULL,"
        "shortlisted INTEGER NOT NULL DEFAULT 0,"
        "published_at TEXT"
        ")"
    )

    with get_connection() as connection:
        connection.execute(schema)

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
            "CREATE INDEX IF NOT EXISTS idx_incorporation_date "
            "ON screened_companies(incorporation_date)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_screened_at "
            "ON screened_companies(screened_at DESC)"
        )
        connection.commit()


def ensure_shortlist_column(dataframe):
    dataframe = dataframe.copy()

    if "Shortlist" not in dataframe.columns:
        dataframe["Shortlist"] = False

    dataframe["Shortlist"] = (
        dataframe["Shortlist"].fillna(False).astype(bool)
    )
    return dataframe


def get_history(start_date, end_date):
    query = (
        "SELECT company_name AS 'Company name', "
        "company_number AS 'Company number', "
        "incorporation_date AS 'Incorporation date', "
        "company_status AS 'Status', "
        "sic_codes AS 'SIC codes', "
        "company_url AS 'Companies House page', "
        "screened_at AS 'Received by worker', "
        "published_at AS 'Published by Companies House', "
        "shortlisted AS 'Shortlist' "
        "FROM screened_companies "
        "WHERE incorporation_date >= ? AND incorporation_date <= ? "
        "ORDER BY screened_at DESC, company_name ASC LIMIT ?"
    )

    with get_connection() as connection:
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


def get_total_count(start_date, end_date):
    query = (
        "SELECT COUNT(*) AS total FROM screened_companies "
        "WHERE incorporation_date >= ? AND incorporation_date <= ?"
    )

    with get_connection() as connection:
        row = connection.execute(
            query,
            (start_date.isoformat(), end_date.isoformat()),
        ).fetchone()

    return int(row["total"])


def update_changed_shortlist(previous_history, edited_history):
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
        (int(current.loc[number]), number)
        for number in changed_numbers
    ]

    with get_connection() as connection:
        connection.executemany(
            "UPDATE screened_companies SET shortlisted = ? "
            "WHERE company_number = ?",
            updates,
        )
        connection.commit()


@st.cache_resource
def start_stream_worker(api_key):
    worker = threading.Thread(
        target=run_worker,
        args=(api_key,),
        daemon=True,
        name="companies-house-stream-worker",
    )
    worker.start()
    return worker


st.set_page_config(
    page_title="Live Companies House Screener",
    page_icon="⚡",
    layout="wide",
)

initialise_database()

streaming_api_key = st.secrets.get("COMPANIES_HOUSE_STREAMING_API_KEY", "")

if not streaming_api_key:
    st.error(
        "Add COMPANIES_HOUSE_STREAMING_API_KEY to Streamlit secrets "
        "before running the app."
    )
    st.stop()

if "STREAM_START_DATE" in st.secrets:
    os.environ.setdefault(
        "STREAM_START_DATE",
        str(st.secrets["STREAM_START_DATE"]),
    )

start_stream_worker(streaming_api_key)
st_autorefresh(
    interval=REFRESH_INTERVAL_MS,
    debounce=True,
    key="live_company_refresh",
)

st.title("Live Companies House Screener")
st.caption(
    "The stream worker runs continuously. The dashboard refreshes once per second."
)

col1, col2 = st.columns(2)
start_date = col1.date_input("From incorporation date", value=date.today())
end_date = col2.date_input("To incorporation date", value=date.today())

if start_date > end_date:
    st.error("The start date must be on or before the end date.")
    st.stop()

history = get_history(start_date, end_date)
total_count = get_total_count(start_date, end_date)

col1, col2, col3 = st.columns(3)
col1.metric("Matching companies", total_count)
col2.metric("Displayed interactively", len(history))
col3.metric(
    "Shortlisted",
    int(history["Shortlist"].sum()) if not history.empty else 0,
)

if total_count > DISPLAY_LIMIT:
    st.info(
        f"Showing the {DISPLAY_LIMIT} newest records for speed. "
        f"{total_count - DISPLAY_LIMIT} older records remain stored."
    )

st.subheader("Live company results")

if history.empty:
    st.info("No matching companies have been received for this date range.")
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
    key="optimised_shortlist_editor",
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
update_changed_shortlist(editable_history, edited_history)

shortlist = edited_history.loc[
    edited_history["Shortlist"]
].drop(
    columns=["Shortlist"],
    errors="ignore",
)

st.subheader("Shortlist")
st.write(f"{len(shortlist)} company(ies) selected.")

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
