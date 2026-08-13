from datetime import date

import pandas as pd
import psycopg
import streamlit as st
from psycopg.rows import dict_row
from streamlit_autorefresh import st_autorefresh

DISPLAY_LIMIT = 250
REFRESH_INTERVAL_MS = 1000


def get_connection():
    return psycopg.connect(
        st.secrets["DATABASE_URL"],
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


def get_history(start_date, end_date):
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
        "ORDER BY received_at DESC, company_name ASC "
        "LIMIT %s"
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
    with get_connection() as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS total FROM screened_companies "
            "WHERE incorporation_date >= %s "
            "AND incorporation_date <= %s",
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
        (bool(current.loc[number]), number)
        for number in changed_numbers
    ]

    with get_connection() as connection:
        connection.executemany(
            "UPDATE screened_companies SET shortlisted = %s "
            "WHERE company_number = %s",
            updates,
        )
        connection.commit()


def get_shortlist_count(start_date, end_date):
    with get_connection() as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS total FROM screened_companies "
            "WHERE incorporation_date >= %s "
            "AND incorporation_date <= %s "
            "AND shortlisted = TRUE",
            (start_date.isoformat(), end_date.isoformat()),
        ).fetchone()

    return int(row["total"])


st.set_page_config(
    page_title="Live Companies House Screener",
    page_icon="⚡",
    layout="wide",
)

if "DATABASE_URL" not in st.secrets:
    st.error("Add DATABASE_URL to Streamlit Secrets before running the app.")
    st.stop()

st_autorefresh(
    interval=REFRESH_INTERVAL_MS,
    debounce=True,
    key="live_company_refresh",
)

st.title("Live Companies House Screener")
st.caption(
    "The background worker receives Companies House events. "
    "This dashboard refreshes once per second."
)

col1, col2 = st.columns(2)
start_date = col1.date_input("From incorporation date", value=date.today())
end_date = col2.date_input("To incorporation date", value=date.today())

if start_date > end_date:
    st.error("The start date must be on or before the end date.")
    st.stop()

try:
    history = get_history(start_date, end_date)
    total_count = get_total_count(start_date, end_date)
    shortlist_count = get_shortlist_count(start_date, end_date)
except Exception as error:
    st.error(f"Could not read Supabase: {error}")
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Matching companies", total_count)
col2.metric("Displayed interactively", len(history))
col3.metric("Shortlisted", shortlist_count)

if total_count > DISPLAY_LIMIT:
    st.info(
        f"Showing the {DISPLAY_LIMIT} newest records for speed. "
        f"{total_count - DISPLAY_LIMIT} older records remain stored in Supabase."
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
    key="supabase_shortlist_editor",
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
