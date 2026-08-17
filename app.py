"""Constant Leads - pre-enrichment dashboard using a direct PostgreSQL connection.

This version does not use the Supabase API client and does not require
SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_ROLE_KEY, or
COMPANIES_HOUSE_API_KEY.
"""

import os
from typing import Any, Dict

import pandas as pd
import psycopg
import streamlit as st


@st.cache_resource
def get_database_connection(database_url: str):
    return psycopg.connect(database_url, connect_timeout=15)


def read_secret(name: str) -> str:
    try:
        value = st.secrets.get(name, "")
    except Exception:
        value = ""
    return str(value or os.getenv(name, "")).strip()


def get_database_url() -> str:
    database_url = read_secret("DATABASE_URL")

    if not database_url:
        st.error(
            "Missing database configuration. Add DATABASE_URL to Streamlit "
            "app Settings → Secrets."
        )
        st.stop()

    return database_url


def load_companies(database_url: str) -> pd.DataFrame:
    query = """
        SELECT *
        FROM public.screened_companies
        ORDER BY received_at DESC NULLS LAST
        LIMIT 500
    """

    with psycopg.connect(database_url, connect_timeout=15) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [description.name for description in cursor.description]

    return pd.DataFrame(rows, columns=column_names)


def format_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return str(value)
    return value


def main() -> None:
    st.set_page_config(
        page_title="Constant Leads",
        page_icon="📋",
        layout="wide",
    )

    st.title("Constant Leads")
    st.caption("Pre-enrichment lead dashboard")

    database_url = get_database_url()

    try:
        companies = load_companies(database_url)
    except Exception as exc:
        st.error(f"Could not connect to Supabase PostgreSQL: {exc}")
        st.info(
            "Check that DATABASE_URL is copied from Supabase → Connect → "
            "Session pooler or Direct connection."
        )
        st.stop()

    if companies.empty:
        st.info("No screened companies found.")
        st.stop()

    for column in companies.columns:
        companies[column] = companies[column].map(format_value)

    metric_columns = st.columns(3)
    metric_columns[0].metric("Screened companies", len(companies))

    if "buzzword_match" in companies.columns:
        metric_columns[1].metric(
            "Buzzword matches",
            int(companies["buzzword_match"].fillna(False).astype(bool).sum()),
        )
    else:
        metric_columns[1].metric("Buzzword matches", "—")

    if "sic_match" in companies.columns:
        metric_columns[2].metric(
            "SIC matches",
            int(companies["sic_match"].fillna(False).astype(bool).sum()),
        )
    else:
        metric_columns[2].metric("SIC matches", "—")

    st.dataframe(
        companies,
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    main()
