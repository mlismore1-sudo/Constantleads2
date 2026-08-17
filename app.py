"""Constant Leads - pre-enrichment dashboard.

This is the rollback version. It does not start an enrichment worker and does
not require COMPANIES_HOUSE_API_KEY or SUPABASE_SERVICE_ROLE_KEY.
"""

import os
from typing import Any, Dict

import pandas as pd
import streamlit as st
from supabase import Client, create_client


@st.cache_resource
def get_supabase(url: str, key: str) -> Client:
    return create_client(url, key)


def read_secret(name: str) -> str:
    try:
        value = st.secrets.get(name, "")
    except Exception:
        value = ""
    return str(value or os.getenv(name, "")).strip()


def read_config() -> Dict[str, str]:
    supabase_url = read_secret("SUPABASE_URL")
    supabase_key = (
        read_secret("SUPABASE_KEY")
        or read_secret("SUPABASE_ANON_KEY")
        or read_secret("SUPABASE_SERVICE_ROLE_KEY")
    )

    if not supabase_url or not supabase_key:
        st.error(
            "Missing Supabase configuration. Add SUPABASE_URL and SUPABASE_KEY "
            "to Streamlit app Settings → Secrets."
        )
        st.stop()

    return {
        "supabase_url": supabase_url,
        "supabase_key": supabase_key,
    }


def load_companies(client: Client) -> pd.DataFrame:
    response = (
        client.table("screened_companies")
        .select("*")
        .order("received_at", desc=True)
        .limit(500)
        .execute()
    )
    return pd.DataFrame(response.data or [])


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

    config = read_config()
    client = get_supabase(
        config["supabase_url"],
        config["supabase_key"],
    )

    try:
        companies = load_companies(client)
    except Exception as exc:
        st.error(f"Could not read screened_companies from Supabase: {exc}")
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
