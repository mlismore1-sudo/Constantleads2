"""Pre-enrichment Streamlit app.

This version only reads and displays screened_companies. It does not start an
 enrichment worker and does not require Supabase or Companies House secrets in
 the app process beyond the existing database configuration used by the prior
 application version.
"""

import os
from typing import Any, Dict

import pandas as pd
import streamlit as st
from supabase import Client, create_client


@st.cache_resource
def get_supabase(url: str, key: str) -> Client:
    return create_client(url, key)


def read_config() -> Dict[str, str]:
    def read(name: str) -> str:
        try:
            value = st.secrets.get(name, "")
        except Exception:
            value = ""
        return str(value or os.getenv(name, "")).strip()

    url = read("SUPABASE_URL")
    key = read("SUPABASE_KEY") or read("SUPABASE_ANON_KEY") or read("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        st.error(
            "Missing Supabase configuration. Add SUPABASE_URL and SUPABASE_KEY "
            "to Streamlit Secrets, using the same names as the previous working app."
        )
        st.stop()

    return {"url": url, "key": key}


def load_companies(client: Client) -> pd.DataFrame:
    response = (
        client.table("screened_companies")
        .select("*")
        .order("received_at", desc=True)
        .limit(500)
        .execute()
    )
    return pd.DataFrame(response.data or [])


def main() -> None:
    st.set_page_config(page_title="Constant Leads", layout="wide")
    st.title("Constant Leads")

    config = read_config()
    client = get_supabase(config["url"], config["key"])

    companies = load_companies(client)
    if companies.empty:
        st.info("No screened companies found.")
        st.stop()

    st.metric("Screened companies", len(companies))
    st.dataframe(companies, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
