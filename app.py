"""All-in-one Streamlit app with Companies House streaming and enrichment workers."""

import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
from supabase import Client, create_client


COMPANIES_HOUSE_API = "https://api.company-information.service.gov.uk"
ENRICHMENT_PAUSE_SECONDS = 0.60
REQUEST_TIMEOUT_SECONDS = 20
MAX_RETRIES = 3


# -----------------------------
# Configuration
# -----------------------------

def env_value(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def get_config() -> Dict[str, str]:
    database_url = env_value("SUPABASE_URL") or env_value("DATABASE_URL")
    supabase_key = env_value("SUPABASE_SERVICE_ROLE_KEY") or env_value("SUPABASE_KEY")
    companies_house_key = env_value("COMPANIES_HOUSE_API_KEY")
    if not database_url or not supabase_key or not companies_house_key:
        st.error(
            "Missing configuration. Set SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, "
            "and COMPANIES_HOUSE_API_KEY in Streamlit secrets or environment variables."
        )
        st.stop()
    return {
        "database_url": database_url,
        "supabase_key": supabase_key,
        "companies_house_key": companies_house_key,
    }


@st.cache_resource
 def get_supabase(database_url: str, supabase_key: str) -> Client:
    return create_client(database_url, supabase_key)


# -----------------------------
# Normalisation and rating
# -----------------------------

def normalise(value: Any) -> str:
    return " ".join(str(value or "").lower().replace("-", " ").replace("_", " ").split())


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def get_director_residence(officer: Dict[str, Any]) -> str:
    address = officer.get("address") or {}
    return normalise(
        officer.get("country_of_residence")
        or officer.get("countryOfResidence")
        or address.get("country")
    )


def get_director_nationality(officer: Dict[str, Any]) -> str:
    return normalise(
        officer.get("nationality")
        or officer.get("nationality_description")
    )


def is_target_country(officer: Dict[str, Any]) -> bool:
    target_terms = {
        "russia",
        "belarus",
        "iran",
        "north korea",
        "syrian arab republic",
        "syria",
    }
    residence = get_director_residence(officer)
    nationality = get_director_nationality(officer)
    return any(term in residence or term in nationality for term in target_terms)


def is_us_person(officer: Dict[str, Any]) -> bool:
    values = [
        get_director_residence(officer),
        get_director_nationality(officer),
    ]
    return any("united states" in value or value == "usa" or "american" in value for value in values)


def is_corporate_psc(psc: Dict[str, Any]) -> bool:
    kind = normalise(psc.get("kind"))
    identification = psc.get("identification") or {}
    return (
        "corporate" in kind
        or "legal person" in kind
        or "corporate entity" in kind
        or bool(identification.get("legal_form"))
    )


def psc_name(psc: Dict[str, Any]) -> str:
    return str(
        psc.get("name")
        or (psc.get("identification") or {}).get("legal_form")
        or ""
    ).strip()


def calculate_star_rating(
    buzzword_match: bool,
    sic_match: bool,
    corporate_psc: bool,
    target_country_director: bool,
    us_person_bonus: bool,
) -> int:
    return sum(
        int(value)
        for value in (
            buzzword_match,
            sic_match,
            corporate_psc,
            target_country_director,
            us_person_bonus,
        )
    )


# -----------------------------
# Companies House requests
# -----------------------------

def companies_house_get(api_key: str, path: str) -> Dict[str, Any]:
    url = f"{COMPANIES_HOUSE_API}{path}"
    last_error: Optional[Exception] = None

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                url,
                auth=(api_key, ""),
                timeout=REQUEST_TIMEOUT_SECONDS,
                headers={"Accept": "application/json"},
            )
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = float(retry_after) if retry_after else 10.0 * (attempt + 1)
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(2.0 * (attempt + 1))

    raise RuntimeError(f"Companies House request failed for {path}: {last_error}")


def fetch_all_items(api_key: str, path: str) -> List[Dict[str, Any]]:
    page = 0
    items: List[Dict[str, Any]] = []
    while True:
        separator = "&" if "?" in path else "?"
        payload = companies_house_get(api_key, f"{path}{separator}items_per_page=100&start_index={page * 100}")
        current = payload.get("items") or []
        items.extend(current)
        if len(current) < 100:
            break
        page += 1
    return items


def enrich_one_company(row: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    company_number = str(row.get("company_number") or "").strip()
    if not company_number:
        raise ValueError("Missing company number")

    officers = fetch_all_items(api_key, f"/company/{company_number}/officers")
    time.sleep(ENRICHMENT_PAUSE_SECONDS)
    pscs = fetch_all_items(api_key, f"/company/{company_number}/persons-with-significant-control")

    corporate_names = [psc_name(item) for item in pscs if is_corporate_psc(item)]
    target_director_names = [
        str(item.get("name") or "").strip()
        for item in officers
        if is_target_country(item)
    ]
    us_person = any(is_us_person(item) for item in officers)

    buzzword_match = bool(row.get("buzzword_match"))
    sic_match = bool(row.get("sic_match"))
    corporate_psc = bool(corporate_names)
    target_country_director = bool(target_director_names)
    star_rating = calculate_star_rating(
        buzzword_match,
        sic_match,
        corporate_psc,
        target_country_director,
        us_person,
    )

    return {
        "corporate_psc": corporate_psc,
        "corporate_psc_names": corporate_names,
        "director_count": len(officers),
        "target_country_director": target_country_director,
        "target_country_director_names": target_director_names,
        "us_person_bonus": us_person,
        "star_rating": star_rating,
        "enrichment_status": "excluded" if target_country_director else "complete",
        "enrichment_error": None,
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }


# -----------------------------
# Database helpers
# -----------------------------

def claim_pending_company(client: Client) -> Optional[Dict[str, Any]]:
    result = (
        client.table("screened_companies")
        .select("*")
        .eq("enrichment_status", "pending")
        .order("received_at")
        .limit(1)
        .execute()
    )
    rows = result.data or []
    if not rows:
        return None

    row = rows[0]
    company_number = row.get("company_number")
    client.table("screened_companies").update(
        {"enrichment_status": "processing"}
    ).eq("company_number", company_number).execute()
    row["enrichment_status"] = "processing"
    return row


def update_enrichment(client: Client, company_number: str, values: Dict[str, Any]) -> None:
    client.table("screened_companies").update(values).eq(
        "company_number", company_number
    ).execute()


# -----------------------------
# Background enrichment worker
# -----------------------------

def enrichment_worker(database_url: str, supabase_key: str, api_key: str) -> None:
    client = create_client(database_url, supabase_key)
    while True:
        try:
            row = claim_pending_company(client)
            if not row:
                time.sleep(3)
                continue

            company_number = str(row.get("company_number") or "")
            try:
                values = enrich_one_company(row, api_key)
                update_enrichment(client, company_number, values)
            except Exception as exc:
                update_enrichment(
                    client,
                    company_number,
                    {
                        "enrichment_status": "error",
                        "enrichment_error": str(exc)[:2000],
                        "enriched_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            time.sleep(ENRICHMENT_PAUSE_SECONDS)
        except Exception:
            time.sleep(5)


# -----------------------------
# Optional stream worker hook
# -----------------------------

def stream_worker(database_url: str, supabase_key: str, api_key: str) -> None:
    """Replace this body with the existing Companies House streaming ingestion code.

    The stream worker should insert matching companies with enrichment_status='pending'.
    The enrichment worker above then processes those rows independently.
    """
    while True:
        time.sleep(10)


@st.cache_resource
 def start_workers_once(database_url: str, supabase_key: str, api_key: str):
    stream_thread = threading.Thread(
        target=stream_worker,
        args=(database_url, supabase_key, api_key),
        daemon=True,
        name="companies-house-stream-worker",
    )
    enrichment_thread = threading.Thread(
        target=enrichment_worker,
        args=(database_url, supabase_key, api_key),
        daemon=True,
        name="companies-house-enrichment-worker",
    )
    stream_thread.start()
    enrichment_thread.start()
    return stream_thread, enrichment_thread


# -----------------------------
# Dashboard
# -----------------------------

def display_rating(row: pd.Series) -> str:
    status = row.get("enrichment_status")
    if status == "pending":
        return "Pending"
    if status == "processing":
        return "Processing"
    if status == "error":
        return "Error"
    if status == "excluded":
        return "Excluded"
    return f"{int(row.get('star_rating') or 0)} ⭐"


def load_companies(client: Client) -> pd.DataFrame:
    result = (
        client.table("screened_companies")
        .select("*")
        .order("received_at", desc=True)
        .limit(500)
        .execute()
    )
    return pd.DataFrame(result.data or [])


def main() -> None:
    st.set_page_config(page_title="Company Enrichment", layout="wide")
    st.title("Company Enrichment Monitor")

    config = get_config()
    client = get_supabase(config["database_url"], config["supabase_key"])
    start_workers_once(
        config["database_url"],
        config["supabase_key"],
        config["companies_house_key"],
    )

    data = load_companies(client)
    if data.empty:
        st.info("No screened companies have been saved yet.")
        st.stop()

    status_counts = data["enrichment_status"].fillna("unknown").value_counts()
    columns = st.columns(4)
    columns[0].metric("Companies", len(data))
    columns[1].metric("Complete", int(status_counts.get("complete", 0)))
    columns[2].metric("Pending", int(status_counts.get("pending", 0)))
    columns[3].metric("Errors", int(status_counts.get("error", 0)))

    data["rating_display"] = data.apply(display_rating, axis=1)
    visible = data[data["enrichment_status"].ne("excluded")].copy()

    preferred_columns = [
        "company_name",
        "company_number",
        "sic_codes",
        "rating_display",
        "enrichment_status",
        "corporate_psc_names",
        "target_country_director_names",
        "director_count",
        "enrichment_error",
        "enriched_at",
    ]
    table_columns = [column for column in preferred_columns if column in visible.columns]
    st.dataframe(visible[table_columns], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
