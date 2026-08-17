import base64
import io
import json
import math
import re
import threading
import time
import wave
from datetime import datetime, timezone
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
import requests
import streamlit as st
from psycopg.rows import dict_row
from streamlit_autorefresh import st_autorefresh

APP_VERSION = "2026-08-17-enriched-all-in-one"
STREAM_URL = "https://stream.companieshouse.gov.uk/companies"
DISPLAY_LIMIT = 250
REFRESH_INTERVAL_MS = 15000
ENRICHMENT_PAUSE_SECONDS = 0.55
UK_TIMEZONE = ZoneInfo("Europe/London")
TARGET_SIC_CODES = {
    "62012", "63110", "64209", "64301", "64999", "72110"
}
TARGET_NAME_KEYWORDS = {
    "labs", "global", "holdings", "capital", "ai", "technology",
    "technologies", "uk", "london", "europe", "inc", "pty", "pvt", "group",
}
EXCLUDED_DIRECTOR_COUNTRIES = {
    "pakistan", "turkey", "nigeria", "china",
}
TARGET_DIRECTOR_COUNTRIES = {
    "austria", "belgium", "bulgaria", "croatia", "cyprus", "czech republic",
    "czechia", "denmark", "estonia", "finland", "france", "germany", "greece",
    "hungary", "ireland", "italy", "latvia", "lithuania", "luxembourg", "malta",
    "netherlands", "poland", "portugal", "romania", "slovakia", "slovenia",
    "spain", "sweden", "norway", "united states", "usa",
    "united states of america", "india",
}
US_COUNTRIES = {"united states", "usa", "united states of america"}


def get_connection(database_url):
    return psycopg.connect(
        database_url,
        row_factory=dict_row,
        connect_timeout=30,
        sslmode="require",
    )


def today_in_uk():
    return datetime.now(UK_TIMEZONE).date().isoformat()


def normalise(value):
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def name_matches_target_keywords(company_name):
    name = normalise(company_name)
    return any(
        re.search(rf"(?<![a-z]){re.escape(keyword)}(?![a-z])", name)
        for keyword in TARGET_NAME_KEYWORDS
    )


def country_value(record, *keys):
    for key in keys:
        value = record.get(key)
        if value:
            return normalise(value)
    address = record.get("address") or {}
    return normalise(
        address.get("country")
        or address.get("country_of_residence")
        or address.get("countryOfResidence")
    )


def is_active_director(officer):
    ceased = officer.get("ceased_on") or officer.get("ceasedOn")
    officer_role = normalise(officer.get("officer_role") or officer.get("officerRole"))
    return not ceased and (not officer_role or "director" in officer_role)


def is_corporate_psc(psc):
    kind = normalise(psc.get("kind"))
    return "corporate" in kind or "legal-person" in kind or bool(
        psc.get("identification", {}).get("legal_form")
    )


def calculate_star_rating(buzzword_match, sic_match, corporate_psc, target_country, us_bonus):
    return int(buzzword_match) + int(sic_match) + int(corporate_psc) + int(target_country) + int(us_bonus)


def create_chime():
    sample_rate = 44100
    duration = 0.35
    frames = bytearray()
    for index in range(int(sample_rate * duration)):
        current_time = index / sample_rate
        frequency = 880 if current_time < 0.16 else 1175
        attack = min(1.0, index / 800)
        release = max(0.0, 1.0 - max(0.0, current_time - 0.20) / 0.15)
        sample = int(
            32767 * 0.25 * attack * release
            * math.sin(2 * math.pi * frequency * current_time)
        )
        frames.extend(sample.to_bytes(2, byteorder="little", signed=True))
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(sample_rate)
        audio.writeframes(frames)
    return buffer.getvalue()


@st.cache_data
def cached_chime():
    return create_chime()


def play_chime():
    encoded = base64.b64encode(cached_chime()).decode("ascii")
    st.markdown(
        f'<audio autoplay><source src="data:audio/wav;base64,{encoded}" type="audio/wav"></audio>',
        unsafe_allow_html=True,
    )


def dataframe_from_query(connection, query, params=()):
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [column.name for column in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def ensure_shortlist_column(dataframe):
    dataframe = dataframe.copy()
    if "Shortlist" not in dataframe.columns:
        dataframe["Shortlist"] = False
    dataframe["Shortlist"] = dataframe["Shortlist"].fillna(False).astype(bool)
    return dataframe


def google_search_name(company_name):
    name = str(company_name or "")
    for suffix in (" Limited", " LIMITED", " Ltd", " LTD"):
        name = name.replace(suffix, "")
    return "https://www.google.com/search?q=" + quote_plus(" ".join(name.split()).strip())


def add_google_search_links(dataframe):
    dataframe = dataframe.copy()
    dataframe["Google search"] = dataframe["Company name"].map(google_search_name)
    return dataframe


def ensure_worker_status_table(connection):
    connection.execute(
        "CREATE TABLE IF NOT EXISTS public.worker_status ("
        "id INTEGER PRIMARY KEY CHECK (id = 1), status TEXT NOT NULL, "
        "last_connected_at TIMESTAMPTZ, last_event_at TIMESTAMPTZ, "
        "last_error TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
    )
    connection.commit()


def update_worker_status(connection, status, error=None, event_received=False):
    connection.execute(
        "INSERT INTO public.worker_status (id, status, last_connected_at, last_event_at, last_error, updated_at) "
        "VALUES (1, %s, CASE WHEN %s = 'connected' THEN NOW() ELSE NULL END, "
        "CASE WHEN %s THEN NOW() ELSE NULL END, %s, NOW()) "
        "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, "
        "last_connected_at = CASE WHEN EXCLUDED.status = 'connected' THEN NOW() ELSE public.worker_status.last_connected_at END, "
        "last_event_at = CASE WHEN %s THEN NOW() ELSE public.worker_status.last_event_at END, "
        "last_error = EXCLUDED.last_error, updated_at = NOW()",
        (status, status, event_received, error, event_received),
    )


def table_exists(connection, table_name):
    row = connection.execute("SELECT to_regclass(%s) AS table_name", (f"public.{table_name}",)).fetchone()
    return row["table_name"] is not None


def check_database_connection(database_url):
    try:
        with get_connection(database_url) as connection:
            info = connection.execute(
                "SELECT current_database() AS database_name, current_schema() AS schema_name, NOW() AS database_time"
            ).fetchone()
            stream = connection.execute(
                "SELECT timepoint, updated_at FROM public.stream_state WHERE id = 1"
            ).fetchone() if table_exists(connection, "stream_state") else None
            worker = connection.execute(
                "SELECT status, last_connected_at, last_event_at, last_error, updated_at FROM public.worker_status WHERE id = 1"
            ).fetchone() if table_exists(connection, "worker_status") else None
        return True, info, stream, worker, None
    except Exception as error:
        return False, None, None, None, f"{type(error).__name__}: {error}"


def get_timepoint(connection):
    row = connection.execute("SELECT timepoint FROM public.stream_state WHERE id = 1").fetchone()
    return row["timepoint"] if row else None


def extract_metadata(event):
    metadata = event.get("event") or {}
    return metadata.get("timepoint", event.get("timepoint")), metadata.get("published_at", event.get("published_at"))


def save_timepoint(connection, timepoint):
    if timepoint is not None:
        connection.execute(
            "INSERT INTO public.stream_state (id, timepoint, updated_at) VALUES (1, %s, NOW()) "
            "ON CONFLICT (id) DO UPDATE SET timepoint = EXCLUDED.timepoint, updated_at = NOW()",
            (int(timepoint),),
        )


def save_matching_company(connection, company, published_at, received_at, test_all_sic_codes):
    company_number = company.get("company_number")
    company_name = company.get("company_name") or "Unnamed company"
    incorporation_date = company.get("date_of_creation")
    sic_codes = {str(code).strip() for code in (company.get("sic_codes") or [])}
    if not company_number or incorporation_date != today_in_uk():
        return False

    sic_match = bool(sic_codes.intersection(TARGET_SIC_CODES))
    buzzword_match = name_matches_target_keywords(company_name)
    if not test_all_sic_codes and not (sic_match or buzzword_match):
        return False

    company_url = f"https://find-and-update.company-information.service.gov.uk/company/{company_number}"
    connection.execute(
        "INSERT INTO public.screened_companies (company_number, company_name, incorporation_date, company_status, sic_codes, company_url, screened_at, shortlisted, published_at, received_at, buzzword_match, sic_match, enrichment_status) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE, %s, %s, %s, %s, 'pending') "
        "ON CONFLICT (company_number) DO UPDATE SET company_name = EXCLUDED.company_name, incorporation_date = EXCLUDED.incorporation_date, company_status = EXCLUDED.company_status, sic_codes = EXCLUDED.sic_codes, company_url = EXCLUDED.company_url, published_at = COALESCE(EXCLUDED.published_at, public.screened_companies.published_at), received_at = EXCLUDED.received_at, buzzword_match = EXCLUDED.buzzword_match, sic_match = EXCLUDED.sic_match, enrichment_status = CASE WHEN public.screened_companies.enrichment_status = 'complete' THEN public.screened_companies.enrichment_status ELSE 'pending' END",
        (company_number, company_name, incorporation_date, company.get("company_status", ""), ", ".join(sorted(sic_codes)), company_url, received_at, published_at, received_at, buzzword_match, sic_match),
    )
    return True


def stream_worker(database_url, api_key, test_all_sic_codes):
    reconnect_delay = 5
    status_interval = 30
    last_status_update = 0.0
    session = requests.Session()
    session.auth = (api_key, "")
    session.headers.update({"Accept": "application/json"})
    print(f"Worker starting. Mode={'ALL SIC' if test_all_sic_codes else 'SIC OR NAME'}. UK date={today_in_uk()}", flush=True)

    while True:
        connection = None
        try:
            connection = get_connection(database_url)
            ensure_worker_status_table(connection)
            update_worker_status(connection, "connecting")
            connection.commit()
            timepoint = get_timepoint(connection)
            params = {"timepoint": timepoint} if timepoint else {}
            with session.get(STREAM_URL, params=params, stream=True, timeout=(30, 300)) as response:
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
                    matched = save_matching_company(connection, company, published_at, received_at, test_all_sic_codes)
                    save_timepoint(connection, event_timepoint)
                    now = time.monotonic()
                    if now - last_status_update >= status_interval:
                        update_worker_status(connection, "connected", event_received=True)
                        last_status_update = now
                    connection.commit()
                    if matched:
                        print(f"Matched {company.get('company_number')} - {company.get('company_name', 'Unnamed company')}", flush=True)
        except (requests.RequestException, json.JSONDecodeError, psycopg.Error, OSError) as error:
            if connection is not None:
                try:
                    update_worker_status(connection, "reconnecting", error=str(error))
                    connection.commit()
                except psycopg.Error:
                    pass
            print(f"Worker disconnected: {error}. Reconnecting in {reconnect_delay} seconds.", flush=True)
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)
        finally:
            if connection is not None:
                connection.close()


def fetch_json(session, api_key, url):
    response = session.get(url, auth=(api_key, ""), timeout=(15, 45))
    response.raise_for_status()
    return response.json()


def enrich_one_company(database_url, api_key, row):
    company_number = row["company_number"]
    base_url = f"https://api.company-information.service.gov.uk/company/{company_number}"
    session = requests.Session()
    officers = fetch_json(session, api_key, f"{base_url}/officers").get("items", [])
    pscs = fetch_json(session, api_key, f"{base_url}/persons-with-significant-control").get("items", [])

    directors = [item for item in officers if is_active_director(item)]
    excluded = []
    target_directors = []
    us_bonus = False
    for director in directors:
        residence = country_value(director, "country_of_residence", "countryOfResidence")
        nationality = country_value(director, "nationality")
        if residence in EXCLUDED_DIRECTOR_COUNTRIES:
            excluded.append(director.get("name", "Unnamed director"))
        if residence in TARGET_DIRECTOR_COUNTRIES and nationality in TARGET_DIRECTOR_COUNTRIES:
            target_directors.append(director.get("name", "Unnamed director"))
            if residence in US_COUNTRIES and nationality in US_COUNTRIES:
                us_bonus = True

    corporate_names = [
        item.get("name", "Unnamed corporate PSC")
        for item in pscs
        if is_corporate_psc(item)
    ]
    corporate_psc = bool(corporate_names)
    target_country = bool(target_directors)
    buzzword = bool(row.get("buzzword_match"))
    sic_match = bool(row.get("sic_match"))
    stars = calculate_star_rating(buzzword, sic_match, corporate_psc, target_country, us_bonus)

    with get_connection(database_url) as connection:
        connection.execute(
            "UPDATE public.screened_companies SET corporate_psc = %s, corporate_psc_names = %s, director_count = %s, excluded_director_country = %s, excluded_director_names = %s, target_country_director = %s, target_country_director_names = %s, us_person_bonus = %s, star_rating = %s, enrichment_status = %s, enriched_at = NOW() WHERE company_number = %s",
            (corporate_psc, ", ".join(corporate_names), len(directors), bool(excluded), ", ".join(excluded), target_country, ", ".join(target_directors), us_bonus, stars, "excluded" if excluded else "complete", company_number),
        )
        connection.commit()


def enrichment_worker(database_url, api_key):
    while True:
        try:
            with get_connection(database_url) as connection:
                row = connection.execute(
                    "SELECT * FROM public.screened_companies WHERE enrichment_status = 'pending' AND incorporation_date = (NOW() AT TIME ZONE 'Europe/London')::date ORDER BY received_at ASC LIMIT 1"
                ).fetchone()
            if row:
                try:
                    enrich_one_company(database_url, api_key, row)
                except Exception as error:
                    with get_connection(database_url) as connection:
                        connection.execute(
                            "UPDATE public.screened_companies SET enrichment_status = 'error', enriched_at = NOW() WHERE company_number = %s",
                            (row["company_number"],),
                        )
                        connection.commit()
                    print(f"Enrichment failed for {row['company_number']}: {error}", flush=True)
                time.sleep(ENRICHMENT_PAUSE_SECONDS)
            else:
                time.sleep(2)
        except Exception as error:
            print(f"Enrichment worker error: {error}", flush=True)
            time.sleep(10)


@st.cache_resource
def start_workers_once(database_url, api_key, test_all_sic_codes):
    stream_thread = threading.Thread(target=stream_worker, args=(database_url, api_key, test_all_sic_codes), daemon=True, name="companies-house-stream-worker")
    enrichment_thread = threading.Thread(target=enrichment_worker, args=(database_url, api_key), daemon=True, name="companies-house-enrichment-worker")
    stream_thread.start()
    enrichment_thread.start()
    return stream_thread, enrichment_thread


def get_history(database_url):
    query = (
        "SELECT company_name AS \"Company name\", company_number AS \"Company number\", incorporation_date AS \"Incorporation date\", company_status AS \"Status\", sic_codes AS \"SIC codes\", company_url AS \"Companies House page\", received_at AS \"Received by worker\", published_at AS \"Published by Companies House\", shortlisted AS \"Shortlist\", corporate_psc AS \"Corporate PSC\", corporate_psc_names AS \"Corporate PSC names\", director_count AS \"Director count\", target_country_director AS \"Target-country director\", target_country_director_names AS \"Target-country director names\", us_person_bonus AS \"US person bonus\", star_rating AS \"Stars\", enrichment_status AS \"Enrichment status\" FROM public.screened_companies WHERE incorporation_date = (NOW() AT TIME ZONE 'Europe/London')::date AND excluded_director_country = FALSE ORDER BY published_at DESC NULLS LAST, received_at DESC NULLS LAST, company_number DESC LIMIT %s"
    )
    with get_connection(database_url) as connection:
        history = dataframe_from_query(connection, query, (DISPLAY_LIMIT,))
    return add_google_search_links(ensure_shortlist_column(history))


def update_changed_shortlist(database_url, previous_history, edited_history):
    previous = ensure_shortlist_column(previous_history).set_index("Company number")["Shortlist"]
    current = ensure_shortlist_column(edited_history).set_index("Company number")["Shortlist"]
    previous = previous.reindex(current.index).fillna(False).astype(bool)
    current = current.fillna(False).astype(bool)
    changed = current.index[previous.ne(current)]
    if len(changed) == 0:
        return
    with get_connection(database_url) as connection:
        connection.executemany(
            "UPDATE public.screened_companies SET shortlisted = %s WHERE company_number = %s",
            [(bool(current.loc[number]), number) for number in changed],
        )
        connection.commit()


def get_shortlist(database_url):
    query = (
        "SELECT company_name AS \"Company name\", company_number AS \"Company number\", incorporation_date AS \"Incorporation date\", company_status AS \"Status\", sic_codes AS \"SIC codes\", company_url AS \"Companies House page\", received_at AS \"Received by worker\", published_at AS \"Published by Companies House\", corporate_psc AS \"Corporate PSC\", corporate_psc_names AS \"Corporate PSC names\", director_count AS \"Director count\", target_country_director AS \"Target-country director\", target_country_director_names AS \"Target-country director names\", us_person_bonus AS \"US person bonus\", star_rating AS \"Stars\" FROM public.screened_companies WHERE incorporation_date = (NOW() AT TIME ZONE 'Europe/London')::date AND shortlisted = TRUE AND excluded_director_country = FALSE ORDER BY published_at DESC NULLS LAST, received_at DESC NULLS LAST, company_number DESC"
    )
    with get_connection(database_url) as connection:
        shortlist = dataframe_from_query(connection, query)
    return add_google_search_links(shortlist)


st.set_page_config(page_title="Live Companies House Screener", page_icon="⚡", layout="wide")
required = ["DATABASE_URL", "COMPANIES_HOUSE_STREAMING_API_KEY"]
missing = [key for key in required if key not in st.secrets]
if missing:
    st.error("Add these missing values to Streamlit Secrets: " + ", ".join(missing))
    st.stop()

database_url = st.secrets["DATABASE_URL"]
api_key = st.secrets["COMPANIES_HOUSE_STREAMING_API_KEY"]
test_all_sic_codes = str(st.secrets.get("TEST_ALL_SIC_CODES", "false")).lower() == "true"
start_workers_once(database_url, api_key, test_all_sic_codes)

st_autorefresh(interval=REFRESH_INTERVAL_MS, debounce=True, key="dashboard_refresh")
st.title("Live Companies House Screener")
st.caption(f"Application version: {APP_VERSION}")
st.caption("New matching companies are enriched in the background without blocking the stream.")

with st.sidebar:
    st.subheader("System status")
    database_ok, info, stream, worker, error = check_database_connection(database_url)
    if database_ok:
        st.success("Database connected")
        if worker and worker["status"] == "connected":
            st.success("Companies House stream connected")
        elif worker:
            st.warning(f"Worker status: {worker['status']}")
            if worker["last_error"]:
                st.error(worker["last_error"])
        else:
            st.warning("No worker status recorded yet")
        if stream:
            st.write(f"Checkpoint: {stream['timepoint']}")
    else:
        st.error("Database disconnected")
        st.code(error)

    st.subheader("Notifications")
    sound_enabled = st.checkbox("Play a chime for new companies", value=st.session_state.get("sound_enabled", False))
    st.session_state.sound_enabled = sound_enabled
    if sound_enabled and st.button("Test chime"):
        play_chime()
        st.success("Chime played")

try:
    history = get_history(database_url)
    status_row = get_connection(database_url).execute("SELECT COUNT(*) AS total FROM public.screened_companies WHERE incorporation_date = (NOW() AT TIME ZONE 'Europe/London')::date").fetchone()
except Exception as error:
    st.error(f"Could not read Supabase: {error}")
    st.stop()

current_count = int(status_row["total"] or 0)
previous_count = st.session_state.get("known_company_count", current_count)
new_company = current_count > previous_count
st.session_state.known_company_count = current_count
if sound_enabled and new_company:
    play_chime()
    st.toast("New company received", icon="🔔")

st.metric("Today's visible companies", len(history))
with st.expander("Rating rules"):
    st.write("One star: buzzword. One star: target SIC. One star: corporate PSC. One star: target-country director. One extra star: US person bonus.")

st.subheader("Today's company results")
if history.empty:
    st.info("No enriched companies are visible yet. New rows may briefly show as pending enrichment.")
    st.stop()

editor_columns = [
    "Shortlist", "Stars", "Company name", "Company number", "Incorporation date", "Status", "SIC codes", "Corporate PSC", "Corporate PSC names", "Director count", "Target-country director", "Target-country director names", "US person bonus", "Enrichment status", "Companies House page", "Google search", "Received by worker", "Published by Companies House",
]
editable = ensure_shortlist_column(history[editor_columns])
edited = st.data_editor(
    editable,
    use_container_width=True,
    hide_index=True,
    key="enriched_editor",
    disabled=[column for column in editor_columns if column != "Shortlist"],
    column_config={
        "Shortlist": st.column_config.CheckboxColumn("Shortlist", default=False, pinned=True),
        "Companies House page": st.column_config.LinkColumn("Companies House page", display_text="Open company page"),
        "Google search": st.column_config.LinkColumn("Google search", display_text="Search Google"),
        "Stars": st.column_config.NumberColumn("Stars", min_value=0, max_value=5, format="%d ⭐"),
    },
)
update_changed_shortlist(database_url, editable, edited)

shortlist = get_shortlist(database_url)
st.subheader("Today's shortlist")
st.write(f"{len(shortlist)} company(ies) selected today.")
if not shortlist.empty:
    st.download_button("Download today's shortlist as CSV", data=shortlist.to_csv(index=False).encode("utf-8"), file_name="companies_house_shortlist.csv", mime="text/csv", type="primary")
    st.dataframe(shortlist, use_container_width=True, hide_index=True)
else:
    st.info("Select companies above to create a shortlist.")
