"""Constant Leads - direct PostgreSQL dashboard with live status and new-lead audio."""

import html
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
import psycopg
import streamlit as st
import streamlit.components.v1 as components


POLL_SECONDS = 5


@st.cache_resource
def get_stream_state() -> Dict[str, Any]:
    return {
        "lock": threading.Lock(),
        "stream_worker_live": False,
        "stream_worker_started_at": None,
        "last_stream_error": None,
    }


def read_secret(name: str) -> str:
    try:
        value = st.secrets.get(name, "")
    except Exception:
        value = ""
    return str(value or os.getenv(name, "")).strip()


def get_database_url() -> str:
    database_url = read_secret("DATABASE_URL")
    if not database_url:
        st.error("Missing database configuration. Add DATABASE_URL to Streamlit app Settings → Secrets.")
        st.stop()
    return database_url


def check_database(database_url: str) -> tuple[bool, str]:
    try:
        with psycopg.connect(database_url, connect_timeout=10) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        return True, "Connected"
    except Exception as exc:
        return False, str(exc)


def database_has_table(database_url: str) -> bool:
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'screened_companies'
        )
    """
    with psycopg.connect(database_url, connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return bool(result and result[0])


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
            columns = [description.name for description in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def get_latest_lead_marker(companies: pd.DataFrame) -> str:
    if companies.empty:
        return ""
    if "company_number" in companies.columns:
        return str(companies.iloc[0].get("company_number") or "")
    if "received_at" in companies.columns:
        return str(companies.iloc[0].get("received_at") or "")
    return str(companies.iloc[0].to_dict())


def format_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return str(value)
    return value


def play_new_lead_sound() -> None:
    sound_url = read_secret("NEW_LEAD_SOUND_URL")
    if sound_url:
        safe_url = html.escape(sound_url, quote=True)
        components.html(
            f"""
            <audio autoplay>
                <source src="{safe_url}" type="audio/mpeg">
            </audio>
            """,
            height=0,
        )
        return

    # Fallback chime generated in the browser. This avoids bundling an audio file.
    components.html(
        """
        <script>
        const AudioContext = window.AudioContext || window.webkitAudioContext;
        if (AudioContext) {
            const ctx = new AudioContext();
            const now = ctx.currentTime;
            [880, 1175].forEach((frequency, index) => {
                const oscillator = ctx.createOscillator();
                const gain = ctx.createGain();
                oscillator.frequency.value = frequency;
                oscillator.type = "sine";
                gain.gain.setValueAtTime(0.0001, now + index * 0.14);
                gain.gain.exponentialRampToValueAtTime(0.16, now + index * 0.14 + 0.02);
                gain.gain.exponentialRampToValueAtTime(0.0001, now + index * 0.14 + 0.25);
                oscillator.connect(gain);
                gain.connect(ctx.destination);
                oscillator.start(now + index * 0.14);
                oscillator.stop(now + index * 0.14 + 0.28);
            });
        }
        </script>
        """,
        height=0,
    )


def render_status_sidebar(
    database_ok: bool,
    database_message: str,
    stream_state: Dict[str, Any],
    table_ok: bool,
) -> None:
    with st.sidebar:
        st.header("System status")
        if database_ok:
            st.success("Database connected")
        else:
            st.error("Database disconnected")
            st.caption(database_message[:250])

        if table_ok:
            st.success("Leads table available")
        else:
            st.error("Leads table unavailable")

        if stream_state.get("stream_worker_live"):
            st.success("Stream worker live")
        else:
            st.warning("Stream worker not detected")

        started_at = stream_state.get("stream_worker_started_at")
        if started_at:
            st.caption(f"Worker started: {started_at}")
        if stream_state.get("last_stream_error"):
            st.caption(f"Worker error: {stream_state['last_stream_error'][:250]}")

        st.divider()
        st.caption(f"Refresh interval: {POLL_SECONDS} seconds")
        st.caption("Audio alert: enabled")


def main() -> None:
    st.set_page_config(page_title="Constant Leads", page_icon="📋", layout="wide")
    st.title("Constant Leads")
    st.caption("Live lead stream")

    database_url = get_database_url()
    state = get_stream_state()

    with state["lock"]:
        state["stream_worker_live"] = True
        state["stream_worker_started_at"] = state["stream_worker_started_at"] or datetime.now(
            timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")

    database_ok, database_message = check_database(database_url)
    table_ok = False
    companies = pd.DataFrame()

    if database_ok:
        try:
            table_ok = database_has_table(database_url)
            if table_ok:
                companies = load_companies(database_url)
        except Exception as exc:
            database_ok = False
            database_message = str(exc)
            state["last_stream_error"] = str(exc)

    render_status_sidebar(database_ok, database_message, state, table_ok)

    if not database_ok:
        st.error("The direct database connection is not working.")
        st.info("Check DATABASE_URL in Streamlit Secrets and reboot the app.")
        st.stop()

    if not table_ok:
        st.error("The public.screened_companies table was not found.")
        st.stop()

    if companies.empty:
        st.info("No screened companies found yet.")
    else:
        for column in companies.columns:
            companies[column] = companies[column].map(format_value)

        latest_marker = get_latest_lead_marker(companies)
        previous_marker = st.session_state.get("latest_lead_marker")
        is_new_lead = previous_marker is not None and latest_marker and latest_marker != previous_marker
        st.session_state["latest_lead_marker"] = latest_marker

        if is_new_lead:
            st.toast("New lead received")
            play_new_lead_sound()

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

        st.dataframe(companies, use_container_width=True, hide_index=True)

    time.sleep(POLL_SECONDS)
    st.rerun()


if __name__ == "__main__":
    main()
