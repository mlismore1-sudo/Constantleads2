--- app.py
+++ app.py
@@
-APP_VERSION = "2026-08-13-single-streamlit-app-health-check"
+APP_VERSION = "2026-08-13-stream-diagnostics"
@@
 def get_connection(database_url):
@@
     )
 
 
+def ensure_worker_status_table(connection):
+    connection.execute(
+        "CREATE TABLE IF NOT EXISTS worker_status ("
+        "id INTEGER PRIMARY KEY CHECK (id = 1), "
+        "status TEXT NOT NULL, "
+        "last_connected_at TIMESTAMPTZ, "
+        "last_event_at TIMESTAMPTZ, "
+        "last_error TEXT, "
+        "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
+        ")"
+    )
+    connection.commit()
+
+
+def update_worker_status(connection, status, error=None, event_received=False):
+    connection.execute(
+        "INSERT INTO worker_status ("
+        "id, status, last_connected_at, last_event_at, last_error, updated_at"
+        ") VALUES ("
+        "1, %s, "
+        "CASE WHEN %s = 'connected' THEN NOW() ELSE NULL END, "
+        "CASE WHEN %s THEN NOW() ELSE NULL END, %s, NOW()"
+        ") ON CONFLICT (id) DO UPDATE SET "
+        "status = EXCLUDED.status, "
+        "last_connected_at = CASE "
+        "WHEN EXCLUDED.status = 'connected' THEN NOW() "
+        "ELSE worker_status.last_connected_at END, "
+        "last_event_at = CASE "
+        "WHEN %s THEN NOW() ELSE worker_status.last_event_at END, "
+        "last_error = EXCLUDED.last_error, "
+        "updated_at = NOW()",
+        (status, status, event_received, error, event_received),
+    )
+    connection.commit()
+
+
 def check_database_connection(database_url):
     try:
         with get_connection(database_url) as connection:
@@
-            stream = connection.execute(
+            stream = connection.execute(
                 "SELECT timepoint, updated_at "
                 "FROM stream_state WHERE id = 1"
             ).fetchone()
-        return True, database, stream, None
+            worker = connection.execute(
+                "SELECT status, last_connected_at, last_event_at, "
+                "last_error, updated_at "
+                "FROM worker_status WHERE id = 1"
+            ).fetchone()
+        return True, database, stream, worker, None
     except Exception as error:
-        return False, None, None, f"{type(error).__name__}: {error}"
+        return False, None, None, None, f"{type(error).__name__}: {error}"
@@
         try:
             connection = get_connection(database_url)
+            ensure_worker_status_table(connection)
+            update_worker_status(connection, "connecting")
             timepoint = get_timepoint(connection)
@@
                 response.raise_for_status()
                 reconnect_delay = 5
+                update_worker_status(connection, "connected")
                 print("Companies House stream connected.", flush=True)
@@
                     save_timepoint(connection, event_timepoint)
                     connection.commit()
+                    update_worker_status(
+                        connection,
+                        "connected",
+                        event_received=True,
+                    )
@@
         ) as error:
+            if connection is not None:
+                try:
+                    update_worker_status(
+                        connection,
+                        "reconnecting",
+                        error=str(error),
+                    )
+                except psycopg.Error:
+                    pass
             print(
@@
-start_date_secret = str(st.secrets.get("STREAM_START_DATE", date.today()))
+start_date_secret = str(st.secrets.get("STREAM_START_DATE", date.today()))
+try:
+    default_start_date = date.fromisoformat(start_date_secret)
+except ValueError:
+    st.error("STREAM_START_DATE must use YYYY-MM-DD format.")
+    st.stop()
@@
-    database_ok, database_info, stream_info, database_error = (
+    database_ok, database_info, stream_info, worker_info, database_error = (
         check_database_connection(database_url)
     )
@@
-        if stream_info:
-            st.success("Stream state found")
-            st.write(f"Last timepoint: {stream_info['timepoint']}")
-            st.write(f"Last stream update: {stream_info['updated_at']}")
+        if worker_info:
+            if worker_info["status"] == "connected":
+                st.success("Companies House stream connected")
+            else:
+                st.warning(f"Worker status: {worker_info['status']}")
+            st.write(f"Last connected: {worker_info['last_connected_at'] or 'None'}")
+            st.write(f"Last event received: {worker_info['last_event_at'] or 'None'}")
+            if worker_info["last_error"]:
+                st.error(f"Latest worker error: {worker_info['last_error']}")
         else:
-            st.warning("No stream state recorded yet")
+            st.warning("Worker has not yet written a status record")
+
+        if stream_info:
+            with st.expander("Stream checkpoint"):
+                st.write(f"Last timepoint: {stream_info['timepoint']}")
+                st.write(f"Last checkpoint update: {stream_info['updated_at']}")
@@
-start_date = col1.date_input("From incorporation date", value=date.today())
+start_date = col1.date_input(
+    "From incorporation date",
+    value=default_start_date,
+)
 end_date = col2.date_input("To incorporation date", value=date.today())
@@
 with st.expander("Connection diagnostics"):
@@
     st.write(f"SIC mode: {'ALL' if test_all_sic_codes else 'REFINED'}")
+    st.write(f"Target SIC codes: {', '.join(sorted(TARGET_SIC_CODES))}")
     st.write(f"Worker start date: {start_date_secret}")
+    st.caption(
+        "The Companies House stream is live-only. Changing the date range "
+        "shows records already stored in Supabase; it does not backfill "
+        "older Companies House events."
+    )
