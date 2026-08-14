--- app.py
+++ app.py
@@
 import base64
 import io
 import json
 import math
 import threading
 import time
 import wave
 from datetime import date, datetime, timezone
+from urllib.parse import quote_plus
@@
 def ensure_shortlist_column(dataframe):
@@
     return dataframe
+
+
+def google_search_name(company_name):
+    """Return a Google search URL with Ltd/Limited removed from the name."""
+    search_name = str(company_name or "")
+    search_name = search_name.replace(" Limited", "")
+    search_name = search_name.replace(" LIMITED", "")
+    search_name = search_name.replace(" Ltd", "")
+    search_name = search_name.replace(" LTD", "")
+    search_name = " ".join(search_name.split()).strip()
+    return "https://www.google.com/search?q=" + quote_plus(search_name)
+
+
+def add_google_search_links(dataframe):
+    dataframe = dataframe.copy()
+    dataframe["Google search"] = dataframe["Company name"].map(
+        google_search_name
+    )
+    return dataframe
@@
-        "published_at AS \"Published by Companies House\", "
+        "published_at AS \"Published by Companies House\", "
         "shortlisted AS \"Shortlist\" FROM screened_companies "
         "WHERE incorporation_date >= %s AND incorporation_date <= %s "
-        "ORDER BY received_at DESC, company_name ASC LIMIT %s"
+        "ORDER BY published_at DESC NULLS LAST, "
+        "received_at DESC NULLS LAST, company_number DESC LIMIT %s"
@@
-    return ensure_shortlist_column(history)
+    return add_google_search_links(ensure_shortlist_column(history))
@@
-        "AND shortlisted = TRUE ORDER BY received_at DESC"
+        "AND shortlisted = TRUE "
+        "ORDER BY published_at DESC NULLS LAST, "
+        "received_at DESC NULLS LAST, company_number DESC"
@@
-    with get_connection(database_url) as connection:
-        return dataframe_from_query(
+    with get_connection(database_url) as connection:
+        shortlist = dataframe_from_query(
             connection, query, (start_date.isoformat(), end_date.isoformat())
         )
+    return add_google_search_links(shortlist)
@@
-    "Status", "SIC codes", "Companies House page", "Received by worker",
-    "Published by Companies House",
+    "Status", "SIC codes", "Companies House page", "Google search",
+    "Received by worker", "Published by Companies House",
@@
         "Companies House page": st.column_config.LinkColumn(
             "Companies House page", display_text="Open company page"
         ),
+        "Google search": st.column_config.LinkColumn(
+            "Google search", display_text="Search Google"
+        ),
@@
             "Companies House page": st.column_config.LinkColumn(
                 "Companies House page", display_text="Open company page"
-            )
+            ),
+            "Google search": st.column_config.LinkColumn(
+                "Google search", display_text="Search Google"
+            ),
*** End Patch
