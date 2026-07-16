"""T1: create usera_project content on the live test cluster.

Creates (as usera, online_enabled=True):
  - FG usera_customers_fg    (customer_id PK, age, country, is_premium, event_time)
  - FG usera_transactions_fg (customer_id PK, num_transactions_30d,
                              total_spend_30d, avg_transaction_value_30d, event_time)
  - FV usera_customers_transactions_fv  -- all columns of both FGs
  - FV usera_txncount_fv                -- only num_transactions_30d from transactions

Offline materialization is skipped (RDRS reads the online store only).
See docs/fg_fv_sharing_fine_grained_test_design.md §1.

Usage:
  source .../server/api_keys
  .../fg_test_env/bin/python t1_create_usera_content.py
"""

import os
import sys
from datetime import datetime, timezone

import pandas as pd
import hopsworks

HOST = "10.115.253.130"
API_KEY = os.environ.get("USERA_API_KEY")
if not API_KEY:
    sys.exit("USERA_API_KEY not set - source the api_keys file first")

project = hopsworks.login(
    host=HOST,
    port=443,
    project="usera_project",
    api_key_value=API_KEY,
)
print(f"logged in: project={project.name} id={project.id}")
fs = project.get_feature_store()
print(f"feature store: {fs.name} id={fs.id}")

now = datetime.now(timezone.utc)

customers_df = pd.DataFrame({
    "customer_id": [1, 2, 3, 4],
    "age": [25, 41, 33, 52],
    "country": ["SE", "PK", "SE", "NO"],
    "is_premium": [0, 1, 1, 0],
    "event_time": [now, now, now, now],
})

transactions_df = pd.DataFrame({
    "customer_id": [1, 2, 3, 4],
    "num_transactions_30d": [3, 8, 12, 1],
    "total_spend_30d": [120.5, 540.0, 880.2, 45.9],
    "avg_transaction_value_30d": [40.17, 67.50, 73.35, 45.90],
    "event_time": [now, now, now, now],
})

customers_fg = fs.get_or_create_feature_group(
    name="usera_customers_fg",
    version=1,
    description="Customer profile features",
    primary_key=["customer_id"],
    event_time="event_time",
    online_enabled=True,
)
customers_fg.insert(
    customers_df,
    write_options={"start_offline_materialization": False},
)
print("usera_customers_fg created + 4 rows inserted (online)")

transactions_fg = fs.get_or_create_feature_group(
    name="usera_transactions_fg",
    version=1,
    description="Customer transaction aggregate features",
    primary_key=["customer_id"],
    event_time="event_time",
    online_enabled=True,
)
transactions_fg.insert(
    transactions_df,
    write_options={"start_offline_materialization": False},
)
print("usera_transactions_fg created + 4 rows inserted (online)")

# Full FV: every column of both FGs
full_query = customers_fg.select(
    ["customer_id", "age", "country", "is_premium"]
).join(
    transactions_fg.select(
        ["num_transactions_30d", "total_spend_30d", "avg_transaction_value_30d"]
    ),
    on=["customer_id"],
)
full_fv = fs.get_or_create_feature_view(
    name="usera_customers_transactions_fv",
    version=1,
    description="All columns of customers + transactions",
    query=full_query,
    labels=[],
)
print("usera_customers_transactions_fv created")

# Narrow FV: only num_transactions_30d from transactions_fg.
# Instrument for partial grants: works when only that column is shared,
# while the full FV above must be denied.
narrow_query = customers_fg.select(["customer_id", "age"]).join(
    transactions_fg.select(["num_transactions_30d"]),
    on=["customer_id"],
)
narrow_fv = fs.get_or_create_feature_view(
    name="usera_txncount_fv",
    version=1,
    description="Only num_transactions_30d from transactions",
    query=narrow_query,
    labels=[],
)
print("usera_txncount_fv created")

# Smoke test: serve one vector through the online store as usera
vec = full_fv.get_feature_vector({"customer_id": 1})
print("feature vector for customer_id=1:", vec)
print("T1 content DONE")
