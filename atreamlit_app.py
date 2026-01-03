import time
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# =====================================================
# Page Configuration
# =====================================================
st.set_page_config(
    page_title="Public Food Inventory Monitoring",
    layout="wide"
)

st.title("🍚 Smart Inventory Monitoring for Public Food Distribution")
st.caption("AI for Good | Snowflake Native Application")

# =====================================================
# Session Timeout Configuration (30 Minutes)
# =====================================================
SESSION_TIMEOUT_SECONDS = 30 * 60  # 30 minutes
current_time = time.time()

if "session_start_time" not in st.session_state:
    st.session_state.session_start_time = current_time

elapsed_time = current_time - st.session_state.session_start_time

if elapsed_time > SESSION_TIMEOUT_SECONDS:
    st.warning("⏳ Restarting session...")
    st.session_state.clear()
    st.rerun()

# Sidebar session countdown
remaining_time = max(0, SESSION_TIMEOUT_SECONDS - int(elapsed_time))
minutes, seconds = divmod(remaining_time, 60)

st.sidebar.info(f"⏱ Session expires in {minutes}m {seconds}s")

# =====================================================
# Snowflake Session
# =====================================================
session = get_active_session()

# =====================================================
# Helper Functions
# =====================================================
def run_query(query: str) -> pd.DataFrame:
    """Execute Snowflake SQL and return Pandas DataFrame."""
    return session.sql(query).to_pandas()


def status_icon(status: str) -> str:
    return {
        "Healthy": "🟢 Healthy",
        "At Risk": "🟡 At Risk",
        "Critical": "🔴 Critical"
    }.get(status, status)


def format_status(row: pd.Series) -> str:
    if row["STOCK_STATUS"] == "Critical":
        if pd.notna(row["SOURCE_STORAGE"]):
            return (
                f"🔴 Critical\n"
                f"➡️ {row['SOURCE_STORAGE']} "
                f"({round(row['APPROX_DISTANCE_KM'], 1)} km)"
            )
        return "🔴 Critical\n➡️ No nearby stock"
    if row["STOCK_STATUS"] == "At Risk":
        return "🟡 At Risk"
    return "🟢 Healthy"

# =====================================================
# KPI Section
# =====================================================
kpi_query = """
SELECT
    COUNT(DISTINCT location_id) AS total_locations,
    COUNT(DISTINCT item_id) AS total_items,
    SUM(CASE WHEN stock_status = 'Critical' THEN 1 ELSE 0 END) AS critical_items
FROM dt_inventory_health
"""

kpi_df = run_query(kpi_query)

col1, col2, col3 = st.columns(3)
col1.metric("📍 Locations Monitored", int(kpi_df["TOTAL_LOCATIONS"][0]))
col2.metric("🍚 Food Items", int(kpi_df["TOTAL_ITEMS"][0]))
col3.metric("🚨 Critical Items", int(kpi_df["CRITICAL_ITEMS"][0]))

# =====================================================
# Inventory Health Heatmap
# =====================================================
st.subheader("📊 Inventory Health Heatmap")

heatmap_query = """
SELECT
    location_name,
    item_name,
    stock_status,
    source_storage,
    approx_distance_km
FROM inventory_health_with_source
"""

inventory_df = run_query(heatmap_query)
inventory_df["STATUS"] = inventory_df["STOCK_STATUS"].apply(status_icon)

basic_heatmap = inventory_df.pivot(
    index="LOCATION_NAME",
    columns="ITEM_NAME",
    values="STATUS"
)

st.dataframe(basic_heatmap, use_container_width=True)

# =====================================================
# Heatmap with Source Recommendation
# =====================================================
st.subheader("📍 Inventory Health Heatmap (with Source Recommendation)")

inventory_df["DISPLAY_STATUS"] = inventory_df.apply(format_status, axis=1)

detailed_heatmap = inventory_df.pivot(
    index="LOCATION_NAME",
    columns="ITEM_NAME",
    values="DISPLAY_STATUS"
)

st.dataframe(detailed_heatmap, use_container_width=True)

# =====================================================
# Inventory Status Overview
# =====================================================
st.subheader("📊 Inventory Status Overview")

status_query = """
SELECT stock_status, COUNT(*) AS count
FROM AI_FOR_GOOD_DB.INVENTORY.DT_INVENTORY_HEALTH
GROUP BY stock_status
"""

status_df = run_query(status_query).set_index("STOCK_STATUS")
st.bar_chart(status_df)

# =====================================================
# Days of Stock Remaining
# =====================================================
st.subheader("⏳ Days of Stock Remaining (Critical & At Risk)")

days_query = """
SELECT item_name, days_of_stock_remaining
FROM AI_FOR_GOOD_DB.INVENTORY.DT_INVENTORY_HEALTH
WHERE stock_status IN ('Critical', 'At Risk')
ORDER BY days_of_stock_remaining ASC
"""

days_df = run_query(days_query).set_index("ITEM_NAME")
st.bar_chart(days_df)

# =====================================================
# Estimated Reorder Cost
# =====================================================
st.subheader("💰 Estimated Reorder Cost by Location")

cost_query = """
SELECT
    location_name,
    SUM(recommended_reorder_qty * unit_price) AS estimated_cost
FROM AI_FOR_GOOD_DB.INVENTORY.DT_INVENTORY_HEALTH
WHERE stock_status = 'Critical'
GROUP BY location_name
"""

cost_df = run_query(cost_query)

if cost_df.empty:
    st.info("No critical items. No immediate reorder cost.")
else:
    st.bar_chart(cost_df.set_index("LOCATION_NAME"))

# =====================================================
# Source Transparency
# =====================================================
st.subheader("📦 Available Stock for Critical Items (Source Transparency)")

source_query = """
SELECT
    demand_location,
    item_name,
    storage_location,
    available_qty,
    approx_distance_km
FROM sourcing_transparency
ORDER BY demand_location, item_name, approx_distance_km
"""

source_df = run_query(source_query)

if source_df.empty:
    st.info("No storage units currently hold stock for critical items.")
else:
    st.dataframe(source_df, use_container_width=True)

# =====================================================
# Item-Level Exploration
# =====================================================
st.subheader("🔍 Explore Stock Availability by Food Item")

items_query = """
SELECT DISTINCT item_name
FROM sourcing_transparency
ORDER BY item_name
"""

items_df = run_query(items_query)

selected_item = st.selectbox(
    "Select Food Item",
    items_df["ITEM_NAME"].tolist()
)

filtered_df = source_df[source_df["ITEM_NAME"] == selected_item]

st.subheader(f"📍 Distance vs Available Quantity — {selected_item}")

dist_df = filtered_df[
    ["STORAGE_LOCATION", "AVAILABLE_QTY", "APPROX_DISTANCE_KM"]
].set_index("STORAGE_LOCATION")

st.dataframe(dist_df, use_container_width=True)
st.bar_chart(dist_df["AVAILABLE_QTY"])

# =====================================================
# Active Alerts
# =====================================================
st.subheader("🚨 Active Critical Alerts")

alerts_query = """
SELECT
    alert_time,
    location_name,
    item_name,
    source_storage,
    approx_distance_km,
    recommended_reorder_qty,
    alert_message
FROM enhanced_inventory_alerts
ORDER BY alert_time DESC
"""

alerts_df = run_query(alerts_query)

if alerts_df.empty:
    st.success("No critical alerts at the moment.")
else:
    st.dataframe(alerts_df, use_container_width=True)

# =====================================================
# Reorder Priority List
# =====================================================
st.subheader("📦 Reorder Priority List")

reorder_query = """
SELECT
    location_name,
    item_name,
    recommended_reorder_qty
FROM dt_inventory_health
WHERE recommended_reorder_qty > 0
ORDER BY recommended_reorder_qty DESC
"""

reorder_df = run_query(reorder_query)
st.dataframe(reorder_df, use_container_width=True)

# =====================================================
# Automated Inventory Risk Summary
# =====================================================
st.subheader("📌 Automated Inventory Risk Summary")

risk_query = "SELECT * FROM inventory_risk_summary"
risk_df = run_query(risk_query)

if risk_df.empty or risk_df["CRITICAL_ITEM_COUNT"][0] == 0:
    st.success("Inventory levels are stable across all locations.")
else:
    st.warning(
        f"🚨 {int(risk_df['CRITICAL_ITEM_COUNT'][0])} critical items detected "
        f"across {int(risk_df['IMPACTED_LOCATIONS'][0])} locations.\n\n"
        f"⚠️ Issues: {risk_df['ISSUE_DETAILS'][0]}\n\n"
        f"💰 Estimated reorder cost: ₹{int(risk_df['ESTIMATED_REORDER_COST'][0])}"
    )

# =====================================================
# Actionable AI Summary
# =====================================================
st.subheader("🧠 Actionable Inventory Summary")

action_query = """
SELECT location_name, item_name, summary_text
FROM inventory_action_summary
"""

action_df = run_query(action_query)

if action_df.empty:
    st.success("No redistribution or procurement actions required.")
else:
    for _, row in action_df.iterrows():
        st.warning(f"📌 {row['SUMMARY_TEXT']}")



