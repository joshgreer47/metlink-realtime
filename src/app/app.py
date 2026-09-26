"""Departures board: upcoming arrivals and service alerts for a stop, as of the latest data refresh."""

import os

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

CATALOG = os.environ.get("METLINK_CATALOG", "metlink")
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

st.set_page_config(page_title="Metlink departures", layout="wide")


@st.cache_resource
def client() -> WorkspaceClient:
    return WorkspaceClient()


@st.cache_data(ttl=300, show_spinner=False)
def query(sql: str, **params) -> pd.DataFrame:
    response = client().statement_execution.execute_statement(
        statement=sql,
        warehouse_id=WAREHOUSE_ID,
        catalog=CATALOG,
        parameters=[StatementParameterListItem(name=k, value=str(v)) for k, v in params.items()],
        wait_timeout="50s",
    )
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(response.status.error.message if response.status.error else response.status.state)
    columns = [c.name for c in response.manifest.schema.columns]
    return pd.DataFrame((response.result.data_array if response.result else None) or [], columns=columns)


def table_exists(name: str) -> bool:
    schema, table = name.split(".")
    found = query(
        "SELECT count(*) AS n FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table",
        schema=schema,
        table=table,
    )
    return int(found["n"].iloc[0]) > 0


st.title("Metlink departures")

stops = query(
    """
    SELECT stop_id, first(stop_name) AS stop_name, count(*) AS arrivals
    FROM silver.stop_arrivals
    WHERE arrival_at >= (SELECT max(arrival_at) FROM silver.stop_arrivals) - INTERVAL 7 DAYS
    GROUP BY stop_id
    ORDER BY arrivals DESC
    """
)
if stops.empty:
    st.info("No arrivals have been processed yet.")
    st.stop()

labels = stops["stop_name"] + " (" + stops["stop_id"] + ")"
choice = st.selectbox("Stop", labels, index=0, help="Stops are ordered by how busy they are.")
stop_id = stops.loc[labels == choice, "stop_id"].iloc[0]

left, right = st.columns([2, 1])

with left:
    if table_exists("ml.arrival_predictions"):
        upcoming = query(
            """
            SELECT from_utc_timestamp(predicted_arrival_at, 'Pacific/Auckland') AS expected,
                   from_utc_timestamp(target_scheduled_at, 'Pacific/Auckland') AS scheduled,
                   route_label AS route, round(predicted_delay_s / 60, 1) AS delay_min, vehicle_id AS vehicle,
                   from_utc_timestamp(snapshot_at, 'Pacific/Auckland') AS as_of
            FROM ml.arrival_predictions
            WHERE target_stop_id = :stop_id
            ORDER BY predicted_arrival_at
            """,
            stop_id=stop_id,
        )
        st.subheader("Expected arrivals")
        if upcoming.empty:
            st.write("No predicted arrivals at this stop in the latest snapshot.")
        else:
            st.caption(f"Predicted by the arrival delay model as of {upcoming['as_of'].iloc[0]} (NZ time).")
            st.dataframe(upcoming.drop(columns="as_of"), hide_index=True, use_container_width=True)

    recent = query(
        """
        SELECT from_utc_timestamp(arrival_at, 'Pacific/Auckland') AS arrived,
               from_utc_timestamp(scheduled_arrival_at, 'Pacific/Auckland') AS scheduled,
               route_label AS route, round(arrival_delay_s / 60, 1) AS delay_min, vehicle_id AS vehicle
        FROM silver.stop_arrivals
        WHERE stop_id = :stop_id AND is_final
        ORDER BY arrival_at DESC
        LIMIT 15
        """,
        stop_id=stop_id,
    )
    st.subheader("Recent arrivals")
    st.dataframe(recent, hide_index=True, use_container_width=True)

with right:
    alerts = query(
        """
        SELECT DISTINCT a.effect, a.header_text AS alert
        FROM silver.service_alert_entities e
        JOIN silver.service_alerts a USING (alert_id)
        LEFT JOIN (SELECT DISTINCT route_id FROM silver.stop_arrivals WHERE stop_id = :stop_id) r
          ON e.route_id = r.route_id
        WHERE a.__END_AT IS NULL AND (e.stop_id = :stop_id OR r.route_id IS NOT NULL)
        """,
        stop_id=stop_id,
    )
    st.subheader("Service alerts")
    if alerts.empty:
        st.write("No active alerts for this stop or its routes.")
    for row in alerts.itertuples():
        st.warning(f"**{row.effect.replace('_', ' ').title()}**: {row.alert}")

st.caption(f"Data: Metlink open data via the `{CATALOG}` catalog. Refreshed with the daily pipeline, so not live.")
