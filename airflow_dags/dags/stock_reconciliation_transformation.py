"""
stock_reconciliation_transformation.py

Bronze -> silver transformation DAG for the `stock_reconciliation` entity.
Triggered exclusively by bronze_to_silver_orchestrator with
conf={"start_time": ..., "end_time": ...}; not scheduled on its own.
"""
from __future__ import annotations

import logging
import os
import sys
import json

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

# Airflow 3's DAG bundle loader doesn't guarantee the dags/ folder is on
# sys.path, unlike plain-module imports elsewhere -- add it explicitly so
# sibling helper modules (clickhouse_utils.py) are importable.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clickhouse_utils import get_clickhouse_client  # noqa: E402
from egov_api_utils import (  # noqa: E402
    attach_boundary_levels,
    extract_boundary_lookup_keys,
    extract_user_lookup_keys,
    parse_additional_fields,
    parse_hierarchy_type,
    resolve_boundary_levels,
    resolve_user_info,
)

log = logging.getLogger(__name__)

DAG_ID = "stock_reconciliation_transformation"

BRONZE_TABLE = "analytics.stg_stock_reconciliation"
FACILITY_TABLE = "analytics.stg_facility"
ADDRESS_TABLE = "analytics.stg_address"
PRODUCT_VARIANT_TABLE = "analytics.stg_product_variant"
PROJECT_TABLE = "analytics.stg_project"
PROJECT_ADDRESS_TABLE = "analytics.stg_project_address"
CHUNK_SIZE_VARIABLE = "bronze_to_silver_chunk_size"
DEFAULT_CHUNK_SIZE = 5000

SILVER_TABLE = "stock_reconciliation_entity"

_EPOCH_DATE = pendulum.Date(1970, 1, 1)
_EPOCH_DATETIME = pendulum.datetime(1970, 1, 1, tz="UTC")

SILVER_COLUMNS = [
    "id", "client_reference_id", "tenant_id", "facility_id", "product_variant_id", "reference_id",
    "reference_id_type", "physical_count", "calculated_count", "comments_on_reconciliation",
    "date_of_reconciliation", "additional_fields", "is_deleted", "row_version",
    "created_by", "last_modified_by", "created_time", "last_modified_time",
    "client_created_by", "client_last_modified_by", "client_created_time", "client_last_modified_time",
    "facility_name", "facility_target", "facility_level", "product_name",
    "user_name", "name_of_user", "role", "user_address",
    "synced_time_stamp", "synced_time", "task_dates", "synced_date",
    "level_one_code", "level_two_code", "level_three_code", "level_four_code", "level_five_code",
    "level_six_code", "level_seven_code", "level_eight_code", "level_nine_code", "hierarchy_type",
    "boundary_code", "additional_details",
    "project_id", "project_type", "project_type_id", "project_name", "campaign_number", "campaign_id",
]


def _parse_window_bound(iso_ts: str):
    return pendulum.parse(iso_ts)


def _count_bronze_records(client, start_dt, end_dt) -> int:
    result = client.query(
        f"SELECT count() FROM {BRONZE_TABLE} "
        f"WHERE _ingested_at >= %(start_dt)s AND _ingested_at < %(end_dt)s",
        parameters={"start_dt": start_dt, "end_dt": end_dt},
    )
    return result.result_rows[0][0]


def _iter_bronze_chunks(client, start_dt, end_dt, chunk_size: int):
    """
    Yields lists of row dicts, walking the [start_dt, end_dt) bronze-ingestion
    window via keyset pagination on (_ingested_at, id) -- the only
    deterministic total order available (bronze has no monotonic
    integer/sequence column, and _ingested_at alone isn't unique across a
    single ingestion flush).
    """
    cursor_ts, cursor_id = None, None
    while True:
        cursor_clause = ""
        params = {"start_dt": start_dt, "end_dt": end_dt, "limit": chunk_size}
        if cursor_ts is not None:
            cursor_clause = "AND (_ingested_at, id) > (%(cursor_ts)s, %(cursor_id)s)"
            params["cursor_ts"] = cursor_ts
            params["cursor_id"] = cursor_id

        result = client.query(
            f"""
            SELECT _ingested_at, id
            FROM {BRONZE_TABLE}
            WHERE _ingested_at >= %(start_dt)s AND _ingested_at < %(end_dt)s
            {cursor_clause}
            ORDER BY _ingested_at, id
            LIMIT %(limit)s
            """,
            parameters=params,
        )
        rows = list(result.named_results())
        if not rows:
            return

        yield rows

        last_row = rows[-1]
        cursor_ts, cursor_id = last_row["_ingested_at"], last_row["id"]
        if len(rows) < chunk_size:
            return


def _fetch_enriched_stock_reconciliation_rows(client, stock_reconciliation_ids: list[str]) -> list[dict]:
    """
    stg_stock_reconciliation has a single, direct facility_id (no
    sender/receiver split, no STAFF concept -- confirmed from the current
    Java model, which is single-facility unlike Stock). sr.* is safe (no
    collision with aliased join columns). No fan-out guard needed --
    every join is by primary key, or the same single-address-per-
    facility/project assumption already used elsewhere.
    """
    result = client.query(
        f"""
        SELECT
            sr.*,
            fac.name                   AS facility_name_raw,
            fac.usage                  AS facility_usage,
            fac.is_permanent           AS facility_is_permanent,
            fac.additional_details     AS facility_additional_details_raw,
            addr.locality_code         AS facility_locality_code,
            pv.sku                     AS product_sku,
            p.project_type             AS project_type,
            p.project_type_id          AS project_type_id,
            p.name                     AS project_name,
            p.reference_id             AS campaign_number,
            p.additional_details       AS project_additional_details,
            paddr.boundary              AS project_boundary_code
        FROM {BRONZE_TABLE} AS sr
        LEFT JOIN {FACILITY_TABLE} AS fac
            ON fac.id = sr.facility_id AND fac.tenant_id = sr.tenant_id AND fac.is_deleted = false
        LEFT JOIN {ADDRESS_TABLE} AS addr
            ON addr.id = fac.address_id AND addr.tenant_id = fac.tenant_id
        LEFT JOIN {PRODUCT_VARIANT_TABLE} AS pv
            ON pv.id = sr.product_variant_id AND pv.tenant_id = sr.tenant_id
        LEFT JOIN {PROJECT_TABLE} AS p
            ON p.id = sr.reference_id AND p.tenant_id = sr.tenant_id
        LEFT JOIN {PROJECT_ADDRESS_TABLE} AS paddr
            ON paddr.project_id = p.id AND paddr.tenant_id = p.tenant_id
        WHERE sr.id IN %(stock_reconciliation_ids)s
        """,
        parameters={"stock_reconciliation_ids": stock_reconciliation_ids},
    )
    return list(result.named_results())


def _resolve_facility_level(usage, is_permanent) -> str | None:
    if not usage:
        return None
    if usage.upper() == "WAREHOUSE":
        return "DISTRICT_WAREHOUSE" if is_permanent else "SATELLITE_WAREHOUSE"
    return None


def _resolve_facility_target(facility_additional_details_raw) -> int | None:
    """Java's Long.valueOf(...) here has no try/catch -- deliberately not
    replicated; falls back to None instead of crashing the chunk (same
    resilience convention already established for stock)."""
    fields = parse_additional_fields(facility_additional_details_raw)
    value = fields.get("target")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_boundary_code(row: dict) -> str | None:
    """Two tiers only, no STAFF case, no third safety-net fallback --
    matches Java's own lack of one here exactly."""
    code = row.get("facility_locality_code")
    if not code and (row.get("reference_id_type") or "").upper() == "PROJECT":
        code = row.get("project_boundary_code")
    return code


def _get_boundary_lookup_key(row: dict) -> tuple[str, str, str] | None:
    hierarchy_type = parse_hierarchy_type(row.get("project_additional_details"))
    code = _resolve_boundary_code(row)
    if not hierarchy_type or not code:
        return None
    return row["tenant_id"], hierarchy_type, code


def _get_user_lookup_key(row: dict) -> tuple[str, str] | None:
    """CLIENT audit's last_modified_by -- distinct from both stock's
    (client created_by) and household's/pgr's own choices; confirmed
    directly from Java, not assumed."""
    user_id = row.get("client_last_modified_by")
    if not user_id:
        return None
    return row["tenant_id"], user_id


ADDITIONAL_DETAILS_DOUBLE_FIELDS = {"received", "issued", "returned", "lost", "gained", "damaged", "inHand"}


def _coerce_double_or_none(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_stock_reconciliation_additional_details(raw_additional_fields) -> dict:
    """
    Mirrors StockReconciliationTransformationService.java's
    additionalFieldsToDetails: stock-movement quantity fields
    (received/issued/returned/lost/gained/damaged/inHand) get numeric
    coercion (falling back to JSON-null on a parse failure, matching
    Java's catch -> put(key, null)), everything else passes through raw.
    No cycleIndex key -- Java never calls fetchCycleIndex in this class.
    """
    fields = parse_additional_fields(raw_additional_fields)
    return {
        key: (_coerce_double_or_none(value) if key in ADDITIONAL_DETAILS_DOUBLE_FIELDS else value)
        for key, value in fields.items()
    }


def _default_str(value) -> str:
    return value if value is not None else ""


def _default_int(value) -> int:
    return 0 if value is None else int(round(value))


def _default_bool(value) -> bool:
    return bool(value) if value is not None else False


def _default_date(epoch_ms):
    return pendulum.from_timestamp(epoch_ms / 1000, tz="UTC").date() if epoch_ms else _EPOCH_DATE


def _default_datetime(epoch_ms):
    return pendulum.from_timestamp(epoch_ms / 1000, tz="UTC") if epoch_ms else _EPOCH_DATETIME


def _build_silver_row(row: dict, resolved_user_info: dict) -> dict:
    """
    Maps one fully-enriched joined_rows dict onto stock_reconciliation_entity's
    exact column set. Unlike stock_entity, this table has its own separate
    client_* audit block, so created_by/last_modified_by/created_time/
    last_modified_time map from the SERVER audit here (the normal
    convention) -- not inverted the way stock's did.
    """
    user_info = resolved_user_info.get((row["tenant_id"], row.get("client_last_modified_by"))) or {}

    return {
        "id": row["id"],
        "client_reference_id": _default_str(row.get("client_reference_id")),
        "tenant_id": _default_str(row.get("tenant_id")),
        "facility_id": _default_str(row.get("facility_id")),
        "product_variant_id": _default_str(row.get("product_variant_id")),
        "reference_id": _default_str(row.get("reference_id")),
        "reference_id_type": _default_str(row.get("reference_id_type")),
        "physical_count": _default_int(row.get("physical_recorded_count")),
        "calculated_count": _default_int(row.get("calculated_count")),
        "comments_on_reconciliation": _default_str(row.get("comments_on_reconciliation")),
        "date_of_reconciliation": _default_int(row.get("date_of_reconciliation")),
        "additional_fields": _default_str(row.get("additional_details")),
        "is_deleted": _default_bool(row.get("is_deleted")),
        "row_version": _default_int(row.get("row_version")),
        "created_by": _default_str(row.get("created_by")),
        "last_modified_by": _default_str(row.get("last_modified_by")),
        "created_time": _default_int(row.get("created_time")),
        "last_modified_time": _default_int(row.get("last_modified_time")),
        "client_created_by": _default_str(row.get("client_created_by")),
        "client_last_modified_by": _default_str(row.get("client_last_modified_by")),
        "client_created_time": _default_int(row.get("client_created_time")),
        "client_last_modified_time": _default_int(row.get("client_last_modified_time")),
        "facility_name": row.get("facility_name_raw") or _default_str(row.get("facility_id")),
        "facility_target": _default_int(_resolve_facility_target(row.get("facility_additional_details_raw"))),
        "facility_level": _resolve_facility_level(row.get("facility_usage"), row.get("facility_is_permanent")) or "",
        "product_name": row.get("product_sku") or _default_str(row.get("product_variant_id")),
        "user_name": _default_str(user_info.get("USERNAME")),
        "name_of_user": _default_str(user_info.get("NAME")),
        "role": _default_str(user_info.get("ROLE")),
        "user_address": _default_str(user_info.get("CITY")),
        "synced_time_stamp": _default_datetime(row.get("last_modified_time")),
        "synced_time": _default_int(row.get("last_modified_time")),
        "task_dates": _default_date(row.get("client_last_modified_time")),
        "synced_date": _default_date(row.get("last_modified_time")),
        "level_one_code": _default_str(row.get("level_one_code")),
        "level_two_code": _default_str(row.get("level_two_code")),
        "level_three_code": _default_str(row.get("level_three_code")),
        "level_four_code": _default_str(row.get("level_four_code")),
        "level_five_code": _default_str(row.get("level_five_code")),
        "level_six_code": _default_str(row.get("level_six_code")),
        "level_seven_code": _default_str(row.get("level_seven_code")),
        "level_eight_code": _default_str(row.get("level_eight_code")),
        "level_nine_code": _default_str(row.get("level_nine_code")),
        "hierarchy_type": _default_str(row.get("hierarchy_type")),
        "boundary_code": _default_str(_resolve_boundary_code(row)),
        "additional_details": json.dumps(_build_stock_reconciliation_additional_details(row.get("additional_details"))),
        "project_id": _default_str(row.get("reference_id")),
        "project_type": _default_str(row.get("project_type")),
        "project_type_id": _default_str(row.get("project_type_id")),
        "project_name": _default_str(row.get("project_name")),
        "campaign_number": _default_str(row.get("campaign_number")),
        "campaign_id": "",  # TODO: needs project-factory service integration (not yet built)
    }


def _build_silver_rows(joined_rows: list[dict], resolved_user_info: dict) -> list[dict]:
    """Builds each row independently; a malformed row is logged and
    skipped rather than failing the whole chunk's write."""
    silver_rows = []
    for row in joined_rows:
        try:
            silver_rows.append(_build_silver_row(row, resolved_user_info))
        except Exception:
            log.exception(
                "stock_reconciliation: failed to build silver row for id=%s; skipping this row",
                row.get("id"),
            )
    return silver_rows


def _write_silver_chunk(client, silver_rows: list[dict]) -> None:
    if not silver_rows:
        return
    data = [[row[column] for column in SILVER_COLUMNS] for row in silver_rows]
    client.insert(SILVER_TABLE, data, column_names=SILVER_COLUMNS)


@dag(
    dag_id=DAG_ID,
    description="Transforms stock_reconciliation bronze events into the stock_reconciliation_entity silver table.",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["bronze-to-silver", "stock_reconciliation"],
)
def stock_reconciliation_transformation():

    @task
    def parse_time_window(**context) -> dict:
        """Reads and validates start_time/end_time injected by the orchestrator."""
        conf = context["dag_run"].conf or {}
        start_time_raw = conf.get("start_time")
        end_time_raw = conf.get("end_time")

        if not start_time_raw or not end_time_raw:
            raise AirflowFailException(
                f"{DAG_ID} requires 'start_time' and 'end_time' in dag_run.conf; "
                f"got conf={conf!r}. This DAG must be triggered by "
                f"bronze_to_silver_orchestrator (or manually with an equivalent conf)."
            )

        start_time = pendulum.parse(start_time_raw)
        end_time = pendulum.parse(end_time_raw)
        return {
            "start_time": start_time.to_iso8601_string(),
            "end_time": end_time.to_iso8601_string(),
        }

    @task
    def transform_bronze_to_silver(time_window: dict) -> None:
        """
        Reads stock_reconciliation bronze rows for this run's window in
        fixed-size chunks via keyset pagination, transforms, and writes
        each chunk to stock_reconciliation_entity before moving to the
        next chunk.

        Filtered on _ingested_at (bronze arrival time), not last_modified_time
        (source modification time) -- see airflow_dags/CLAUDE.md's "Bronze
        read window column" convention for why.
        """
        start_dt = _parse_window_bound(time_window["start_time"])
        end_dt = _parse_window_bound(time_window["end_time"])
        chunk_size = int(Variable.get(CHUNK_SIZE_VARIABLE, default_var=DEFAULT_CHUNK_SIZE))

        client = get_clickhouse_client()

        total = _count_bronze_records(client, start_dt, end_dt)
        log.info(
            "stock_reconciliation bronze records ingested in [%s, %s): %d (chunk_size=%d)",
            time_window["start_time"], time_window["end_time"], total, chunk_size,
        )
        if total == 0:
            return

        chunk_num = 0
        rows_seen = 0
        for chunk in _iter_bronze_chunks(client, start_dt, end_dt, chunk_size):
            chunk_num += 1
            rows_seen += len(chunk)
            log.info(
                "stock_reconciliation chunk %d: %d rows (cumulative %d/%d)",
                chunk_num, len(chunk), rows_seen, total,
            )

            stock_reconciliation_ids = [row["id"] for row in chunk]
            joined_rows = _fetch_enriched_stock_reconciliation_rows(client, stock_reconciliation_ids)
            log.info(
                "stock_reconciliation chunk %d: %d rows after LEFT JOIN",
                chunk_num, len(joined_rows),
            )

            lookup_keys = extract_boundary_lookup_keys(joined_rows, _get_boundary_lookup_key)
            resolved_levels = resolve_boundary_levels(lookup_keys)
            attach_boundary_levels(joined_rows, resolved_levels, _get_boundary_lookup_key)
            log.info(
                "stock_reconciliation chunk %d: attached boundary hierarchy levels to %d rows",
                chunk_num, len(joined_rows),
            )

            user_lookup_keys = extract_user_lookup_keys(joined_rows, _get_user_lookup_key)
            resolved_user_info = resolve_user_info(user_lookup_keys)
            log.info(
                "stock_reconciliation chunk %d: resolved user info for %d unique user(s)",
                chunk_num, len(user_lookup_keys),
            )

            silver_rows = _build_silver_rows(joined_rows, resolved_user_info)
            _write_silver_chunk(client, silver_rows)
            log.info(
                "stock_reconciliation chunk %d: wrote %d/%d rows to %s",
                chunk_num, len(silver_rows), len(joined_rows), SILVER_TABLE,
            )

    transform_bronze_to_silver(parse_time_window())


stock_reconciliation_transformation()
