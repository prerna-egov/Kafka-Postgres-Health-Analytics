"""
hf_referral_transformation.py

Bronze -> silver transformation DAG for the `hf_referral` entity
(HFReferral -> HfReferralIndexV1 in the Java reference). Triggered
exclusively by bronze_to_silver_orchestrator with
conf={"start_time": ..., "end_time": ...}; not scheduled on its own.

Simplest of the referral-domain entities so far: a single direct project
FK, no project-staff bridge, no project-beneficiary/individual bridge.
"""
from __future__ import annotations

import json
import logging
import os
import sys

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
    fetch_cycle_index,
    get_project_cycles,
    parse_additional_fields,
    parse_hierarchy_type,
    resolve_boundary_levels,
    resolve_user_info,
)

log = logging.getLogger(__name__)

DAG_ID = "hf_referral_transformation"

BRONZE_TABLE = "analytics.stg_hf_referral"
PROJECT_TABLE = "analytics.stg_project"
PROJECT_ADDRESS_TABLE = "analytics.stg_project_address"
CHUNK_SIZE_VARIABLE = "bronze_to_silver_chunk_size"
DEFAULT_CHUNK_SIZE = 5000

SILVER_TABLE = "hf_referral_entity"

_EPOCH_DATE = pendulum.Date(1970, 1, 1)

SILVER_COLUMNS = [
    "id", "client_reference_id", "tenant_id", "project_id", "project_facility_id", "symptom",
    "symptom_survey_id", "beneficiary_id", "referral_code", "national_level_id", "is_deleted",
    "row_version", "created_by", "last_modified_by", "created_time", "last_modified_time",
    "client_created_by", "client_last_modified_by", "client_created_time", "client_last_modified_time",
    "additional_fields", "user_name", "role", "user_address",
    "level_one_code", "level_two_code", "level_three_code", "level_four_code", "level_five_code",
    "level_six_code", "level_seven_code", "level_eight_code", "level_nine_code", "hierarchy_type",
    "task_dates", "synced_date", "additional_details",
    "project_type", "project_type_id", "project_name", "campaign_number", "campaign_id",
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


def _fetch_enriched_hf_referral_rows(client, hf_referral_ids: list[str]) -> list[dict]:
    """
    A single direct FK lookup, inlined here rather than a separate bridge
    (per this session's own "1-2 hop direct FK -> fine to inline"
    guidance). hf.* is safe; no fan-out risk (both joins by primary key).
    """
    result = client.query(
        f"""
        SELECT
            hf.*,
            p.project_type        AS project_type,
            p.project_type_id     AS project_type_id,
            p.name                AS project_name,
            p.reference_id        AS campaign_number,
            p.additional_details  AS project_additional_details,
            paddr.boundary        AS project_boundary_code
        FROM {BRONZE_TABLE} AS hf
        LEFT JOIN {PROJECT_TABLE} AS p
            ON p.id = hf.project_id AND p.tenant_id = hf.tenant_id
        LEFT JOIN {PROJECT_ADDRESS_TABLE} AS paddr
            ON paddr.project_id = p.id AND paddr.tenant_id = p.tenant_id
        WHERE hf.id IN %(hf_referral_ids)s
        """,
        parameters={"hf_referral_ids": hf_referral_ids},
    )
    return list(result.named_results())


def _get_hf_referral_additional_fields_locality_code(row: dict) -> str | None:
    """
    Mirrors CommonUtils.getLocalityCodeFromAdditionalFields(Object) --
    flattens hfReferral's OWN additionalFields ({"fields":[...]}) and reads
    a boundaryCode entry off the FLATTENED map. NOT the same shape as
    parse_boundary_code (a top-level key or bare string) -- that helper
    would be wrong here.
    """
    fields = parse_additional_fields(row.get("additional_details"))
    return fields.get("boundaryCode") or None


def _get_boundary_lookup_key(row: dict) -> tuple[str, str, str] | None:
    """
    Three-tier code: bronze's own direct locality_code column FIRST, then
    hfReferral's own additionalFields-derived code SECOND (Java's literal
    path), then the resolved project's own boundary address THIRD.
    hierarchy_type always comes from the resolved project's own
    additional_details (this repo's established convention).
    """
    hierarchy_type = parse_hierarchy_type(row.get("project_additional_details"))
    if not hierarchy_type:
        return None
    code = (
        row.get("locality_code")
        or _get_hf_referral_additional_fields_locality_code(row)
        or row.get("project_boundary_code")
    )
    if not code:
        return None
    return row["tenant_id"], hierarchy_type, code


def _get_user_lookup_key(row: dict) -> tuple[str, str] | None:
    user_id = row.get("client_created_by")  # CLIENT audit, not server
    if not user_id:
        return None
    return row["tenant_id"], user_id


def _format_cycle_index(row: dict) -> str | None:
    """Same "%02d" formatting as referral_transformation.py's own helper,
    but matched against CLIENT audit's created_time -- confirmed from
    source, a genuine per-entity difference."""
    cycles = get_project_cycles(row.get("project_additional_details"))
    if not cycles:
        return None
    matched_id = fetch_cycle_index(cycles, row.get("client_created_time"))
    return f"{matched_id:02d}" if matched_id is not None else None


def _default_str(value) -> str:
    return value if value is not None else ""


def _default_int(value) -> int:
    return 0 if value is None else int(round(value))


def _default_date(epoch_ms):
    return pendulum.from_timestamp(epoch_ms / 1000, tz="UTC").date() if epoch_ms else _EPOCH_DATE


def _build_silver_row(row: dict) -> dict:
    """
    Maps one fully-enriched joined row onto hf_referral_entity's exact
    column set. additional_fields is bronze's raw additional_details
    passthrough; additional_details (derived) is just {"cycleIndex": ...}
    -- no individual involved in this entity at all.
    """
    return {
        "id": row["id"],
        "client_reference_id": _default_str(row.get("client_reference_id")),
        "tenant_id": _default_str(row.get("tenant_id")),
        "project_id": _default_str(row.get("project_id")),
        "project_facility_id": _default_str(row.get("project_facility_id")),
        "symptom": _default_str(row.get("symptom")),
        "symptom_survey_id": _default_str(row.get("symptom_survey_id")),
        "beneficiary_id": _default_str(row.get("beneficiary_id")),
        "referral_code": _default_str(row.get("referral_code")),
        "national_level_id": _default_str(row.get("national_level_id")),
        "is_deleted": bool(row.get("is_deleted")),
        "row_version": _default_int(row.get("row_version")),
        "created_by": _default_str(row.get("created_by")),
        "last_modified_by": _default_str(row.get("last_modified_by")),
        "created_time": _default_int(row.get("created_time")),
        "last_modified_time": _default_int(row.get("last_modified_time")),
        "client_created_by": _default_str(row.get("client_created_by")),
        "client_last_modified_by": _default_str(row.get("client_last_modified_by")),
        "client_created_time": _default_int(row.get("client_created_time")),
        "client_last_modified_time": _default_int(row.get("client_last_modified_time")),
        "additional_fields": _default_str(row.get("additional_details")),
        "user_name": _default_str(row.get("user_name")),
        "role": _default_str(row.get("role")),
        "user_address": _default_str(row.get("user_address")),
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
        "task_dates": _default_date(row.get("client_last_modified_time")),
        "synced_date": _default_date(row.get("last_modified_time")),
        "additional_details": json.dumps({"cycleIndex": _format_cycle_index(row)}),
        "project_type": _default_str(row.get("project_type")),
        "project_type_id": _default_str(row.get("project_type_id")),
        "project_name": _default_str(row.get("project_name")),
        "campaign_number": _default_str(row.get("campaign_number")),
        "campaign_id": "",  # TODO: needs project-factory service integration (not yet built)
    }


def _build_silver_rows(joined_rows: list[dict]) -> list[dict]:
    """Builds each row independently; a malformed row is logged and
    skipped rather than failing the whole chunk's write."""
    silver_rows = []
    for row in joined_rows:
        try:
            silver_rows.append(_build_silver_row(row))
        except Exception:
            log.exception(
                "hf_referral: failed to build silver row for hf_referral id=%s; skipping this row",
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
    description="Transforms hf_referral bronze events into the hf_referral_entity silver table.",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["bronze-to-silver", "hf_referral"],
)
def hf_referral_transformation():

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
        Reads hf_referral bronze rows for this run's window in fixed-size
        chunks via keyset pagination, transforms, and writes each chunk to
        hf_referral_entity before moving to the next chunk.

        Filtered on _ingested_at (bronze arrival time), not
        last_modified_time (source modification time) -- see
        airflow_dags/CLAUDE.md's "Bronze read window column" convention for
        why.
        """
        start_dt = _parse_window_bound(time_window["start_time"])
        end_dt = _parse_window_bound(time_window["end_time"])
        chunk_size = int(Variable.get(CHUNK_SIZE_VARIABLE, default_var=DEFAULT_CHUNK_SIZE))

        client = get_clickhouse_client()

        total = _count_bronze_records(client, start_dt, end_dt)
        log.info(
            "hf_referral bronze records ingested in [%s, %s): %d (chunk_size=%d)",
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
                "hf_referral chunk %d: %d hf_referral rows (cumulative %d/%d)",
                chunk_num, len(chunk), rows_seen, total,
            )

            hf_referral_ids = [row["id"] for row in chunk]
            joined_rows = _fetch_enriched_hf_referral_rows(client, hf_referral_ids)
            log.info(
                "hf_referral chunk %d: %d rows after LEFT JOIN with project",
                chunk_num, len(joined_rows),
            )

            lookup_keys = extract_boundary_lookup_keys(joined_rows, _get_boundary_lookup_key)
            resolved_levels = resolve_boundary_levels(lookup_keys)
            attach_boundary_levels(joined_rows, resolved_levels, _get_boundary_lookup_key)
            log.info(
                "hf_referral chunk %d: attached boundary hierarchy levels to %d rows",
                chunk_num, len(joined_rows),
            )

            user_lookup_keys = extract_user_lookup_keys(joined_rows, _get_user_lookup_key)
            resolved_user_info = resolve_user_info(user_lookup_keys)
            for row in joined_rows:
                info = resolved_user_info.get(_get_user_lookup_key(row)) or {}
                row["user_name"] = info.get("USERNAME") or ""
                row["role"] = info.get("ROLE") or ""
                row["user_address"] = info.get("CITY") or ""
            log.info(
                "hf_referral chunk %d: attached user info to %d rows (%d unique user(s))",
                chunk_num, len(joined_rows), len(user_lookup_keys),
            )

            silver_rows = _build_silver_rows(joined_rows)
            _write_silver_chunk(client, silver_rows)
            log.info(
                "hf_referral chunk %d: wrote %d/%d rows to %s",
                chunk_num, len(silver_rows), len(joined_rows), SILVER_TABLE,
            )

    transform_bronze_to_silver(parse_time_window())


hf_referral_transformation()
