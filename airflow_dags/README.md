# airflow_dags

Airflow DAGs that populate the ClickHouse silver tables from bronze (see the root pipeline description in [`CLAUDE.md`](CLAUDE.md)).

- `dags/bronze-to-silver_orcestrator.py` — orchestrator. Triggers each entity's transformation DAG in order, waiting for one to finish before starting the next.
- `dags/<entity>_transformation.py` — one DAG per entity (e.g. `project_task_transformation.py`), doing the actual bronze→silver work for that entity. Only triggered by the orchestrator (or manually with an equivalent `conf`); not scheduled on its own.
- `dags/clickhouse_utils.py` — shared helper for connecting to ClickHouse (see [Connecting entity DAGs to ClickHouse](#connecting-entity-dags-to-clickhouse)).
- `dags/egov_api_utils.py` — shared helpers for calling external eGov DIGIT services (boundary-service, user-service, MDMS) and for enriching a chunk of rows from them (see [Calling external eGov services](#calling-external-egov-services)).

## Configuring the orchestrator via Airflow Variables

Nothing about *which* entities run, in *what order*, or *how often* the orchestrator runs is hardcoded — it's all read from Airflow Variables at DAG-parse/task-execution time. Change them via the Airflow UI (**Admin → Variables**) or the CLI, no code deploy needed.

| Variable | What it controls | Valid values | Default if unset |
|---|---|---|---|
| `entity_transformation_order` | Which entities to transform, and in what order | JSON array of entity keys, e.g. `["project_task"]` or `["project", "project_staff", "project_task"]`. Each key must be snake_case and match an existing `<key>_transformation.py` DAG file (key = filename minus `_transformation.py`). | none set → orchestrator has zero trigger tasks (parses fine, just does nothing) |
| `bronze_to_silver_orchestrator_schedule` | How often the orchestrator DAG runs | A cron expression (`"0 */2 * * *"`), an Airflow schedule preset (`"@hourly"`, `"@daily"`, `"@once"`, ...), or the literal string `"None"` to make it unscheduled / manually-triggered-only | `@hourly` |
| `bronze_to_silver_chunk_size` | How many bronze rows each entity DAG reads per page when paginating its bronze read | A positive integer, e.g. `5000` or `10000` | `5000` |
| `egov_boundary_service_base_url` | Base URL for `dags/egov_api_utils.py`'s boundary-service calls | A host URL, e.g. `"http://localhost:8081"` | none — required; calls fail if unset |
| `egov_user_service_base_url` | Base URL for `dags/egov_api_utils.py`'s user-service calls | A host URL, e.g. `"http://localhost:8284"` | none — required; calls fail if unset |
| `egov_mdms_service_base_url` | Base URL for `dags/egov_api_utils.py`'s MDMS calls (used only when a user has more than one role, to rank/pick one) | A host URL | none — required only if a multi-role user is looked up |

### Setting them via the CLI

```bash
# activate the local dev environment first (see below), then:
airflow variables set entity_transformation_order '["project_task"]'
airflow variables set bronze_to_silver_orchestrator_schedule "@hourly"
airflow variables set bronze_to_silver_chunk_size "5000"
airflow variables set egov_boundary_service_base_url "http://localhost:8081"
airflow variables set egov_user_service_base_url "http://localhost:8284"
airflow variables set egov_mdms_service_base_url "http://localhost:<mdms-port>"

# inspect current values
airflow variables get entity_transformation_order
airflow variables get bronze_to_silver_orchestrator_schedule
airflow variables get bronze_to_silver_chunk_size
airflow variables get egov_boundary_service_base_url

# remove an override (falls back to the code default)
airflow variables delete bronze_to_silver_orchestrator_schedule
```

### Setting them via the UI

Airflow UI → **Admin → Variables → +** → set `Key` to the variable name above and `Val` to the JSON array (for `entity_transformation_order`) or the schedule string (for `bronze_to_silver_orchestrator_schedule`).

### Things to know before changing these

- **Only list entities that have a DAG file today.** An entity in `entity_transformation_order` with no matching `<entity>_transformation.py` is skipped gracefully (logged as a warning) rather than failing the run — useful for staging a future order, but a typo in the key will silently skip that entity too, so double-check spelling.
- **A real failure still halts the chain.** If a triggered entity DAG actually fails (as opposed to not existing), the orchestrator run stops there; entities later in the order don't run for that scheduled interval.
- **Changes aren't instant.** `entity_transformation_order` and `bronze_to_silver_orchestrator_schedule` are read when the DAG file is parsed, which happens on Airflow's normal file-processing cadence (`[scheduler] min_file_process_interval`, a few seconds by default) — not the moment you save the Variable. A schedule change also only affects *future* runs, not the data interval of a run already in progress. `bronze_to_silver_chunk_size`, by contrast, is read at *task-execution* time (inside `transform_bronze_to_silver`), so a change takes effect on the very next task run — no reparse needed.

## Connecting entity DAGs to ClickHouse

Each `<entity>_transformation.py` DAG reads its bronze table via [`clickhouse-connect`](https://github.com/ClickHouse/clickhouse-connect) (installed in the local venv — see below), using a shared helper (`dags/clickhouse_utils.py`) that looks up connection details from a standard Airflow Connection, `conn_id="clickhouse_default"`.

Create it once, via the CLI:

```bash
airflow connections add clickhouse_default \
  --conn-type http \
  --conn-host <clickhouse-host> \
  --conn-port 8123 \
  --conn-login <user> \
  --conn-password <password> \
  --conn-schema analytics
```

or via the UI: **Admin → Connections → +**, `Connection Id` = `clickhouse_default`, `Connection Type` = `HTTP`, then fill in host/port/login/password/schema (database name) as above.

## Calling external eGov services

`dags/egov_api_utils.py` wraps calls to three DIGIT services — boundary-service, user-service, and MDMS (see the base-URL Variables table above) — and is meant to be reused by any entity DAG, not just `project_task_transformation.py`.

**What it provides:**
- `get_boundary_hierarchy_levels_bulk(tenant_id, hierarchy_type, boundary_codes)` — resolves boundary codes into `level_one_code..level_nine_code` + `hierarchy_type`. Tries one bulk `codes=` call per batch of not-yet-cached codes first; if that fails, falls back to a whole-tree fetch (cached for the rest of the run) — the fallback is sticky, so a deployment where bulk `codes=` genuinely doesn't work only pays the whole-tree cost once, not on every call.
- `get_user_info(tenant_id, user_id)` — resolves a user uuid into `{USERNAME, NAME, ROLE, ID, CITY}`, with role ranking (via MDMS, only when a user has more than one role) and an in-process cache.
- `extract_boundary_lookup_keys` / `resolve_boundary_levels` / `attach_boundary_levels` and `extract_user_lookup_keys` / `resolve_user_info` — the reusable three-stage pattern: an entity file supplies a small `get_key(row)` function (its own column names → a generic lookup key), and these handle grouping/deduping across a chunk, batched resolution, and merging results back onto rows. See `project_task_transformation.py`'s `_get_boundary_lookup_key`/`_get_user_lookup_key` for what an entity-specific `get_key` looks like.

**Failure handling**: every function that makes an actual HTTP call is wrapped in try/except — on any failure it logs the exception and returns an empty/default result, never raises. A single external-service outage degrades that chunk's enrichment (rows get empty boundary/user fields) rather than failing the whole task.

**Same pattern applies to ClickHouse-side lookups that are mutually exclusive per row** (e.g. a task's beneficiary is a household *or* an individual, never both) — see `project_task_transformation.py`'s `_extract_beneficiary_lookup_keys`/`_resolve_household_details`/`_resolve_individual_details` for a ClickHouse-query version of the same extract/resolve/attach shape, which runs a targeted query per branch instead of joining every branch's table onto the main read.

## Local dev environment

Airflow isn't installed system-wide; it lives in a project-local virtualenv (gitignored) so it doesn't touch system Python.

```bash
cd airflow_dags

# one-time setup
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip
curl -sfL -o /tmp/airflow-constraints.txt \
  "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt"
./.venv/bin/pip install "apache-airflow==3.0.6" apache-airflow-providers-standard \
  --constraint /tmp/airflow-constraints.txt
./.venv/bin/pip install clickhouse-connect

# every session
export AIRFLOW_HOME="$(pwd)/.airflow"
export PATH="$(pwd)/.venv/bin:$PATH"
```

> Always install with `--constraint` pinned to the matching Airflow release/Python version. Installing without it lets pip resolve dependency versions (e.g. `starlette`/`fastapi`) newer than what that Airflow release was tested against, which breaks the webserver at runtime.

First-time initialization:

```bash
# point dags_folder at this repo's dags/ and disable example DAGs
airflow config list >/dev/null   # generates airflow.cfg on first run
sed -i "s|^dags_folder = .*|dags_folder = $(pwd)/dags|" "$AIRFLOW_HOME/airflow.cfg"
sed -i "s|^load_examples = .*|load_examples = False|" "$AIRFLOW_HOME/airflow.cfg"

airflow db migrate
```

Run it:

```bash
airflow standalone
```

This starts the webserver, scheduler, dag-processor, and triggerer together, and prints/generates admin login credentials (see `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` if you miss them in the log). The UI is at http://localhost:8080. Both DAGs start paused — unpause them from the UI or with `airflow dags unpause <dag_id>` before triggering.

`.venv/` and `.airflow/` are gitignored — safe to delete and rebuild at any time.
