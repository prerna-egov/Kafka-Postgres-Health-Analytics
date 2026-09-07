# airflow_dags

Airflow DAGs that populate the ClickHouse silver tables from bronze (see the root pipeline description in [`CLAUDE.md`](CLAUDE.md)).

- `dags/bronze-to-silver_orcestrator.py` — orchestrator. Triggers each entity's transformation DAG in order, waiting for one to finish before starting the next.
- `dags/<entity>_transformation.py` — one DAG per entity (e.g. `project_task_transformation.py`), doing the actual bronze→silver work for that entity. Only triggered by the orchestrator (or manually with an equivalent `conf`); not scheduled on its own.
- `dags/clickhouse_utils.py` — shared helper for connecting to ClickHouse (see [Connecting entity DAGs to ClickHouse](#connecting-entity-dags-to-clickhouse)).
- `dags/egov_api_utils.py` — shared helpers for calling external eGov DIGIT services (boundary-service, user-service, MDMS) and for enriching a chunk of rows from them (see [Calling external eGov services](#calling-external-egov-services)).

## Configuring the orchestrator via Airflow Variables

Nothing about *which* entities run, in *what order*, or *how often* the orchestrator runs is hardcoded — it's all read from Airflow Variables at DAG-parse/task-execution time. Change them via the Airflow UI (**Admin → Variables**) or the CLI, no code deploy needed.

| Variable                                   | What it controls                                                                                                                    | Valid values                                                                                                                                                                                                                                   | Default if unset                                                                 |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `entity_transformation_order`            | Which entities to transform, and in what order                                                                                      | JSON array of entity keys, e.g.`["project_task"]` or `["project", "project_staff", "project_task"]`. Each key must be snake_case and match an existing `<key>_transformation.py` DAG file (key = filename minus `_transformation.py`). | none set → orchestrator has zero trigger tasks (parses fine, just does nothing) |
| `bronze_to_silver_orchestrator_schedule` | How often the orchestrator DAG runs                                                                                                 | A cron expression (`"0 */2 * * *"`), an Airflow schedule preset (`"@hourly"`, `"@daily"`, `"@once"`, ...), or the literal string `"None"` to make it unscheduled / manually-triggered-only                                           | `@hourly`                                                                      |
| `bronze_to_silver_chunk_size`            | How many bronze rows each entity DAG reads per page when paginating its bronze read                                                 | A positive integer, e.g.`5000` or `10000`                                                                                                                                                                                                  | `5000`                                                                         |
| `bronze_to_silver_window_start_override` | Manually pins the start of the bronze read window for the next orchestrator run, instead of using this run's`data_interval_start` | An ISO 8601 datetime string, e.g.`"2026-08-01T00:00:00+00:00"`                                                                                                                                                                               | unset → falls back to`data_interval_start`                                    |
| `bronze_to_silver_window_end_override`   | Manually pins the end of the bronze read window for the next orchestrator run, instead of using this run's`data_interval_end`     | An ISO 8601 datetime string                                                                                                                                                                                                                    | unset → falls back to`data_interval_end`                                      |

eGov service base URLs (boundary-service, user-service, MDMS, workflow-service) are **not** Airflow Variables — see [Configuring eGov service URLs via environment variables](#configuring-egov-service-urls-via-environment-variables) below for why and how to set them.

### Setting them via the CLI

```bash
# activate the local dev environment first (see below), then:
airflow variables set entity_transformation_order '["project_task"]'
airflow variables set bronze_to_silver_orchestrator_schedule "@hourly"
airflow variables set bronze_to_silver_chunk_size "5000"

# pin an explicit window for the next run (e.g. a backfill), both required together
airflow variables set bronze_to_silver_window_start_override "2026-08-01T00:00:00+00:00"
airflow variables set bronze_to_silver_window_end_override "2026-08-02T00:00:00+00:00"

# inspect current values
airflow variables get entity_transformation_order
airflow variables get bronze_to_silver_orchestrator_schedule
airflow variables get bronze_to_silver_chunk_size

# remove an override (falls back to the code default / this run's data_interval)
airflow variables delete bronze_to_silver_orchestrator_schedule
airflow variables delete bronze_to_silver_window_start_override
airflow variables delete bronze_to_silver_window_end_override
```

### Setting them via the UI

Airflow UI → **Admin → Variables → +** → set `Key` to the variable name above and `Val` to the JSON array (for `entity_transformation_order`) or the schedule string (for `bronze_to_silver_orchestrator_schedule`).

### Things to know before changing these

- **Only list entities that have a DAG file today.** An entity in `entity_transformation_order` with no matching `<entity>_transformation.py` is skipped gracefully (logged as a warning) rather than failing the run — useful for staging a future order, but a typo in the key will silently skip that entity too, so double-check spelling.
- **A real failure does not halt the chain.** If a triggered entity DAG actually fails (as opposed to not existing), that's logged via its own failed `trigger_<entity>` task, but the orchestrator still moves on and attempts every later entity in the order for that run. The orchestrator's own DagRun is still marked failed overall (Airflow derives run state from every task's terminal state, not just the last one), so monitoring/alerting on the orchestrator DAG still catches it — only the chain-blocking behavior changed.
- **Changes aren't instant.** `entity_transformation_order` and `bronze_to_silver_orchestrator_schedule` are read when the DAG file is parsed, which happens on Airflow's normal file-processing cadence (`[scheduler] min_file_process_interval`, a few seconds by default) — not the moment you save the Variable. A schedule change also only affects *future* runs, not the data interval of a run already in progress. `bronze_to_silver_chunk_size` and the two `bronze_to_silver_window_*_override` variables, by contrast, are read at *task-execution* time, so a change takes effect on the very next task/orchestrator run — no reparse needed.
- **The two window override variables are all-or-nothing.** They're checked together in the orchestrator's `resolve_time_window` task: both must be non-empty to override the read window for that run. If only one is set, the override is ignored entirely (logged as a warning) and both bounds fall back to that run's `data_interval_start`/`data_interval_end` — never a half-overridden window.
- **The override variables are only ever consulted on a manual/backfill run, or a `"@once"` schedule.** `resolve_time_window` checks this run's `run_type` (Airflow tags every DagRun as `scheduled`, `manual`, `backfill`, or `asset_triggered`) and only reads the override variables at all when `run_type != scheduled` — or when `bronze_to_silver_orchestrator_schedule` is literally `"@once"`, since that DAG's one-and-only firing is technically `run_type=scheduled` but can't recur. A normal recurring schedule (`@hourly`, `@daily`, any cron) always ignores the override variables, even if they're still set — so forgetting to `airflow variables delete` them after a manual backfill can't silently freeze every future scheduled run to the same window. Still good practice to delete them once you're done, to avoid confusion on the next manual run.
- **A cron/preset schedule gets a real, non-zero-width data interval.** `_resolve_schedule` explicitly wraps `@hourly`/`@daily`/etc. (and raw cron expressions) in a `CronDataIntervalTimetable`, so a scheduled run's `data_interval_start`/`data_interval_end` is a genuine `[previous tick, this tick)` window (e.g. the previous hour for `@hourly`) — not Airflow's newer default `CronTriggerTimetable` behavior of `data_interval_start == data_interval_end == the trigger time`, which would otherwise make `resolve_time_window`'s fallback a zero-width, empty read window on every scheduled run. `"None"`/`"@once"` have no periodicity to restore and are left as-is; if `resolve_time_window` ever ends up with a zero-width window anyway (e.g. `"None"`/`"@once"` with no valid override set), it fails loudly (`AirflowFailException`) rather than silently processing nothing.

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

In production, this Connection doesn't need to be created via the CLI/UI at all — set it as an `AIRFLOW_CONN_CLICKHOUSE_DEFAULT` environment variable in URI form (e.g. `AIRFLOW_CONN_CLICKHOUSE_DEFAULT=clickhouse://user:pass@host:8123/analytics`), populated by the Helm chart from a Kubernetes Secret. Airflow resolves a `conn_id` by checking this env var first, then any configured secrets backend, and only falls back to a manually-created Connection in the metadata DB — the `airflow connections add ...` command above is the local-dev equivalent of that env var, not something ops repeats by hand per deployment.

## Connecting the orchestrator to Airflow's own REST API

`dags/bronze-to-silver_orcestrator.py` checks whether each entity's target DAG is registered before triggering it, via Airflow's stable REST API (`GET /api/v2/dags/{dag_id}`) rather than a direct metadata-DB query — task code under Airflow 3's Task SDK can't access the metadata DB directly, regardless of executor. This needs a Connection, `conn_id="airflow_api_default"`, whose `host` is the full base URL (including scheme) of this Airflow instance's own webserver, and whose `login`/`password` are credentials for an account with API read access.

In production, this Connection is expected to be provisioned by the deployment's Helm chart as a Kubernetes-secret-backed `AIRFLOW_CONN_AIRFLOW_API_DEFAULT` environment variable in URI form (e.g. `AIRFLOW_CONN_AIRFLOW_API_DEFAULT=http://user:pass@airflow-webserver.airflow.svc.cluster.local:8080`) — same resolution chain as `clickhouse_default` above (env var, then secrets backend, then metadata DB), not created by hand.

For local dev, create it via the CLI, using the generated admin password from `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`:

```bash
airflow connections add airflow_api_default \
  --conn-type http \
  --conn-host http://localhost:8080 \
  --conn-login admin \
  --conn-password <password from simple_auth_manager_passwords.json.generated>
```

or via the UI: **Admin → Connections → +**, `Connection Id` = `airflow_api_default`, `Connection Type` = `HTTP`, `Host` = `http://localhost:8080`, `Login`/`Password` = the admin credentials above.

## Configuring eGov service URLs via environment variables

Unlike the Airflow Variables above, the base URLs for the DIGIT services `dags/egov_api_utils.py` calls are **plain environment variables**, read via `os.getenv()`, not Airflow Variables:

| Env var | What it controls | Valid values | Default if unset |
|---|---|---|---|
| `EGOV_BOUNDARY_SERVICE_BASE_URL` | Base URL for boundary-service calls | A host URL, e.g. `"http://localhost:8081"` | unset — boundary lookups are skipped (logged once, not per row) |
| `EGOV_USER_SERVICE_BASE_URL` | Base URL for user-service calls | A host URL, e.g. `"http://localhost:8284"` | unset — user lookups are skipped (logged once, not per row) |
| `EGOV_MDMS_SERVICE_BASE_URL` | Base URL for MDMS calls (used only when a user has more than one role, to rank/pick one) | A host URL | unset — role-ranking lookups are skipped (logged once, not per row) |
| `EGOV_WORKFLOW_SERVICE_BASE_URL` | Base URL for workflow-service calls (bill/PGR workflow-status enrichment) | A host URL | unset — workflow-status lookups are skipped (logged once, not per row) |

**Why env vars instead of Airflow Variables**: the external calls these back in `egov_api_utils.py` are made once per row (or per cache-miss) across a chunk of hundreds/thousands of rows, across every entity DAG. `Variable.get()` is a network round-trip to the Airflow webserver/metadata DB on every call — at that call volume it noticeably balloons task time and memory. `os.getenv()` is a zero-cost in-process read, so `egov_api_utils.py` reads each of these once at import time into a module-level constant (`BOUNDARY_SERVICE_BASE_URL`, etc.) and every call site just uses that constant directly — no per-call lookup at all. If a base URL is unset, that service's calls are skipped outright (not attempted-then-failed) and a single warning is logged the first time, not once per row — these constants can't change mid-process, so there's nothing to gain from retrying.

In production, these are set directly on the worker container by the Helm chart (no Connection/Variable indirection needed, same idea as `AIRFLOW_CONN_CLICKHOUSE_DEFAULT` above). For local dev, `egov_api_utils.py` automatically loads `airflow_dags/.env` on import (via `python-dotenv`, already part of this venv's dependency tree — no new install needed) — just fill in the values there and restart the scheduler/webserver (or trigger a fresh task run); no manual `export`/`source` needed, and it never overrides a real env var that's already set (so it's harmless to leave in place even if you *also* export these manually). `airflow_dags/.env` is gitignored — per-developer, never committed — and production doesn't use it at all (there's no such file in the deployed image).

Unlike an Airflow Variable, these aren't settable via the Airflow UI/CLI at runtime — changing one requires restarting the Airflow process(es) that read it.

## Calling external eGov services

`dags/egov_api_utils.py` wraps calls to four DIGIT services — boundary-service, user-service, MDMS, and workflow-service (see [Configuring eGov service URLs via environment variables](#configuring-egov-service-urls-via-environment-variables) above) — and is meant to be reused by any entity DAG, not just `project_task_transformation.py`.

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
