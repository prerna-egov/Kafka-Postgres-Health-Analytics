"""
bronze-to-silver_orcestrator.py

Reads the ordered list of entities to transform from the Airflow Variable
`entity_transformation_order`, and for each entity (in order) triggers that
entity's `<entity>_transformation` DAG, blocking until it finishes before
moving on to the next one. The time window passed to every entity DAG is
this orchestrator DAG's own scheduled data interval.

If an entity in the order list has no matching DAG registered yet (e.g. its
transformation file hasn't been written), that entity is skipped and the
chain continues with the next one. A genuine failure in a triggered DAG
still halts the chain.
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.models import DAG, Variable
from airflow.decorators import task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)

ENTITY_ORDER_VARIABLE = "entity_transformation_order"
SCHEDULE_VARIABLE = "bronze_to_silver_orchestrator_schedule"
DEFAULT_SCHEDULE = "@hourly"
DAG_ID_SUFFIX = "_transformation"


def _dag_id_for_entity(entity: str) -> str:
    """
    Single source of truth for the entity -> dag_id mapping.
    The mapping between the entity and the dag_id is : dag_id = {entity_name}_transformation
    For example: entity = project_task then the dag_id/python file name has to be project_task_transformation
    """
    return f"{entity}{DAG_ID_SUFFIX}"


def _resolve_schedule(raw: str) -> str | None:
    """Maps the literal string "none" (any case) to Python None (unscheduled,
    manually/externally triggered only); anything else is passed through as-is
    (cron expression or preset like "@hourly")."""
    return None if raw.strip().lower() == "none" else raw


def _target_dag_exists(dag_id: str) -> bool:
    """
    Checks whether a DAG is registered in the metadata DB.

    Assumes task code has direct DB access (true for LocalExecutor/CeleryExecutor
    deployments). If the deployment instead runs workers against Airflow 3's
    remote Task Execution API without direct DB access, this needs to go
    through the stable REST API (`GET /api/v2/dags/{dag_id}`) instead.
    """
    from airflow.models import DagModel
    from airflow.utils.session import create_session

    with create_session() as session:
        return session.query(DagModel).filter(DagModel.dag_id == dag_id).first() is not None


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_to_silver_orchestrator",
    description=(
        "Sequentially triggers per-entity bronze-to-silver transformation DAGs "
        "for this DAG's own scheduled data interval."
    ),
    # Read at parse time, same as ENTITY_ORDER_VARIABLE, so cadence can be
    # changed via the Airflow UI/CLI without a code deploy. Accepts a cron
    # expression ("0 * * * *"), a preset ("@hourly", "@daily", ...), or the
    # literal string "None" to make the DAG unscheduled (manually/externally
    # triggered only). Takes effect on the next time the scheduler/dag-processor
    # reparses this file ([scheduler] min_file_process_interval), not instantly.
    schedule=_resolve_schedule(Variable.get(SCHEDULE_VARIABLE, default_var=DEFAULT_SCHEDULE)),
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bronze-to-silver", "orchestrator"],
) as dag:

    entity_order = Variable.get(
        ENTITY_ORDER_VARIABLE,
        deserialize_json=True,
        default_var=[],
    )

    if not entity_order:
        log.warning(
            "Airflow Variable '%s' is missing or empty at parse time; "
            "'%s' will have zero trigger tasks until it is populated.",
            ENTITY_ORDER_VARIABLE, dag.dag_id,
        )

    conf_template = {
        "start_time": "{{ data_interval_start }}",
        "end_time": "{{ data_interval_end }}",
    }

    previous_task = None
    for entity in entity_order:
        target_dag_id = _dag_id_for_entity(entity)

        @task.short_circuit(
            task_id=f"check_{entity}_dag_exists",
            trigger_rule="none_failed",
            ignore_downstream_trigger_rules=False,
        )
        def check_dag_exists(target_dag_id: str = target_dag_id) -> bool:
            exists = _target_dag_exists(target_dag_id)
            if not exists:
                log.warning(
                    "DAG '%s' is not registered yet; skipping this entity and "
                    "continuing with the rest of the order list.",
                    target_dag_id,
                )
            return exists

        check_task = check_dag_exists()

        trigger_task = TriggerDagRunOperator(
            task_id=f"trigger_{entity}",
            trigger_dag_id=target_dag_id,
            conf=conf_template,
            wait_for_completion=True,
            poke_interval=30,
            execution_timeout=timedelta(hours=2),
            allowed_states=["success"],
            failed_states=["failed"],
            reset_dag_run=True,
            trigger_run_id=f"orchestrator__{{{{ dag_run.run_id }}}}__{entity}",
        )

        check_task >> trigger_task
        if previous_task is not None:
            previous_task >> check_task
        previous_task = trigger_task
