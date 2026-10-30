# Python bronze-to-silver DAGs (reference only)

These are the original Python implementations of the bronze-to-silver DAGs. They
are **not deployed**: this folder is outside `dags/`, so Airflow never parses
them, and they no longer match the silver DDL (for example, they still write the
campaign-specific columns that moved to the `<entity>_campaign_field` tables).

Production silver is written by `dags/sql_based_dags/<entity>_transformation.py`,
which keep the same dag_ids (`<entity>_transformation`) and are triggered by
`dags/bronze-to-silver_orcestrator.py`. Each SQL DAG's docstring lists its known,
deliberate differences from the Python file of the same name here.
