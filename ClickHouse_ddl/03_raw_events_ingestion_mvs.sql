-- ============================================================================
-- MATERIALIZED VIEWS: Kafka -> Event Store
-- ============================================================================
-- Purpose: Move raw JSON payloads from Kafka engine tables to raw MergeTree tables
-- Rules:
--   - No JSON parsing at this layer
--   - Just pass through the raw String as-is
--   - Parsing/extraction happens in the Airflow DAG that loads 04 (Bronze)
-- ============================================================================
--
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_household_events_raw
TO analytics.household_events_raw
AS
SELECT raw
FROM analytics.kafka_household_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_household_member_events_raw
TO analytics.household_member_events_raw
AS
SELECT raw
FROM analytics.kafka_household_member_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_project_task_events_raw
TO analytics.project_task_events_raw
AS
SELECT raw
FROM analytics.kafka_project_task_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_task_resource_events_raw
TO analytics.task_resource_events_raw
AS
SELECT raw
FROM analytics.kafka_task_resource_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_address_events_raw
TO analytics.address_events_raw
AS
SELECT raw
FROM analytics.kafka_address_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_project_events_raw
TO analytics.project_events_raw
AS
SELECT raw
FROM analytics.kafka_project_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_project_target_events_raw
TO analytics.project_target_events_raw
AS
SELECT raw
FROM analytics.kafka_project_target_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_project_address_events_raw
TO analytics.project_address_events_raw
AS
SELECT raw
FROM analytics.kafka_project_address_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_project_beneficiary_events_raw
TO analytics.project_beneficiary_events_raw
AS
SELECT raw
FROM analytics.kafka_project_beneficiary_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_project_staff_events_raw
TO analytics.project_staff_events_raw
AS
SELECT raw
FROM analytics.kafka_project_staff_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_project_facility_events_raw
TO analytics.project_facility_events_raw
AS
SELECT raw
FROM analytics.kafka_project_facility_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_individual_events_raw
TO analytics.individual_events_raw
AS
SELECT raw
FROM analytics.kafka_individual_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_stock_events_raw
TO analytics.stock_events_raw
AS
SELECT raw
FROM analytics.kafka_stock_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_stock_reconciliation_events_raw
TO analytics.stock_reconciliation_events_raw
AS
SELECT raw
FROM analytics.kafka_stock_reconciliation_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_facility_events_raw
TO analytics.facility_events_raw
AS
SELECT raw
FROM analytics.kafka_facility_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_product_events_raw
TO analytics.product_events_raw
AS
SELECT raw
FROM analytics.kafka_product_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_product_variant_events_raw
TO analytics.product_variant_events_raw
AS
SELECT raw
FROM analytics.kafka_product_variant_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_service_events_raw
TO analytics.service_events_raw
AS
SELECT raw
FROM analytics.kafka_service_events;

