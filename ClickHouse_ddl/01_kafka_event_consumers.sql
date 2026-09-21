-- ============================================================================
-- KAFKA INGESTION TABLES
-- ============================================================================
-- Purpose: Ingest raw JSON payloads from Kafka topics
-- Rule: No JSON parsing here - store as raw String using JSONAsString
--
-- These are consumers, not storage: reading from one advances the group offset
-- and the rows are gone, so only the ingestion MVs in 03 select from them.
-- ============================================================================

CREATE TABLE IF NOT EXISTS analytics.kafka_household_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.household',
    kafka_group_name = 'clickhouse-household-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_household_member_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.household_member',
    kafka_group_name = 'clickhouse-household-member-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_project_task_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.project_task',
    kafka_group_name = 'clickhouse-project-task-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_task_resource_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.task_resource',
    kafka_group_name = 'clickhouse-task-resource-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_address_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.address',
    kafka_group_name = 'clickhouse-address-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_project_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.project',
    kafka_group_name = 'clickhouse-project-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_project_target_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.project_target',
    kafka_group_name = 'clickhouse-project-target-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_project_address_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.project_address',
    kafka_group_name = 'clickhouse-project-address-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_project_beneficiary_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.project_beneficiary',
    kafka_group_name = 'clickhouse-project-beneficiary-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_project_staff_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.project_staff',
    kafka_group_name = 'clickhouse-project-staff-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_project_facility_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.project_facility',
    kafka_group_name = 'clickhouse-project-facility-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_individual_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.individual',
    kafka_group_name = 'clickhouse-individual-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_stock_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.stock',
    kafka_group_name = 'clickhouse-stock-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_stock_reconciliation_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.stock_reconciliation_log',
    kafka_group_name = 'clickhouse-stock-reconciliation-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_facility_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.facility',
    kafka_group_name = 'clickhouse-facility-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_product_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.product',
    kafka_group_name = 'clickhouse-product-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_product_variant_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.product_variant',
    kafka_group_name = 'clickhouse-product-variant-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_service_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_service',
    kafka_group_name = 'clickhouse-service-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;


CREATE TABLE IF NOT EXISTS analytics.kafka_pgr_service_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_pgr_service_v2',
    kafka_group_name = 'clickhouse-pgr-service-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_pgr_address_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_pgr_address_v2',
    kafka_group_name = 'clickhouse-pgr-address-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_attendance_register_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_wms_attendance_register',
    kafka_group_name = 'clickhouse-attendance-register-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_attendance_staff_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_wms_attendance_staff',
    kafka_group_name = 'clickhouse-attendance-staff-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_attendance_attendee_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_wms_attendance_attendee',
    kafka_group_name = 'clickhouse-attendance-attendee-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_attendance_log_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_wms_attendance_log',
    kafka_group_name = 'clickhouse-attendance-log-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_expense_bill_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_expense_bill',
    kafka_group_name = 'clickhouse-expense-bill-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_expense_party_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_expense_party',
    kafka_group_name = 'clickhouse-expense-party-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_expense_billdetail_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_expense_billdetail',
    kafka_group_name = 'clickhouse-expense-billdetail-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_expense_lineitem_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_expense_lineitem',
    kafka_group_name = 'clickhouse-expense-lineitem-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_referral_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.referral',
    kafka_group_name = 'clickhouse-referral-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_side_effect_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.side_effect',
    kafka_group_name = 'clickhouse-side-effect-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_hf_referral_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.hf_referral',
    kafka_group_name = 'clickhouse-hf-referral-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_individual_address_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.individual_address',
    kafka_group_name = 'clickhouse-individual-address-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_user_action_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.user_action',
    kafka_group_name = 'clickhouse-user-action-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_service_attribute_value_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_service_attribute_value',
    kafka_group_name = 'clickhouse-service-attribute-value-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_service_definition_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_service_definition',
    kafka_group_name = 'clickhouse-service-definition-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_muster_roll_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_wms_muster_roll',
    kafka_group_name = 'clickhouse-muster-roll-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_attendance_summary_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_wms_attendance_summary',
    kafka_group_name = 'clickhouse-attendance-summary-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS analytics.kafka_device_tokens_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-kraft.backbone.svc.cluster.local:9092',
    kafka_topic_list = 'unified-dev.health.eg_push_device_tokens',
    kafka_group_name = 'clickhouse-device-tokens-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;
