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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-household-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-household-member-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-project-task-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-task-resource-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-address-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-project-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-project-target-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-project-address-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-project-beneficiary-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-project-staff-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-project-facility-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-individual-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-stock-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-stock-reconciliation-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-facility-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-product-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-product-variant-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-service-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-pgr-service-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-pgr-address-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-attendance-register-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-attendance-staff-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-attendance-attendee-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-attendance-log-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-expense-bill-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-expense-party-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-expense-billdetail-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-expense-lineitem-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-referral-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-side-effect-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-hf-referral-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-individual-address-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-user-action-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-service-attribute-value-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-service-definition-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-muster-roll-events',
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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'clickhouse-attendance-summary-events',
    kafka_group_name = 'clickhouse-attendance-summary-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;
