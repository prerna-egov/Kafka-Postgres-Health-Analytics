CREATE TABLE IF NOT EXISTS analytics.kafka_household_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'save-household-events',
    kafka_group_name = 'clickhouse-household-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;