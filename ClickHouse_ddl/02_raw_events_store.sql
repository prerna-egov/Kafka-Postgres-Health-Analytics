-- ============================================================================
-- EVENT STORE (APPEND-ONLY, JSON-AS-STRING)
-- ============================================================================
-- Purpose: Store every incoming change event as an immutable JSON payload
-- Rules:
--   - MergeTree only (NO ReplacingMergeTree)
--   - NO UPDATE, NO DELETE
--   - Raw JSON stored as String -- no parsing at this layer
-- ============================================================================

-- ############################################################################
-- HOUSEHOLD EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.household_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- HOUSEHOLD MEMBER EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.household_member_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- PROJECT TASK EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.project_task_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- TASK RESOURCE EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.task_resource_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- ADDRESS EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.address_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- PROJECT EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.project_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- PROJECT TARGET EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.project_target_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- PROJECT ADDRESS EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.project_address_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- PROJECT BENEFICIARY EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.project_beneficiary_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- PROJECT STAFF EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.project_staff_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- PROJECT FACILITY EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.project_facility_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- INDIVIDUAL EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.individual_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- STOCK EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.stock_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- STOCK RECONCILIATION EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.stock_reconciliation_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- FACILITY EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.facility_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- PRODUCT EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.product_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- PRODUCT VARIANT EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.product_variant_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- SERVICE EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.service_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- PGR SERVICE EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.pgr_service_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- PGR ADDRESS EVENTS RAW
-- ############################################################################
CREATE TABLE IF NOT EXISTS analytics.pgr_address_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;
