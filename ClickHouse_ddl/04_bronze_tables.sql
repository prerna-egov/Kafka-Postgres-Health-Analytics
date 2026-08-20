-- ============================================================================
-- BRONZE TABLES (POSTGRES REPLICATION TABLES)
-- ============================================================================
-- Purpose: A typed replica of each Postgres source table, one row per source row
-- Rules:
--   - Columns are 1:1 with the Postgres table; _ingested_at is the only addition
--   - snake_case names so Silver (05) reads Bronze without a rename layer
--   - Non-Nullable: a field absent from the event lands as '' / 0 / false
--   - Epoch millis stay Int64 -- the DateTime conversion belongs in Silver
--   - ReplacingMergeTree(last_modified_time) ORDER BY (tenant_id, id)
--
-- Loaded by an Airflow DAG that parses the envelopes in 02, not by an MV.
-- Tables without audit columns or tenant_id deviate from the last rule; see the
-- comment above each.
-- ============================================================================

CREATE TABLE IF NOT EXISTS analytics.stg_household
(
    _ingested_at              DateTime64(3) DEFAULT now64(3),
    id                        String,
    tenant_id                 LowCardinality(String),
    client_reference_id       String,
    member_count              Int32,
    household_type            LowCardinality(String),
    address_id                String,
    additional_details        String,
    created_by                String,
    last_modified_by          String,
    created_time              Int64,
    last_modified_time        Int64,
    client_created_time       Int64,
    client_last_modified_time Int64,
    client_created_by         String,
    client_last_modified_by   String,
    row_version               Int64,
    is_deleted                Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_household_member
(
    _ingested_at                   DateTime64(3) DEFAULT now64(3),
    id                             String,
    client_reference_id            String,
    tenant_id                      LowCardinality(String),
    individual_id                  String,
    individual_client_reference_id String,
    household_id                   String,
    household_client_reference_id  String,
    is_head_of_household           Bool,
    additional_details             String,
    created_by                     String,
    created_time                   Int64,
    last_modified_by               String,
    last_modified_time             Int64,
    client_created_time            Int64,
    client_last_modified_time      Int64,
    client_created_by              String,
    client_last_modified_by        String,
    row_version                    Int64,
    is_deleted                     Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_project_task
(
    _ingested_at                            DateTime64(3) DEFAULT now64(3),
    id                                      String,
    client_reference_id                     String,
    tenant_id                               LowCardinality(String),
    project_id                              String,
    project_beneficiary_id                  String,
    project_beneficiary_client_reference_id String,
    planned_start_date                      Int64,
    planned_end_date                        Int64,
    actual_start_date                       Int64,
    actual_end_date                         Int64,
    address_id                              String,
    additional_details                      String,
    created_by                              String,
    created_time                            Int64,
    last_modified_by                        String,
    last_modified_time                      Int64,
    row_version                             Int64,
    is_deleted                              Bool,
    client_created_time                     Int64,
    client_last_modified_time               Int64,
    client_created_by                       String,
    client_last_modified_by                 String,
    status                                  LowCardinality(String)
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_task_resource
(
    _ingested_at             DateTime64(3) DEFAULT now64(3),
    id                       String,
    tenant_id                LowCardinality(String),
    product_variant_id       String,
    task_id                  String,
    quantity                 Float64,
    is_delivered             Bool,
    reason_if_not_delivered  String,
    created_by               String,
    created_time             Int64,
    last_modified_by         String,
    last_modified_time       Int64,
    is_deleted               Bool,
    client_reference_id      String,
    additional_details       String
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_address
(
    _ingested_at        DateTime64(3) DEFAULT now64(3),
    id                  String,
    tenant_id           LowCardinality(String),
    door_no             String,
    latitude            Float64,
    longitude           Float64,
    location_accuracy   Int32,
    type                LowCardinality(String),
    address_line1       String,
    address_line2       String,
    landmark            String,
    city                String,
    pincode             String,
    building_name       String,
    street              String,
    locality_code       String,
    client_reference_id String,
    ward_code           String
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_project
(
    _ingested_at        DateTime64(3) DEFAULT now64(3),
    id                  String,
    tenant_id           LowCardinality(String),
    project_number      String,
    name                String,
    project_type_id     String,
    project_type        LowCardinality(String),
    project_sub_type    LowCardinality(String),
    department          LowCardinality(String),
    description         String,
    reference_id        String,
    nature_of_work      LowCardinality(String),
    address_id          String,
    start_date          Int64,
    end_date            Int64,
    is_task_enabled     Bool,
    parent              String,
    project_hierarchy   String,
    additional_details  String,
    created_by          String,
    created_time        Int64,
    last_modified_by    String,
    last_modified_time  Int64,
    row_version         Int64,
    is_deleted          Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_project_target
(
    _ingested_at       DateTime64(3) DEFAULT now64(3),
    id                 String,
    project_id         String,
    beneficiary_type   LowCardinality(String),
    total_no           Int64,
    target_no          Int64,
    is_deleted         Bool,
    created_by         String,
    last_modified_by   String,
    created_time       Int64,
    last_modified_time Int64
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_project_address
(
    _ingested_at      DateTime64(3) DEFAULT now64(3),
    id                String,
    tenant_id         LowCardinality(String),
    project_id        String,
    door_no           String,
    latitude          Float64,
    longitude         Float64,
    location_accuracy Int64,
    type              LowCardinality(String),
    address_line1     String,
    address_line2     String,
    landmark          String,
    city              String,
    pincode           String,
    building_name     String,
    street            String,
    boundary          LowCardinality(String),
    boundary_type     LowCardinality(String)
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_project_beneficiary
(
    _ingested_at                    DateTime64(3) DEFAULT now64(3),
    id                              String,
    tenant_id                       LowCardinality(String),
    project_id                      String,
    beneficiary_id                  String,
    client_reference_id             String,
    beneficiary_client_reference_id String,
    created_by                      String,
    last_modified_by                String,
    date_of_registration            Int64,
    additional_details              String,
    created_time                    Int64,
    last_modified_time              Int64,
    row_version                     Int64,
    is_deleted                      Bool,
    client_created_time             Int64,
    client_last_modified_time       Int64,
    client_created_by               String,
    client_last_modified_by         String,
    tag                             String
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_project_staff
(
    _ingested_at       DateTime64(3) DEFAULT now64(3),
    id                 String,
    tenant_id          LowCardinality(String),
    project_id         String,
    staff_id           String,
    start_date         Int64,
    end_date           Int64,
    additional_details String,
    created_by         String,
    last_modified_by   String,
    created_time       Int64,
    last_modified_time Int64,
    row_version        Int64,
    is_deleted         Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_project_facility
(
    _ingested_at       DateTime64(3) DEFAULT now64(3),
    id                 String,
    tenant_id          LowCardinality(String),
    project_id         String,
    facility_id        String,
    additional_details String,
    created_by         String,
    last_modified_by   String,
    created_time       Int64,
    last_modified_time Int64,
    row_version        Int64,
    is_deleted         Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_individual
(
    _ingested_at              DateTime64(3) DEFAULT now64(3),
    id                        String,
    user_id                   String,
    user_uuid                 String,
    client_reference_id       String,
    individual_id             String,
    tenant_id                 LowCardinality(String),
    given_name                String,
    family_name               String,
    other_names               String,
    date_of_birth             Date32,
    gender                    LowCardinality(String),
    blood_group               LowCardinality(String),
    mobile_number             String,
    alt_contact_number        String,
    email                     String,
    father_name               String,
    husband_name              String,
    relationship              LowCardinality(String),
    photo                     String,
    type                      LowCardinality(String),
    user_name                 String,
    roles                     String,
    is_system_user            Bool,
    is_system_user_active     Bool,
    additional_details        String,
    created_by                String,
    last_modified_by          String,
    created_time              Int64,
    last_modified_time        Int64,
    client_created_time       Int64,
    client_last_modified_time Int64,
    client_created_by         String,
    client_last_modified_by   String,
    row_version               Int64,
    is_deleted                Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_stock
(
    _ingested_at              DateTime64(3) DEFAULT now64(3),
    id                        String,
    client_reference_id       String,
    tenant_id                 LowCardinality(String),
    facility_id               String,
    product_variant_id        String,
    quantity                  Int64,
    waybill_number            String,
    date_of_entry             Int64,
    campaign_number           LowCardinality(String),
    reference_id              String,
    reference_id_type         LowCardinality(String),
    transaction_type          LowCardinality(String),
    transaction_reason        LowCardinality(String),
    transacting_party_id      String,
    transacting_party_type    LowCardinality(String),
    sender_type               LowCardinality(String),
    sender_id                 String,
    receiver_type             LowCardinality(String),
    receiver_id               String,
    additional_details        String,
    created_by                String,
    created_time              Int64,
    last_modified_by          String,
    last_modified_time        Int64,
    client_created_time       Int64,
    client_last_modified_time Int64,
    client_created_by         String,
    client_last_modified_by   String,
    row_version               Int64,
    is_deleted                Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_stock_reconciliation
(
    _ingested_at               DateTime64(3) DEFAULT now64(3),
    id                         String,
    client_reference_id        String,
    tenant_id                  LowCardinality(String),
    facility_id                String,
    product_variant_id         String,
    reference_id               String,
    reference_id_type          LowCardinality(String),
    date_of_reconciliation     Int64,
    calculated_count           Int32,
    physical_recorded_count    Int32,
    comments_on_reconciliation String,
    additional_details         String,
    created_by                 String,
    created_time               Int64,
    last_modified_by           String,
    last_modified_time         Int64,
    client_created_time        Int64,
    client_last_modified_time  Int64,
    client_created_by          String,
    client_last_modified_by    String,
    row_version                Int64,
    is_deleted                 Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_facility
(
    _ingested_at        DateTime64(3) DEFAULT now64(3),
    id                  String,
    tenant_id           LowCardinality(String),
    client_reference_id String,
    is_permanent        Bool,
    name                String,
    usage               LowCardinality(String),
    storage_capacity    Int64,
    address_id          String,
    additional_details  String,
    created_by          String,
    created_time        Int64,
    last_modified_by    String,
    last_modified_time  Int64,
    row_version         Int64,
    is_deleted          Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_product
(
    _ingested_at       DateTime64(3) DEFAULT now64(3),
    id                 String,
    tenant_id          LowCardinality(String),
    type               LowCardinality(String),
    name               String,
    manufacturer       String,
    additional_details String,
    created_by         String,
    last_modified_by   String,
    created_time       Int64,
    last_modified_time Int64,
    row_version        Int64,
    is_deleted         Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_product_variant
(
    _ingested_at       DateTime64(3) DEFAULT now64(3),
    id                 String,
    tenant_id          LowCardinality(String),
    product_id         String,
    sku                String,
    variation          String,
    additional_details String,
    created_by         String,
    last_modified_by   String,
    created_time       Int64,
    last_modified_time Int64,
    row_version        Int64,
    is_deleted         Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_service
(
    _ingested_at       DateTime64(3) DEFAULT now64(3),
    id                 String,
    tenant_id          LowCardinality(String),
    service_def_id     String,
    reference_id       String,
    account_id         String,
    client_id          String,
    additional_details String,
    created_by         String,
    last_modified_by   String,
    created_time       Int64,
    last_modified_time Int64
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_pgr_service
(
    _ingested_at        DateTime64(3) DEFAULT now64(3),
    id                  String,
    tenant_id           LowCardinality(String),
    service_code        LowCardinality(String),
    service_request_id  String,
    description         String,
    account_id          String,
    additional_details  String,
    application_status  LowCardinality(String),
    rating              Int16,
    source              LowCardinality(String),
    created_by          String,
    created_time        Int64,
    last_modified_by    String,
    last_modified_time  Int64,
    active              Bool,
    self_complaint      Bool
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, service_request_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.stg_pgr_address
(
    _ingested_at       DateTime64(3) DEFAULT now64(3),
    tenant_id          LowCardinality(String),
    id                 String,
    parent_id          String,
    door_no            String,
    plot_no            String,
    building_name      String,
    street             String,
    landmark           String,
    city               String,
    pincode            String,
    locality           String,
    district           LowCardinality(String),
    region             LowCardinality(String),
    state              LowCardinality(String),
    country            LowCardinality(String),
    latitude           Float64,
    longitude          Float64,
    created_by         String,
    created_time       Int64,
    last_modified_by   String,
    last_modified_time Int64,
    additional_details String
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;
