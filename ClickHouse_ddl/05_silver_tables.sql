CREATE TABLE IF NOT EXISTS household_entity (
    -- ==========================================
    -- CORE FIELDS 
    -- ==========================================
    id                              String,
    tenant_id                       LowCardinality(String),
    client_reference_id             String,
    member_count                    Int32,
    is_deleted                      Bool,
    row_version                     Int32,

    -- Household.address (flattened)
    address_id                      String,
    address_latitude                Float64,
    address_longitude               Float64,
    address_location_accuracy       Float64,
    address_type                    LowCardinality(String),
    address_locality                String, -- JSON is typically stored as String in ClickHouse

    -- Household.additionalFields
    household_additional_fields     String, -- JSON stored as String

    -- Household.auditDetails (Server)
    created_by                      String,
    last_modified_by                String,
    created_time                    Int64,
    last_modified_time              Int64,

    -- Household.clientAuditDetails (Client)
    client_created_by               String,
    client_last_modified_by         String,
    client_created_time             Int64,
    client_last_modified_time       Int64,

    -- ==========================================
    -- DOWNSTREAM FIELDS 
    -- ==========================================
    user_name                       LowCardinality(String),
    name_of_user                    LowCardinality(String),
    role                            LowCardinality(String),
    user_address                    String,

    -- Stored as Dates/Timestamps
    task_dates                      Date32,
    synced_date                     Date32,
    synced_time_stamp               DateTime64(3, 'UTC'), -- Millisecond precision with timezone

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    -- Extracted from geoPoint 
    geo_point_lat                   Float64,
    geo_point_lon                   Float64,

    -- Mapped from ObjectNode
    additional_details              String,

    -- ==========================================
    -- EXTRA FIELDS 
    -- ==========================================
    project_id                      String,
    project_type                    LowCardinality(String),
    project_type_id                 String,
    project_name                    String,
    campaign_number                 LowCardinality(String),
    campaign_id                     LowCardinality(String),

    -- Secondary Data-Skipping Index (Replicates Postgres B-Tree)
    INDEX idx_hh_entity_geo ( level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(client_last_modified_time) 
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS household_member_entity (
    -- HouseholdMember core fields
    id                                      String,
    tenant_id                               LowCardinality(String),
    client_reference_id                     String,
    household_id                            String,
    household_client_reference_id           String,
    individual_id                           String,
    individual_client_reference_id          String,
    is_head_of_household                    Bool,
    is_deleted                              Bool,
    row_version                             Int32,

    -- HouseholdMember.additionalFields
    member_additional_fields                String, -- JSON stored as String

    -- HouseholdMember.auditDetails
    created_by                              String,
    last_modified_by                        String,
    created_time                            Int64,
    last_modified_time                      Int64,

    -- HouseholdMember.clientAuditDetails
    client_created_by                       String,
    client_last_modified_by                 String,
    client_created_time                     Int64,
    client_last_modified_time               Int64,

    -- HouseholdMemberIndexV1 top-level fields

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    date_of_birth                           Int64,
    age                                     Int32,
    gender                                  LowCardinality(String),
    user_name                               LowCardinality(String),
    name_of_user                            LowCardinality(String),
    role                                    LowCardinality(String),
    user_address                            String,
    task_dates                              Date32,
    synced_date                             Date32,
    synced_time_stamp                       DateTime64(3, 'UTC'),
    geo_point_lat                           Float64,
    geo_point_lon                           Float64,
    boundary_code                           LowCardinality(String),
    additional_details                      String, -- JSON stored as String

    -- ==========================================
    -- EXTRA FIELDS USED BY TRANSFORMER
    -- ==========================================
    project_id                              String,
    project_type                            LowCardinality(String),
    project_type_id                         String,
    project_name                            String,
    campaign_number                         LowCardinality(String),
    campaign_id                             LowCardinality(String),

    -- Secondary Data-Skipping Index for Geographic Grouping
    INDEX idx_hhm_entity_geo ( level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(client_last_modified_time) 
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS project_beneficiary_entity (
    -- ProjectBeneficiary core fields
    id                                      String,
    tenant_id                               LowCardinality(String),
    project_id                              String,
    beneficiary_id                          String,
    beneficiary_client_reference_id         String,
    client_reference_id                     String,
    date_of_registration                    Int64,
    tag                                     String,
    is_deleted                              Bool,
    row_version                             Int32,

    -- ProjectBeneficiary.additionalFields
    beneficiary_additional_fields           String, -- JSON stored as String

    -- ProjectBeneficiary.auditDetails
    created_by                              String,
    last_modified_by                        String,
    created_time                            Int64,
    last_modified_time                      Int64,

    -- ProjectBeneficiary.clientAuditDetails
    client_created_by                       String,
    client_last_modified_by                 String,
    client_created_time                     Int64,
    client_last_modified_time               Int64,

    -- ProjectBeneficiaryIndexV1 top-level fields

   -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    user_name                               LowCardinality(String),
    name_of_user                            LowCardinality(String),
    role                                    LowCardinality(String),
    user_address                            String,
    task_dates                              Date32,
    synced_date                             Date32,
    synced_time_stamp                       DateTime64(3, 'UTC'),
    additional_details                      String, -- JSON stored as String

    -- ==========================================
    -- EXTRA FIELDS USED BY TRANSFORMER
    -- ==========================================
    project_type                            LowCardinality(String),
    project_type_id                         String,
    project_name                            String,
    campaign_number                         LowCardinality(String),
    campaign_id                             LowCardinality(String),

    -- Secondary Data-Skipping Index for Geographic Grouping
    INDEX idx_pb_entity_geo ( level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(client_last_modified_time) 
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;



----------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS pgr_complaints_entity (
    id                                      String,
    tenant_id                               LowCardinality(String),
    service_code                            String,
    service_request_id                      String,
    description                             String,
    account_id                              String,
    rating                                  Int32,
    application_status                      LowCardinality(String),
    source                                  String,
    active                                  Bool,
    self_complaint                          Bool,
    service_additional_detail               String,
    complainant_id                          Int64,
    complainant_user_name                   String,
    complainant_name                        String,
    complainant_type                        LowCardinality(String),
    complainant_mobile_number               String,
    complainant_email_id                    String,
    complainant_tenant_id                   LowCardinality(String),
    complainant_uuid                        String,
    complainant_active                      Bool,
    complainant_roles                       String,
    address_id                              String,
    address_locality                        String,
    address_addition_details                String,
    address_geo_lat                         Float64,
    address_geo_lon                         Float64,
    address_geo_additional_details          String,
    created_by                              String,
    last_modified_by                        String,
    created_time                            Int64,
    last_modified_time                      Int64,
    user_name                               LowCardinality(String),
    name_of_user                            LowCardinality(String),
    role                                    LowCardinality(String),
    user_address                            String,
    
    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    task_dates                              Date32,
    boundary_code                           LowCardinality(String),
    additional_details                      String,
    project_id                              String,
    project_type                            LowCardinality(String),
    project_type_id                         String,
    project_name                            String,
    campaign_number                         LowCardinality(String),
    campaign_id                             LowCardinality(String),
    INDEX idx_pgr_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
) ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS project_staff_entity (
    id                              String,
    tenant_id                       LowCardinality(String),
    user_id                         LowCardinality(String),
    user_name                       LowCardinality(String),
    name_of_user                    LowCardinality(String),
    user_address                    String,
    role                            LowCardinality(String),
    boundary_code                   LowCardinality(String),
    is_deleted                      Bool,
    created_by                      String,
    created_time                    Int64,
    task_dates                      String,
    additional_details              String,

   -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    project_id                      String,
    project_type                    LowCardinality(String),
    project_type_id                 String,
    project_name                    String,
    campaign_number                 LowCardinality(String),
    campaign_id                     LowCardinality(String),
    INDEX idx_pst_geo ( level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
) ENGINE = ReplacingMergeTree(created_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;



CREATE TABLE IF NOT EXISTS stock_entity (
    id                                String,
    facility_id                       LowCardinality(String),
    transacting_facility_id           LowCardinality(String),
    facility_name                     LowCardinality(String),
    transacting_facility_name         LowCardinality(String),
    product_variant                   LowCardinality(String),
    product_name                      LowCardinality(String),
    physical_count                    Int32,
    event_type                        LowCardinality(String),
    reason                            LowCardinality(String),
    user_name                         LowCardinality(String),
    name_of_user                      LowCardinality(String),
    role                              LowCardinality(String),
    user_address                      String,
    date_of_entry                     Int64,

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),


    created_by                        String,
    last_modified_by                  String,
    created_time                      Int64,
    last_modified_time                Int64,
    synced_time_stamp                 DateTime64(3, 'UTC'),
    synced_time                       Int64,
    additional_fields                 String,
    client_reference_id               String,
    tenant_id                         LowCardinality(String),
    facility_type                     LowCardinality(String),
    transacting_facility_type         LowCardinality(String),
    facility_level                    LowCardinality(String),
    transacting_facility_level        LowCardinality(String),
    facility_target                   Int64,
    task_dates                        Date32,
    synced_date                       Date32,
    additional_details                String,
    waybill_number                    String,
    project_id                        String,
    project_type                      LowCardinality(String),
    project_type_id                   String,
    project_name                      String,
    campaign_number                   LowCardinality(String),
    campaign_id                       LowCardinality(String),
    INDEX idx_stock_geo ( facility_id,  level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1,
    INDEX idx_stock_cat (facility_type, product_variant, product_name) TYPE set(0) GRANULARITY 1,
    INDEX idx_stock_date_entry (date_of_entry) TYPE minmax GRANULARITY 1,
    INDEX idx_stock_task_dates (task_dates) TYPE minmax GRANULARITY 1,
    INDEX idx_stock_synced_time (synced_time_stamp) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS service_task_entity (
    id                                String,
    created_time                      Int64,
    created_by                        String,
    supervisor_level                  LowCardinality(String),
    checklist_name                    String,
    service_definition_id             String,
    user_name                         LowCardinality(String),
    name_of_user                      LowCardinality(String),
    role                              LowCardinality(String),
    user_address                      String,

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    tenant_id                         LowCardinality(String),

    user_id                           String,
    client_reference_id               String,
    synced_time_stamp                 DateTime64(3, 'UTC'),
    synced_time                       Int64,
    task_dates                        Date32,
    additional_details                String,
    latitude                          Float64,
    longitude                         Float64,
    project_id                        String,
    project_type                      LowCardinality(String),
    project_type_id                   String,
    project_name                      String,
    campaign_number                   LowCardinality(String),
    campaign_id                       LowCardinality(String)
) ENGINE = ReplacingMergeTree(created_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS service_task_attribute_entity (
    id                           String,
    reference_id                 String,
    attribute_code               LowCardinality(String),
    value                        String,
    created_by                   String,
    last_modified_by             String,
    created_time                 Int64,
    last_modified_time           Int64,
    additional_details           String,
    client_reference_id          String,
    service_client_reference_id  String
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS stock_reconciliation_entity (
    id                              String,
    client_reference_id             String,
    tenant_id                       LowCardinality(String),
    facility_id                     String,
    product_variant_id              String,
    reference_id                    String,
    reference_id_type               String,
    physical_count                  Int32,
    calculated_count                Int32,
    comments_on_reconciliation      String,
    date_of_reconciliation          Int64,
    additional_fields               String,
    is_deleted                      Bool,
    row_version                     Int32,
    created_by                      String,
    last_modified_by                String,
    created_time                    Int64,
    last_modified_time              Int64,
    client_created_by               String,
    client_last_modified_by         String,
    client_created_time             Int64,
    client_last_modified_time       Int64,
    facility_name                   String,
    facility_target                 Int64,
    facility_level                  LowCardinality(String),
    product_name                    String,
    user_name                       LowCardinality(String),
    name_of_user                    LowCardinality(String),
    role                            LowCardinality(String),
    user_address                    String,
    synced_time_stamp               DateTime64(3, 'UTC'),
    synced_time                     Int64,
    task_dates                      Date32,
    synced_date                     Date32,

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),


    boundary_code                   LowCardinality(String),
    additional_details              String,
    project_id                      String,
    project_type                    LowCardinality(String),
    project_type_id                 String,
    project_name                    String,
    campaign_number                 LowCardinality(String),
    campaign_id                     LowCardinality(String),
    INDEX idx_stock_recon_geo (facility_id,  level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
) 
ENGINE = ReplacingMergeTree(client_last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS project_entity (
    id                              String,
    tenant_id                       LowCardinality(String),
    project_number                  String,
    reference_id                    String,
    created_by                      String,
    created_time                    Int64,
    last_modified_time              Int64,
    project_beneficiary_type        LowCardinality(String),
    sub_project_type                LowCardinality(String),
    overall_target                  Int32,
    target_per_day                  Int32,
    campaign_duration_in_days       Int32,
    start_date                      Int64,
    end_date                        Int64,
    product_variant                 LowCardinality(String),
    product_name                    LowCardinality(String),
    target_type                     LowCardinality(String),
    boundary_code                   LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    task_dates                      String,
    additional_details              String,
    project_id                      String,
    project_type                    LowCardinality(String),
    project_type_id                 String,
    project_name                    String,
    campaign_number                 LowCardinality(String),
    campaign_id                     LowCardinality(String),
    INDEX idx_project_geo ( level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id, target_type)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS project_task_entity (
    id                                      String,
    task_id                                 String,
    task_type                               LowCardinality(String),
    status                                  LowCardinality(String),
    tenant_id                               LowCardinality(String),
    administration_status                   LowCardinality(String),
    client_reference_id                     String,
    task_client_reference_id                String,
    project_beneficiary_client_reference_id String,
    created_by                              String,
    last_modified_by                        String,
    created_time                            Int64,
    last_modified_time                      Int64,
    product_variant                         String,
    product_name                            String,
    quantity                                Int64,
    delivered_to                            String,
    is_delivered                            Bool,
    delivery_comments                       String,
    household_id                            String,
    member_count                            Int32,
    individual_id                           String,
    user_name                               LowCardinality(String),
    name_of_user                            LowCardinality(String),
    role                                    LowCardinality(String),
    user_address                            String,
    latitude                                Float64,
    longitude                               Float64,
    location_accuracy                       Float64,
    boundary_code                           LowCardinality(String),
    geo_point                               String,

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    --Beneficiary Info
    --TODO: add default values
    age                                     UInt32,
    gender                                  LowCardinality(String),
    date_of_birth                           Date,

    cycleIndex                              UInt8,
    doseIndex                               UInt8,
    delivery_strategy                       LowCardinality(String),

    synced_time_stamp                       DateTime64(3, 'UTC'),
    synced_date                             Date32,
    synced_time                             Int64,
    task_dates                              Date32,
    additional_details                      String,
    project_id                              String,
    project_type                            LowCardinality(String),
    project_type_id                         String,
    project_name                            String,
    campaign_number                         LowCardinality(String),
    campaign_id                             LowCardinality(String),

    INDEX idx_pt_entity_geo (campaign_number,  level_two_code, level_three_code, level_four_code, level_five_code, level_six_code, administration_status) TYPE set(0) GRANULARITY 1,
    INDEX idx_pt_entity_task_dates (campaign_number, task_dates) TYPE minmax GRANULARITY 1,
    INDEX idx_pt_entity_synced_time (synced_time_stamp) TYPE minmax GRANULARITY 1
) 
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS attendance_log_entity (
    id                                      String,
    tenant_id                               LowCardinality(String),
    register_id                             String,
    individual_id                           String,
    log_user_name                           String,
    time                                    Int64,
    type                                    LowCardinality(String),
    status                                  LowCardinality(String),
    document_ids                            String,
    log_additional_details                  String,
    created_by                              String,
    last_modified_by                        String,
    created_time                            Int64,
    last_modified_time                      Int64,
    attendance_taker_user_name              LowCardinality(String),
    attendance_taker_name_of_user           LowCardinality(String),
    user_name                               LowCardinality(String),
    name_of_user                            LowCardinality(String),
    role                                    LowCardinality(String),
    attendance_time                         String,
    register_service_code                   LowCardinality(String),
    register_name                           LowCardinality(String),
    register_number                         LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),


    project_id                              String,
    project_type                            LowCardinality(String),
    project_type_id                         String,
    project_name                            String,
    campaign_number                         LowCardinality(String),
    campaign_id                             LowCardinality(String),
    INDEX idx_atr_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
) 
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS attendance_register_entity (
    id                                      String,
    tenant_id                               LowCardinality(String),
    register_number                         String,
    name                                    String,
    reference_id                            String,
    service_code                            String,
    start_date                              Int64,
    end_date                                Int64,
    status                                  LowCardinality(String),
    register_additional_details             String,
    created_by                              String,
    last_modified_by                        String,
    created_time                            Int64,
    last_modified_time                      Int64,
    attendees_info                          String,
    transformer_time_stamp                  DateTime64(3, 'UTC'),
    staffs_count                            Int64,
    attendees_count                         Int64,

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    project_id                              String,
    project_type                            LowCardinality(String),
    project_type_id                         String,
    project_name                            String,
    campaign_number                         LowCardinality(String),
    campaign_id                             LowCardinality(String),
    INDEX idx_arr_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
) ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS attendee_entity (
    id                              String,
    tenant_id                       LowCardinality(String),
    register_id                     String,
    individual_id                   String,
    enrollment_date                 Float64,
    denrollment_date                Float64,
    created_by                      String,
    last_modified_by                String,
    created_time                    Int64,
    last_modified_time              Int64,
    additional_details              String,
    user_name                       LowCardinality(String),
    name_of_user                    LowCardinality(String),
    role                            LowCardinality(String),
    register_service_code           String,
    register_name                   String,
    register_number                 String,
    
    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    project_id                      String,
    project_type                    LowCardinality(String),
    project_type_id                 String,
    project_name                    String,
    campaign_number                 LowCardinality(String),
    campaign_id                     LowCardinality(String),
    INDEX idx_attendee_dates (enrollment_date, denrollment_date) TYPE minmax GRANULARITY 1,
    INDEX idx_attendee_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1,
    INDEX idx_attendee_role (role) TYPE set(0) GRANULARITY 1
) 
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS attendance_staff_entity (
    id                              String,
    tenant_id                       LowCardinality(String),
    register_id                     String,
    user_id                         String,
    enrollment_date                 Float64,
    denrollment_date                Float64,
    created_by                      String,
    last_modified_by                String,
    created_time                    Int64,
    last_modified_time              Int64,
    additional_details              String,
    user_name                       LowCardinality(String),
    name_of_user                    LowCardinality(String),
    role                            LowCardinality(String),
    register_service_code           String,
    register_name                   String,
    register_number                 String,

   -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    project_id                      String,
    project_type                    LowCardinality(String),
    project_type_id                 String,
    project_name                    String,
    campaign_number                 LowCardinality(String),
    campaign_id                     LowCardinality(String),
    INDEX idx_att_staff_dates (enrollment_date, denrollment_date) TYPE minmax GRANULARITY 1,
    INDEX idx_att_staff_role (role) TYPE set(0) GRANULARITY 1,
    INDEX idx_att_staff_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
) 
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, campaign_number, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS bill_entity (
    id                              String,
    tenant_id                       LowCardinality(String),
    boundary_code                   LowCardinality(String),
    bill_date                       Int64,
    due_date                        Int64,
    total_amount                    Decimal(12, 2),
    total_wage_amount               Decimal(12, 2),
    total_food_amount               Decimal(12, 2),
    total_transport_amount          Decimal(12, 2),
    total_paid_amount               Decimal(12, 2),
    business_service                String,
    reference_id                    String,
    from_period                     Int64,
    to_period                       Int64,
    payment_status                  LowCardinality(String),
    status                          LowCardinality(String),
    bill_number                     String,
    payer                           String,
    bill_details                    String,
    additional_details              String,
    created_by                      String,
    last_modified_by                String,
    created_time                    Int64,
    last_modified_time              Int64,
    wf_status                       LowCardinality(String),
    process_instance                String,
    wf_status_info                  String,
    user_name                       LowCardinality(String),
    name_of_user                    LowCardinality(String),
    role                            LowCardinality(String),
    
    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    project_id                      String,
    project_type                    LowCardinality(String),
    project_type_id                 String,
    project_name                    String,
    campaign_number                 LowCardinality(String),
    campaign_id                     LowCardinality(String),
    INDEX idx_bill_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1,
    INDEX idx_bill_period (from_period, to_period) TYPE minmax GRANULARITY 1,
    INDEX idx_bill_status (status, wf_status) TYPE set(0) GRANULARITY 1
) ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS bill_detail_entity (
    id                              String,
    tenant_id                       LowCardinality(String),
    bill_id                         String,
    total_amount                    Decimal(12, 2),
    total_paid_amount               Decimal(12, 2),
    reference_id                    String,
    payment_status                  LowCardinality(String),
    status                          LowCardinality(String),
    from_period                     Int64,
    to_period                       Int64,
    worker_id                       String,
    payee                           String,
    line_items                      String,
    payable_line_items              String,
    created_by                      String,
    last_modified_by                String,
    created_time                    Int64,
    last_modified_time              Int64,
    additional_details              String,
    total_attendance                Decimal(12, 2),
    wf_status                       LowCardinality(String),
    process_instance                String,
    bill_detail_edited              Bool,
    bill_wf_status_info             String,
    wf_status_info                  String,
    user_name                       LowCardinality(String),
    name_of_user                    LowCardinality(String),
    role                            LowCardinality(String),
    
    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    project_id                      String,
    project_type                    LowCardinality(String),
    project_type_id                 String,
    project_name                    String,
    campaign_number                 LowCardinality(String),
    campaign_id                     LowCardinality(String)
) ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;



CREATE TABLE IF NOT EXISTS side_effect_entity (
    id                                          String,
    client_reference_id                         String,
    task_id                                     LowCardinality(String),
    task_client_reference_id                    String,
    project_beneficiary_id                      String,
    project_beneficiary_client_reference_id     String,
    raw_symptoms                                String,
    tenant_id                                   LowCardinality(String),
    is_deleted                                  Bool,
    row_version                                 Int32,
    created_by                                  String,
    last_modified_by                            String,
    created_time                                Int64,
    last_modified_time                          Int64,
    client_created_by                           String,
    client_last_modified_by                     String,
    client_created_time                         Int64,
    client_last_modified_time                   Int64,
    additional_fields                           String,
    date_of_birth                               Int64,
    age                                         Int32,
    
    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    boundary_code                               LowCardinality(String),
    individual_id                               String,
    gender                                      LowCardinality(String),
    symptoms                                    String,
    user_name                                   LowCardinality(String),
    name_of_user                                LowCardinality(String),
    role                                        LowCardinality(String),
    user_address                                String,
    task_dates                                  Date32,
    synced_date                                 Date32,
    additional_details                          String,
    project_id                                  String,
    project_type                                LowCardinality(String),
    project_type_id                             String,
    project_name                                String,
    campaign_number                             LowCardinality(String),
    campaign_id                                 LowCardinality(String),
    INDEX idx_side_effect_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1,
    INDEX idx_side_effect_dates (task_dates) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(client_last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS referral_entity (
    id                                          String,
    client_reference_id                         String,
    project_beneficiary_id                      String,
    project_beneficiary_client_reference_id     String,
    referrer_id                                 String,
    recipient_type                              LowCardinality(String),
    recipient_id                                String,
    reasons                                     String,
    side_effect                                 String,
    referral_code                                String,
    tenant_id                                   LowCardinality(String),
    is_deleted                                  Bool,
    row_version                                 Int32,
    created_by                                  String,
    last_modified_by                            String,
    created_time                                Int64,
    last_modified_time                          Int64,
    client_created_by                           String,
    client_last_modified_by                     String,
    client_created_time                         Int64,
    client_last_modified_time                   Int64,
    additional_fields                           String,
    date_of_birth                               Int64,
    user_name                                   LowCardinality(String),
    name_of_user                                LowCardinality(String),
    role                                        LowCardinality(String),
    user_address                                String,
    age                                         Int32,
    
    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    facility_name                               LowCardinality(String),
    individual_id                               String,
    gender                                      LowCardinality(String),
    task_dates                                  Date32,
    synced_date                                 Date32,
    additional_details                          String,
    project_id                                  String,
    project_type                                LowCardinality(String),
    project_type_id                             String,
    project_name                                String,
    campaign_number                             LowCardinality(String),
    campaign_id                                 LowCardinality(String),
    INDEX idx_ref_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
) ENGINE = ReplacingMergeTree(client_last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS hf_referral_entity (
    id                                  String,
    client_reference_id                 String,
    tenant_id                           LowCardinality(String),
    project_id                          String,
    project_facility_id                 String,
    symptom                             LowCardinality(String),
    symptom_survey_id                   LowCardinality(String),
    beneficiary_id                      String,
    referral_code                       String,
    national_level_id                   String,
    is_deleted                          Bool,
    row_version                         Int32,
    created_by                          String,
    last_modified_by                    String,
    created_time                        Int64,
    last_modified_time                  Int64,
    client_created_by                   String,
    client_last_modified_by             String,
    client_created_time                 Int64,
    client_last_modified_time           Int64,
    additional_fields                   String,
    user_name                           LowCardinality(String),
    role                                LowCardinality(String),
    user_address                        String,
    
    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    task_dates                          Date32,
    synced_date                         Date32,
    additional_details                  String,
    project_type                        LowCardinality(String),
    project_type_id                     String,
    project_name                        String,
    campaign_number                     LowCardinality(String),
    campaign_id                         LowCardinality(String),

    INDEX idx_hf_ref_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
) 
ENGINE = ReplacingMergeTree(client_last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS user_action_entity (
    id                                  String,
    tenant_id                           LowCardinality(String),
    client_reference_id                 String,
    latitude                            Float64,
    longitude                           Float64,
    location_accuracy                   Float64,
    boundary_code                       String,
    action                              LowCardinality(String),
    beneficiary_tag                     String,
    resource_tag                        String,
    is_deleted                          Bool,
    additional_fields                   String,
    created_by                          String,
    last_modified_by                    String,
    created_time                        Int64,
    last_modified_time                  Int64,
    client_created_by                   String,
    client_last_modified_by             String,
    client_created_time                 Int64,
    client_last_modified_time           Int64,
    project_id                          String,
    project_type                        LowCardinality(String),
    project_type_id                     String,
    user_name                           LowCardinality(String),
    name_of_user                        LowCardinality(String),
    role                                LowCardinality(String),
    synced_time_stamp                   DateTime64(3, 'UTC'),
    synced_time                         Int64,
    task_dates                          Date32,
    synced_date                         Date32,
    geo_latitude                        Float64,
    geo_longitude                       Float64,
    
     -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    additional_details                  String,
    project_name                        String,
    campaign_number                     LowCardinality(String),
    campaign_id                         LowCardinality(String)
) 
ENGINE = ReplacingMergeTree(client_last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS muster_roll_entity (
    id                                  String,
    tenant_id                           LowCardinality(String),
    muster_roll_number                  String,
    register_id                         String,
    status                              LowCardinality(String),
    muster_roll_status                  LowCardinality(String),
    start_date                          Int64,
    end_date                            Int64,

    --Individual Entry fields
    individual_entry_id                 String,
    individual_id                       String,
    actual_total_attendance             Decimal(4, 2),


    reference_id                        String,
    service_code                        String,
    billing_period_id                   String,
    additional_details                  String,
    created_by                          String,
    last_modified_by                    String,
    created_time                        Int64,
    last_modified_time                  Int64,
    edited                              Bool,
    user_name                           LowCardinality(String),
    name_of_user                        LowCardinality(String),
    role                                LowCardinality(String),
    
     -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    project_id                          String,
    project_type                        LowCardinality(String),
    project_type_id                     String,
    project_name                        String,
    campaign_number                     LowCardinality(String),
    campaign_id                         LowCardinality(String),

    INDEX idx_muster_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1,
    INDEX idx_muster_role (role) TYPE set(0) GRANULARITY 1
) 
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id, individual_entry_id)
SETTINGS index_granularity = 8192;



CREATE TABLE IF NOT EXISTS device_token_entity (

    _ingested_at                        DateTime64(3) DEFAULT now64(3),
    id                                  String,
    user_id                             String,
    device_type                         LowCardinality(String),
    tenant_id                           LowCardinality(String),
    created_by                          String,
    last_modified_by                    String,
    created_time                        Int64,
    last_modified_time                  Int64,
    facility_id                         String,
    user_roles                          String,
    user_name                           LowCardinality(String),
    role                                LowCardinality(String),

     -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    hierarchy_type                      LowCardinality(String),

    task_dates                          Date32,
    synced_date                         Date32,

    -- EXTRA FIELDS USED BY TRANSFORMER
    project_id                          String,
    project_type                        LowCardinality(String),
    project_type_id                     String,
    project_name                        String,
    campaign_number                     String,
    campaign_id                         String

)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, id)
SETTINGS index_granularity = 8192;