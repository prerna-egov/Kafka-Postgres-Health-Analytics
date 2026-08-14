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
ORDER BY (campaign_number, id)
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
ORDER BY (campaign_number, id)
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
ORDER BY (campaign_number, task_dates, id)
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
    country_code                            LowCardinality(String),
    region_code                             LowCardinality(String),
    district_code                           LowCardinality(String),
    health_facility_code                    LowCardinality(String),
    settlement_code                         LowCardinality(String),
    task_dates                              Date32,
    boundary_code                           LowCardinality(String),
    additional_details                      String,
    project_id                              String,
    project_type                            LowCardinality(String),
    project_type_id                         String,
    project_name                            String,
    campaign_number                         LowCardinality(String),
    campaign_id                             LowCardinality(String),
    INDEX idx_pgr_geo (region_code, district_code, health_facility_code, settlement_code) TYPE set(0) GRANULARITY 1
) ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, campaign_number, id)
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
ORDER BY (tenant_id, campaign_number, id)
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
ORDER BY (tenant_id, campaign_number, id)
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
    attributes                        String,
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
ORDER BY (tenant_id, campaign_number, id)
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
ORDER BY (tenant_id, campaign_number, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS project_entity (
    id                              String,
    tenant_id                       LowCardinality(String),
    project_number                  String,
    reference_id                    String,
    created_by                      String,
    created_time                    Int64,
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
ENGINE = ReplacingMergeTree(created_time)
ORDER BY (tenant_id, campaign_number, id, target_type)
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
    date_of_birth                           Int64,
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
ORDER BY (tenant_id, campaign_number, task_dates, id)
SETTINGS index_granularity = 8192;
