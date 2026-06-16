CREATE TABLE IF NOT EXISTS household_enriched (
    -- Household core fields
    id                              VARCHAR(64)         PRIMARY KEY,
    tenant_id                       VARCHAR(1000)       NOT NULL,
    client_reference_id             VARCHAR(64),
    member_count                    INTEGER             NOT NULL,
    is_deleted                      BOOLEAN,
    row_version                     INTEGER,

    -- Household.address (flattened)
    address_id                      VARCHAR(64),
    address_latitude                DOUBLE PRECISION,
    address_longitude               DOUBLE PRECISION,
    address_location_accuracy       DOUBLE PRECISION,
    address_type                    VARCHAR(64),
    address_locality                JSONB,

    -- Household.additionalFields
    household_additional_fields     JSONB,

    -- Household.auditDetails
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    -- Household.clientAuditDetails
    client_created_by               VARCHAR(64),
    client_last_modified_by         VARCHAR(64),
    client_created_time             BIGINT,
    client_last_modified_time       BIGINT,

    -- HouseholdIndexV1 top-level fields
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    user_address                    VARCHAR(440),
    task_dates                      DATE,
    synced_date                     DATE,
    role                            VARCHAR(128),
    boundary_hierarchy              JSONB,
    boundary_hierarchy_code         JSONB,
    geo_point_lat                   DOUBLE PRECISION,
    geo_point_lon                   DOUBLE PRECISION,
    synced_time_stamp               TIMESTAMPTZ,
    synced_time                     BIGINT,
    additional_details              JSONB

    -- ==========================================
    -- EXTRA FIELDS USED BY TRANSFORMER
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS household_member_enriched (
    -- HouseholdMember core fields
    id                                      VARCHAR(64)     PRIMARY KEY,
    tenant_id                               VARCHAR(1000)   NOT NULL,
    client_reference_id                     VARCHAR(64)     NOT NULL,
    household_id                            VARCHAR(64),
    household_client_reference_id           VARCHAR(64),
    individual_id                           VARCHAR(64),
    individual_client_reference_id          VARCHAR(64),
    is_head_of_household                    BOOLEAN,
    is_deleted                              BOOLEAN,
    row_version                             INTEGER,

    -- HouseholdMember.additionalFields
    member_additional_fields                JSONB,

    -- HouseholdMember.auditDetails
    created_by                              VARCHAR(64),
    last_modified_by                        VARCHAR(64),
    created_time                            BIGINT,
    last_modified_time                      BIGINT,

    -- HouseholdMember.clientAuditDetails
    client_created_by                       VARCHAR(64),
    client_last_modified_by                 VARCHAR(64),
    client_created_time                     BIGINT,
    client_last_modified_time               BIGINT,

    -- HouseholdMemberIndexV1 top-level fields
    boundary_hierarchy                      JSONB,
    boundary_hierarchy_code                 JSONB,
    date_of_birth                           BIGINT,
    age                                     INTEGER,
    gender                                  VARCHAR(64),
    user_name                               VARCHAR(180),
    name_of_user                            VARCHAR(250),
    role                                    VARCHAR(128),
    user_address                            VARCHAR(440),
    task_dates                              DATE,
    synced_date                             DATE,
    synced_time_stamp                       TIMESTAMPTZ,
    geo_point_lat                           DOUBLE PRECISION,
    geo_point_lon                           DOUBLE PRECISION,
    locality_code                           VARCHAR(128),
    additional_details                      JSONB

    -- ==========================================
    -- EXTRA FIELDS USED BY TRANSFORMER
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS project_beneficiary_enriched (
    -- ProjectBeneficiary core fields
    id                                      VARCHAR(64)     PRIMARY KEY,
    tenant_id                               VARCHAR(64)     NOT NULL,
    project_id                              VARCHAR(64)     NOT NULL,
    beneficiary_id                          VARCHAR(64),
    beneficiary_client_reference_id         VARCHAR(64),
    client_reference_id                     VARCHAR(64),
    date_of_registration                    BIGINT,
    tag                                     VARCHAR(1000),
    is_deleted                              BOOLEAN,
    row_version                             INTEGER,

    -- ProjectBeneficiary.additionalFields
    beneficiary_additional_fields           JSONB,

    -- ProjectBeneficiary.auditDetails
    created_by                              VARCHAR(64),
    last_modified_by                        VARCHAR(64),
    created_time                            BIGINT,
    last_modified_time                      BIGINT,

    -- ProjectBeneficiary.clientAuditDetails
    client_created_by                       VARCHAR(64),
    client_last_modified_by                 VARCHAR(64),
    client_created_time                     BIGINT,
    client_last_modified_time               BIGINT,

    -- ProjectBeneficiaryIndexV1 top-level fields
    boundary_hierarchy                      JSONB,
    boundary_hierarchy_code                 JSONB,
    user_name                               VARCHAR(180),
    name_of_user                            VARCHAR(250),
    role                                    VARCHAR(128),
    user_address                            VARCHAR(440),
    task_dates                              DATE,
    synced_date                             DATE,
    synced_time_stamp                       TIMESTAMPTZ,
    additional_details                      JSONB

    -- ==========================================
    -- EXTRA FIELDS USED BY TRANSFORMER
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)

);

CREATE TABLE IF NOT EXISTS attendance_log_enriched (
    -- AttendanceLog core fields
    id                                      VARCHAR(256)     PRIMARY KEY,
    tenant_id                               VARCHAR(64),
    register_id                             VARCHAR(256),
    individual_id                           VARCHAR(256),
    log_user_name                           VARCHAR(180),
    time                                    BIGINT,
    type                                    VARCHAR(64),
    status                                  VARCHAR(64),
    log_additional_details                  JSONB,

    -- AttendanceLog.auditDetails
    created_by                              VARCHAR(64),
    last_modified_by                        VARCHAR(64),
    created_time                            BIGINT,
    last_modified_time                      BIGINT,

    -- AttendanceLogIndexV1 top-level fields
    given_name                              VARCHAR(250),
    family_name                             VARCHAR(250),
    attendee_given_name                     VARCHAR(200),
    attendee_family_name                    VARCHAR(200),
    attendee_other_names                    VARCHAR(200),
    user_name                               VARCHAR(180),
    name_of_user                            VARCHAR(250),
    role                                    VARCHAR(128),
    user_address                            VARCHAR(440),
    attendance_time                         TIMESTAMPTZ,
    register_service_code                   VARCHAR(128),
    register_name                           VARCHAR(256),
    register_number                         VARCHAR(256),
    boundary_hierarchy                      JSONB,
    boundary_hierarchy_code                 JSONB,
    additional_details                      JSONB
    -- ==========================================
    -- EXTRA FIELDS USED BY TRANSFORMER
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS attendance_register_enriched (
    -- AttendanceRegister core fields
    id                                      VARCHAR(256)    PRIMARY KEY,
    tenant_id                               VARCHAR(64),
    register_number                         VARCHAR(256),
    name                                    VARCHAR(250),
    reference_id                            VARCHAR(64),
    service_code                            VARCHAR(128),
    start_date                              BIGINT,
    end_date                                BIGINT,
    status                                  VARCHAR(64),
    staff                                   JSONB, --TODO: should this be kept in a separate table?
    attendees                               JSONB, --TODO: should this be kept in a separate table?

    -- AttendanceRegister.additionalDetails
    register_additional_details             JSONB,

    -- AttendanceRegister.auditDetails
    created_by                              VARCHAR(64),
    last_modified_by                        VARCHAR(64),
    created_time                            BIGINT,
    last_modified_time                      BIGINT,

    -- AttendanceRegisterIndexV1 top-level fields
    attendees_info                          JSONB,
    transformer_time_stamp                  TIMESTAMPTZ
    -- ==========================================
    -- EXTRA FIELDS USED BY TRANSFORMER
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS pgr_complaints_enriched (
    -- Service core fields
    id                                      VARCHAR(64)     PRIMARY KEY,
    tenant_id                               VARCHAR(64)     NOT NULL,
    service_code                            VARCHAR(128)    NOT NULL,
    service_request_id                      VARCHAR(128),
    description                             TEXT,
    account_id                              VARCHAR(64),
    rating                                  INTEGER,
    application_status                      VARCHAR(128),
    source                                  VARCHAR(64)     NOT NULL,
    active                                  BOOLEAN,
    self_complaint                          BOOLEAN,
    service_additional_detail               JSONB,

    -- Service.user (complainant, flattened)
    complainant_id                          BIGINT,
    complainant_user_name                   VARCHAR(180),
    complainant_name                        VARCHAR(250),
    complainant_type                        VARCHAR(64),
    complainant_mobile_number               VARCHAR(20),
    complainant_email_id                    VARCHAR(250),
    complainant_tenant_id                   VARCHAR(64),
    complainant_uuid                        VARCHAR(64),
    complainant_active                      BOOLEAN,
    complainant_roles                       JSONB,

    -- Service.address (flattened)
    address_id                              VARCHAR(64),
    address_locality                        JSONB,
    address_addition_details                JSONB,
    address_geo_lat                         DOUBLE PRECISION,
    address_geo_lon                         DOUBLE PRECISION,
    address_geo_additional_details          JSONB,

    -- Service.auditDetails
    created_by                              VARCHAR(64),
    last_modified_by                        VARCHAR(64),
    created_time                            BIGINT,
    last_modified_time                      BIGINT,

    -- PGRIndex top-level fields
    user_name                               VARCHAR(180),
    name_of_user                            VARCHAR(250),
    role                                    VARCHAR(128),
    user_address                            VARCHAR(440),
    boundary_hierarchy                      JSONB,
    boundary_hierarchy_code                 JSONB,
    task_dates                              DATE,
    locality_code                           VARCHAR(128),
    additional_details                      JSONB

    -- ==========================================
    -- EXTRA FIELDS USED BY TRANSFORMER
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS project_staff_enriched (
    -- ==========================================
    -- UPSTREAM FIELDS (From ProjectStaff.java)
    -- ==========================================
    id                              VARCHAR(64)     PRIMARY KEY,
    tenant_id                       VARCHAR(1000)   NOT NULL,
    user_id                         VARCHAR(64)     NOT NULL,
    project_id                      VARCHAR(64)     NOT NULL,
    start_date                      BIGINT,
    end_date                        BIGINT,
    channel                         VARCHAR(64),
    is_deleted                      BOOLEAN         DEFAULT FALSE,
    row_version                     INTEGER,
    additional_fields               JSONB,

    -- Upstream Audit Details
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    -- ==========================================
    -- DOWNSTREAM FIELDS (From ProjectStaffIndexV1.java)
    -- ==========================================
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    user_address                    VARCHAR(440),
    role                            VARCHAR(128),
    task_dates                      JSONB,          -- Stored as JSONB to accommodate List<String>
    boundary_hierarchy              JSONB,
    boundary_hierarchy_code         JSONB,
    additional_details              JSONB,
    locality_code                   VARCHAR(256),

    -- ==========================================
    -- EXTRA FIELDS USED BY TRANSFORMER
    -- ==========================================
    project_id                      VARCHAR(64),    
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS project_staff_enriched (
    -- ==========================================
    -- Fields from ProjectStaff.java
    -- ==========================================
    id                              VARCHAR(64)     PRIMARY KEY,
    tenant_id                       VARCHAR(1000)   NOT NULL,
    user_id                         VARCHAR(64)     NOT NULL,
    project_id                      VARCHAR(64)     NOT NULL,
    start_date                      BIGINT,
    end_date                        BIGINT,
    channel                         VARCHAR(64),
    is_deleted                      BOOLEAN         DEFAULT FALSE,
    row_version                     INTEGER,
    additional_fields               JSONB,

    -- Expanded AuditDetails (from ProjectStaff.java)
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    -- ==========================================
    -- Fields from ProjectStaffIndexV1.java
    -- (Skipping overlapping fields already defined above:
    -- id, userId, tenantId, isDeleted, createdBy, createdTime)
    -- ==========================================
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    user_address                    VARCHAR(440),
    role                            VARCHAR(128),
    task_dates                      JSONB,          -- Stored as JSONB to accommodate List<String>
    boundary_hierarchy              JSONB,
    boundary_hierarchy_code         JSONB,
    additional_details              JSONB,
    locality_code                   VARCHAR(256),
    -- ==========================================
    -- EXTRA FIELDS USED BY TRANSFORMER
    -- ==========================================
    project_id                      VARCHAR(64),    
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);



CREATE TABLE IF NOT EXISTS referral_enriched (
    -- ==========================================
    -- UPSTREAM FIELDS (From Referral.java)
    -- (Nested under "referral" in JSON, requires SMT Flattening)
    -- ==========================================
    id                                          VARCHAR(64)     PRIMARY KEY,
    client_reference_id                         VARCHAR(64),
    project_beneficiary_id                      VARCHAR(64),
    project_beneficiary_client_reference_id     VARCHAR(64),
    referrer_id                                 VARCHAR(64),
    recipient_type                              VARCHAR(64),
    recipient_id                                VARCHAR(64),
    reasons                                     JSONB           NOT NULL, -- List<String>
    side_effect                                 JSONB,                    -- Enriched JSON object
    tenant_id                                   VARCHAR(1000)   NOT NULL,
    is_deleted                                  BOOLEAN         DEFAULT FALSE,
    row_version                                 INTEGER,

    -- Upstream Audit Details
    created_by                                  VARCHAR(64),
    last_modified_by                            VARCHAR(64),
    created_time                                BIGINT,
    last_modified_time                          BIGINT,

    -- Client Audit Details
    client_created_by                           VARCHAR(64),
    client_last_modified_by                     VARCHAR(64),
    client_created_time                         BIGINT,
    client_last_modified_time                   BIGINT,

    additional_fields                           JSONB,

    -- ==========================================
    -- DOWNSTREAM FIELDS (From ReferralIndexV1.java)
    -- ==========================================
    date_of_birth                               BIGINT,
    user_name                                   VARCHAR(180),
    name_of_user                                VARCHAR(250),
    role                                        VARCHAR(128),
    user_address                                VARCHAR(440),
    age                                         INTEGER,
    boundary_hierarchy                          JSONB,
    boundary_hierarchy_code                     JSONB,
    facility_name                               VARCHAR(256),
    individual_id                               VARCHAR(64),
    gender                                      VARCHAR(64),
    task_dates                                  VARCHAR(128),
    synced_date                                 VARCHAR(128),
    additional_details                          JSONB,

    -- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
    -- ==========================================
    project_id                                  VARCHAR(64),
    project_type                                VARCHAR(64),
    project_type_id                             VARCHAR(64),
    project_name                                VARCHAR(256),
    campaign_number                             VARCHAR(128),
    campaign_id                                 VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS device_token_enriched (
    -- ==========================================
    -- Fields from DeviceToken.java
    -- ==========================================
    id                              VARCHAR(64)     PRIMARY KEY,
    user_id                         VARCHAR(64)     NOT NULL,
    device_token                    VARCHAR(512)    NOT NULL, -- Expanded length, Firebase/FCM tokens are long
    device_type                     VARCHAR(64)     NOT NULL,
    tenant_id                       VARCHAR(1000)   NOT NULL,
    facility_id                     VARCHAR(64),
    facility_ids                    JSONB,                    -- Mapped from List<String>
    user_roles                      VARCHAR(512),             -- Comma-separated roles

-- Expanded AuditDetails (from DeviceToken.java)
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    -- ==========================================
    -- Fields from DeviceTokenIndexV1.java
    -- (Skipping 'deviceToken' wrapper object)
    -- ==========================================
    user_name                       VARCHAR(180),
    role                            VARCHAR(128),             -- Distinct from userRoles above
    boundary_hierarchy              JSONB,                    -- Mapped from Map<String, String>
    boundary_hierarchy_code         JSONB,                    -- Mapped from Map<String, String>
    task_dates                      VARCHAR(128),             -- Stored as String per Java model
    synced_date                     VARCHAR(128),             -- Stored as String per Java model

-- ==========================================
-- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
-- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS device_token_enriched (
    -- ==========================================
    -- Fields from DeviceToken.java
    -- ==========================================
    id                              VARCHAR(64)     PRIMARY KEY,
    user_id                         VARCHAR(64)     NOT NULL,
    device_token                    VARCHAR(512)    NOT NULL, -- Expanded length, Firebase/FCM tokens are long
    device_type                     VARCHAR(64)     NOT NULL,
    tenant_id                       VARCHAR(1000)   NOT NULL,
    facility_id                     VARCHAR(64),
    facility_ids                    JSONB,                    -- Mapped from List<String>
    user_roles                      VARCHAR(512),             -- Comma-separated roles

-- Expanded AuditDetails (from DeviceToken.java)
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    -- ==========================================
    -- Fields from DeviceTokenIndexV1.java
    -- ==========================================
    user_name                       VARCHAR(180),
    role                            VARCHAR(128),             -- Distinct from userRoles above
    boundary_hierarchy              JSONB,                    -- Mapped from Map<String, String>
    boundary_hierarchy_code         JSONB,                    -- Mapped from Map<String, String>
    task_dates                      VARCHAR(128),             -- Stored as String per Java model
    synced_date                     VARCHAR(128),             -- Stored as String per Java model

-- ==========================================
-- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
-- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);


CREATE TABLE IF NOT EXISTS hf_referral_enriched (
    -- ==========================================
    -- UPSTREAM FIELDS (From HFReferral.java)
    -- ==========================================
    id                                  VARCHAR(64)     PRIMARY KEY,
    client_reference_id                 VARCHAR(64),
    tenant_id                           VARCHAR(1000)   NOT NULL,
    project_id                          VARCHAR(64),
    project_facility_id                 VARCHAR(64),
    symptom                             VARCHAR(256)    NOT NULL,
    symptom_survey_id                   VARCHAR(100),
    beneficiary_id                      VARCHAR(100),
    referral_code                       VARCHAR(100),
    national_level_id                   VARCHAR(100),
    is_deleted                          BOOLEAN         DEFAULT FALSE,
    row_version                         INTEGER,

    -- Upstream Audit Details
    created_by                          VARCHAR(64),
    last_modified_by                    VARCHAR(64),
    created_time                        BIGINT,
    last_modified_time                  BIGINT,

    -- Client Audit Details
    client_created_by                   VARCHAR(64),
    client_last_modified_by             VARCHAR(64),
    client_created_time                 BIGINT,
    client_last_modified_time           BIGINT,

    additional_fields                   JSONB,

    -- ==========================================
    -- DOWNSTREAM FIELDS (From HfReferralIndexV1.java)
    -- ==========================================
    user_name                           VARCHAR(180),
    role                                VARCHAR(128),
    user_address                        VARCHAR(440),
    boundary_hierarchy                  JSONB,          -- Mapped from Map<String, String>
    boundary_hierarchy_code             JSONB,          -- Mapped from Map<String, String>
    task_dates                          VARCHAR(128),   -- Stored as String per Java model
    synced_date                         VARCHAR(128),   -- Stored as String per Java model
    additional_details                  JSONB,          -- Mapped from ObjectNode

-- ==========================================
-- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
-- ==========================================
    project_type                        VARCHAR(64),
    project_type_id                     VARCHAR(64),
    project_name                        VARCHAR(256),
    campaign_number                     VARCHAR(128),
    campaign_id                         VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS bill_enriched (
    -- ==========================================
    -- Fields from Bill.java
    -- ==========================================
    id                              VARCHAR(64)     PRIMARY KEY,
    tenant_id                       VARCHAR(64)     NOT NULL,
    locality_code                   VARCHAR(256),
    bill_date                       BIGINT          NOT NULL,
    due_date                        BIGINT,
    total_amount                    NUMERIC(12, 2)  DEFAULT 0,
    total_wage_amount               NUMERIC(12, 2)  DEFAULT 0,
    total_food_amount               NUMERIC(12, 2)  DEFAULT 0,
    total_transport_amount          NUMERIC(12, 2)  DEFAULT 0,
    total_paid_amount               NUMERIC(12, 2)  DEFAULT 0,
    business_service                VARCHAR(128)    NOT NULL,
    reference_id                    VARCHAR(256)    NOT NULL,
    from_period                     BIGINT,
    to_period                       BIGINT,

    -- Enums mapped to VARCHAR for database flexibility
    payment_status                  VARCHAR(64),
    status                          VARCHAR(64),

    bill_number                     VARCHAR(128),

    payer                           JSONB           NOT NULL,
    bill_details                    JSONB           NOT NULL,
    additional_details              JSONB,

    -- Expanded AuditDetails
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    wf_status                       VARCHAR(64),
    process_instance                JSONB,

    -- ==========================================
    -- Fields from BillIndexV1.java
    -- ==========================================
    wf_status_info                  JSONB,
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    role                            VARCHAR(128),
    boundary_hierarchy              JSONB,
    boundary_hierarchy_code         JSONB,

    -- ==========================================
-- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);


CREATE TABLE IF NOT EXISTS attendee_enriched (
    -- ==========================================
    -- UPSTREAM FIELDS (From IndividualEntry.java)
    -- ==========================================
    id                              VARCHAR(64)     PRIMARY KEY,
    tenant_id                       VARCHAR(1000)   NOT NULL,
    register_id                     VARCHAR(256),
    individual_id                   VARCHAR(256),
    enrollment_date                 NUMERIC,        -- Mapped from BigDecimal
    denrollment_date                NUMERIC,        -- Mapped from BigDecimal

-- Upstream Audit Details
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    additional_details              JSONB,          -- Mapped from Object

-- ==========================================
-- DOWNSTREAM FIELDS (From AttendeeIndexV1.java)
-- ==========================================
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    role                            VARCHAR(128),
    register_service_code           VARCHAR(128),
    register_name                   VARCHAR(256),
    register_number                 VARCHAR(256),
    boundary_hierarchy              JSONB,          -- Mapped from Map<String, String>
    boundary_hierarchy_code         JSONB,          -- Mapped from Map<String, String>

-- ==========================================
-- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
-- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);


CREATE TABLE IF NOT EXISTS side_effect_enriched (
    -- ==========================================
    -- UPSTREAM FIELDS (From SideEffect.java)
    -- ==========================================
    id                                          VARCHAR(64)     PRIMARY KEY,
    client_reference_id                         VARCHAR(64),
    task_id                                     VARCHAR(64),
    task_client_reference_id                    VARCHAR(64)     NOT NULL,
    project_beneficiary_id                      VARCHAR(64),
    project_beneficiary_client_reference_id     VARCHAR(64),
    raw_symptoms                                JSONB           NOT NULL, -- Upstream List<String>
    tenant_id                                   VARCHAR(1000)   NOT NULL,
    is_deleted                                  BOOLEAN         DEFAULT FALSE,
    row_version                                 INTEGER,

    -- Upstream Audit Details
    created_by                                  VARCHAR(64),
    last_modified_by                            VARCHAR(64),
    created_time                                BIGINT,
    last_modified_time                          BIGINT,
    client_created_by                           VARCHAR(64),
    client_last_modified_by                     VARCHAR(64),
    client_created_time                         BIGINT,
    client_last_modified_time                   BIGINT,

    additional_fields                           JSONB,

    -- ==========================================
    -- DOWNSTREAM FIELDS (From SideEffectsIndexV1.java)
    -- ==========================================
    date_of_birth                               BIGINT,
    age                                         INTEGER,
    boundary_hierarchy                          JSONB,
    boundary_hierarchy_code                     JSONB,
    locality_code                               VARCHAR(256),
    individual_id                               VARCHAR(64),
    gender                                      VARCHAR(64),
    symptoms                                    TEXT,           -- Downstream comma-separated String
    user_name                                   VARCHAR(180),
    name_of_user                                VARCHAR(250),
    role                                        VARCHAR(128),
    user_address                                VARCHAR(440),
    task_dates                                  VARCHAR(128),
    synced_date                                 VARCHAR(128),
    additional_details                          JSONB,

    -- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
    -- ==========================================
    project_id                                  VARCHAR(64),
    project_type                                VARCHAR(64),
    project_type_id                             VARCHAR(64),
    project_name                                VARCHAR(256),
    campaign_number                             VARCHAR(128),
    campaign_id                                 VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS muster_roll_enriched (
    -- ==========================================
    -- UPSTREAM FIELDS (From MusterRoll.java)
    -- ==========================================
    id                              VARCHAR(128)    PRIMARY KEY,
    tenant_id                       VARCHAR(64)     NOT NULL,
    muster_roll_number              VARCHAR(128),
    register_id                     VARCHAR(256)    NOT NULL,
    status                          VARCHAR(64),
    muster_roll_status              VARCHAR(64),
    start_date                      NUMERIC         NOT NULL,
    end_date                        NUMERIC,
    individual_entries              JSONB,
    reference_id                    VARCHAR(256),
    service_code                    VARCHAR(128),
    billing_period_id               VARCHAR(64),
    additional_details              JSONB,

    -- Upstream Audit Details
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    process_instance                JSONB,
    reports                         JSONB,

    -- ==========================================
    -- DOWNSTREAM FIELDS (From MusterRollIndexV1.java)
    -- ==========================================
    edited                          BOOLEAN,
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    role                            VARCHAR(128),
    boundary_hierarchy              JSONB,
    boundary_hierarchy_code         JSONB,

    -- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS stock_enriched (
    -- ==========================================
    -- DOWNSTREAM FIELDS (From StockIndexV1.java)
    -- ==========================================
    id                                VARCHAR(64),
    facility_id                       VARCHAR(64),
    transacting_facility_id           VARCHAR(64),
    facility_name                     VARCHAR(256),
    transacting_facility_name         VARCHAR(256),
    product_variant                   VARCHAR(64),
    product_name                      VARCHAR(256),
    physical_count                    INTEGER,
    event_type                        VARCHAR(64),
    reason                            VARCHAR(64),
    user_name                         VARCHAR(180),
    name_of_user                      VARCHAR(250),
    role                              VARCHAR(128),
    user_address                      VARCHAR(440),
    date_of_entry                     BIGINT,
    boundary_hierarchy                JSONB,
    boundary_hierarchy_code           JSONB,
    created_by                        VARCHAR(256),
    last_modified_by                  VARCHAR(256),
    created_time                      BIGINT,
    last_modified_time                BIGINT,
    synced_time_stamp                 VARCHAR(128),
    synced_time                       BIGINT,
    additional_fields                 JSONB,
    client_reference_id               VARCHAR(128),
    tenant_id                         VARCHAR(128),
    facility_type                     VARCHAR(64),
    transacting_facility_type         VARCHAR(64),
    facility_level                    VARCHAR(64),
    transacting_facility_level        VARCHAR(64),
    facility_target                   BIGINT,
    task_dates                        VARCHAR(128),
    synced_date                       VARCHAR(128),
    additional_details                JSONB,
    waybill_number                    VARCHAR(128)

    -- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS attendance_staff_enriched (
    -- ==========================================
    -- Fields from StaffPermission.java
    -- ==========================================
    id                              VARCHAR(64)     PRIMARY KEY,
    tenant_id                       VARCHAR(1000)   NOT NULL,
    register_id                     VARCHAR(256),
    user_id                         VARCHAR(64),
    enrollment_date                 NUMERIC,        -- Mapped from BigDecimal
    denrollment_date                NUMERIC,        -- Mapped from BigDecimal

-- Expanded AuditDetails
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    additional_details              JSONB,          -- Mapped from Object

-- ==========================================
-- Fields from AttendanceStaffIndexV1.java
-- ==========================================
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    role                            VARCHAR(128),

    register_service_code           VARCHAR(128),
    register_name                   VARCHAR(256),
    register_number                 VARCHAR(256),

    boundary_hierarchy              JSONB,          -- Mapped from Map<String, String>
    boundary_hierarchy_code         JSONB,          -- Mapped from Map<String, String>

-- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
-- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS service_task_enriched (
    id VARCHAR(255) PRIMARY KEY,

    created_time BIGINT,
    created_by VARCHAR(255),

    supervisor_level VARCHAR(255),
    checklist_name VARCHAR(255),
    service_definition_id VARCHAR(255),

    user_name VARCHAR(255),
    name_of_user VARCHAR(255),
    role VARCHAR(255),
    user_address TEXT,

    boundary_hierarchy JSONB,
    boundary_hierarchy_code JSONB,

    tenant_id VARCHAR(255),
    user_id VARCHAR(255),

    attributes JSONB,

    client_reference_id VARCHAR(255),

    synced_time_stamp VARCHAR(255),
    synced_time BIGINT,

    task_dates VARCHAR(255),

    additional_details JSONB,

    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION

-- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
-- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);


CREATE TABLE IF NOT EXISTS bill_detail_enriched (
    -- ==========================================
    -- Fields from BillDetail.java
    -- ==========================================
    id                              VARCHAR(128)    PRIMARY KEY, 
    tenant_id                       VARCHAR(64)     NOT NULL,
    bill_id                         VARCHAR(64),
    total_amount                    NUMERIC(12, 2)  DEFAULT 0,
    total_paid_amount               NUMERIC(12, 2)  DEFAULT 0,
    reference_id                    VARCHAR(64),

    -- Enums mapped to VARCHAR
    payment_status                  VARCHAR(64),
    status                          VARCHAR(64),

    from_period                     BIGINT,
    to_period                       BIGINT,
    worker_id                       VARCHAR(64),

    -- Complex objects and lists mapped to JSONB
    payee                           JSONB           NOT NULL,
    line_items                      JSONB,
    payable_line_items              JSONB           NOT NULL,

    -- Expanded AuditDetails
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    additional_details              JSONB,
    total_attendance                NUMERIC(12, 2),
    wf_status                       VARCHAR(64),
    process_instance                JSONB,

    -- ==========================================
    -- Fields from BillDetailIndexV1.java
    -- ==========================================
    bill_detail_edited              BOOLEAN,
    bill_wf_status_info             JSONB,          -- Mapped from Map<String, Object>
    wf_status_info                  JSONB,          -- Mapped from Map<String, Object>

    -- User Enrichment
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    role                            VARCHAR(128),

    -- Boundary Enrichment
    boundary_hierarchy              JSONB,          -- Mapped from Map<String, String>
    boundary_hierarchy_code         JSONB,          -- Mapped from Map<String, String>

-- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
-- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);


CREATE TABLE IF NOT EXISTS stock_reconciliation_enriched (
    -- ==========================================
    -- CORE FIELDS: From StockReconciliation.java
    -- ==========================================
    id                              VARCHAR(64)     PRIMARY KEY,
    client_reference_id             VARCHAR(64),
    tenant_id                       VARCHAR(1000)   NOT NULL,
    facility_id                     VARCHAR(64)     NOT NULL,
    product_variant_id              VARCHAR(64)     NOT NULL,
    reference_id                    VARCHAR(64),
    reference_id_type               VARCHAR(64),
    physical_count                  INTEGER,
    calculated_count                INTEGER,
    comments_on_reconciliation      TEXT,
    date_of_reconciliation          BIGINT,
    additional_fields               JSONB,
    is_deleted                      BOOLEAN         DEFAULT FALSE,
    row_version                     INTEGER,

    -- Expanded AuditDetails (Server)
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    -- Expanded AuditDetails (Client)
    client_created_by               VARCHAR(64),
    client_last_modified_by         VARCHAR(64),
    client_created_time             BIGINT,
    client_last_modified_time       BIGINT,

    -- ==========================================
    -- ENRICHED DOWNSTREAM FIELDS: StockReconciliationIndexV1.java
    -- ==========================================
    -- Facility Enrichment
    facility_name                   VARCHAR(256),
    facility_target                 BIGINT,
    facility_level                  VARCHAR(64),
    product_name                    VARCHAR(256),

    -- User Enrichment
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    role                            VARCHAR(128),
    user_address                    VARCHAR(440),

    -- Sync Telemetry
    synced_time_stamp               VARCHAR(128),
    synced_time                     BIGINT,
    task_dates                      VARCHAR(128),
    synced_date                     VARCHAR(128),

    -- Hierarchy & Location mappings
    boundary_hierarchy              JSONB,
    boundary_hierarchy_code         JSONB,
    locality_code                   VARCHAR(256),

    -- Dynamic extensions
    additional_details              JSONB,

    -- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS referral_service_task_enriched (

    -- ==========================================
    -- PRIMARY KEY
    -- ==========================================
    -- Composite PK handles the 1-to-Many explosion for the 2 age groups
    id                              VARCHAR(64)     NOT NULL,
    age_group                       VARCHAR(64)     NOT NULL,

    -- ==========================================
    -- CORE UPSTREAM IDENTIFIERS & METADATA
    -- ==========================================
    tenant_id                       VARCHAR(1000)   NOT NULL,
    service_def_id                  VARCHAR(64),
    reference_id                    VARCHAR(128),
    account_id                      VARCHAR(64),
    client_id                       VARCHAR(64),

    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    -- ==========================================
    -- ENRICHED DOWNSTREAM METRICS
    -- ==========================================
    supervisor_level                VARCHAR(128),
    checklist_name                  VARCHAR(256),

    -- User Enrichment
    user_name                       VARCHAR(180),
    role                            VARCHAR(128),
    user_address                    VARCHAR(440),

    -- Sync Telemetry
    synced_time                     BIGINT,
    synced_time_stamp               VARCHAR(128),
    task_dates                      VARCHAR(128),

    -- Malaria Questionnaire Metrics
    children_presented_us           VARCHAR(256),
    malaria_positive_us             VARCHAR(256),
    malaria_negative_us             VARCHAR(256),
    children_presented_ape          VARCHAR(256),
    malaria_positive_ape            VARCHAR(256),
    malaria_negative_ape            VARCHAR(256),



    -- ==========================================
    -- COMPLEX JSONB MAPPINGS (Arrays & Objects)
    -- ==========================================
    boundary_hierarchy              JSONB,          -- Extracted hierarchy Map
    boundary_hierarchy_code         JSONB,          -- Extracted hierarchy code Map
    raw_attributes                  JSONB,          -- Upstream List<AttributeValue>
    additional_fields               JSONB,          -- Upstream JsonNode
    additional_details_upstream     JSONB,          -- Upstream JsonNode
    additional_details              JSONB,          -- Downstream ObjectNode (Contains cycle_index)

    -- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128),

    PRIMARY KEY (id, age_group)
);


CREATE TABLE IF NOT EXISTS project_enriched (
    -- ==========================================
    -- PRIMARY KEY
    -- ==========================================
    id                              VARCHAR(128)    PRIMARY KEY,

    -- ==========================================
    -- CORE METADATA & IDENTIFIERS
    -- ==========================================
    tenant_id                       VARCHAR(1000)   NOT NULL,
    project_number                  VARCHAR(64),
    reference_id                    VARCHAR(128),
    created_by                      VARCHAR(64),
    created_time                    BIGINT,

    -- ==========================================
    -- ENRICHED DOWNSTREAM METRICS
    -- ==========================================
    project_beneficiary_type        VARCHAR(128),
    sub_project_type                VARCHAR(128),
    overall_target                  INTEGER,
    target_per_day                  INTEGER,
    campaign_duration_in_days       INTEGER,
    start_date                      BIGINT,
    end_date                        BIGINT,
    product_variant                 VARCHAR(256),
    product_name                    VARCHAR(256),
    target_type                     VARCHAR(128),
    locality_code                   VARCHAR(128),

    -- ==========================================
    -- COMPLEX JSONB MAPPINGS (Arrays & Objects)
    -- ==========================================
    boundary_hierarchy              JSONB,          -- Downstream Map<String, String>
    boundary_hierarchy_code         JSONB,          -- Downstream Map<String, String>
    task_dates                      JSONB,          -- Downstream List<String>
    additional_details              JSONB,          -- Downstream JsonNode

    -- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
    -- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);


CREATE TABLE IF NOT EXISTS bill_report_enriched (
    -- ==========================================
    -- CORE UPSTREAM FIELDS: From BillReport.java
    -- ==========================================
    id                              VARCHAR(64)     PRIMARY KEY,
    bill_id                         VARCHAR(64),
    bill_ids                        JSONB,          -- List<String> mapped to JSONB
    tenant_id                       VARCHAR(1000)   NOT NULL,
    type                            VARCHAR(64),    -- ReportType enum
    status                          VARCHAR(64),    -- ReportStatus enum
    file_store_id                   VARCHAR(64),
    error_details                   JSONB,          -- Object mapped to JSONB

-- Expanded AuditDetails
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    -- ==========================================
    -- ENRICHED DOWNSTREAM FIELDS: From BillReportIndexV1.java
    -- ==========================================
    bill_report_generation_time     BIGINT,

    -- User Enrichment
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    role                            VARCHAR(128),

    -- Geography Enrichment
    boundary_hierarchy              JSONB,          -- Map<String, String>
    boundary_hierarchy_code         JSONB,          -- Map<String, String>

-- ==========================================
    -- EXTRA FIELDS USED BY THE TRANSFORMER DURING TRANSFORMATION
-- ==========================================
    project_id                      VARCHAR(64),
    project_type                    VARCHAR(64),
    project_type_id                 VARCHAR(64),
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);





CREATE TABLE IF NOT EXISTS user_action_enriched (
    -- ==========================================
    -- UPSTREAM FIELDS (From UserAction.java & EgovOfflineModel)
    -- (Nested under "userAction" in JSON, requires SMT Flattening)
    -- ==========================================
    id                              VARCHAR(64)     PRIMARY KEY,
    tenant_id                       VARCHAR(1000)   NOT NULL,
    client_reference_id             VARCHAR(64),
    
    -- Using prefix to avoid collision with downstream explicit project_id
    raw_project_id                  VARCHAR(64),    
    
    latitude                        DOUBLE PRECISION,
    longitude                       DOUBLE PRECISION,
    location_accuracy               DOUBLE PRECISION,
    boundary_code                   VARCHAR(256),
    action                          VARCHAR(64),    -- Enum UserActionEnum mapped to String
    beneficiary_tag                 VARCHAR(64),
    resource_tag                    VARCHAR(64),
    is_deleted                      BOOLEAN         DEFAULT FALSE,
    additional_fields               JSONB,

    -- Upstream Audit Details (Server)
    created_by                      VARCHAR(64),
    last_modified_by                VARCHAR(64),
    created_time                    BIGINT,
    last_modified_time              BIGINT,

    -- Upstream Audit Details (Client)
    client_created_by               VARCHAR(64),
    client_last_modified_by         VARCHAR(64),
    client_created_time             BIGINT,
    client_last_modified_time       BIGINT,

    -- ==========================================
    -- DOWNSTREAM FIELDS (From UserActionIndexV1.java)
    -- ==========================================
    project_id                      VARCHAR(64),    -- Explicitly declared in V1 Index
    project_type                    VARCHAR(64),    -- Explicitly declared in V1 Index
    project_type_id                 VARCHAR(64),    -- Explicitly declared in V1 Index
    
    user_name                       VARCHAR(180),
    name_of_user                    VARCHAR(250),
    role                            VARCHAR(128),
    
    -- Sync & Telemetry
    synced_time_stamp               VARCHAR(128),
    synced_time                     BIGINT,
    task_dates                      VARCHAR(128),
    synced_date                     VARCHAR(128),
    --GeoPoint in expanded form 
    geo_latitude                    DOUBLE PRECISION,
    geo_longitude                   DOUBLE PRECISION,
    -- Complex JSONB Arrays & Objects
    boundary_hierarchy              JSONB,          -- Mapped from Map<String, String>
    boundary_hierarchy_code         JSONB,          -- Mapped from Map<String, String>
    additional_details              JSONB,          -- Mapped from ObjectNode (parsed fields)

    -- ==========================================
    --Extra fields used by the transformer for transformation
    -- ==========================================
    project_name                    VARCHAR(256),
    campaign_number                 VARCHAR(128),
    campaign_id                     VARCHAR(128)
);



CREATE TABLE IF NOT EXISTS project_task_enriched (
    -- ==========================================
    -- PRIMARY KEY
    -- ==========================================
    id                                      VARCHAR(128)    PRIMARY KEY,

    -- ==========================================
    -- CORE TASK IDENTIFIERS & METADATA
    -- ==========================================
    task_id                                 VARCHAR(64),
    task_type                               VARCHAR(64),
    status                                  VARCHAR(64),
    tenant_id                               VARCHAR(1000)   NOT NULL,
    administration_status                   VARCHAR(64),

    -- Reference IDs
    client_reference_id                     VARCHAR(64),
    task_client_reference_id                VARCHAR(64),
    project_beneficiary_client_reference_id VARCHAR(64),

    -- ==========================================
    -- AUDIT DETAILS
    -- ==========================================
    created_by                              VARCHAR(64),
    last_modified_by                        VARCHAR(64),
    created_time                            BIGINT,
    last_modified_time                      BIGINT,

    -- ==========================================
    -- DELIVERY & PRODUCT METRICS
    -- ==========================================
    product_variant                         VARCHAR(64),
    product_name                            VARCHAR(256),
    quantity                                BIGINT,
    delivered_to                            VARCHAR(128),
    is_delivered                            BOOLEAN,
    delivery_comments                       TEXT,

    -- ==========================================
    -- BENEFICIARY DEMOGRAPHICS
    -- ==========================================
    household_id                            VARCHAR(64),
    member_count                            INTEGER,
    individual_id                           VARCHAR(64),
    date_of_birth                           BIGINT,

    -- ==========================================
    -- USER DEMOGRAPHICS
    -- ==========================================
    user_name                               VARCHAR(180),
    name_of_user                            VARCHAR(250),
    role                                    VARCHAR(128),
    user_address                            VARCHAR(440),

    -- ==========================================
    -- GEOGRAPHY & LOCATION
    -- ==========================================
    latitude                                DOUBLE PRECISION,
    longitude                               DOUBLE PRECISION,
    location_accuracy                       DOUBLE PRECISION,
    locality_code                           VARCHAR(256),
    geo_point                               JSONB,          -- List<Double> Array [lon, lat]
    boundary_hierarchy                      JSONB,          -- Map<String, String>
    boundary_hierarchy_code                 JSONB,          -- Map<String, String>

    -- ==========================================
    -- SYNC TELEMETRY
    -- ==========================================
    synced_time_stamp                       VARCHAR(128),
    synced_date                             VARCHAR(128),
    synced_time                             BIGINT,
    task_dates                              VARCHAR(128),

    -- ==========================================
    -- DYNAMIC EXTENSIONS
    -- ==========================================
    additional_details                      JSONB,          -- ObjectNode

    -- ==========================================
    -- EXTRA FIELDS (Inherited via ProjectInfo)
    -- ==========================================
    project_id                              VARCHAR(64),
    project_type                            VARCHAR(64),
    project_type_id                         VARCHAR(64),
    project_name                            VARCHAR(256),
    campaign_number                         VARCHAR(128),
    campaign_id                             VARCHAR(128)
);
