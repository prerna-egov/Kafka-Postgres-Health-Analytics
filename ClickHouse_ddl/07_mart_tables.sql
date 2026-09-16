CREATE TABLE IF NOT EXISTS analytics.mart_referral
(
    tenant_id               LowCardinality(String),
    campaign_number         LowCardinality(String),
    campaign_id             LowCardinality(String),
    project_id              String,
    hierarchy_type          LowCardinality(String),
    level_one_code          LowCardinality(String),
    level_two_code          LowCardinality(String),
    level_three_code        LowCardinality(String),
    level_four_code         LowCardinality(String),
    level_five_code         LowCardinality(String),
    level_six_code          LowCardinality(String),
    level_seven_code        LowCardinality(String),
    level_eight_code        LowCardinality(String),
    level_nine_code         LowCardinality(String),
    facility_id             String,
    facility_name           LowCardinality(String),
    children_referred       UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_id, project_id, hierarchy_type, level_one_code, level_two_code, level_three_code, facility_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.mart_hf_referral
(
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    campaign_id                 LowCardinality(String),
    project_id                  String,
    hierarchy_type              LowCardinality(String),
    level_one_code              LowCardinality(String),
    level_two_code              LowCardinality(String),
    level_three_code            LowCardinality(String),
    level_four_code             LowCardinality(String),
    level_five_code             LowCardinality(String),
    level_six_code              LowCardinality(String),
    level_seven_code            LowCardinality(String),
    level_eight_code            LowCardinality(String),
    level_nine_code             LowCardinality(String),
    facility_id                 String,
    symptom                     LowCardinality(String),
    referrals                   UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_id, project_id, hierarchy_type, level_one_code, level_two_code, level_three_code, facility_id, symptom)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.mart_hf_checklist_outcome
(
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    campaign_id                 LowCardinality(String),
    project_id                  String,
    hierarchy_type              LowCardinality(String),
    level_one_code              LowCardinality(String),
    level_two_code              LowCardinality(String),
    level_three_code            LowCardinality(String),
    level_four_code             LowCardinality(String),
    level_five_code             LowCardinality(String),
    level_six_code              LowCardinality(String),
    level_seven_code            LowCardinality(String),
    level_eight_code            LowCardinality(String),
    level_nine_code             LowCardinality(String),
    checklist_name              LowCardinality(String),
    role                        LowCardinality(String),
    attribute_code              LowCardinality(String),
    value                       String,
    checklists                  UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_id, project_id, hierarchy_type, level_one_code, level_two_code, level_three_code, checklist_name, attribute_code, value)
SETTINGS index_granularity = 8192;
