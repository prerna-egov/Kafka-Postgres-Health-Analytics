-- =============================================================================
-- KPI DATA MART LAYER — Registration & Administration KPIs
-- =============================================================================
-- Generated from approved implementation plan
-- Database: PostgreSQL (requires JSONB, MATERIALIZED VIEW, FILTER clause)
--
-- Execution order:
--   1. Foundation view
--   2. All data mart materialized views
--   3. All indexes
--   4. Refresh script (run periodically)
-- =============================================================================


-- #############################################################################
-- SECTION 0: FOUNDATION MATERIALIZED VIEW
-- #############################################################################
-- Performs the one-to-one beneficiary ↔ task join ONCE.
-- All downstream data marts consume this view instead of raw tables.
-- JSONB field extractions are done here so marts use native typed columns.
-- #############################################################################

-- TIER 1: Beneficiary Level Base Mart
CREATE MATERIALIZED VIEW dm_registration_beneficiary_base AS
SELECT
    pb.beneficiary_id,
    MAX(pb.client_reference_id) AS client_reference_id,
    pb.campaign_id,
    (pb.beneficiary_additional_fields->>'ageMonths')::NUMERIC AS age_months,
    pb.beneficiary_additional_fields->>'settlementType' AS settlement_type,
    pb.additional_details->>'gender' AS gender,
    pb.additional_details->>'guestMember' AS guest_member,
    pb.region_code,
    pb.district_code,
    pb.healthfacility_code,
    pb.settlement_code,
    MAX(CASE WHEN pt.administration_status IN ('VISITED', 'ADMINISTRATION_SUCCESS') THEN 1 ELSE 0 END) AS is_vaccinated,
    MAX(CASE WHEN pt.id IS NOT NULL AND pt.administration_status IN ('VISITED', 'ADMINISTRATION_SUCCESS') THEN 1 ELSE 0 END) AS has_delivery_record,
    MAX(CASE WHEN LOWER(pt.additional_details->>'receivedOPVBefore') = 'no' THEN 1 ELSE 0 END) AS is_zero_dose
FROM project_beneficiary_enriched pb
LEFT JOIN project_task_enriched pt ON pt.project_beneficiary_client_reference_id = pb.client_reference_id
WHERE pb.is_deleted IS NOT TRUE
GROUP BY 
    pb.beneficiary_id, pb.campaign_id,
    pb.beneficiary_additional_fields->>'ageMonths', pb.beneficiary_additional_fields->>'settlementType',
    pb.additional_details->>'gender', pb.additional_details->>'guestMember',
    pb.region_code, pb.district_code, pb.healthfacility_code, pb.settlement_code;

CREATE UNIQUE INDEX idx_dm_reg_ben_base ON dm_registration_beneficiary_base (beneficiary_id, COALESCE(campaign_id, 'NONE'));

-- TIER 2A: Aggregate Metrics Base Mart
CREATE MATERIALIZED VIEW dm_registration_metrics_base AS
SELECT
    campaign_id, region_code, district_code, healthfacility_code, settlement_code,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
        ELSE 'Other'
    END AS age_band,
    COALESCE(gender, 'Unknown') AS gender,
    COALESCE(settlement_type, 'Unknown') AS settlement_type,
    COUNT(*) AS total_enumerated_all,
    COUNT(*) FILTER (WHERE age_months <= 59) AS enumerated_u5_count,
    COUNT(*) FILTER (WHERE LOWER(guest_member) = 'yes') AS guest_member_count,
    SUM(is_zero_dose) AS zero_dose_count,
    SUM(is_vaccinated) FILTER (WHERE age_months <= 59) AS vaccinated_u5_count,
    SUM(has_delivery_record) FILTER (WHERE age_months <= 59) AS delivered_u5_count
FROM dm_registration_beneficiary_base
GROUP BY 
    campaign_id, region_code, district_code, healthfacility_code, settlement_code,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
        ELSE 'Other'
    END, 
    COALESCE(gender, 'Unknown'), 
    COALESCE(settlement_type, 'Unknown');

CREATE UNIQUE INDEX idx_dm_reg_metrics_base ON dm_registration_metrics_base (campaign_id, region_code, district_code, healthfacility_code, settlement_code, age_band, gender, settlement_type);

-- TIER 2B: Household Metrics Base Mart  (household_id doubt)
CREATE MATERIALIZED VIEW dm_household_metrics_base AS
SELECT
    campaign_id, region_code, district_code, healthfacility_code, settlement_code,
    COUNT(DISTINCT id) AS total_households_registered
FROM household_enriched
WHERE is_deleted IS NOT TRUE
GROUP BY campaign_id, region_code, district_code, healthfacility_code, settlement_code;

CREATE UNIQUE INDEX idx_dm_hh_metrics_base ON dm_household_metrics_base (campaign_id, region_code, district_code, healthfacility_code, settlement_code);


-- ---------------------------------------------------------------
-- OPTIMIZATION & INDEXES (Geographic Drill-down Tuples)
-- ---------------------------------------------------------------
-- Indexes for dm_registration_metrics_base
CREATE INDEX IF NOT EXISTS idx_reg_metrics_campaign_region ON dm_registration_metrics_base (campaign_id, region_code);
CREATE INDEX IF NOT EXISTS idx_reg_metrics_campaign_district ON dm_registration_metrics_base (campaign_id, district_code);
CREATE INDEX IF NOT EXISTS idx_reg_metrics_campaign_hc ON dm_registration_metrics_base (campaign_id, healthfacility_code);
CREATE INDEX IF NOT EXISTS idx_reg_metrics_campaign_village ON dm_registration_metrics_base (campaign_id, settlement_code);

-- Indexes for dm_household_metrics_base
CREATE INDEX IF NOT EXISTS idx_hh_metrics_campaign_region ON dm_household_metrics_base (campaign_id, region_code);
CREATE INDEX IF NOT EXISTS idx_hh_metrics_campaign_district ON dm_household_metrics_base (campaign_id, district_code);
CREATE INDEX IF NOT EXISTS idx_hh_metrics_campaign_hc ON dm_household_metrics_base (campaign_id, healthfacility_code);
CREATE INDEX IF NOT EXISTS idx_hh_metrics_campaign_village ON dm_household_metrics_base (campaign_id, settlement_code);



