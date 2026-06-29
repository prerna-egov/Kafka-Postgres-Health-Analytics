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
    pb.country_code,
    pb.province_code,
    pb.district_code,
    pb.spp_code,
    pb.health_center_code,
    pb.village_code,
    MAX(CASE WHEN pt.administration_status IN ('VISITED', 'ADMINISTRATION_SUCCESS') THEN 1 ELSE 0 END) AS is_vaccinated,
    MAX(CASE WHEN pt.id IS NOT NULL AND pt.administration_status IN ('VISITED', 'ADMINISTRATION_SUCCESS') THEN 1 ELSE 0 END) AS has_delivery_record,
    MAX(CASE WHEN LOWER(pt.additional_details->>'receivedOPVBefore') = 'no' AND NULLIF(TRIM(pt.additional_details->>'ageInMonths'), '')::NUMERIC > 0.5 THEN 1 ELSE 0 END) AS is_zero_dose
FROM project_beneficiary_enriched pb
LEFT JOIN project_task_enriched pt ON pt.project_beneficiary_client_reference_id = pb.client_reference_id
WHERE pb.is_deleted IS NOT TRUE
GROUP BY 
    pb.beneficiary_id, pb.campaign_id,
    pb.beneficiary_additional_fields->>'ageMonths', pb.beneficiary_additional_fields->>'settlementType',
    pb.additional_details->>'gender', pb.additional_details->>'guestMember',
    pb.country_code, pb.province_code, pb.district_code, pb.spp_code, pb.health_center_code, pb.village_code;

CREATE UNIQUE INDEX idx_dm_reg_ben_base ON dm_registration_beneficiary_base (beneficiary_id, COALESCE(campaign_id, 'NONE'));

-- TIER 2A: Aggregate Metrics Base Mart
CREATE MATERIALIZED VIEW dm_registration_metrics_base AS
SELECT
    campaign_id, country_code, province_code, district_code, spp_code, health_center_code, village_code,
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
    campaign_id, country_code, province_code, district_code, spp_code, health_center_code, village_code,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
        ELSE 'Other'
    END, 
    COALESCE(gender, 'Unknown'), 
    COALESCE(settlement_type, 'Unknown');

CREATE UNIQUE INDEX idx_dm_reg_metrics_base ON dm_registration_metrics_base (campaign_id, country_code, province_code, district_code, spp_code, health_center_code, village_code, age_band, gender, settlement_type);

-- TIER 2B: Household Metrics Base Mart
CREATE MATERIALIZED VIEW dm_household_metrics_base AS
SELECT
    campaign_id, country_code, province_code, district_code, spp_code, health_center_code, village_code,
    COUNT(DISTINCT id) AS total_households_registered
FROM household_enriched
WHERE is_deleted IS NOT TRUE
GROUP BY campaign_id, country_code, province_code, district_code, spp_code, health_center_code, village_code;

CREATE UNIQUE INDEX idx_dm_hh_metrics_base ON dm_household_metrics_base (campaign_id, country_code, province_code, district_code, spp_code, health_center_code, village_code);


-- #############################################################################
-- SECTION 1: KPI — Total Children Enumerated
-- #############################################################################
-- COUNT(enumeration records WHERE age_months <= 59)
-- Grain: one row per boundary_code × campaign_id
-- #############################################################################

-- Country
CREATE MATERIALIZED VIEW dm_children_enumerated_country AS
SELECT country_code, campaign_id, SUM(enumerated_u5_count) AS total_children_enumerated
FROM dm_registration_metrics_base
GROUP BY country_code, campaign_id;

-- Province
CREATE MATERIALIZED VIEW dm_children_enumerated_province AS
SELECT country_code, province_code, campaign_id, SUM(enumerated_u5_count) AS total_children_enumerated
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, campaign_id;

-- District
CREATE MATERIALIZED VIEW dm_children_enumerated_district AS
SELECT country_code, province_code, district_code, campaign_id, SUM(enumerated_u5_count) AS total_children_enumerated
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, campaign_id;

-- SPP
CREATE MATERIALIZED VIEW dm_children_enumerated_spp AS
SELECT country_code, province_code, district_code, spp_code, campaign_id, SUM(enumerated_u5_count) AS total_children_enumerated
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, campaign_id;

-- Health Center
CREATE MATERIALIZED VIEW dm_children_enumerated_health_center AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, campaign_id, SUM(enumerated_u5_count) AS total_children_enumerated
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Village
CREATE MATERIALIZED VIEW dm_children_enumerated_village AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, SUM(enumerated_u5_count) AS total_children_enumerated
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id;

-- Indexes
CREATE UNIQUE INDEX idx_dce_country_pk  ON dm_children_enumerated_country (country_code, campaign_id);
CREATE UNIQUE INDEX idx_dce_province_pk ON dm_children_enumerated_province (country_code, province_code, campaign_id);
CREATE UNIQUE INDEX idx_dce_district_pk ON dm_children_enumerated_district (country_code, province_code, district_code, campaign_id);
CREATE UNIQUE INDEX idx_dce_spp_pk      ON dm_children_enumerated_spp (country_code, province_code, district_code, spp_code, campaign_id);
CREATE UNIQUE INDEX idx_dce_hc_pk       ON dm_children_enumerated_health_center (country_code, province_code, district_code, spp_code, health_center_code, campaign_id);
CREATE UNIQUE INDEX idx_dce_village_pk  ON dm_children_enumerated_village (country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id);


-- #############################################################################
-- SECTION 2: KPI — Total Households Registered
-- #############################################################################
-- COUNT(DISTINCT id) FROM household_enriched GROUP BY campaign_id
-- Source: household_enriched directly (NOT the foundation view)
-- Grain: one row per boundary_code × campaign_id
-- #############################################################################

-- Country
CREATE MATERIALIZED VIEW dm_households_registered_country AS
SELECT country_code, campaign_id, SUM(total_households_registered) AS total_households_registered
FROM dm_household_metrics_base
GROUP BY country_code, campaign_id;

-- Province
CREATE MATERIALIZED VIEW dm_households_registered_province AS
SELECT country_code, province_code, campaign_id, SUM(total_households_registered) AS total_households_registered
FROM dm_household_metrics_base
GROUP BY country_code, province_code, campaign_id;

-- District
CREATE MATERIALIZED VIEW dm_households_registered_district AS
SELECT country_code, province_code, district_code, campaign_id, SUM(total_households_registered) AS total_households_registered
FROM dm_household_metrics_base
GROUP BY country_code, province_code, district_code, campaign_id;

-- SPP
CREATE MATERIALIZED VIEW dm_households_registered_spp AS
SELECT country_code, province_code, district_code, spp_code, campaign_id, SUM(total_households_registered) AS total_households_registered
FROM dm_household_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, campaign_id;

-- Health Center
CREATE MATERIALIZED VIEW dm_households_registered_health_center AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, campaign_id, SUM(total_households_registered) AS total_households_registered
FROM dm_household_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Village
CREATE MATERIALIZED VIEW dm_households_registered_village AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, SUM(total_households_registered) AS total_households_registered
FROM dm_household_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id;

-- Indexes
CREATE UNIQUE INDEX idx_dhr_country_pk  ON dm_households_registered_country (country_code, campaign_id);
CREATE UNIQUE INDEX idx_dhr_province_pk ON dm_households_registered_province (country_code, province_code, campaign_id);
CREATE UNIQUE INDEX idx_dhr_district_pk ON dm_households_registered_district (country_code, province_code, district_code, campaign_id);
CREATE UNIQUE INDEX idx_dhr_spp_pk      ON dm_households_registered_spp (country_code, province_code, district_code, spp_code, campaign_id);
CREATE UNIQUE INDEX idx_dhr_hc_pk       ON dm_households_registered_health_center (country_code, province_code, district_code, spp_code, health_center_code, campaign_id);
CREATE UNIQUE INDEX idx_dhr_village_pk  ON dm_households_registered_village (country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id);


-- #############################################################################
-- SECTION 3: KPI — Children by Age Band
-- #############################################################################
-- COUNT(enumeration records) grouped by age_band (0-11m, 12-23m, 24-59m)
-- Grain: one row per boundary_code × campaign_id × age_band
-- #############################################################################

-- Country
CREATE MATERIALIZED VIEW dm_children_age_band_country AS
SELECT country_code, campaign_id, age_band, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
WHERE age_band != \'Other\'
GROUP BY country_code, campaign_id, age_band;

-- Province
CREATE MATERIALIZED VIEW dm_children_age_band_province AS
SELECT country_code, province_code, campaign_id, age_band, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
WHERE age_band != \'Other\'
GROUP BY country_code, province_code, campaign_id, age_band;

-- District
CREATE MATERIALIZED VIEW dm_children_age_band_district AS
SELECT country_code, province_code, district_code, campaign_id, age_band, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
WHERE age_band != \'Other\'
GROUP BY country_code, province_code, district_code, campaign_id, age_band;

-- SPP
CREATE MATERIALIZED VIEW dm_children_age_band_spp AS
SELECT country_code, province_code, district_code, spp_code, campaign_id, age_band, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
WHERE age_band != \'Other\'
GROUP BY country_code, province_code, district_code, spp_code, campaign_id, age_band;

-- Health Center
CREATE MATERIALIZED VIEW dm_children_age_band_health_center AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, campaign_id, age_band, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
WHERE age_band != \'Other\'
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id, age_band;

-- Village
CREATE MATERIALIZED VIEW dm_children_age_band_village AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, age_band, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
WHERE age_band != \'Other\'
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, age_band;

-- Indexes
CREATE UNIQUE INDEX idx_dcab_country_pk  ON dm_children_age_band_country (country_code, campaign_id, age_band);
CREATE UNIQUE INDEX idx_dcab_province_pk ON dm_children_age_band_province (country_code, province_code, campaign_id, age_band);
CREATE UNIQUE INDEX idx_dcab_district_pk ON dm_children_age_band_district (country_code, province_code, district_code, campaign_id, age_band);
CREATE UNIQUE INDEX idx_dcab_spp_pk      ON dm_children_age_band_spp (country_code, province_code, district_code, spp_code, campaign_id, age_band);
CREATE UNIQUE INDEX idx_dcab_hc_pk       ON dm_children_age_band_health_center (country_code, province_code, district_code, spp_code, health_center_code, campaign_id, age_band);
CREATE UNIQUE INDEX idx_dcab_village_pk  ON dm_children_age_band_village (country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, age_band);


-- #############################################################################
-- SECTION 4: KPI — Gender Breakdown
-- #############################################################################
-- COUNT(enumeration records) grouped by gender
-- Grain: one row per boundary_code × campaign_id × gender
-- #############################################################################

-- Country
CREATE MATERIALIZED VIEW dm_gender_breakdown_country AS
SELECT country_code, campaign_id, gender, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
GROUP BY country_code, campaign_id, gender;

-- Province
CREATE MATERIALIZED VIEW dm_gender_breakdown_province AS
SELECT country_code, province_code, campaign_id, gender, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, campaign_id, gender;

-- District
CREATE MATERIALIZED VIEW dm_gender_breakdown_district AS
SELECT country_code, province_code, district_code, campaign_id, gender, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, campaign_id, gender;

-- SPP
CREATE MATERIALIZED VIEW dm_gender_breakdown_spp AS
SELECT country_code, province_code, district_code, spp_code, campaign_id, gender, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, campaign_id, gender;

-- Health Center
CREATE MATERIALIZED VIEW dm_gender_breakdown_health_center AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, campaign_id, gender, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id, gender;

-- Village
CREATE MATERIALIZED VIEW dm_gender_breakdown_village AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, gender, SUM(enumerated_u5_count) AS children_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, gender;

-- Indexes
CREATE UNIQUE INDEX idx_dgb_country_pk  ON dm_gender_breakdown_country (country_code, campaign_id, gender);
CREATE UNIQUE INDEX idx_dgb_province_pk ON dm_gender_breakdown_province (country_code, province_code, campaign_id, gender);
CREATE UNIQUE INDEX idx_dgb_district_pk ON dm_gender_breakdown_district (country_code, province_code, district_code, campaign_id, gender);
CREATE UNIQUE INDEX idx_dgb_spp_pk      ON dm_gender_breakdown_spp (country_code, province_code, district_code, spp_code, campaign_id, gender);
CREATE UNIQUE INDEX idx_dgb_hc_pk       ON dm_gender_breakdown_health_center (country_code, province_code, district_code, spp_code, health_center_code, campaign_id, gender);
CREATE UNIQUE INDEX idx_dgb_village_pk  ON dm_gender_breakdown_village (country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, gender);


-- #############################################################################
-- SECTION 5: KPI — Zero-dose children identified
-- #############################################################################
-- COUNT(enumeration records where received_opv_before = No AND task_age_months > 0.5)
-- Grain: one row per boundary_code × campaign_id
-- #############################################################################

-- Country
CREATE MATERIALIZED VIEW dm_zero_dose_children_country AS
SELECT country_code, campaign_id, SUM(zero_dose_count) AS zero_dose_count
FROM dm_registration_metrics_base
GROUP BY country_code, campaign_id;

-- Province
CREATE MATERIALIZED VIEW dm_zero_dose_children_province AS
SELECT country_code, province_code, campaign_id, SUM(zero_dose_count) AS zero_dose_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, campaign_id;

-- District
CREATE MATERIALIZED VIEW dm_zero_dose_children_district AS
SELECT country_code, province_code, district_code, campaign_id, SUM(zero_dose_count) AS zero_dose_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, campaign_id;

-- SPP
CREATE MATERIALIZED VIEW dm_zero_dose_children_spp AS
SELECT country_code, province_code, district_code, spp_code, campaign_id, SUM(zero_dose_count) AS zero_dose_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, campaign_id;

-- Health Center
CREATE MATERIALIZED VIEW dm_zero_dose_children_health_center AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, campaign_id, SUM(zero_dose_count) AS zero_dose_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Village
CREATE MATERIALIZED VIEW dm_zero_dose_children_village AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, SUM(zero_dose_count) AS zero_dose_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id;

-- Indexes
CREATE UNIQUE INDEX idx_dzdc_country_pk  ON dm_zero_dose_children_country (country_code, campaign_id);
CREATE UNIQUE INDEX idx_dzdc_province_pk ON dm_zero_dose_children_province (country_code, province_code, campaign_id);
CREATE UNIQUE INDEX idx_dzdc_district_pk ON dm_zero_dose_children_district (country_code, province_code, district_code, campaign_id);
CREATE UNIQUE INDEX idx_dzdc_spp_pk      ON dm_zero_dose_children_spp (country_code, province_code, district_code, spp_code, campaign_id);
CREATE UNIQUE INDEX idx_dzdc_hc_pk       ON dm_zero_dose_children_health_center (country_code, province_code, district_code, spp_code, health_center_code, campaign_id);
CREATE UNIQUE INDEX idx_dzdc_village_pk  ON dm_zero_dose_children_village (country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id);


-- #############################################################################
-- SECTION 6: KPI — Guest Member Count
-- #############################################################################
-- COUNT(enumeration records WHERE guest_member = 'Yes')
-- guest_member from: additional_details->>'guestMember'
-- Grain: one row per boundary_code × campaign_id
-- #############################################################################

-- Country
CREATE MATERIALIZED VIEW dm_guest_member_country AS
SELECT country_code, campaign_id, SUM(guest_member_count) AS guest_member_count
FROM dm_registration_metrics_base
GROUP BY country_code, campaign_id;

-- Province
CREATE MATERIALIZED VIEW dm_guest_member_province AS
SELECT country_code, province_code, campaign_id, SUM(guest_member_count) AS guest_member_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, campaign_id;

-- District
CREATE MATERIALIZED VIEW dm_guest_member_district AS
SELECT country_code, province_code, district_code, campaign_id, SUM(guest_member_count) AS guest_member_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, campaign_id;

-- SPP
CREATE MATERIALIZED VIEW dm_guest_member_spp AS
SELECT country_code, province_code, district_code, spp_code, campaign_id, SUM(guest_member_count) AS guest_member_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, campaign_id;

-- Health Center
CREATE MATERIALIZED VIEW dm_guest_member_health_center AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, campaign_id, SUM(guest_member_count) AS guest_member_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Village
CREATE MATERIALIZED VIEW dm_guest_member_village AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, SUM(guest_member_count) AS guest_member_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id;

-- Indexes
CREATE UNIQUE INDEX idx_dgm_country_pk  ON dm_guest_member_country (country_code, campaign_id);
CREATE UNIQUE INDEX idx_dgm_province_pk ON dm_guest_member_province (country_code, province_code, campaign_id);
CREATE UNIQUE INDEX idx_dgm_district_pk ON dm_guest_member_district (country_code, province_code, district_code, campaign_id);
CREATE UNIQUE INDEX idx_dgm_spp_pk      ON dm_guest_member_spp (country_code, province_code, district_code, spp_code, campaign_id);
CREATE UNIQUE INDEX idx_dgm_hc_pk       ON dm_guest_member_health_center (country_code, province_code, district_code, spp_code, health_center_code, campaign_id);
CREATE UNIQUE INDEX idx_dgm_village_pk  ON dm_guest_member_village (country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id);


-- #############################################################################
-- SECTION 7: KPI — Enumerated but Not Yet Vaccinated (Health Facilities)
-- #############################################################################
-- Health facilities with enumeration records AND zero delivery records
-- SINGLE LEVEL ONLY: health_center_code
-- Parent hierarchy codes included for filtering/drill-down
-- Grain: one row per health_center_code × campaign_id
-- #############################################################################

CREATE MATERIALIZED VIEW dm_enumerated_not_vaccinated_health_center AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, campaign_id,
    SUM(enumerated_u5_count) AS enumeration_count,
    SUM(delivered_u5_count) AS delivered_count
FROM dm_registration_metrics_base
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Index
CREATE UNIQUE INDEX idx_denv_hc_pk ON dm_enumerated_not_vaccinated_health_center (country_code, province_code, district_code, spp_code, health_center_code, campaign_id);


-- #############################################################################
-- SECTION 8: KPI — Coverage by Settlement Type
-- #############################################################################
-- (Children vaccinated / Children enumerated) grouped by settlement_type
-- Grain: one row per boundary_code × campaign_id × settlement_type
-- #############################################################################

-- Country
CREATE MATERIALIZED VIEW dm_coverage_settlement_country AS
SELECT country_code, campaign_id, settlement_type,
    SUM(enumerated_u5_count) AS enumerated_count,
    SUM(vaccinated_u5_count) AS vaccinated_count,
    ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS coverage_pct
FROM dm_registration_metrics_base
WHERE settlement_type != \'Unknown\'
GROUP BY country_code, campaign_id, settlement_type;

-- Province
CREATE MATERIALIZED VIEW dm_coverage_settlement_province AS
SELECT country_code, province_code, campaign_id, settlement_type,
    SUM(enumerated_u5_count) AS enumerated_count,
    SUM(vaccinated_u5_count) AS vaccinated_count,
    ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS coverage_pct
FROM dm_registration_metrics_base
WHERE settlement_type != \'Unknown\'
GROUP BY country_code, province_code, campaign_id, settlement_type;

-- District
CREATE MATERIALIZED VIEW dm_coverage_settlement_district AS
SELECT country_code, province_code, district_code, campaign_id, settlement_type,
    SUM(enumerated_u5_count) AS enumerated_count,
    SUM(vaccinated_u5_count) AS vaccinated_count,
    ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS coverage_pct
FROM dm_registration_metrics_base
WHERE settlement_type != \'Unknown\'
GROUP BY country_code, province_code, district_code, campaign_id, settlement_type;

-- SPP
CREATE MATERIALIZED VIEW dm_coverage_settlement_spp AS
SELECT country_code, province_code, district_code, spp_code, campaign_id, settlement_type,
    SUM(enumerated_u5_count) AS enumerated_count,
    SUM(vaccinated_u5_count) AS vaccinated_count,
    ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS coverage_pct
FROM dm_registration_metrics_base
WHERE settlement_type != \'Unknown\'
GROUP BY country_code, province_code, district_code, spp_code, campaign_id, settlement_type;

-- Health Center
CREATE MATERIALIZED VIEW dm_coverage_settlement_health_center AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, campaign_id, settlement_type,
    SUM(enumerated_u5_count) AS enumerated_count,
    SUM(vaccinated_u5_count) AS vaccinated_count,
    ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS coverage_pct
FROM dm_registration_metrics_base
WHERE settlement_type != \'Unknown\'
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id, settlement_type;

-- Village
CREATE MATERIALIZED VIEW dm_coverage_settlement_village AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, settlement_type,
    SUM(enumerated_u5_count) AS enumerated_count,
    SUM(vaccinated_u5_count) AS vaccinated_count,
    ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS coverage_pct
FROM dm_registration_metrics_base
WHERE settlement_type != \'Unknown\'
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, settlement_type;

-- Indexes
CREATE UNIQUE INDEX idx_dcs_country_pk  ON dm_coverage_settlement_country (country_code, campaign_id, settlement_type);
CREATE UNIQUE INDEX idx_dcs_province_pk ON dm_coverage_settlement_province (country_code, province_code, campaign_id, settlement_type);
CREATE UNIQUE INDEX idx_dcs_district_pk ON dm_coverage_settlement_district (country_code, province_code, district_code, campaign_id, settlement_type);
CREATE UNIQUE INDEX idx_dcs_spp_pk      ON dm_coverage_settlement_spp (country_code, province_code, district_code, spp_code, campaign_id, settlement_type);
CREATE UNIQUE INDEX idx_dcs_hc_pk       ON dm_coverage_settlement_health_center (country_code, province_code, district_code, spp_code, health_center_code, campaign_id, settlement_type);
CREATE UNIQUE INDEX idx_dcs_village_pk  ON dm_coverage_settlement_village (country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id, settlement_type);


-- #############################################################################
-- SECTION 9: KPI — Hard-to-Reach Vaccination Rate
-- #############################################################################
-- Filtered from dm_coverage_settlement_* WHERE settlement_type IN
-- ('Hard to Reach', 'Nomads', 'Refugees')
-- Grain: one row per boundary_code × campaign_id (pre-aggregated across HTR types)
-- #############################################################################

-- Country
CREATE MATERIALIZED VIEW dm_htr_vaccination_country AS
SELECT country_code, campaign_id,
    SUM(htr_enumerated_count) AS htr_enumerated_count,
    SUM(htr_vaccinated_count) AS htr_vaccinated_count,
    ROUND(SUM(htr_vaccinated_count)::NUMERIC / NULLIF(SUM(htr_enumerated_count), 0) * 100, 2) AS htr_vaccination_rate
FROM dm_coverage_settlement_country
WHERE settlement_type IN (\'Hard to Reach\', \'Nomads\', \'Refugees\')
GROUP BY country_code, campaign_id;

-- Province
CREATE MATERIALIZED VIEW dm_htr_vaccination_province AS
SELECT country_code, province_code, campaign_id,
    SUM(htr_enumerated_count) AS htr_enumerated_count,
    SUM(htr_vaccinated_count) AS htr_vaccinated_count,
    ROUND(SUM(htr_vaccinated_count)::NUMERIC / NULLIF(SUM(htr_enumerated_count), 0) * 100, 2) AS htr_vaccination_rate
FROM dm_coverage_settlement_province
WHERE settlement_type IN (\'Hard to Reach\', \'Nomads\', \'Refugees\')
GROUP BY country_code, province_code, campaign_id;

-- District
CREATE MATERIALIZED VIEW dm_htr_vaccination_district AS
SELECT country_code, province_code, district_code, campaign_id,
    SUM(htr_enumerated_count) AS htr_enumerated_count,
    SUM(htr_vaccinated_count) AS htr_vaccinated_count,
    ROUND(SUM(htr_vaccinated_count)::NUMERIC / NULLIF(SUM(htr_enumerated_count), 0) * 100, 2) AS htr_vaccination_rate
FROM dm_coverage_settlement_district
WHERE settlement_type IN (\'Hard to Reach\', \'Nomads\', \'Refugees\')
GROUP BY country_code, province_code, district_code, campaign_id;

-- SPP
CREATE MATERIALIZED VIEW dm_htr_vaccination_spp AS
SELECT country_code, province_code, district_code, spp_code, campaign_id,
    SUM(htr_enumerated_count) AS htr_enumerated_count,
    SUM(htr_vaccinated_count) AS htr_vaccinated_count,
    ROUND(SUM(htr_vaccinated_count)::NUMERIC / NULLIF(SUM(htr_enumerated_count), 0) * 100, 2) AS htr_vaccination_rate
FROM dm_coverage_settlement_spp
WHERE settlement_type IN (\'Hard to Reach\', \'Nomads\', \'Refugees\')
GROUP BY country_code, province_code, district_code, spp_code, campaign_id;

-- Health Center
CREATE MATERIALIZED VIEW dm_htr_vaccination_health_center AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, campaign_id,
    SUM(htr_enumerated_count) AS htr_enumerated_count,
    SUM(htr_vaccinated_count) AS htr_vaccinated_count,
    ROUND(SUM(htr_vaccinated_count)::NUMERIC / NULLIF(SUM(htr_enumerated_count), 0) * 100, 2) AS htr_vaccination_rate
FROM dm_coverage_settlement_health_center
WHERE settlement_type IN (\'Hard to Reach\', \'Nomads\', \'Refugees\')
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Village
CREATE MATERIALIZED VIEW dm_htr_vaccination_village AS
SELECT country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id,
    SUM(htr_enumerated_count) AS htr_enumerated_count,
    SUM(htr_vaccinated_count) AS htr_vaccinated_count,
    ROUND(SUM(htr_vaccinated_count)::NUMERIC / NULLIF(SUM(htr_enumerated_count), 0) * 100, 2) AS htr_vaccination_rate
FROM dm_coverage_settlement_village
WHERE settlement_type IN (\'Hard to Reach\', \'Nomads\', \'Refugees\')
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id;

-- Indexes
CREATE UNIQUE INDEX idx_dhtr_country_pk  ON dm_htr_vaccination_country (country_code, campaign_id);
CREATE UNIQUE INDEX idx_dhtr_province_pk ON dm_htr_vaccination_province (country_code, province_code, campaign_id);
CREATE UNIQUE INDEX idx_dhtr_district_pk ON dm_htr_vaccination_district (country_code, province_code, district_code, campaign_id);
CREATE UNIQUE INDEX idx_dhtr_spp_pk      ON dm_htr_vaccination_spp (country_code, province_code, district_code, spp_code, campaign_id);
CREATE UNIQUE INDEX idx_dhtr_hc_pk       ON dm_htr_vaccination_health_center (country_code, province_code, district_code, spp_code, health_center_code, campaign_id);
CREATE UNIQUE INDEX idx_dhtr_village_pk  ON dm_htr_vaccination_village (country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id);


-- #############################################################################
-- SECTION 10: REFRESH SCRIPT (run periodically, respects dependency DAG)
-- #############################################################################
-- Refresh Order:
--   Step 1: Foundation view (all downstream depend on this)
--   Step 2: All independent data marts (can run in parallel)
--   Step 3: Enumerated-not-vaccinated (depends on foundation)
--   Step 4: HTR marts (depend on coverage settlement marts from step 2)
-- #############################################################################

-- Step 1: Foundation view
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_registration_beneficiary_base;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_registration_metrics_base;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_household_metrics_base;

-- Step 2: Independent data marts (can run in parallel)
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_enumerated_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_enumerated_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_enumerated_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_enumerated_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_enumerated_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_enumerated_village;

REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_age_band_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_age_band_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_age_band_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_age_band_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_age_band_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_children_age_band_village;

REFRESH MATERIALIZED VIEW CONCURRENTLY dm_gender_breakdown_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_gender_breakdown_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_gender_breakdown_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_gender_breakdown_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_gender_breakdown_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_gender_breakdown_village;

REFRESH MATERIALIZED VIEW CONCURRENTLY dm_zero_dose_children_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_zero_dose_children_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_zero_dose_children_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_zero_dose_children_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_zero_dose_children_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_zero_dose_children_village;

REFRESH MATERIALIZED VIEW CONCURRENTLY dm_guest_member_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_guest_member_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_guest_member_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_guest_member_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_guest_member_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_guest_member_village;

REFRESH MATERIALIZED VIEW CONCURRENTLY dm_households_registered_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_households_registered_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_households_registered_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_households_registered_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_households_registered_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_households_registered_village;

REFRESH MATERIALIZED VIEW CONCURRENTLY dm_coverage_settlement_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_coverage_settlement_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_coverage_settlement_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_coverage_settlement_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_coverage_settlement_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_coverage_settlement_village;

-- Step 3: Enumerated-not-vaccinated (depends on foundation view)
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_enumerated_not_vaccinated_health_center;

-- Step 4: HTR marts (depend on coverage settlement marts from step 2)
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_htr_vaccination_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_htr_vaccination_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_htr_vaccination_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_htr_vaccination_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_htr_vaccination_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_htr_vaccination_village;


-- #############################################################################
-- SECTION 11: EXAMPLE KPI RETRIEVAL QUERIES (for dashboard consumption)
-- #############################################################################

-- KPI 1: Total Children Enumerated — national
SELECT campaign_id, total_children_enumerated
FROM dm_children_enumerated_country
WHERE country_code = :country_code;

-- KPI 1: Total Children Enumerated — province drill-down
SELECT province_code, campaign_id, total_children_enumerated
FROM dm_children_enumerated_province
WHERE country_code = :country_code;

-- KPI 2: Total Households Registered — national per campaign
SELECT campaign_id, total_households_registered
FROM dm_households_registered_country
WHERE country_code = :country_code;

-- KPI 2: Total Households Registered — province drill-down
SELECT province_code, campaign_id, total_households_registered
FROM dm_households_registered_province
WHERE country_code = :country_code;

-- KPI 3: Children by Age Band — stacked bar chart
SELECT age_band, SUM(children_count) AS children_count
FROM dm_children_age_band_country
WHERE country_code = :country_code
  AND campaign_id = :campaign_id
GROUP BY age_band
ORDER BY age_band;

-- KPI 4: Gender Breakdown — donut chart
SELECT gender, SUM(children_count) AS children_count
FROM dm_gender_breakdown_country
WHERE country_code = :country_code
  AND campaign_id = :campaign_id
GROUP BY gender;

-- KPI 5: Zero-dose children identified — national
SELECT campaign_id, zero_dose_count
FROM dm_zero_dose_children_country
WHERE country_code = :country_code;

-- KPI 6: Guest Member Count — national
SELECT campaign_id, guest_member_count
FROM dm_guest_member_country
WHERE country_code = :country_code;

-- KPI 7: Enumerated but Not Yet Vaccinated — table of health facilities
SELECT
    health_center_code,
    country_code,
    province_code,
    district_code,
    spp_code,
    campaign_id,
    enumeration_count,
    delivered_count
FROM dm_enumerated_not_vaccinated_health_center
WHERE delivered_count = 0
  AND enumeration_count > 0
  AND campaign_id = :campaign_id
ORDER BY enumeration_count DESC;

-- KPI 7: Filtered by province
SELECT health_center_code, enumeration_count, delivered_count
FROM dm_enumerated_not_vaccinated_health_center
WHERE delivered_count = 0
  AND enumeration_count > 0
  AND province_code = :province_code
  AND campaign_id = :campaign_id
ORDER BY enumeration_count DESC;

-- KPI 8: Coverage by Settlement Type — bar chart
SELECT
    settlement_type,
    SUM(enumerated_count)  AS enumerated_count,
    SUM(vaccinated_count)  AS vaccinated_count,
    ROUND(SUM(vaccinated_count)::NUMERIC / NULLIF(SUM(enumerated_count), 0) * 100, 2) AS coverage_pct
FROM dm_coverage_settlement_country
WHERE country_code = :country_code
  AND campaign_id = :campaign_id
GROUP BY settlement_type
ORDER BY settlement_type;

-- KPI 9: Hard-to-Reach Vaccination Rate — KPI card
SELECT
    campaign_id,
    htr_enumerated_count,
    htr_vaccinated_count,
    htr_vaccination_rate
FROM dm_htr_vaccination_country
WHERE country_code = :country_code
  AND campaign_id = :campaign_id;

-- KPI 9: Hard-to-Reach — bar chart by settlement type
SELECT
    settlement_type,
    enumerated_count,
    vaccinated_count,
    coverage_pct
FROM dm_coverage_settlement_country
WHERE country_code = :country_code
  AND campaign_id = :campaign_id
  AND settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees')
ORDER BY settlement_type;

-- KPI 9: Hard-to-Reach — province drill-down
SELECT province_code, htr_vaccination_rate, htr_vaccinated_count, htr_enumerated_count
FROM dm_htr_vaccination_province
WHERE country_code = :country_code
  AND campaign_id = :campaign_id
ORDER BY htr_vaccination_rate DESC;
