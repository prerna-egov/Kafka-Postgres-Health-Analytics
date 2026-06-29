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

CREATE MATERIALIZED VIEW v_beneficiary_task_joined AS
SELECT
    -- Beneficiary identifiers
    pb.beneficiary_id                                       AS enumeration_record_id,
    pb.client_reference_id                                  AS pb_client_reference_id,
    pb.project_id,
    pb.campaign_id,

    -- Extracted beneficiary attributes (from JSONB)
    (pb.beneficiary_additional_fields->>'ageMonths')::NUMERIC   AS age_months,
    pb.beneficiary_additional_fields->>'settlementType'         AS settlement_type,
    pb.additional_details->>'gender'                            AS gender,
    pb.additional_details->>'guestMember'                       AS guest_member,

    -- Boundary hierarchy from beneficiary
    pb.country_code,
    pb.province_code,
    pb.district_code,
    pb.spp_code,
    pb.health_center_code,
    pb.village_code,

    -- Task (vaccination/delivery) attributes
    pt.id                                                       AS task_id,
    pt.administration_status,
    CASE WHEN pt.administration_status = 'SUCCESS' THEN 1 ELSE 0 END AS is_vaccinated,
    
    pt.additional_details->>'receivedOPVBefore'                 AS received_opv_before,
    NULLIF(TRIM(pt.additional_details->>'ageInMonths'), '')::NUMERIC AS task_age_months,

    -- Delivery flag
    CASE WHEN pt.id IS NOT NULL AND pt.administration_status = 'SUCCESS'
         THEN 1 ELSE 0 END                                     AS has_delivery_record

FROM project_beneficiary_enriched pb
LEFT JOIN project_task_enriched pt
    ON pt.project_beneficiary_client_reference_id = pb.client_reference_id
WHERE pb.is_deleted IS NOT TRUE;

-- Foundation view indexes
CREATE INDEX idx_vbtj_country       ON v_beneficiary_task_joined (country_code);
CREATE INDEX idx_vbtj_province      ON v_beneficiary_task_joined (province_code);
CREATE INDEX idx_vbtj_district      ON v_beneficiary_task_joined (district_code);
CREATE INDEX idx_vbtj_spp           ON v_beneficiary_task_joined (spp_code);
CREATE INDEX idx_vbtj_health        ON v_beneficiary_task_joined (health_center_code);
CREATE INDEX idx_vbtj_village       ON v_beneficiary_task_joined (village_code);
CREATE UNIQUE INDEX idx_vbtj_pk     ON v_beneficiary_task_joined (enumeration_record_id, COALESCE(task_id, 'NONE'));


-- #############################################################################
-- SECTION 1: KPI — Total Children Enumerated
-- #############################################################################
-- COUNT(enumeration records WHERE age_months <= 59)
-- Grain: one row per boundary_code × campaign_id
-- #############################################################################

-- Country
CREATE MATERIALIZED VIEW dm_children_enumerated_country AS
SELECT
    country_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS total_children_enumerated
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, campaign_id;

-- Province
CREATE MATERIALIZED VIEW dm_children_enumerated_province AS
SELECT
    country_code,
    province_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS total_children_enumerated
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, campaign_id;

-- District
CREATE MATERIALIZED VIEW dm_children_enumerated_district AS
SELECT
    country_code,
    province_code,
    district_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS total_children_enumerated
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, district_code, campaign_id;

-- SPP
CREATE MATERIALIZED VIEW dm_children_enumerated_spp AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS total_children_enumerated
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, district_code, spp_code, campaign_id;

-- Health Center
CREATE MATERIALIZED VIEW dm_children_enumerated_health_center AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS total_children_enumerated
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Village
CREATE MATERIALIZED VIEW dm_children_enumerated_village AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    village_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS total_children_enumerated
FROM v_beneficiary_task_joined
WHERE age_months <= 59
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
SELECT
    country_code,
    campaign_id,
    COUNT(DISTINCT id) AS total_households_registered
FROM household_enriched
WHERE is_deleted IS NOT TRUE
GROUP BY country_code, campaign_id;

-- Province
CREATE MATERIALIZED VIEW dm_households_registered_province AS
SELECT
    country_code,
    province_code,
    campaign_id,
    COUNT(DISTINCT id) AS total_households_registered
FROM household_enriched
WHERE is_deleted IS NOT TRUE
GROUP BY country_code, province_code, campaign_id;

-- District
CREATE MATERIALIZED VIEW dm_households_registered_district AS
SELECT
    country_code,
    province_code,
    district_code,
    campaign_id,
    COUNT(DISTINCT id) AS total_households_registered
FROM household_enriched
WHERE is_deleted IS NOT TRUE
GROUP BY country_code, province_code, district_code, campaign_id;

-- SPP
CREATE MATERIALIZED VIEW dm_households_registered_spp AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    campaign_id,
    COUNT(DISTINCT id) AS total_households_registered
FROM household_enriched
WHERE is_deleted IS NOT TRUE
GROUP BY country_code, province_code, district_code, spp_code, campaign_id;

-- Health Center
CREATE MATERIALIZED VIEW dm_households_registered_health_center AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    campaign_id,
    COUNT(DISTINCT id) AS total_households_registered
FROM household_enriched
WHERE is_deleted IS NOT TRUE
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Village
CREATE MATERIALIZED VIEW dm_households_registered_village AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    village_code,
    campaign_id,
    COUNT(DISTINCT id) AS total_households_registered
FROM household_enriched
WHERE is_deleted IS NOT TRUE
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
SELECT
    country_code,
    campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END AS age_band,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END;

-- Province
CREATE MATERIALIZED VIEW dm_children_age_band_province AS
SELECT
    country_code,
    province_code,
    campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END AS age_band,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END;

-- District
CREATE MATERIALIZED VIEW dm_children_age_band_district AS
SELECT
    country_code,
    province_code,
    district_code,
    campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END AS age_band,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, district_code, campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END;

-- SPP
CREATE MATERIALIZED VIEW dm_children_age_band_spp AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END AS age_band,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, district_code, spp_code, campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END;

-- Health Center
CREATE MATERIALIZED VIEW dm_children_age_band_health_center AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END AS age_band,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END;

-- Village
CREATE MATERIALIZED VIEW dm_children_age_band_village AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    village_code,
    campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END AS age_band,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, village_code, campaign_id,
    CASE
        WHEN age_months BETWEEN 0  AND 11 THEN '0-11m'
        WHEN age_months BETWEEN 12 AND 23 THEN '12-23m'
        WHEN age_months BETWEEN 24 AND 59 THEN '24-59m'
    END;

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
SELECT
    country_code,
    campaign_id,
    gender,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, campaign_id, gender;

-- Province
CREATE MATERIALIZED VIEW dm_gender_breakdown_province AS
SELECT
    country_code,
    province_code,
    campaign_id,
    gender,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, campaign_id, gender;

-- District
CREATE MATERIALIZED VIEW dm_gender_breakdown_district AS
SELECT
    country_code,
    province_code,
    district_code,
    campaign_id,
    gender,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, district_code, campaign_id, gender;

-- SPP
CREATE MATERIALIZED VIEW dm_gender_breakdown_spp AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    campaign_id,
    gender,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, district_code, spp_code, campaign_id, gender;

-- Health Center
CREATE MATERIALIZED VIEW dm_gender_breakdown_health_center AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    campaign_id,
    gender,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id, gender;

-- Village
CREATE MATERIALIZED VIEW dm_gender_breakdown_village AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    village_code,
    campaign_id,
    gender,
    COUNT(DISTINCT enumeration_record_id) AS children_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
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
SELECT
    country_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS zero_dose_count
FROM v_beneficiary_task_joined
WHERE LOWER(received_opv_before) = 'no' 
  AND task_age_months > 0.5
GROUP BY country_code, campaign_id;

-- Province
CREATE MATERIALIZED VIEW dm_zero_dose_children_province AS
SELECT
    country_code,
    province_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS zero_dose_count
FROM v_beneficiary_task_joined
WHERE LOWER(received_opv_before) = 'no' 
  AND task_age_months > 0.5
GROUP BY country_code, province_code, campaign_id;

-- District
CREATE MATERIALIZED VIEW dm_zero_dose_children_district AS
SELECT
    country_code,
    province_code,
    district_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS zero_dose_count
FROM v_beneficiary_task_joined
WHERE LOWER(received_opv_before) = 'no' 
  AND task_age_months > 0.5
GROUP BY country_code, province_code, district_code, campaign_id;

-- SPP
CREATE MATERIALIZED VIEW dm_zero_dose_children_spp AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS zero_dose_count
FROM v_beneficiary_task_joined
WHERE LOWER(received_opv_before) = 'no' 
  AND task_age_months > 0.5
GROUP BY country_code, province_code, district_code, spp_code, campaign_id;

-- Health Center
CREATE MATERIALIZED VIEW dm_zero_dose_children_health_center AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS zero_dose_count
FROM v_beneficiary_task_joined
WHERE LOWER(received_opv_before) = 'no' 
  AND task_age_months > 0.5
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Village
CREATE MATERIALIZED VIEW dm_zero_dose_children_village AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    village_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS zero_dose_count
FROM v_beneficiary_task_joined
WHERE LOWER(received_opv_before) = 'no' 
  AND task_age_months > 0.5
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
SELECT
    country_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS guest_member_count
FROM v_beneficiary_task_joined
WHERE LOWER(guest_member) = 'yes'
GROUP BY country_code, campaign_id;

-- Province
CREATE MATERIALIZED VIEW dm_guest_member_province AS
SELECT
    country_code,
    province_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS guest_member_count
FROM v_beneficiary_task_joined
WHERE LOWER(guest_member) = 'yes'
GROUP BY country_code, province_code, campaign_id;

-- District
CREATE MATERIALIZED VIEW dm_guest_member_district AS
SELECT
    country_code,
    province_code,
    district_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS guest_member_count
FROM v_beneficiary_task_joined
WHERE LOWER(guest_member) = 'yes'
GROUP BY country_code, province_code, district_code, campaign_id;

-- SPP
CREATE MATERIALIZED VIEW dm_guest_member_spp AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS guest_member_count
FROM v_beneficiary_task_joined
WHERE LOWER(guest_member) = 'yes'
GROUP BY country_code, province_code, district_code, spp_code, campaign_id;

-- Health Center
CREATE MATERIALIZED VIEW dm_guest_member_health_center AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS guest_member_count
FROM v_beneficiary_task_joined
WHERE LOWER(guest_member) = 'yes'
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Village
CREATE MATERIALIZED VIEW dm_guest_member_village AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    village_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id) AS guest_member_count
FROM v_beneficiary_task_joined
WHERE LOWER(guest_member) = 'yes'
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
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    campaign_id,
    COUNT(DISTINCT enumeration_record_id)                               AS enumeration_count,
    COUNT(DISTINCT CASE WHEN has_delivery_record = 1
                        THEN enumeration_record_id END)                 AS delivered_count
FROM v_beneficiary_task_joined
WHERE age_months <= 59
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
SELECT
    country_code,
    campaign_id,
    settlement_type,
    COUNT(DISTINCT enumeration_record_id) AS enumerated_count,
    COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END) AS vaccinated_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END)::NUMERIC
        / NULLIF(COUNT(DISTINCT enumeration_record_id), 0) * 100, 2
    ) AS coverage_pct
FROM v_beneficiary_task_joined
WHERE age_months <= 59
  AND settlement_type IS NOT NULL
GROUP BY country_code, campaign_id, settlement_type;

-- Province
CREATE MATERIALIZED VIEW dm_coverage_settlement_province AS
SELECT
    country_code,
    province_code,
    campaign_id,
    settlement_type,
    COUNT(DISTINCT enumeration_record_id) AS enumerated_count,
    COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END) AS vaccinated_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END)::NUMERIC
        / NULLIF(COUNT(DISTINCT enumeration_record_id), 0) * 100, 2
    ) AS coverage_pct
FROM v_beneficiary_task_joined
WHERE age_months <= 59
  AND settlement_type IS NOT NULL
GROUP BY country_code, province_code, campaign_id, settlement_type;

-- District
CREATE MATERIALIZED VIEW dm_coverage_settlement_district AS
SELECT
    country_code,
    province_code,
    district_code,
    campaign_id,
    settlement_type,
    COUNT(DISTINCT enumeration_record_id) AS enumerated_count,
    COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END) AS vaccinated_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END)::NUMERIC
        / NULLIF(COUNT(DISTINCT enumeration_record_id), 0) * 100, 2
    ) AS coverage_pct
FROM v_beneficiary_task_joined
WHERE age_months <= 59
  AND settlement_type IS NOT NULL
GROUP BY country_code, province_code, district_code, campaign_id, settlement_type;

-- SPP
CREATE MATERIALIZED VIEW dm_coverage_settlement_spp AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    campaign_id,
    settlement_type,
    COUNT(DISTINCT enumeration_record_id) AS enumerated_count,
    COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END) AS vaccinated_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END)::NUMERIC
        / NULLIF(COUNT(DISTINCT enumeration_record_id), 0) * 100, 2
    ) AS coverage_pct
FROM v_beneficiary_task_joined
WHERE age_months <= 59
  AND settlement_type IS NOT NULL
GROUP BY country_code, province_code, district_code, spp_code, campaign_id, settlement_type;

-- Health Center
CREATE MATERIALIZED VIEW dm_coverage_settlement_health_center AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    campaign_id,
    settlement_type,
    COUNT(DISTINCT enumeration_record_id) AS enumerated_count,
    COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END) AS vaccinated_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END)::NUMERIC
        / NULLIF(COUNT(DISTINCT enumeration_record_id), 0) * 100, 2
    ) AS coverage_pct
FROM v_beneficiary_task_joined
WHERE age_months <= 59
  AND settlement_type IS NOT NULL
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id, settlement_type;

-- Village
CREATE MATERIALIZED VIEW dm_coverage_settlement_village AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    village_code,
    campaign_id,
    settlement_type,
    COUNT(DISTINCT enumeration_record_id) AS enumerated_count,
    COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END) AS vaccinated_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN is_vaccinated = 1 THEN enumeration_record_id END)::NUMERIC
        / NULLIF(COUNT(DISTINCT enumeration_record_id), 0) * 100, 2
    ) AS coverage_pct
FROM v_beneficiary_task_joined
WHERE age_months <= 59
  AND settlement_type IS NOT NULL
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
SELECT
    country_code,
    campaign_id,
    SUM(enumerated_count)  AS htr_enumerated_count,
    SUM(vaccinated_count)  AS htr_vaccinated_count,
    ROUND(
        SUM(vaccinated_count)::NUMERIC
        / NULLIF(SUM(enumerated_count), 0) * 100, 2
    ) AS htr_vaccination_rate
FROM dm_coverage_settlement_country
WHERE settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees')
GROUP BY country_code, campaign_id;

-- Province
CREATE MATERIALIZED VIEW dm_htr_vaccination_province AS
SELECT
    country_code,
    province_code,
    campaign_id,
    SUM(enumerated_count)  AS htr_enumerated_count,
    SUM(vaccinated_count)  AS htr_vaccinated_count,
    ROUND(
        SUM(vaccinated_count)::NUMERIC
        / NULLIF(SUM(enumerated_count), 0) * 100, 2
    ) AS htr_vaccination_rate
FROM dm_coverage_settlement_province
WHERE settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees')
GROUP BY country_code, province_code, campaign_id;

-- District
CREATE MATERIALIZED VIEW dm_htr_vaccination_district AS
SELECT
    country_code,
    province_code,
    district_code,
    campaign_id,
    SUM(enumerated_count)  AS htr_enumerated_count,
    SUM(vaccinated_count)  AS htr_vaccinated_count,
    ROUND(
        SUM(vaccinated_count)::NUMERIC
        / NULLIF(SUM(enumerated_count), 0) * 100, 2
    ) AS htr_vaccination_rate
FROM dm_coverage_settlement_district
WHERE settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees')
GROUP BY country_code, province_code, district_code, campaign_id;

-- SPP
CREATE MATERIALIZED VIEW dm_htr_vaccination_spp AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    campaign_id,
    SUM(enumerated_count)  AS htr_enumerated_count,
    SUM(vaccinated_count)  AS htr_vaccinated_count,
    ROUND(
        SUM(vaccinated_count)::NUMERIC
        / NULLIF(SUM(enumerated_count), 0) * 100, 2
    ) AS htr_vaccination_rate
FROM dm_coverage_settlement_spp
WHERE settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees')
GROUP BY country_code, province_code, district_code, spp_code, campaign_id;

-- Health Center
CREATE MATERIALIZED VIEW dm_htr_vaccination_health_center AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    campaign_id,
    SUM(enumerated_count)  AS htr_enumerated_count,
    SUM(vaccinated_count)  AS htr_vaccinated_count,
    ROUND(
        SUM(vaccinated_count)::NUMERIC
        / NULLIF(SUM(enumerated_count), 0) * 100, 2
    ) AS htr_vaccination_rate
FROM dm_coverage_settlement_health_center
WHERE settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees')
GROUP BY country_code, province_code, district_code, spp_code, health_center_code, campaign_id;

-- Village
CREATE MATERIALIZED VIEW dm_htr_vaccination_village AS
SELECT
    country_code,
    province_code,
    district_code,
    spp_code,
    health_center_code,
    village_code,
    campaign_id,
    SUM(enumerated_count)  AS htr_enumerated_count,
    SUM(vaccinated_count)  AS htr_vaccinated_count,
    ROUND(
        SUM(vaccinated_count)::NUMERIC
        / NULLIF(SUM(enumerated_count), 0) * 100, 2
    ) AS htr_vaccination_rate
FROM dm_coverage_settlement_village
WHERE settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees')
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
REFRESH MATERIALIZED VIEW CONCURRENTLY v_beneficiary_task_joined;

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
