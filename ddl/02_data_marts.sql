-- ============================================================================
-- ============================================================================
--                             DATA MART 1
--                 COVERAGE MART (Unified and Flattened)
-- ============================================================================
-- ============================================================================

-- 1. COUNTRY LEVEL
CREATE MATERIALIZED VIEW dm_coverage_daily_country AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code;

CREATE UNIQUE INDEX idx_dm_cov_daily_country ON dm_coverage_daily_country(campaign_id, tenant_id, task_date, country_code);

-- 2. PROVINCE LEVEL
CREATE MATERIALIZED VIEW dm_coverage_daily_province AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code, province_code,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code;

CREATE UNIQUE INDEX idx_dm_cov_daily_province ON dm_coverage_daily_province(campaign_id, tenant_id, task_date, country_code, province_code);

-- 3. DISTRICT LEVEL
CREATE MATERIALIZED VIEW dm_coverage_daily_district AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code, province_code, district_code,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code;

CREATE UNIQUE INDEX idx_dm_cov_daily_district ON dm_coverage_daily_district(campaign_id, tenant_id, task_date, country_code, province_code, district_code);

-- 4. HEALTH CENTER LEVEL
CREATE MATERIALIZED VIEW dm_coverage_daily_healthcenter AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code, province_code, district_code, health_center_code,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code;

CREATE UNIQUE INDEX idx_dm_cov_daily_healthcenter ON dm_coverage_daily_healthcenter(campaign_id, tenant_id, task_date, country_code, province_code, district_code, health_center_code);

-- 5. SPP LEVEL
CREATE MATERIALIZED VIEW dm_coverage_daily_spp AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code, province_code, district_code, health_center_code, spp_code,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code;

CREATE UNIQUE INDEX idx_dm_cov_daily_spp ON dm_coverage_daily_spp(campaign_id, tenant_id, task_date, country_code, province_code, district_code, health_center_code, spp_code);

-- 6. VILLAGE LEVEL
CREATE MATERIALIZED VIEW dm_coverage_daily_village AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code, province_code, district_code, health_center_code, spp_code, village_code,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code, village_code;

CREATE UNIQUE INDEX idx_dm_cov_daily_village ON dm_coverage_daily_village(campaign_id, tenant_id, task_date, country_code, province_code, district_code, health_center_code, spp_code, village_code);


-- ============================================================================
-- ============================================================================
--                             DATA MART 2
--                  REGISTRATION / ENUMERATION MART (No Joins)
-- ============================================================================
-- ============================================================================

-- 1. COUNTRY LEVEL
CREATE MATERIALIZED VIEW dm_registration_daily_country AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code,
    COUNT(CASE WHEN age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT household_id) AS total_households
FROM household_member_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code;

CREATE UNIQUE INDEX idx_dm_reg_daily_country ON dm_registration_daily_country(campaign_id, tenant_id, task_date, country_code);

-- 2. PROVINCE LEVEL
CREATE MATERIALIZED VIEW dm_registration_daily_province AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code, province_code,
    COUNT(CASE WHEN age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT household_id) AS total_households
FROM household_member_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code;

CREATE UNIQUE INDEX idx_dm_reg_daily_province ON dm_registration_daily_province(campaign_id, tenant_id, task_date, country_code, province_code);

-- 3. DISTRICT LEVEL
CREATE MATERIALIZED VIEW dm_registration_daily_district AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code, province_code, district_code,
    COUNT(CASE WHEN age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT household_id) AS total_households
FROM household_member_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code;

CREATE UNIQUE INDEX idx_dm_reg_daily_district ON dm_registration_daily_district(campaign_id, tenant_id, task_date, country_code, province_code, district_code);

-- 4. HEALTH CENTER LEVEL
CREATE MATERIALIZED VIEW dm_registration_daily_healthcenter AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code, province_code, district_code, health_center_code,
    COUNT(CASE WHEN age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT household_id) AS total_households
FROM household_member_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code;

CREATE UNIQUE INDEX idx_dm_reg_daily_healthcenter ON dm_registration_daily_healthcenter(campaign_id, tenant_id, task_date, country_code, province_code, district_code, health_center_code);

-- 5. SPP LEVEL
CREATE MATERIALIZED VIEW dm_registration_daily_spp AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code, province_code, district_code, health_center_code, spp_code,
    COUNT(CASE WHEN age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT household_id) AS total_households
FROM household_member_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code;

CREATE UNIQUE INDEX idx_dm_reg_daily_spp ON dm_registration_daily_spp(campaign_id, tenant_id, task_date, country_code, province_code, district_code, health_center_code, spp_code);

-- 6. VILLAGE LEVEL
CREATE MATERIALIZED VIEW dm_registration_daily_village AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date, country_code, province_code, district_code, health_center_code, spp_code, village_code,
    COUNT(CASE WHEN age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT household_id) AS total_households
FROM household_member_enriched
WHERE is_deleted = FALSE
GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code, village_code;

CREATE UNIQUE INDEX idx_dm_reg_daily_village ON dm_registration_daily_village(campaign_id, tenant_id, task_date, country_code, province_code, district_code, health_center_code, spp_code, village_code);


-- ============================================================================
-- ============================================================================
--                             DATA MART 3
--                           REFUSALS MART
-- ============================================================================
-- ============================================================================

-- 1. COUNTRY LEVEL
CREATE MATERIALIZED VIEW dm_refusals_daily_country AS
WITH total_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, COUNT(id) AS total_task_submissions
    FROM project_task_enriched WHERE is_deleted = FALSE GROUP BY campaign_id, tenant_id, task_dates, country_code
),
failed_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END AS reason_type,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN') AS reason_code,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NULL AND additional_details->>'absenceReason' IS NOT NULL THEN 1 END) AS absence_count
    FROM project_task_enriched WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_UNSUCCESSFUL'
    GROUP BY campaign_id, tenant_id, task_dates, country_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN')
)
SELECT f.campaign_id, f.tenant_id, f.task_dates AS task_date, f.country_code, f.reason_type, f.reason_code, f.refusal_count, f.absence_count, COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f LEFT JOIN total_tasks t ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates AND f.country_code = t.country_code;

CREATE UNIQUE INDEX idx_dm_refusals_country ON dm_refusals_daily_country(campaign_id, tenant_id, task_date, country_code, reason_type, reason_code);

-- 2. PROVINCE LEVEL
CREATE MATERIALIZED VIEW dm_refusals_daily_province AS
WITH total_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, province_code, COUNT(id) AS total_task_submissions
    FROM project_task_enriched WHERE is_deleted = FALSE GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code
),
failed_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, province_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END AS reason_type,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN') AS reason_code,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NULL AND additional_details->>'absenceReason' IS NOT NULL THEN 1 END) AS absence_count
    FROM project_task_enriched WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_UNSUCCESSFUL'
    GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN')
)
SELECT f.campaign_id, f.tenant_id, f.task_dates AS task_date, f.country_code, f.province_code, f.reason_type, f.reason_code, f.refusal_count, f.absence_count, COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f LEFT JOIN total_tasks t ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates AND f.country_code = t.country_code AND f.province_code = t.province_code;

CREATE UNIQUE INDEX idx_dm_refusals_province ON dm_refusals_daily_province(campaign_id, tenant_id, task_date, country_code, province_code, reason_type, reason_code);

-- 3. DISTRICT LEVEL
CREATE MATERIALIZED VIEW dm_refusals_daily_district AS
WITH total_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, province_code, district_code, COUNT(id) AS total_task_submissions
    FROM project_task_enriched WHERE is_deleted = FALSE GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code
),
failed_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, province_code, district_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END AS reason_type,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN') AS reason_code,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NULL AND additional_details->>'absenceReason' IS NOT NULL THEN 1 END) AS absence_count
    FROM project_task_enriched WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_UNSUCCESSFUL'
    GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN')
)
SELECT f.campaign_id, f.tenant_id, f.task_dates AS task_date, f.country_code, f.province_code, f.district_code, f.reason_type, f.reason_code, f.refusal_count, f.absence_count, COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f LEFT JOIN total_tasks t ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates AND f.country_code = t.country_code AND f.province_code = t.province_code AND f.district_code = t.district_code;

CREATE UNIQUE INDEX idx_dm_refusals_district ON dm_refusals_daily_district(campaign_id, tenant_id, task_date, country_code, province_code, district_code, reason_type, reason_code);

-- 4. HEALTH CENTER LEVEL
CREATE MATERIALIZED VIEW dm_refusals_daily_healthcenter AS
WITH total_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, COUNT(id) AS total_task_submissions
    FROM project_task_enriched WHERE is_deleted = FALSE GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code
),
failed_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END AS reason_type,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN') AS reason_code,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NULL AND additional_details->>'absenceReason' IS NOT NULL THEN 1 END) AS absence_count
    FROM project_task_enriched WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_UNSUCCESSFUL'
    GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN')
)
SELECT f.campaign_id, f.tenant_id, f.task_dates AS task_date, f.country_code, f.province_code, f.district_code, f.health_center_code, f.reason_type, f.reason_code, f.refusal_count, f.absence_count, COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f LEFT JOIN total_tasks t ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates AND f.country_code = t.country_code AND f.province_code = t.province_code AND f.district_code = t.district_code AND f.health_center_code = t.health_center_code;

CREATE UNIQUE INDEX idx_dm_refusals_healthcenter ON dm_refusals_daily_healthcenter(campaign_id, tenant_id, task_date, country_code, province_code, district_code, health_center_code, reason_type, reason_code);

-- 5. SPP LEVEL
CREATE MATERIALIZED VIEW dm_refusals_daily_spp AS
WITH total_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code, COUNT(id) AS total_task_submissions
    FROM project_task_enriched WHERE is_deleted = FALSE GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code
),
failed_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END AS reason_type,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN') AS reason_code,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NULL AND additional_details->>'absenceReason' IS NOT NULL THEN 1 END) AS absence_count
    FROM project_task_enriched WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_UNSUCCESSFUL'
    GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN')
)
SELECT f.campaign_id, f.tenant_id, f.task_dates AS task_date, f.country_code, f.province_code, f.district_code, f.health_center_code, f.spp_code, f.reason_type, f.reason_code, f.refusal_count, f.absence_count, COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f LEFT JOIN total_tasks t ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates AND f.country_code = t.country_code AND f.province_code = t.province_code AND f.district_code = t.district_code AND f.health_center_code = t.health_center_code AND f.spp_code = t.spp_code;

CREATE UNIQUE INDEX idx_dm_refusals_spp ON dm_refusals_daily_spp(campaign_id, tenant_id, task_date, country_code, province_code, district_code, health_center_code, spp_code, reason_type, reason_code);

-- 6. VILLAGE LEVEL
CREATE MATERIALIZED VIEW dm_refusals_daily_village AS
WITH total_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code, village_code, COUNT(id) AS total_task_submissions
    FROM project_task_enriched WHERE is_deleted = FALSE GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code, village_code
),
failed_tasks AS (
    SELECT campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code, village_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END AS reason_type,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN') AS reason_code,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'refusalReason' IS NULL AND additional_details->>'absenceReason' IS NOT NULL THEN 1 END) AS absence_count
    FROM project_task_enriched WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_UNSUCCESSFUL'
    GROUP BY campaign_id, tenant_id, task_dates, country_code, province_code, district_code, health_center_code, spp_code, village_code,
        CASE WHEN additional_details->>'refusalReason' IS NOT NULL THEN 'REFUSAL' WHEN additional_details->>'absenceReason' IS NOT NULL THEN 'ABSENCE' ELSE 'UNSPECIFIED' END,
        COALESCE(additional_details->>'refusalReason', additional_details->>'absenceReason', 'UNKNOWN')
)
SELECT f.campaign_id, f.tenant_id, f.task_dates AS task_date, f.country_code, f.province_code, f.district_code, f.health_center_code, f.spp_code, f.village_code, f.reason_type, f.reason_code, f.refusal_count, f.absence_count, COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f LEFT JOIN total_tasks t ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates AND f.country_code = t.country_code AND f.province_code = t.province_code AND f.district_code = t.district_code AND f.health_center_code = t.health_center_code AND f.spp_code = t.spp_code AND f.village_code = t.village_code;

CREATE UNIQUE INDEX idx_dm_refusals_village ON dm_refusals_daily_village(campaign_id, tenant_id, task_date, country_code, province_code, district_code, health_center_code, spp_code, village_code, reason_type, reason_code);


-- ============================================================================
-- ============================================================================
--                             DATA MART 4
--                  STOCK INVENTORY MART (Event-Driven)
-- ============================================================================
-- ============================================================================

CREATE TABLE IF NOT EXISTS dm_stock_daily AS
    WITH unified_stock_events AS (
        SELECT
        campaign_id, facility_id, facility_name, facility_level, facility_target, product_name,
        TO_TIMESTAMP(date_of_entry / 1000)::DATE AS transaction_date,
    CASE WHEN event_type = 'RECEIVED' THEN physical_count ELSE 0 END AS received_qty,
    CASE WHEN event_type = 'ISSUED' THEN physical_count ELSE 0 END AS issued_qty,
    CASE WHEN event_type = 'RETURNED' THEN physical_count ELSE 0 END AS returned_qty,
    CASE WHEN event_type = 'DAMAGED' THEN physical_count ELSE 0 END AS damaged_qty,
    date_of_entry AS last_movement_time_ms
    FROM stock_enriched
    WHERE event_type IN ('RECEIVED', 'ISSUED', 'RETURNED', 'DAMAGED')
    )
SELECT
    campaign_id, facility_id, facility_name, facility_level, facility_target, product_name, transaction_date,
    SUM(received_qty) AS total_received,
    SUM(issued_qty) AS total_issued,
    SUM(returned_qty) AS total_returned,
    SUM(damaged_qty) AS total_damaged,
    (SUM(received_qty) - SUM(issued_qty) + SUM(returned_qty) - SUM(damaged_qty)) AS calculated_balance,
    TO_TIMESTAMP(NULLIF(MAX(last_movement_time_ms), 0) / 1000) AS last_movement_timestamp
FROM unified_stock_events
GROUP BY campaign_id, facility_id, facility_name, facility_level, facility_target, product_name, transaction_date;


-- ============================================================================
-- ============================================================================
--                             DATA MART 5
--                    TEAM PERFORMANCE / TELEMETRY MART
-- ============================================================================
-- ============================================================================

CREATE TABLE IF NOT EXISTS dm_team_performance_daily AS
SELECT
    campaign_id, user_name, TO_TIMESTAMP(created_time / 1000)::DATE AS task_date, locality_code,
    MAX(country_code) AS country_code, MAX(province_code) AS province_code, MAX(district_code) AS district_code, MAX(health_center_code) AS health_center_code,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS total_failed,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_vaccinated,
    COUNT(id) AS total_submissions,
    MIN(created_time) AS first_submission_time_ms,
    MAX(created_time) AS last_submission_time_ms,
    MIN(synced_time) AS first_synced_time_ms,
    AVG(CASE WHEN synced_time IS NOT NULL AND created_time IS NOT NULL THEN GREATEST((synced_time - created_time), 0) / 60000.0 ELSE NULL END) AS avg_sync_lag_minutes,
    COUNT(CASE WHEN (synced_time - created_time) / 60000.0 < 60 THEN 1 END) AS sync_within_1hr,
    COUNT(CASE WHEN (synced_time - created_time) / 60000.0 >= 60 AND (synced_time - created_time) / 60000.0 < 360 THEN 1 END) AS sync_1_6hr,
    COUNT(CASE WHEN (synced_time - created_time) / 60000.0 >= 360 AND (synced_time - created_time) / 60000.0 <= 1440 THEN 1 END) AS sync_6_24hr,
    COUNT(CASE WHEN (synced_time - created_time) / 60000.0 > 1440 THEN 1 END) AS sync_over_24hr
FROM project_task_enriched
WHERE task_type IS NOT NULL
GROUP BY campaign_id, user_name, TO_TIMESTAMP(created_time / 1000)::DATE, locality_code;

CREATE INDEX IF NOT EXISTS idx_team_perf_daily ON dm_team_performance_daily (campaign_id, user_name, task_date, locality_code);



-- ============================================================================
-- ============================================================================
--                             DATA MART 8
--                         DATA QUALITY MART
-- ============================================================================
-- ============================================================================

CREATE MATERIALIZED VIEW dm_data_quality_daily AS
SELECT
    project_id,
    campaign_id,
    tenant_id,
    task_dates AS task_date,
    locality_code,

    -- Flattened Geographic Columns (Aggregated cleanly at the locality grain)
    MAX(country_code) AS country_code,
    MAX(province_code) AS province_code,
    MAX(district_code) AS district_code,
    MAX(health_center_code) AS health_center_code,
    MAX(spp_code) AS spp_code,
    MAX(village_code) AS village_code,

    -- ==========================================
    -- DATA QUALITY METRICS
    -- ==========================================
    COUNT(id) AS total_records,

    COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) AS gps_value_count,

    COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) AS gps_missing,

    COUNT(CASE WHEN location_accuracy > 50 THEN 1 END) AS gps_accuracy_flagged_count,

    ROUND(AVG(location_accuracy)::NUMERIC, 2) AS avg_gps_accuracy,

    COUNT(CASE WHEN created_time < synced_time THEN 1 END) AS timestamp_valid_count,

    -- Form Completeness (Checking required fields)
    COUNT(CASE
              WHEN project_beneficiary_client_reference_id IS NOT NULL
                  AND delivered_to IS NOT NULL
                  AND product_variant IS NOT NULL
                  THEN 1
        END) AS form_complete_count

FROM project_task_enriched
WHERE is_deleted = FALSE
GROUP BY
    project_id,
    campaign_id,
    tenant_id,
    task_dates,
    locality_code;

-- Create Unique Index for Concurrent Refreshes
CREATE UNIQUE INDEX idx_dm_data_quality_daily
    ON dm_data_quality_daily (project_id, campaign_id, tenant_id, task_date, locality_code);


