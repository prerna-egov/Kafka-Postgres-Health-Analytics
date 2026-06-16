--========================================================
-- DATA MART 1
--========================================================
-- ============================================================================
-- 1. COUNTRY LEVEL COVERAGE MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_coverage_daily_country AS
SELECT
    campaign_id,
    tenant_id,
    task_dates AS task_date,
    boundary_hierarchy_code->>'country' AS country_code,

    -- Aggregations (Aligned with spec names)
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE project_type = 'IMMUNIZATION'
  AND is_deleted = FALSE
GROUP BY
    campaign_id, tenant_id, task_dates,
    boundary_hierarchy_code->>'country';

CREATE UNIQUE INDEX idx_dm_cov_daily_country
    ON dm_coverage_daily_country(campaign_id, tenant_id, task_date, country_code);


-- ============================================================================
-- 2. PROVINCE LEVEL COVERAGE MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_coverage_daily_province AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date,
    boundary_hierarchy_code->>'country' AS country_code,
    boundary_hierarchy_code->>'province' AS province_code,

    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE project_type = 'IMMUNIZATION' AND is_deleted = FALSE
GROUP BY
    campaign_id, tenant_id, task_dates,
    boundary_hierarchy_code->>'country',
    boundary_hierarchy_code->>'province';

CREATE UNIQUE INDEX idx_dm_cov_daily_province
    ON dm_coverage_daily_province(campaign_id, tenant_id, task_date, country_code, province_code);


-- ============================================================================
-- 3. DISTRICT LEVEL COVERAGE MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_coverage_daily_district AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date,
    boundary_hierarchy_code->>'country' AS country_code,
    boundary_hierarchy_code->>'province' AS province_code,
    boundary_hierarchy_code->>'district' AS district_code,

    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE project_type = 'IMMUNIZATION' AND is_deleted = FALSE
GROUP BY
    campaign_id, tenant_id, task_dates,
    boundary_hierarchy_code->>'country',
    boundary_hierarchy_code->>'province',
    boundary_hierarchy_code->>'district';

CREATE UNIQUE INDEX idx_dm_cov_daily_district
    ON dm_coverage_daily_district(campaign_id, tenant_id, task_date, country_code, province_code, district_code);


-- ============================================================================
-- 4. HEALTH CENTER LEVEL COVERAGE MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_coverage_daily_healthcenter AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date,
    boundary_hierarchy_code->>'country' AS country_code,
    boundary_hierarchy_code->>'province' AS province_code,
    boundary_hierarchy_code->>'district' AS district_code,
    boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,

    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE project_type = 'IMMUNIZATION' AND is_deleted = FALSE
GROUP BY
    campaign_id, tenant_id, task_dates,
    boundary_hierarchy_code->>'country',
    boundary_hierarchy_code->>'province',
    boundary_hierarchy_code->>'district',
    boundary_hierarchy_code->>'healthCenter';

CREATE UNIQUE INDEX idx_dm_cov_daily_healthcenter
    ON dm_coverage_daily_healthcenter(campaign_id, tenant_id, task_date, country_code, province_code, district_code, healthcenter_code);


-- ============================================================================
-- 5. SPP LEVEL COVERAGE MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_coverage_daily_spp AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date,
    boundary_hierarchy_code->>'country' AS country_code,
    boundary_hierarchy_code->>'province' AS province_code,
    boundary_hierarchy_code->>'district' AS district_code,
    boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
    boundary_hierarchy_code->>'spp' AS spp_code,

    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE project_type = 'IMMUNIZATION' AND is_deleted = FALSE
GROUP BY
    campaign_id, tenant_id, task_dates,
    boundary_hierarchy_code->>'country',
    boundary_hierarchy_code->>'province',
    boundary_hierarchy_code->>'district',
    boundary_hierarchy_code->>'healthCenter',
    boundary_hierarchy_code->>'spp';

CREATE UNIQUE INDEX idx_dm_cov_daily_spp
    ON dm_coverage_daily_spp(campaign_id, tenant_id, task_date, country_code, province_code, district_code, healthcenter_code, spp_code);


-- ============================================================================
-- 6. VILLAGE LEVEL COVERAGE MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_coverage_daily_village AS
SELECT
    campaign_id, tenant_id, task_dates AS task_date,
    boundary_hierarchy_code->>'country' AS country_code,
    boundary_hierarchy_code->>'province' AS province_code,
    boundary_hierarchy_code->>'district' AS district_code,
    boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
    boundary_hierarchy_code->>'spp' AS spp_code,
    boundary_hierarchy_code->>'village' AS village_code,

    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE project_type = 'IMMUNIZATION' AND is_deleted = FALSE
GROUP BY
    campaign_id, tenant_id, task_dates,
    boundary_hierarchy_code->>'country',
    boundary_hierarchy_code->>'province',
    boundary_hierarchy_code->>'district',
    boundary_hierarchy_code->>'healthCenter',
    boundary_hierarchy_code->>'spp',
    boundary_hierarchy_code->>'village';

CREATE UNIQUE INDEX idx_dm_cov_daily_village
    ON dm_coverage_daily_village(campaign_id, tenant_id, task_date, country_code, province_code, district_code, healthcenter_code, spp_code, village_code);


-- ============================================================================
-- KPI QUERIES (DASHBOARD LAYER)
-- ============================================================================

-- Total Children Vaccinated (Overall Campaign)
SELECT
    campaign_id,
    SUM(total_administered_successfully) AS total_children_vaccinated
FROM dm_coverage_daily_country
GROUP BY campaign_id;

-- Total Could Not Vaccinate (Overall Campaign)
SELECT
    campaign_id,
    SUM(could_not_vaccinate_count) AS total_could_not_vaccinate
FROM dm_coverage_daily_country
GROUP BY campaign_id;

-- Daily Coverage Rate (By Date)
SELECT
    task_date,
    SUM(total_administered_successfully) AS daily_children_vaccinated,
    SUM(total_task_submissions) AS daily_total_submissions,
    ROUND((SUM(total_administered_successfully)::NUMERIC / NULLIF(SUM(total_task_submissions), 0)) * 100, 2) AS daily_coverage_rate_percentage
FROM dm_coverage_daily_country
GROUP BY task_date
ORDER BY task_date DESC;

-- District Performance Summary
SELECT
    district_code,
    SUM(total_administered_successfully) AS district_children_vaccinated,
    SUM(could_not_vaccinate_count) AS district_failed_vaccinations,
    SUM(total_task_submissions) AS district_total_tasks,
    ROUND((SUM(total_administered_successfully)::NUMERIC / NULLIF(SUM(total_task_submissions), 0)) * 100, 2) AS district_success_rate
FROM dm_coverage_daily_district
GROUP BY district_code;

-- Health Center Coverage Rate
SELECT
    healthcenter_code,
    SUM(total_administered_successfully) AS hc_children_vaccinated,
    ROUND((SUM(total_administered_successfully)::NUMERIC / NULLIF(SUM(total_task_submissions), 0)) * 100, 2) AS hc_coverage_rate
FROM dm_coverage_daily_healthcenter
GROUP BY healthcenter_code;






--========================================================
--  DATA MART 2
--========================================================
-- ============================================================================
-- 1. COUNTRY LEVEL MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_registration_daily_country AS
SELECT
    pbe.campaign_id,
    hme.tenant_id,
    hme.task_dates AS task_date,
    hme.boundary_hierarchy_code->>'country' AS country_code,

    COUNT(CASE WHEN hme.age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN hme.age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN hme.age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(hme.gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(hme.gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT hme.household_id) AS total_households
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe
ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id, hme.tenant_id, hme.task_dates,
    hme.boundary_hierarchy_code->>'country';

CREATE UNIQUE INDEX idx_dm_reg_daily_country
    ON dm_registration_daily_country(campaign_id, tenant_id, task_date, country_code);


-- ============================================================================
-- 2. PROVINCE LEVEL MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_registration_daily_province AS
SELECT
    pbe.campaign_id,
    hme.tenant_id,
    hme.task_dates AS task_date,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    hme.boundary_hierarchy_code->>'province' AS province_code,

    COUNT(CASE WHEN hme.age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN hme.age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN hme.age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(hme.gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(hme.gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT hme.household_id) AS total_households
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe
ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id, hme.tenant_id, hme.task_dates,
    hme.boundary_hierarchy_code->>'country',
    hme.boundary_hierarchy_code->>'province';

CREATE UNIQUE INDEX idx_dm_reg_daily_province
    ON dm_registration_daily_province(campaign_id, tenant_id, task_date, country_code, province_code);


-- ============================================================================
-- 3. DISTRICT LEVEL MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_registration_daily_district AS
SELECT
    pbe.campaign_id,
    hme.tenant_id,
    hme.task_dates AS task_date,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    hme.boundary_hierarchy_code->>'province' AS province_code,
    hme.boundary_hierarchy_code->>'district' AS district_code,

    COUNT(CASE WHEN hme.age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN hme.age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN hme.age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(hme.gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(hme.gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT hme.household_id) AS total_households
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe
ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id, hme.tenant_id, hme.task_dates,
    hme.boundary_hierarchy_code->>'country',
    hme.boundary_hierarchy_code->>'province',
    hme.boundary_hierarchy_code->>'district';

CREATE UNIQUE INDEX idx_dm_reg_daily_district
    ON dm_registration_daily_district(campaign_id, tenant_id, task_date, country_code, province_code, district_code);


-- ============================================================================
-- 4. HEALTH CENTER LEVEL MART (Corrected from Post-Admin)
-- ============================================================================
CREATE MATERIALIZED VIEW dm_registration_daily_healthcenter AS
SELECT
    pbe.campaign_id,
    hme.tenant_id,
    hme.task_dates AS task_date,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    hme.boundary_hierarchy_code->>'province' AS province_code,
    hme.boundary_hierarchy_code->>'district' AS district_code,
    hme.boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,

    COUNT(CASE WHEN hme.age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN hme.age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN hme.age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(hme.gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(hme.gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT hme.household_id) AS total_households
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe
ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id, hme.tenant_id, hme.task_dates,
    hme.boundary_hierarchy_code->>'country',
    hme.boundary_hierarchy_code->>'province',
    hme.boundary_hierarchy_code->>'district',
    hme.boundary_hierarchy_code->>'healthCenter';

CREATE UNIQUE INDEX idx_dm_reg_daily_healthcenter
    ON dm_registration_daily_healthcenter(campaign_id, tenant_id, task_date, country_code, province_code, district_code, healthcenter_code);


-- ============================================================================
-- 5. SPP LEVEL MART (Corrected from Locality)
-- ============================================================================
CREATE MATERIALIZED VIEW dm_registration_daily_spp AS
SELECT
    pbe.campaign_id,
    hme.tenant_id,
    hme.task_dates AS task_date,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    hme.boundary_hierarchy_code->>'province' AS province_code,
    hme.boundary_hierarchy_code->>'district' AS district_code,
    hme.boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
    hme.boundary_hierarchy_code->>'spp' AS spp_code,

    COUNT(CASE WHEN hme.age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN hme.age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN hme.age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(hme.gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(hme.gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT hme.household_id) AS total_households
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe
ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id, hme.tenant_id, hme.task_dates,
    hme.boundary_hierarchy_code->>'country',
    hme.boundary_hierarchy_code->>'province',
    hme.boundary_hierarchy_code->>'district',
    hme.boundary_hierarchy_code->>'healthCenter',
    hme.boundary_hierarchy_code->>'spp';

CREATE UNIQUE INDEX idx_dm_reg_daily_spp
    ON dm_registration_daily_spp(campaign_id, tenant_id, task_date, country_code, province_code, district_code, healthcenter_code, spp_code);


-- ============================================================================
-- 6. VILLAGE LEVEL MART (Updated to include HC and SPP)
-- ============================================================================
CREATE MATERIALIZED VIEW dm_registration_daily_village AS
SELECT
    pbe.campaign_id,
    hme.tenant_id,
    hme.task_dates AS task_date,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    hme.boundary_hierarchy_code->>'province' AS province_code,
    hme.boundary_hierarchy_code->>'district' AS district_code,
    hme.boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
    hme.boundary_hierarchy_code->>'spp' AS spp_code,
    hme.boundary_hierarchy_code->>'village' AS village_code,

    COUNT(CASE WHEN hme.age BETWEEN 0 AND 11 THEN 1 END) AS children_0_11m,
    COUNT(CASE WHEN hme.age BETWEEN 12 AND 23 THEN 1 END) AS children_12_23m,
    COUNT(CASE WHEN hme.age BETWEEN 24 AND 59 THEN 1 END) AS children_24_59m,
    COUNT(CASE WHEN UPPER(hme.gender) = 'MALE' THEN 1 END) AS male_count,
    COUNT(CASE WHEN UPPER(hme.gender) = 'FEMALE' THEN 1 END) AS female_count,
    COUNT(DISTINCT hme.household_id) AS total_households
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe
ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id, hme.tenant_id, hme.task_dates,
    hme.boundary_hierarchy_code->>'country',
    hme.boundary_hierarchy_code->>'province',
    hme.boundary_hierarchy_code->>'district',
    hme.boundary_hierarchy_code->>'healthCenter',
    hme.boundary_hierarchy_code->>'spp',
    hme.boundary_hierarchy_code->>'village';

CREATE UNIQUE INDEX idx_dm_reg_daily_village
    ON dm_registration_daily_village(campaign_id, tenant_id, task_date, country_code, province_code, district_code, healthcenter_code, spp_code, village_code);


-- ============================================================================
-- KPI QUERIES
-- ============================================================================

-- KPI 1: TOTAL HOUSEHOLDS REGISTERED
SELECT
    pbe.campaign_id,
    COUNT(DISTINCT hme.household_id) AS total_households_registered
FROM household_member_enriched hme
         JOIN project_beneficiary_enriched pbe
              ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY pbe.campaign_id;

-- KPI 2: TOTAL CHILDREN ENUMERATED (0-59m)
SELECT
    campaign_id,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated
FROM dm_registration_daily_country
GROUP BY campaign_id;

-- KPI 3: AGE BAND BREAKDOWN
SELECT
    campaign_id,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months
FROM dm_registration_daily_country
GROUP BY campaign_id;

-- KPI 4: GENDER BREAKDOWN
SELECT
    campaign_id,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females
FROM dm_registration_daily_country
GROUP BY campaign_id;







/*========================================================
  DATA MART 3
========================================================*/

-- ============================================================================
-- PHASE 1: MATERIALIZED VIEWS (DAILY AGGREGATIONS BY ALL BOUNDARY LEVELS)
-- ============================================================================

-- 1. COUNTRY LEVEL REFUSALS MART
CREATE MATERIALIZED VIEW dm_refusals_daily_country AS
WITH total_tasks AS (
    SELECT
        campaign_id,
        tenant_id,
        task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        COUNT(id) AS total_task_submissions
    FROM project_task_enriched
    WHERE is_deleted = FALSE
    GROUP BY campaign_id, tenant_id, task_dates, boundary_hierarchy_code->>'country'
),
failed_tasks AS (
    SELECT
        campaign_id,
        tenant_id,
        task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        additional_details->>'reason_type' AS reason_type,
        additional_details->>'reason_code' AS reason_code,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'REFUSAL' THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'ABSENCE' THEN 1 END) AS absence_count
    FROM project_task_enriched
    WHERE is_deleted = FALSE
      AND administration_status = 'ADMINISTRATION_FAILED'
    GROUP BY campaign_id, tenant_id, task_dates, boundary_hierarchy_code->>'country',
             additional_details->>'reason_type', additional_details->>'reason_code'
)
SELECT
    f.campaign_id,
    f.tenant_id,
    f.task_dates AS task_date,
    f.country_code,
    f.reason_type,
    f.reason_code,
    f.refusal_count,
    f.absence_count,
    COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f
         LEFT JOIN total_tasks t
                   ON f.campaign_id = t.campaign_id
                       AND f.tenant_id = t.tenant_id
                       AND f.task_dates = t.task_dates
                       AND f.country_code = t.country_code;

CREATE UNIQUE INDEX idx_dm_refusals_country
    ON dm_refusals_daily_country(campaign_id, tenant_id, task_date, country_code, reason_type, reason_code);


-- 2. PROVINCE LEVEL REFUSALS MART
CREATE MATERIALIZED VIEW dm_refusals_daily_province AS
WITH total_tasks AS (
    SELECT
        campaign_id, tenant_id, task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        boundary_hierarchy_code->>'province' AS province_code,
        COUNT(id) AS total_task_submissions
    FROM project_task_enriched
    WHERE is_deleted = FALSE
    GROUP BY 1, 2, 3, 4, 5
),
failed_tasks AS (
    SELECT
        campaign_id, tenant_id, task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        boundary_hierarchy_code->>'province' AS province_code,
        additional_details->>'reason_type' AS reason_type,
        additional_details->>'reason_code' AS reason_code,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'REFUSAL' THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'ABSENCE' THEN 1 END) AS absence_count
    FROM project_task_enriched
    WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_FAILED'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT
    f.campaign_id, f.tenant_id, f.task_dates AS task_date,
    f.country_code, f.province_code,
    f.reason_type, f.reason_code, f.refusal_count, f.absence_count,
    COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f
         LEFT JOIN total_tasks t
                   ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates
                       AND f.country_code = t.country_code AND f.province_code = t.province_code;

CREATE UNIQUE INDEX idx_dm_refusals_province
    ON dm_refusals_daily_province(campaign_id, tenant_id, task_date, country_code, province_code, reason_type, reason_code);


-- 3. DISTRICT LEVEL REFUSALS MART
CREATE MATERIALIZED VIEW dm_refusals_daily_district AS
WITH total_tasks AS (
    SELECT
        campaign_id, tenant_id, task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        boundary_hierarchy_code->>'province' AS province_code,
        boundary_hierarchy_code->>'district' AS district_code,
        COUNT(id) AS total_task_submissions
    FROM project_task_enriched
    WHERE is_deleted = FALSE
    GROUP BY 1, 2, 3, 4, 5, 6
),
failed_tasks AS (
    SELECT
        campaign_id, tenant_id, task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        boundary_hierarchy_code->>'province' AS province_code,
        boundary_hierarchy_code->>'district' AS district_code,
        additional_details->>'reason_type' AS reason_type,
        additional_details->>'reason_code' AS reason_code,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'REFUSAL' THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'ABSENCE' THEN 1 END) AS absence_count
    FROM project_task_enriched
    WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_FAILED'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT
    f.campaign_id, f.tenant_id, f.task_dates AS task_date,
    f.country_code, f.province_code, f.district_code,
    f.reason_type, f.reason_code, f.refusal_count, f.absence_count,
    COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f
         LEFT JOIN total_tasks t
                   ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates
                       AND f.country_code = t.country_code AND f.province_code = t.province_code AND f.district_code = t.district_code;

CREATE UNIQUE INDEX idx_dm_refusals_district
    ON dm_refusals_daily_district(campaign_id, tenant_id, task_date, country_code, province_code, district_code, reason_type, reason_code);


-- 4. HEALTH CENTER LEVEL REFUSALS MART
CREATE MATERIALIZED VIEW dm_refusals_daily_healthcenter AS
WITH total_tasks AS (
    SELECT
        campaign_id, tenant_id, task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        boundary_hierarchy_code->>'province' AS province_code,
        boundary_hierarchy_code->>'district' AS district_code,
        boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
        COUNT(id) AS total_task_submissions
    FROM project_task_enriched
    WHERE is_deleted = FALSE
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
failed_tasks AS (
    SELECT
        campaign_id, tenant_id, task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        boundary_hierarchy_code->>'province' AS province_code,
        boundary_hierarchy_code->>'district' AS district_code,
        boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
        additional_details->>'reason_type' AS reason_type,
        additional_details->>'reason_code' AS reason_code,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'REFUSAL' THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'ABSENCE' THEN 1 END) AS absence_count
    FROM project_task_enriched
    WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_FAILED'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)
SELECT
    f.campaign_id, f.tenant_id, f.task_dates AS task_date,
    f.country_code, f.province_code, f.district_code, f.healthcenter_code,
    f.reason_type, f.reason_code, f.refusal_count, f.absence_count,
    COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f
         LEFT JOIN total_tasks t
                   ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates
                       AND f.country_code = t.country_code AND f.province_code = t.province_code
                       AND f.district_code = t.district_code AND f.healthcenter_code = t.healthcenter_code;

CREATE UNIQUE INDEX idx_dm_refusals_healthcenter
    ON dm_refusals_daily_healthcenter(campaign_id, tenant_id, task_date, country_code, province_code, district_code, healthcenter_code, reason_type, reason_code);


-- 5. SPP LEVEL REFUSALS MART
CREATE MATERIALIZED VIEW dm_refusals_daily_spp AS
WITH total_tasks AS (
    SELECT
        campaign_id, tenant_id, task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        boundary_hierarchy_code->>'province' AS province_code,
        boundary_hierarchy_code->>'district' AS district_code,
        boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
        boundary_hierarchy_code->>'spp' AS spp_code,
        COUNT(id) AS total_task_submissions
    FROM project_task_enriched
    WHERE is_deleted = FALSE
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),
failed_tasks AS (
    SELECT
        campaign_id, tenant_id, task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        boundary_hierarchy_code->>'province' AS province_code,
        boundary_hierarchy_code->>'district' AS district_code,
        boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
        boundary_hierarchy_code->>'spp' AS spp_code,
        additional_details->>'reason_type' AS reason_type,
        additional_details->>'reason_code' AS reason_code,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'REFUSAL' THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'ABSENCE' THEN 1 END) AS absence_count
    FROM project_task_enriched
    WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_FAILED'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
SELECT
    f.campaign_id, f.tenant_id, f.task_dates AS task_date,
    f.country_code, f.province_code, f.district_code, f.healthcenter_code, f.spp_code,
    f.reason_type, f.reason_code, f.refusal_count, f.absence_count,
    COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f
         LEFT JOIN total_tasks t
                   ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates
                       AND f.country_code = t.country_code AND f.province_code = t.province_code
                       AND f.district_code = t.district_code AND f.healthcenter_code = t.healthcenter_code
                       AND f.spp_code = t.spp_code;

CREATE UNIQUE INDEX idx_dm_refusals_spp
    ON dm_refusals_daily_spp(campaign_id, tenant_id, task_date, country_code, province_code, district_code, healthcenter_code, spp_code, reason_type, reason_code);


-- 6. VILLAGE LEVEL REFUSALS MART
CREATE MATERIALIZED VIEW dm_refusals_daily_village AS
WITH total_tasks AS (
    SELECT
        campaign_id, tenant_id, task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        boundary_hierarchy_code->>'province' AS province_code,
        boundary_hierarchy_code->>'district' AS district_code,
        boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
        boundary_hierarchy_code->>'spp' AS spp_code,
        boundary_hierarchy_code->>'village' AS village_code,
        COUNT(id) AS total_task_submissions
    FROM project_task_enriched
    WHERE is_deleted = FALSE
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
failed_tasks AS (
    SELECT
        campaign_id, tenant_id, task_dates,
        boundary_hierarchy_code->>'country' AS country_code,
        boundary_hierarchy_code->>'province' AS province_code,
        boundary_hierarchy_code->>'district' AS district_code,
        boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
        boundary_hierarchy_code->>'spp' AS spp_code,
        boundary_hierarchy_code->>'village' AS village_code,
        additional_details->>'reason_type' AS reason_type,
        additional_details->>'reason_code' AS reason_code,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'REFUSAL' THEN 1 END) AS refusal_count,
        COUNT(CASE WHEN additional_details->>'reason_type' = 'ABSENCE' THEN 1 END) AS absence_count
    FROM project_task_enriched
    WHERE is_deleted = FALSE AND administration_status = 'ADMINISTRATION_FAILED'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
SELECT
    f.campaign_id, f.tenant_id, f.task_dates AS task_date,
    f.country_code, f.province_code, f.district_code, f.healthcenter_code, f.spp_code, f.village_code,
    f.reason_type, f.reason_code, f.refusal_count, f.absence_count,
    COALESCE(t.total_task_submissions, 0) AS total_task_submissions
FROM failed_tasks f
         LEFT JOIN total_tasks t
                   ON f.campaign_id = t.campaign_id AND f.tenant_id = t.tenant_id AND f.task_dates = t.task_dates
                       AND f.country_code = t.country_code AND f.province_code = t.province_code
                       AND f.district_code = t.district_code AND f.healthcenter_code = t.healthcenter_code
                       AND f.spp_code = t.spp_code AND f.village_code = t.village_code;

CREATE UNIQUE INDEX idx_dm_refusals_village
    ON dm_refusals_daily_village(campaign_id, tenant_id, task_date, country_code, province_code, district_code, healthcenter_code, spp_code, village_code, reason_type, reason_code);


-- ============================================================================
-- PHASE 2: PRODUCTION-READY KPI QUERIES (DASHBOARD LAYER)
-- ============================================================================

-- KPI 1: REFUSAL / ABSENCE REASONS BREAKDOWN
-- Identifies behavioral factors and trends behind system deployment drop-offs
SELECT
    reason_code,
    SUM(refusal_count) AS total_refusals,
    SUM(absence_count) AS total_absences
FROM dm_refusals_daily_country
WHERE reason_type IN ('REFUSAL', 'ABSENCE')
GROUP BY reason_code
ORDER BY total_refusals DESC;


-- KPI 2: TOTAL CAMPAIGN REFUSAL & ABSENCE RATES (%)
-- Safely aggregates multi-day time boundaries without cross-row structural inflation
WITH DailyTotals AS (
    SELECT
        campaign_id,
        task_date,
        SUM(refusal_count) AS daily_refusals,
        SUM(absence_count) AS daily_absences,
        MAX(total_task_submissions) AS daily_tasks
    FROM dm_refusals_daily_country
    GROUP BY campaign_id, task_date
)
SELECT
    campaign_id,
    SUM(daily_refusals) AS total_refusals,
    SUM(daily_absences) AS total_absences,
    SUM(daily_tasks) AS overarching_task_total,
    (SUM(daily_refusals)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS refusal_rate_percentage,
    (SUM(daily_absences)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS absence_rate_percentage
FROM DailyTotals
GROUP BY campaign_id;


-- KPI 3: REFUSAL & ABSENCE RATES BY DISTRICT (%)
-- Feeds geographic map visualizations to locate operational performance gaps
WITH DistrictDailyTotals AS (
    SELECT
        district_code,
        task_date,
        SUM(refusal_count) AS daily_refusals,
        SUM(absence_count) AS daily_absences,
        MAX(total_task_submissions) AS daily_tasks
    FROM dm_refusals_daily_district
    GROUP BY district_code, task_date
)
SELECT
    district_code,
    SUM(daily_refusals) AS district_refusals,
    SUM(daily_absences) AS district_absences,
    SUM(daily_tasks) AS district_task_total,
    (SUM(daily_refusals)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS district_refusal_rate,
    (SUM(daily_absences)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS district_absence_rate
FROM DistrictDailyTotals
GROUP BY district_code;



