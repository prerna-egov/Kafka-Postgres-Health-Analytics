--==========================================================================
    
    
                 -- DATA MART 1 
    
    
--==========================================================================

-- ============================================================================
-- 1. COUNTRY LEVEL COVERAGE MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_coverage_daily_country AS
SELECT
    campaign_id,
    tenant_id,
    task_dates AS task_date,
    boundary_hierarchy_code->>'country' AS country_code,

    -- Aggregations
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE is_deleted = FALSE
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
WHERE is_deleted = FALSE
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
WHERE is_deleted = FALSE
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
WHERE is_deleted = FALSE
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
WHERE is_deleted = FALSE
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
WHERE is_deleted = FALSE
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
    country_code,
    province_code,
    district_code,
    SUM(total_administered_successfully) AS district_children_vaccinated,
    SUM(could_not_vaccinate_count) AS district_failed_vaccinations,
    SUM(total_task_submissions) AS district_total_tasks,
    ROUND((SUM(total_administered_successfully)::NUMERIC / NULLIF(SUM(total_task_submissions), 0)) * 100, 2) AS district_success_rate
FROM dm_coverage_daily_district
GROUP BY country_code, province_code, district_code;

-- Health Center Coverage Rate
SELECT
    country_code,
    province_code,
    district_code,
    healthcenter_code,
    SUM(total_administered_successfully) AS hc_children_vaccinated,
    ROUND((SUM(total_administered_successfully)::NUMERIC / NULLIF(SUM(total_task_submissions), 0)) * 100, 2) AS hc_coverage_rate
FROM dm_coverage_daily_healthcenter
GROUP BY country_code, province_code, district_code, healthcenter_code;


--==========================================================================


                            -- DATA MART 2


--==========================================================================

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
-- 4. HEALTH CENTER LEVEL MART
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
-- 5. SPP LEVEL MART
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
-- 6. VILLAGE LEVEL MART
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



-- KPI 1: TOTAL HOUSEHOLDS REGISTERED (Country)
SELECT
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    COUNT(DISTINCT hme.household_id) AS total_households_registered
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY pbe.campaign_id, hme.boundary_hierarchy_code->>'country';

-- KPI 2, 3 & 4: ENUMERATION, AGE, AND GENDER (Country)
SELECT
    campaign_id,
    country_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females
FROM dm_registration_daily_country
GROUP BY campaign_id, country_code;

-- KPI 1: TOTAL HOUSEHOLDS REGISTERED (Province)
SELECT
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    hme.boundary_hierarchy_code->>'province' AS province_code,
    COUNT(DISTINCT hme.household_id) AS total_households_registered
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country',
    hme.boundary_hierarchy_code->>'province';

-- KPI 2, 3 & 4: ENUMERATION, AGE, AND GENDER (Province)
SELECT
    campaign_id,
    country_code,
    province_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females
FROM dm_registration_daily_province
GROUP BY campaign_id, country_code, province_code;

-- KPI 1: TOTAL HOUSEHOLDS REGISTERED (District)
SELECT
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    hme.boundary_hierarchy_code->>'province' AS province_code,
    hme.boundary_hierarchy_code->>'district' AS district_code,
    COUNT(DISTINCT hme.household_id) AS total_households_registered
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country',
    hme.boundary_hierarchy_code->>'province',
    hme.boundary_hierarchy_code->>'district';

-- KPI 2, 3 & 4: ENUMERATION, AGE, AND GENDER (District)
SELECT
    campaign_id,
    country_code,
    province_code,
    district_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females
FROM dm_registration_daily_district
GROUP BY campaign_id, country_code, province_code, district_code;

-- KPI 1: TOTAL HOUSEHOLDS REGISTERED (Health Center)
SELECT
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    hme.boundary_hierarchy_code->>'province' AS province_code,
    hme.boundary_hierarchy_code->>'district' AS district_code,
    hme.boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
    COUNT(DISTINCT hme.household_id) AS total_households_registered
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country',
    hme.boundary_hierarchy_code->>'province',
    hme.boundary_hierarchy_code->>'district',
    hme.boundary_hierarchy_code->>'healthCenter';

-- KPI 2, 3 & 4: ENUMERATION, AGE, AND GENDER (Health Center)
SELECT
    campaign_id,
    country_code,
    province_code,
    district_code,
    healthcenter_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females
FROM dm_registration_daily_healthcenter
GROUP BY campaign_id, country_code, province_code, district_code, healthcenter_code;

-- KPI 1: TOTAL HOUSEHOLDS REGISTERED (SPP)
SELECT
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    hme.boundary_hierarchy_code->>'province' AS province_code,
    hme.boundary_hierarchy_code->>'district' AS district_code,
    hme.boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
    hme.boundary_hierarchy_code->>'spp' AS spp_code,
    COUNT(DISTINCT hme.household_id) AS total_households_registered
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country',
    hme.boundary_hierarchy_code->>'province',
    hme.boundary_hierarchy_code->>'district',
    hme.boundary_hierarchy_code->>'healthCenter',
    hme.boundary_hierarchy_code->>'spp';

-- KPI 2, 3 & 4: ENUMERATION, AGE, AND GENDER (SPP)
SELECT
    campaign_id,
    country_code,
    province_code,
    district_code,
    healthcenter_code,
    spp_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females
FROM dm_registration_daily_spp
GROUP BY campaign_id, country_code, province_code, district_code, healthcenter_code, spp_code;

-- KPI 1: TOTAL HOUSEHOLDS REGISTERED (Village)
SELECT
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country' AS country_code,
    hme.boundary_hierarchy_code->>'province' AS province_code,
    hme.boundary_hierarchy_code->>'district' AS district_code,
    hme.boundary_hierarchy_code->>'healthCenter' AS healthcenter_code,
    hme.boundary_hierarchy_code->>'spp' AS spp_code,
    hme.boundary_hierarchy_code->>'village' AS village_code,
    COUNT(DISTINCT hme.household_id) AS total_households_registered
FROM household_member_enriched hme
    JOIN project_beneficiary_enriched pbe ON hme.individual_id = pbe.beneficiary_id
WHERE hme.is_deleted = FALSE AND pbe.is_deleted = FALSE
GROUP BY
    pbe.campaign_id,
    hme.boundary_hierarchy_code->>'country',
    hme.boundary_hierarchy_code->>'province',
    hme.boundary_hierarchy_code->>'district',
    hme.boundary_hierarchy_code->>'healthCenter',
    hme.boundary_hierarchy_code->>'spp',
    hme.boundary_hierarchy_code->>'village';

-- KPI 2, 3 & 4: ENUMERATION, AGE, AND GENDER (Village)
SELECT
    campaign_id,
    country_code,
    province_code,
    district_code,
    healthcenter_code,
    spp_code,
    village_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females
FROM dm_registration_daily_village
GROUP BY campaign_id, country_code, province_code, district_code, healthcenter_code, spp_code, village_code;




--=================================================================================


                                -- DATA MART 3


--==================================================================================


-- ============================================================================
-- 1. COUNTRY LEVEL COVERAGE MART
-- ============================================================================
CREATE MATERIALIZED VIEW dm_coverage_daily_country AS
SELECT
    campaign_id,
    tenant_id,
    task_dates AS task_date,
    boundary_hierarchy_code->>'country' AS country_code,

    -- Aggregations
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_SUCCESS' THEN 1 END) AS total_administered_successfully,
    COUNT(CASE WHEN administration_status = 'ADMINISTRATION_FAILED' THEN 1 END) AS could_not_vaccinate_count,
    COUNT(id) AS total_task_submissions
FROM project_task_enriched
WHERE is_deleted = FALSE
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
WHERE is_deleted = FALSE
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
WHERE is_deleted = FALSE
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
WHERE is_deleted = FALSE
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
WHERE is_deleted = FALSE
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
WHERE is_deleted = FALSE
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
    country_code,
    province_code,
    district_code,
    SUM(total_administered_successfully) AS district_children_vaccinated,
    SUM(could_not_vaccinate_count) AS district_failed_vaccinations,
    SUM(total_task_submissions) AS district_total_tasks,
    ROUND((SUM(total_administered_successfully)::NUMERIC / NULLIF(SUM(total_task_submissions), 0)) * 100, 2) AS district_success_rate
FROM dm_coverage_daily_district
GROUP BY country_code, province_code, district_code;

-- Health Center Coverage Rate
SELECT
    country_code,
    province_code,
    district_code,
    healthcenter_code,
    SUM(total_administered_successfully) AS hc_children_vaccinated,
    ROUND((SUM(total_administered_successfully)::NUMERIC / NULLIF(SUM(total_task_submissions), 0)) * 100, 2) AS hc_coverage_rate
FROM dm_coverage_daily_healthcenter
GROUP BY country_code, province_code, district_code, healthcenter_code;


--==========================================================================


                            -- DATA MART 4


--==========================================================================

CREATE TABLE IF NOT EXISTS dm_stock_daily AS

    -- ==========================================
-- 1. Isolate the final stock check of the day
-- ==========================================
    WITH latest_daily_reconciliation AS (
        SELECT
        campaign_id,
        facility_id,
        facility_name,
        facility_level,
        facility_target,
        product_name,
        TO_TIMESTAMP(date_of_reconciliation / 1000)::DATE AS transaction_date,
    physical_count,
    date_of_reconciliation, -- Keep the raw timestamp for the audit trail
    ROW_NUMBER() OVER (
                          PARTITION BY campaign_id, facility_id, product_name, TO_TIMESTAMP(date_of_reconciliation / 1000)::DATE
    ORDER BY date_of_reconciliation DESC
    ) as daily_rank
    FROM stock_reconciliation_enriched
    WHERE is_deleted = false
    ),

    -- ==========================================
-- 2. The UNION ALL (Now carrying timestamp data)
-- ==========================================
    unified_stock_events AS (
    -- Top Half: Everyday Stock Movements
                                SELECT
                                campaign_id, facility_id, facility_name, facility_level, facility_target, product_name,
                                TO_TIMESTAMP(date_of_entry / 1000)::DATE AS transaction_date,
    CASE WHEN event_type = 'RECEIVED' THEN physical_count ELSE 0 END AS received_qty,
    CASE WHEN event_type = 'ISSUED' THEN physical_count ELSE 0 END AS issued_qty,
    CASE WHEN event_type = 'RETURNED' THEN physical_count ELSE 0 END AS returned_qty,
    CASE WHEN event_type = 'DAMAGED' THEN physical_count ELSE 0 END AS damaged_qty,
    0 AS reconciliation_physical_count,

    -- NEW: Pass the movement timestamp, put 0 for reconciliation
    date_of_entry AS last_movement_time_ms,
    0::BIGINT AS last_reconciliation_time_ms
    FROM stock_enriched
    WHERE event_type IN ('RECEIVED', 'ISSUED', 'RETURNED', 'DAMAGED')

    UNION ALL

    -- Bottom Half: Manual Stock Checks
    SELECT
    campaign_id, facility_id, facility_name, facility_level, facility_target, product_name,
    transaction_date,
    0 AS received_qty, 0 AS issued_qty, 0 AS returned_qty, 0 AS damaged_qty,
    physical_count AS reconciliation_physical_count,

    -- NEW: Put 0 for movement, pass the reconciliation timestamp
    0::BIGINT AS last_movement_time_ms,
    date_of_reconciliation AS last_reconciliation_time_ms
    FROM latest_daily_reconciliation
    WHERE daily_rank = 1
    )

-- ==========================================
-- 3. Final Aggregation
-- ==========================================
SELECT
    campaign_id,
    facility_id,
    facility_name,
    facility_level,
    facility_target,
    product_name,
    transaction_date,
    SUM(received_qty) AS total_received,
    SUM(issued_qty) AS total_issued,
    SUM(returned_qty) AS total_returned,
    SUM(damaged_qty) AS total_damaged,
    (SUM(received_qty) - SUM(issued_qty) + SUM(returned_qty) - SUM(damaged_qty)) AS calculated_balance,
    SUM(reconciliation_physical_count) AS reconciled_physical_count,

    -- NEW: Extract the latest timestamps and convert them to readable formats
    TO_TIMESTAMP(NULLIF(MAX(last_movement_time_ms), 0) / 1000) AS last_movement_timestamp,
    TO_TIMESTAMP(NULLIF(MAX(last_reconciliation_time_ms), 0) / 1000) AS last_reconciliation_timestamp

FROM unified_stock_events
GROUP BY
    campaign_id, facility_id, facility_name, facility_level, facility_target, product_name, transaction_date;





SELECT
    campaign_id,
    facility_id,
    facility_name,
    facility_level,
    product_name,
    transaction_date,

    -- ==========================================
    -- KPI 1: Vials Issued per Team/DayN

    )
END AS wastage_rate_pct,

    -- ==========================================
    -- KPI 4: Stock Reconciliation Rate (%)
    -- ==========================================
    CASE
        -- If the calculated balance is 0, we can't calculate a standard ratio
        WHEN calculated_balance = 0 THEN NULL
        ELSE ROUND(
                (reconciled_physical_count::NUMERIC / calculated_balance) * 100,
                2
             )
END AS stock_reconciliation_rate_pct,

    -- ==========================================
    -- KPI 5: Stock-out Risk (%)
    -- ==========================================
    CASE
        -- Cannot calculate risk if there is no target set for the facility
        WHEN facility_target = 0 OR facility_target IS NULL THEN NULL
        ELSE ROUND(
                (
                    -- Prioritize physical count if available, otherwise fallback to calculated balance
                    COALESCE(NULLIF(reconciled_physical_count, 0), calculated_balance)::NUMERIC
                / facility_target
                    ) * 100,
                2
             )
END AS stock_out_risk_pct,

    -- Carry forward audit timestamps for dashboard context
    last_movement_timestamp,
    last_reconciliation_timestamp

FROM dm_stock_daily;
    
    
    
--==========================================================================


                            -- DATA MART 5


--==========================================================================


CREATE TABLE IF NOT EXISTS dm_team_performance_daily AS

SELECT
    campaign_id,
    user_name,
    TO_TIMESTAMP(created_time / 1000)::DATE AS task_date,
    locality_code, -- Now a grouping dimension

    -- 1. Geography Aggregation (JSONB must still be aggregated)
    MAX(boundary_hierarchy_code::TEXT)::JSONB AS boundary_hierarchy_code,

    -- 2. Core Task Metrics
    COUNT(CASE WHEN administration_status = 'ADMINISTERED' THEN 1 END) AS total_vaccinated,
    COUNT(id) AS total_submissions,

    -- 3. Time Boundary Metrics
    MIN(created_time) AS first_submission_time_ms,
    MAX(created_time) AS last_submission_time_ms,
    MIN(synced_time) AS first_synced_time_ms,

    -- 4. Average Sync Lag (in minutes)
    AVG(
            CASE
                WHEN synced_time IS NOT NULL AND created_time IS NOT NULL
                    THEN GREATEST((synced_time - created_time), 0) / 60000.0
                ELSE NULL
                END
    ) AS avg_sync_lag_minutes,

    -- 5. Sync Distribution Buckets
    COUNT(CASE WHEN (synced_time - created_time) / 60000.0 < 60 THEN 1 END) AS sync_within_1hr,
    COUNT(
            CASE WHEN (synced_time - created_time) / 60000.0 >= 60
                AND (synced_time - created_time) / 60000.0 < 360 THEN 1 END
    ) AS sync_1_6hr,
    COUNT(
            CASE WHEN (synced_time - created_time) / 60000.0 >= 360
                AND (synced_time - created_time) / 60000.0 <= 1440 THEN 1 END
    ) AS sync_6_24hr,
    COUNT(CASE WHEN (synced_time - created_time) / 60000.0 > 1440 THEN 1 END) AS sync_over_24hr

FROM project_task_enriched
WHERE task_type IS NOT NULL
GROUP BY
    campaign_id,
    user_name,
    TO_TIMESTAMP(created_time / 1000)::DATE,
    locality_code; -- Added to the GROUP BY clause

-- Update the index to match the new grain
CREATE INDEX IF NOT EXISTS idx_team_perf_daily
    ON dm_team_performance_daily (campaign_id, user_name, task_date, locality_code);


--========================================================
    
                        -- KPI's 
    
--=========================================================
SELECT
    campaign_id,
    user_name,
    task_date,
    locality_code,

    -- ==========================================
    -- KPI 1: Children Vaccinated & Submission Velocity (Per Locality)
    -- ==========================================
    total_vaccinated AS children_vaccinated_in_locality,
    total_submissions AS locality_submission_velocity,

    -- ==========================================
    -- KPI 2: Average Sync Lag (Per Locality)
    -- ==========================================
    ROUND(avg_sync_lag_minutes::NUMERIC, 2) AS avg_sync_lag_minutes,

    -- ==========================================
    -- KPI 3: Sync Rate (%)
    -- ==========================================
    CASE
        WHEN total_submissions = 0 THEN 0.00
        ELSE ROUND(
                (
                    (sync_within_1hr + sync_1_6hr + sync_6_24hr + sync_over_24hr)::NUMERIC
                    / total_submissions
            ) * 100,
            2
        )
        END AS sync_rate_pct,

    -- ==========================================
    -- KPI 4: Sync Timing Distribution (%)
    -- ==========================================
    CASE WHEN total_submissions = 0 THEN 0.00 ELSE ROUND((sync_within_1hr::NUMERIC / total_submissions) * 100, 2) END AS pct_synced_under_1hr,
    CASE WHEN total_submissions = 0 THEN 0.00 ELSE ROUND((sync_1_6hr::NUMERIC / total_submissions) * 100, 2) END AS pct_synced_1_to_6hr,
    CASE WHEN total_submissions = 0 THEN 0.00 ELSE ROUND((sync_6_24hr::NUMERIC / total_submissions) * 100, 2) END AS pct_synced_6_to_24hr,
    CASE WHEN total_submissions = 0 THEN 0.00 ELSE ROUND((sync_over_24hr::NUMERIC / total_submissions) * 100, 2) END AS pct_synced_over_24hr,

    -- ==========================================
    -- KPI 5: Locality Work Hours
    -- ==========================================
    TO_TIMESTAMP(first_submission_time_ms / 1000) AS locality_start_time,
    TO_TIMESTAMP(last_submission_time_ms / 1000) AS locality_end_time

FROM dm_team_performance_daily;
