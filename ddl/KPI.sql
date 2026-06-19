-- ============================================================================
-- ============================================================================
--                             DATA MART 1 KPIs
--                                COVERAGE
-- ============================================================================
-- ============================================================================

-- Total Children Vaccinated & Could Not Vaccinate (Overall Campaign)
SELECT
    campaign_id,
    SUM(total_administered_successfully) AS total_children_vaccinated,
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
    health_center_code,
    SUM(total_administered_successfully) AS hc_children_vaccinated,
    ROUND((SUM(total_administered_successfully)::NUMERIC / NULLIF(SUM(total_task_submissions), 0)) * 100, 2) AS hc_coverage_rate
FROM dm_coverage_daily_healthcenter
GROUP BY country_code, province_code, district_code, health_center_code;


-- ============================================================================
-- ============================================================================
--                             DATA MART 2 KPIs
--                        REGISTRATION / ENUMERATION
-- ============================================================================
-- ============================================================================

-- Consolidated KPI 1, 2, 3 & 4 (Country)
SELECT
    campaign_id, country_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females,
    SUM(total_households) AS total_households_registered
FROM dm_registration_daily_country
GROUP BY campaign_id, country_code;

-- Consolidated KPI 1, 2, 3 & 4 (Province)
SELECT
    campaign_id, country_code, province_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females,
    SUM(total_households) AS total_households_registered
FROM dm_registration_daily_province
GROUP BY campaign_id, country_code, province_code;

-- Consolidated KPI 1, 2, 3 & 4 (District)
SELECT
    campaign_id, country_code, province_code, district_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females,
    SUM(total_households) AS total_households_registered
FROM dm_registration_daily_district
GROUP BY campaign_id, country_code, province_code, district_code;

-- Consolidated KPI 1, 2, 3 & 4 (Health Center)
SELECT
    campaign_id, country_code, province_code, district_code, health_center_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females,
    SUM(total_households) AS total_households_registered
FROM dm_registration_daily_healthcenter
GROUP BY campaign_id, country_code, province_code, district_code, health_center_code;

-- Consolidated KPI 1, 2, 3 & 4 (SPP)
SELECT
    campaign_id, country_code, province_code, district_code, health_center_code, spp_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females,
    SUM(total_households) AS total_households_registered
FROM dm_registration_daily_spp
GROUP BY campaign_id, country_code, province_code, district_code, health_center_code, spp_code;

-- Consolidated KPI 1, 2, 3 & 4 (Village)
SELECT
    campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code,
    SUM(children_0_11m + children_12_23m + children_24_59m) AS total_children_enumerated,
    SUM(children_0_11m) AS sum_0_to_11_months,
    SUM(children_12_23m) AS sum_12_to_23_months,
    SUM(children_24_59m) AS sum_24_to_59_months,
    SUM(male_count) AS total_males,
    SUM(female_count) AS total_females,
    SUM(total_households) AS total_households_registered
FROM dm_registration_daily_village
GROUP BY campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code;


-- ============================================================================
-- ============================================================================
--                             DATA MART 3 KPIs
--                                REFUSALS
-- ============================================================================
-- ============================================================================

-- KPI 1: REFUSAL / ABSENCE REASONS BREAKDOWN
SELECT
    campaign_id, country_code, reason_type, reason_code,
    SUM(refusal_count) AS total_refusals, SUM(absence_count) AS total_absences
FROM dm_refusals_daily_country
WHERE reason_type IN ('REFUSAL', 'ABSENCE')
GROUP BY campaign_id, country_code, reason_type, reason_code
ORDER BY total_refusals DESC, total_absences DESC;

-- KPI 2: TOTAL CAMPAIGN REFUSAL & ABSENCE RATES (COUNTRY)
WITH DailyTotals AS (
    SELECT campaign_id, country_code, task_date, SUM(refusal_count) AS daily_refusals, SUM(absence_count) AS daily_absences, MAX(total_task_submissions) AS daily_tasks
    FROM dm_refusals_daily_country GROUP BY campaign_id, country_code, task_date
)
SELECT
    campaign_id, country_code, SUM(daily_refusals) AS total_refusals, SUM(daily_absences) AS total_absences, SUM(daily_tasks) AS overarching_task_total,
    (SUM(daily_refusals)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS refusal_rate_percentage,
    (SUM(daily_absences)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS absence_rate_percentage
FROM DailyTotals GROUP BY campaign_id, country_code;

-- KPI 3: REFUSAL & ABSENCE RATES (PROVINCE)
WITH ProvinceDailyTotals AS (
    SELECT campaign_id, country_code, province_code, task_date, SUM(refusal_count) AS daily_refusals, SUM(absence_count) AS daily_absences, MAX(total_task_submissions) AS daily_tasks
    FROM dm_refusals_daily_province GROUP BY campaign_id, country_code, province_code, task_date
)
SELECT
    campaign_id, country_code, province_code, SUM(daily_refusals) AS province_refusals, SUM(daily_absences) AS province_absences, SUM(daily_tasks) AS province_task_total,
    (SUM(daily_refusals)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS province_refusal_rate,
    (SUM(daily_absences)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS province_absence_rate
FROM ProvinceDailyTotals GROUP BY campaign_id, country_code, province_code;

-- KPI 4: REFUSAL & ABSENCE RATES (DISTRICT)
WITH DistrictDailyTotals AS (
    SELECT campaign_id, country_code, province_code, district_code, task_date, SUM(refusal_count) AS daily_refusals, SUM(absence_count) AS daily_absences, MAX(total_task_submissions) AS daily_tasks
    FROM dm_refusals_daily_district GROUP BY campaign_id, country_code, province_code, district_code, task_date
)
SELECT
    campaign_id, country_code, province_code, district_code, SUM(daily_refusals) AS district_refusals, SUM(daily_absences) AS district_absences, SUM(daily_tasks) AS district_task_total,
    (SUM(daily_refusals)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS district_refusal_rate,
    (SUM(daily_absences)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS district_absence_rate
FROM DistrictDailyTotals GROUP BY campaign_id, country_code, province_code, district_code;

-- KPI 5: REFUSAL & ABSENCE RATES (HEALTH CENTER)
WITH HealthCenterDailyTotals AS (
    SELECT campaign_id, country_code, province_code, district_code, health_center_code, task_date, SUM(refusal_count) AS daily_refusals, SUM(absence_count) AS daily_absences, MAX(total_task_submissions) AS daily_tasks
    FROM dm_refusals_daily_healthcenter GROUP BY campaign_id, country_code, province_code, district_code, health_center_code, task_date
)
SELECT
    campaign_id, country_code, province_code, district_code, health_center_code, SUM(daily_refusals) AS hc_refusals, SUM(daily_absences) AS hc_absences, SUM(daily_tasks) AS hc_task_total,
    (SUM(daily_refusals)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS hc_refusal_rate,
    (SUM(daily_absences)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS hc_absence_rate
FROM HealthCenterDailyTotals GROUP BY campaign_id, country_code, province_code, district_code, health_center_code;

-- KPI 6: REFUSAL & ABSENCE RATES (SPP)
WITH SppDailyTotals AS (
    SELECT campaign_id, country_code, province_code, district_code, health_center_code, spp_code, task_date, SUM(refusal_count) AS daily_refusals, SUM(absence_count) AS daily_absences, MAX(total_task_submissions) AS daily_tasks
    FROM dm_refusals_daily_spp GROUP BY campaign_id, country_code, province_code, district_code, health_center_code, spp_code, task_date
)
SELECT
    campaign_id, country_code, province_code, district_code, health_center_code, spp_code, SUM(daily_refusals) AS spp_refusals, SUM(daily_absences) AS spp_absences, SUM(daily_tasks) AS spp_task_total,
    (SUM(daily_refusals)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS spp_refusal_rate,
    (SUM(daily_absences)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS spp_absence_rate
FROM SppDailyTotals GROUP BY campaign_id, country_code, province_code, district_code, health_center_code, spp_code;

-- KPI 7: REFUSAL & ABSENCE RATES (VILLAGE)
WITH VillageDailyTotals AS (
    SELECT campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code, task_date, SUM(refusal_count) AS daily_refusals, SUM(absence_count) AS daily_absences, MAX(total_task_submissions) AS daily_tasks
    FROM dm_refusals_daily_village GROUP BY campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code, task_date
)
SELECT
    campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code, SUM(daily_refusals) AS village_refusals, SUM(daily_absences) AS village_absences, SUM(daily_tasks) AS village_task_total,
    (SUM(daily_refusals)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS village_refusal_rate,
    (SUM(daily_absences)::DECIMAL / NULLIF(SUM(daily_tasks), 0)) * 100 AS village_absence_rate
FROM VillageDailyTotals GROUP BY campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code;


-- ============================================================================
-- ============================================================================
--                             DATA MART 4 KPIs
--                             STOCK INVENTORY
-- ============================================================================
-- ============================================================================

SELECT
    campaign_id,
    facility_id,
    facility_name,
    facility_level,
    product_name,
    transaction_date,

    -- KPI 1: Vials Issued per Team/Day
    total_issued AS vials_issued,

    -- KPI 2: Doses Administered (Estimated)
    (total_issued - total_returned) * 10 AS estimated_doses_administered,

    -- KPI 3: Wastage Rate (%)
    CASE
        WHEN (total_issued - total_returned + total_damaged) = 0 THEN 0.00
        ELSE ROUND(
                (total_damaged::NUMERIC / (total_issued - total_returned + total_damaged)) * 100,
                2
             )
        END AS wastage_rate_pct,

    -- KPI 4: Stock-out Risk (%)
    CASE
        WHEN facility_target = 0 OR facility_target IS NULL THEN NULL
        ELSE ROUND(
                (calculated_balance::NUMERIC / facility_target) * 100,
                2
             )
        END AS stock_out_risk_pct,

    last_movement_timestamp

FROM dm_stock_daily;


-- ============================================================================
-- ============================================================================
--                             DATA MART 5 KPIs
--                       TEAM PERFORMANCE / TELEMETRY
-- ============================================================================
-- ============================================================================

SELECT
    campaign_id, user_name, task_date, locality_code, health_center_code,

    -- KPI 1: Children Vaccinated & Submission Velocity (Per Locality)
    total_vaccinated AS children_vaccinated_in_locality,
    total_submissions AS locality_submission_velocity,

    -- KPI 2: Average Sync Lag (Per Locality)
    ROUND(avg_sync_lag_minutes::NUMERIC, 2) AS avg_sync_lag_minutes,

    -- KPI 3: Sync Rate (%)
    CASE
        WHEN total_submissions = 0 THEN 0.00
        ELSE ROUND(((sync_within_1hr + sync_1_6hr + sync_6_24hr + sync_over_24hr)::NUMERIC / total_submissions) * 100, 2)
        END AS sync_rate_pct,

    -- KPI 4: Sync Timing Distribution (%)
    CASE WHEN total_submissions = 0 THEN 0.00 ELSE ROUND((sync_within_1hr::NUMERIC / total_submissions) * 100, 2) END AS pct_synced_under_1hr,
    CASE WHEN total_submissions = 0 THEN 0.00 ELSE ROUND((sync_1_6hr::NUMERIC / total_submissions) * 100, 2) END AS pct_synced_1_to_6hr,
    CASE WHEN total_submissions = 0 THEN 0.00 ELSE ROUND((sync_6_24hr::NUMERIC / total_submissions) * 100, 2) END AS pct_synced_6_to_24hr,
    CASE WHEN total_submissions = 0 THEN 0.00 ELSE ROUND((sync_over_24hr::NUMERIC / total_submissions) * 100, 2) END AS pct_synced_over_24hr,

    -- KPI 5: Locality Work Hours
    TO_TIMESTAMP(first_submission_time_ms / 1000) AS locality_start_time,
    TO_TIMESTAMP(last_submission_time_ms / 1000) AS locality_end_time
FROM dm_team_performance_daily;



-- ============================================================================
-- ============================================================================
--                             DATA MART 8 KPIs
--                             DATA QUALITY
-- ============================================================================
-- ============================================================================

SELECT
    campaign_id,
    task_date,
    locality_code,
    country_code,
    province_code,
    district_code,
    health_center_code,

    total_records,

    -- ==========================================
    -- KPI 1: GPS Capture Rate (%)
    -- ==========================================
    CASE
        WHEN total_records = 0 THEN 0.00
        ELSE ROUND((gps_value_count::NUMERIC / total_records) * 100, 2)
        END AS gps_capture_rate_pct,

    -- ==========================================
    -- KPI 2: GPS Accuracy Risk (%)
    -- (Percentage of captured GPS points with poor accuracy)
    -- ==========================================
    CASE
        WHEN gps_value_count = 0 THEN 0.00
        ELSE ROUND((gps_accuracy_flagged_count::NUMERIC / gps_value_count) * 100, 2)
        END AS gps_poor_accuracy_pct,

    avg_gps_accuracy,

    -- ==========================================
    -- KPI 3: Timestamp Validity Rate (%)
    -- (Created time is logically before synced time)
    -- ==========================================
    CASE
        WHEN total_records = 0 THEN 0.00
        ELSE ROUND((timestamp_valid_count::NUMERIC / total_records) * 100, 2)
        END AS timestamp_validity_rate_pct,

    -- ==========================================
    -- KPI 4: Form Completion Rate (%)
    -- ==========================================
    CASE
        WHEN total_records = 0 THEN 0.00
        ELSE ROUND((form_complete_count::NUMERIC / total_records) * 100, 2)
        END AS form_completion_rate_pct

FROM dm_data_quality_daily;