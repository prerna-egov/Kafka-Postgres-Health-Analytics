-- ==========================================================================
-- COVERAGE OVERVIEW KPI DATA MARTS
-- ==========================================================================
-- Generated: 2026-06-29
-- Source Tables: project_task_enriched, project_enriched, project_beneficiary_enriched
-- Join Key: project_task_enriched.project_id = project_enriched.id
-- Delivery Filter: administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
-- ==========================================================================

-- ==========================================================================
-- SECTION 1: FOUNDATIONAL BASE MARTS
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_successful_deliveries_base AS
SELECT
    country_code,
    region_code,
    district_code,
    healthfacility_code,
    settlement_code,
    campaign_id,
    TO_DATE(task_dates, 'YYYY-MM-DD') AS event_date,
    COUNT(*) AS total_vaccinated
FROM project_task_enriched
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
GROUP BY country_code, region_code, district_code, healthfacility_code, settlement_code, campaign_id, TO_DATE(task_dates, 'YYYY-MM-DD');

CREATE UNIQUE INDEX idx_sd_base_pk ON dm_successful_deliveries_base (
    campaign_id, country_code, region_code, district_code, healthfacility_code, settlement_code, event_date
);
CREATE INDEX idx_sd_base_date ON dm_successful_deliveries_base (event_date);
CREATE INDEX idx_sd_base_camp_date ON dm_successful_deliveries_base (campaign_id, event_date DESC);

CREATE MATERIALIZED VIEW dm_enumerated_health_centers AS
SELECT DISTINCT
    campaign_id,
    country_code,
    region_code,
    district_code,
    healthfacility_code
FROM project_beneficiary_enriched
WHERE healthfacility_code IS NOT NULL
  AND is_deleted IS NOT TRUE;

CREATE UNIQUE INDEX idx_enum_hc_pk ON dm_enumerated_health_centers (
    campaign_id, country_code, region_code, district_code, healthfacility_code
);


CREATE MATERIALIZED VIEW dm_district_targets AS
SELECT
    country_code,
    region_code,
    district_code,
    campaign_id,
    SUM(overall_target) AS target_population,
    TO_TIMESTAMP(MIN(start_date) / 1000)::DATE AS start_date,
    TO_TIMESTAMP(MAX(end_date)   / 1000)::DATE AS end_date,
    MAX(campaign_duration_in_days) AS total_days
FROM project_enriched
WHERE district_code IS NOT NULL
  AND start_date IS NOT NULL
  AND end_date IS NOT NULL
  AND health_center_code IS NULL
GROUP BY country_code, region_code, district_code, campaign_id;

CREATE UNIQUE INDEX idx_district_targets_pk ON dm_district_targets (campaign_id, district_code);



-- ==========================================================================
-- KPI 2: OVERALL COVERAGE RATE (Data Mart)
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_campaign_coverage AS
WITH campaign_deliveries AS (
    SELECT campaign_id, SUM(total_vaccinated) AS total_vaccinated
    FROM dm_successful_deliveries_base
    GROUP BY campaign_id
),
campaign_targets_cte AS (
    SELECT campaign_id, SUM(target_population) AS target_population
    FROM dm_district_targets
    GROUP BY campaign_id
)
SELECT
    t.campaign_id,
    COALESCE(d.total_vaccinated, 0) AS total_vaccinated,
    t.target_population,
    ROUND(
        COALESCE(d.total_vaccinated, 0)::NUMERIC / NULLIF(t.target_population, 0) * 100, 2
    ) AS coverage_percentage
FROM campaign_targets_cte t
LEFT JOIN campaign_deliveries d ON t.campaign_id = d.campaign_id;

CREATE UNIQUE INDEX idx_campaign_coverage_pk ON dm_campaign_coverage (campaign_id);


-- ==========================================================================
-- KPI 3: HEALTH FACILITY COVERAGE RATE
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_health_facility_status AS
SELECT 
    COALESCE(e.campaign_id, d.campaign_id) AS campaign_id,
    COALESCE(e.country_code, d.country_code) AS country_code,
    COALESCE(e.region_code, d.region_code) AS region_code,
    COALESCE(e.district_code, d.district_code) AS district_code,
    COALESCE(e.healthfacility_code, d.healthfacility_code) AS healthfacility_code,
    CASE WHEN e.healthfacility_code IS NOT NULL THEN TRUE ELSE FALSE END AS is_enumerated,
    CASE WHEN COALESCE(d.total_vaccinated, 0) > 0 THEN TRUE ELSE FALSE END AS is_delivered,
    COALESCE(d.total_vaccinated, 0) AS delivery_count
FROM dm_enumerated_health_centers e
FULL OUTER JOIN (
    SELECT country_code, region_code, district_code, healthfacility_code, campaign_id, SUM(total_vaccinated) AS total_vaccinated
    FROM dm_successful_deliveries_base 
    WHERE healthfacility_code IS NOT NULL
    GROUP BY country_code, region_code, district_code, healthfacility_code, campaign_id
) d 
    ON e.campaign_id = d.campaign_id 
   AND e.healthfacility_code = d.healthfacility_code;

CREATE UNIQUE INDEX idx_hf_status_pk ON dm_health_facility_status (campaign_id, healthfacility_code);
CREATE INDEX idx_hf_status_inactive ON dm_health_facility_status (campaign_id, is_enumerated, is_delivered);
-- Geographic drill-down optimization tuples for KPIs 3 & 4
CREATE INDEX IF NOT EXISTS idx_hf_status_camp_country ON dm_health_facility_status (campaign_id, country_code);
CREATE INDEX IF NOT EXISTS idx_hf_status_camp_region ON dm_health_facility_status (campaign_id, region_code);
CREATE INDEX IF NOT EXISTS idx_hf_status_camp_district ON dm_health_facility_status (campaign_id, district_code);






-- ==========================================================================
-- KPI 6: CAMPAIGN COMPLETION FORECAST
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_campaign_forecast AS
WITH campaign_dimensions AS (
    SELECT
        campaign_id,
        TO_TIMESTAMP(MIN(start_date) / 1000)::DATE AS start_date,
        TO_TIMESTAMP(MAX(end_date) / 1000)::DATE   AS end_date,
        MAX(campaign_duration_in_days)             AS total_days
    FROM project_enriched
    WHERE start_date IS NOT NULL
      AND end_date   IS NOT NULL
    GROUP BY campaign_id
),
campaign_stats AS (
    SELECT
        cov.campaign_id,
        cov.total_vaccinated,
        cov.target_population,
        cov.coverage_percentage AS current_coverage_rate,
        cd.start_date           AS campaign_start_date,
        cd.end_date             AS campaign_end_date,
        cd.total_days           AS total_campaign_days,
        LEAST(GREATEST((CURRENT_DATE - cd.start_date) + 1, 1), cd.total_days) AS days_elapsed
    FROM dm_campaign_coverage cov
    JOIN campaign_dimensions cd ON cd.campaign_id = cov.campaign_id
)
SELECT
    campaign_id,
    total_vaccinated,
    target_population,
    current_coverage_rate,
    campaign_start_date,
    campaign_end_date,
    days_elapsed,
    total_campaign_days,
    ROUND((current_coverage_rate / NULLIF(days_elapsed, 0)) * total_campaign_days, 2) AS projected_coverage,
    CASE 
        WHEN ROUND((current_coverage_rate / NULLIF(days_elapsed, 0)) * total_campaign_days, 2) >= 100.0 THEN TRUE
        ELSE FALSE
    END AS on_track
FROM campaign_stats;

CREATE UNIQUE INDEX idx_campaign_forecast_pk ON dm_campaign_forecast (campaign_id);
CREATE INDEX idx_campaign_forecast_leaderboard ON dm_campaign_forecast (on_track, projected_coverage DESC);


-- ==========================================================================
-- KPI 7: DISTRICT PERFORMANCE SUMMARY
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_district_performance AS
WITH district_deliveries AS (
    SELECT district_code, campaign_id, SUM(total_vaccinated) AS delivery_count
    FROM dm_successful_deliveries_base
    GROUP BY district_code, campaign_id
),
district_metrics AS (
    SELECT
        dt.country_code,
        dt.region_code,
        dt.district_code,
        dt.campaign_id,
        COALESCE(dd.delivery_count, 0) AS delivery_count,
        dt.target_population,
        ROUND(COALESCE(dd.delivery_count, 0)::NUMERIC / NULLIF(dt.target_population, 0) * 100, 2) AS actual_coverage,
        dt.start_date AS campaign_start_date,
        dt.end_date   AS campaign_end_date,
        dt.total_days AS total_campaign_days,
        LEAST(GREATEST((CURRENT_DATE - dt.start_date) + 1, 1), dt.total_days) AS days_elapsed
    FROM dm_district_targets dt
    LEFT JOIN district_deliveries dd
        ON dd.district_code = dt.district_code
       AND dd.campaign_id   = dt.campaign_id
),
district_metrics_computed AS (
    SELECT
        *,
        ROUND(LEAST(days_elapsed::NUMERIC / NULLIF(total_campaign_days, 0) * 100, 100.00), 2) AS expected_coverage
    FROM district_metrics
)
SELECT
    country_code,
    region_code,
    district_code,
    campaign_id,
    delivery_count,
    target_population,
    actual_coverage,
    expected_coverage,
    days_elapsed,
    total_campaign_days,
    campaign_start_date,
    campaign_end_date,
    CURRENT_DATE AS snapshot_date,
    RANK() OVER (
        PARTITION BY campaign_id
        ORDER BY actual_coverage DESC NULLS LAST
    ) AS coverage_rank
FROM district_metrics_computed;

CREATE UNIQUE INDEX idx_dist_perf_pk ON dm_district_performance (campaign_id, district_code);
CREATE INDEX idx_dist_perf_province ON dm_district_performance (campaign_id, region_code);
CREATE INDEX idx_dist_perf_rank ON dm_district_performance (campaign_id, coverage_rank);
CREATE INDEX idx_dist_perf_coverage ON dm_district_performance (campaign_id, actual_coverage);



-- ==========================================================================
-- SECTION: REFRESH STRATEGY
-- ==========================================================================

-- STEP 1: Independent base marts
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_successful_deliveries_base;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_enumerated_health_centers;



-- STEP 3: Dependent marts
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_campaign_coverage;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_health_facility_status;

REFRESH MATERIALIZED VIEW CONCURRENTLY dm_campaign_forecast;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_district_performance;
