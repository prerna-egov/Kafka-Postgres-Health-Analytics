-- ==========================================================================
-- COVERAGE OVERVIEW KPI DATA MARTS
-- ==========================================================================
-- Generated: 2026-06-29
-- Source Tables: project_task_enriched, project_enriched, project_beneficiary_enriched
-- Join Key: project_task_enriched.project_id = project_enriched.id
-- Delivery Filter: administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
-- ==========================================================================

-- ==========================================================================
-- SECTION 0: SOURCE TABLE INDEXES
-- ==========================================================================
-- project_task_enriched indexes
CREATE INDEX IF NOT EXISTS idx_pte_admin_status ON project_task_enriched (administration_status);
CREATE INDEX IF NOT EXISTS idx_pte_campaign ON project_task_enriched (campaign_id);
CREATE INDEX IF NOT EXISTS idx_pte_admin_campaign ON project_task_enriched (administration_status, campaign_id);
CREATE INDEX IF NOT EXISTS idx_pte_project_id ON project_task_enriched (project_id);
CREATE INDEX IF NOT EXISTS idx_pte_country ON project_task_enriched (country_code);
CREATE INDEX IF NOT EXISTS idx_pte_province ON project_task_enriched (province_code);
CREATE INDEX IF NOT EXISTS idx_pte_district ON project_task_enriched (district_code);
CREATE INDEX IF NOT EXISTS idx_pte_hc ON project_task_enriched (health_center_code);
CREATE INDEX IF NOT EXISTS idx_pte_spp ON project_task_enriched (spp_code);
CREATE INDEX IF NOT EXISTS idx_pte_village ON project_task_enriched (village_code);
CREATE INDEX IF NOT EXISTS idx_pte_task_dates ON project_task_enriched (task_dates);
CREATE INDEX IF NOT EXISTS idx_pte_admin_task_dates ON project_task_enriched (administration_status, task_dates, campaign_id);

-- project_enriched indexes
CREATE INDEX IF NOT EXISTS idx_pe_campaign ON project_enriched (campaign_id);
CREATE INDEX IF NOT EXISTS idx_pe_village_campaign ON project_enriched (campaign_id) WHERE village_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pe_hc_campaign ON project_enriched (campaign_id, health_center_code) WHERE health_center_code IS NOT NULL;

-- project_beneficiary_enriched indexes
CREATE INDEX IF NOT EXISTS idx_pbe_campaign_hc ON project_beneficiary_enriched (campaign_id, health_center_code) WHERE health_center_code IS NOT NULL;


-- ==========================================================================
-- SECTION 1: FOUNDATIONAL BASE MARTS
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_successful_deliveries_base AS
SELECT
    country_code,
    province_code,
    district_code,
    health_center_code,
    spp_code,
    village_code,
    campaign_id,
    TO_DATE(task_dates, 'YYYY-MM-DD') AS event_date,
    COUNT(*) AS successful_delivery_count
FROM project_task_enriched
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
GROUP BY country_code, province_code, district_code, health_center_code, spp_code, village_code, campaign_id, TO_DATE(task_dates, 'YYYY-MM-DD');

CREATE UNIQUE INDEX idx_sd_base_pk ON dm_successful_deliveries_base (
    country_code, province_code, district_code, health_center_code, spp_code, village_code, campaign_id, event_date
);
CREATE INDEX idx_sd_base_campaign ON dm_successful_deliveries_base (campaign_id);
CREATE INDEX idx_sd_base_date ON dm_successful_deliveries_base (event_date);

CREATE MATERIALIZED VIEW dm_enumerated_health_centers AS
SELECT DISTINCT
    country_code,
    province_code,
    district_code,
    health_center_code,
    spp_code,
    village_code,
    campaign_id
FROM project_beneficiary_enriched
WHERE health_center_code IS NOT NULL
  AND is_deleted IS NOT TRUE;

CREATE UNIQUE INDEX idx_enum_hc_pk ON dm_enumerated_health_centers (
    country_code, province_code, district_code, health_center_code, spp_code, village_code, campaign_id
);

CREATE MATERIALIZED VIEW dm_campaign_targets AS
SELECT
    campaign_id,
    SUM(overall_target) AS target_population,
    TO_TIMESTAMP(MIN(start_date) / 1000)::DATE AS start_date,
    TO_TIMESTAMP(MAX(end_date) / 1000)::DATE   AS end_date,
    MAX(campaign_duration_in_days)             AS total_days
FROM project_enriched
WHERE village_code IS NOT NULL
GROUP BY campaign_id;

CREATE UNIQUE INDEX idx_campaign_targets_pk ON dm_campaign_targets (campaign_id);

CREATE MATERIALIZED VIEW dm_district_targets AS
SELECT
    country_code,
    province_code,
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
GROUP BY country_code, province_code, district_code, campaign_id;

CREATE UNIQUE INDEX idx_district_targets_pk ON dm_district_targets (district_code, campaign_id);


-- ==========================================================================
-- KPI 1: TOTAL CHILDREN VACCINATED
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_successful_deliveries_country AS
SELECT country_code, campaign_id, SUM(successful_delivery_count) AS successful_delivery_count
FROM dm_successful_deliveries_base
GROUP BY country_code, campaign_id;

CREATE UNIQUE INDEX idx_sd_country_pk ON dm_successful_deliveries_country (country_code, campaign_id);

CREATE MATERIALIZED VIEW dm_successful_deliveries_province AS
SELECT country_code, province_code, campaign_id, SUM(successful_delivery_count) AS successful_delivery_count
FROM dm_successful_deliveries_base
GROUP BY country_code, province_code, campaign_id;

CREATE UNIQUE INDEX idx_sd_province_pk ON dm_successful_deliveries_province (country_code, province_code, campaign_id);
CREATE INDEX idx_sd_province_drill ON dm_successful_deliveries_province (country_code, campaign_id);

CREATE MATERIALIZED VIEW dm_successful_deliveries_district AS
SELECT country_code, province_code, district_code, campaign_id, SUM(successful_delivery_count) AS successful_delivery_count
FROM dm_successful_deliveries_base
GROUP BY country_code, province_code, district_code, campaign_id;

CREATE UNIQUE INDEX idx_sd_district_pk ON dm_successful_deliveries_district (country_code, province_code, district_code, campaign_id);
CREATE INDEX idx_sd_district_drill ON dm_successful_deliveries_district (province_code, campaign_id);

CREATE MATERIALIZED VIEW dm_successful_deliveries_health_center AS
SELECT country_code, province_code, district_code, health_center_code, campaign_id, SUM(successful_delivery_count) AS successful_delivery_count
FROM dm_successful_deliveries_base
GROUP BY country_code, province_code, district_code, health_center_code, campaign_id;

CREATE UNIQUE INDEX idx_sd_hc_pk ON dm_successful_deliveries_health_center (country_code, province_code, district_code, health_center_code, campaign_id);
CREATE INDEX idx_sd_hc_drill ON dm_successful_deliveries_health_center (district_code, campaign_id);

CREATE MATERIALIZED VIEW dm_successful_deliveries_spp AS
SELECT country_code, province_code, district_code, health_center_code, spp_code, campaign_id, SUM(successful_delivery_count) AS successful_delivery_count
FROM dm_successful_deliveries_base
GROUP BY country_code, province_code, district_code, health_center_code, spp_code, campaign_id;

CREATE UNIQUE INDEX idx_sd_spp_pk ON dm_successful_deliveries_spp (country_code, province_code, district_code, health_center_code, spp_code, campaign_id);
CREATE INDEX idx_sd_spp_drill ON dm_successful_deliveries_spp (health_center_code, campaign_id);

CREATE MATERIALIZED VIEW dm_successful_deliveries_village AS
SELECT country_code, province_code, district_code, health_center_code, spp_code, village_code, campaign_id, SUM(successful_delivery_count) AS successful_delivery_count
FROM dm_successful_deliveries_base
GROUP BY country_code, province_code, district_code, health_center_code, spp_code, village_code, campaign_id;

CREATE UNIQUE INDEX idx_sd_village_pk ON dm_successful_deliveries_village (country_code, province_code, district_code, health_center_code, spp_code, village_code, campaign_id);
CREATE INDEX idx_sd_village_drill ON dm_successful_deliveries_village (spp_code, campaign_id);


-- ==========================================================================
-- KPI 2: OVERALL COVERAGE RATE
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_campaign_coverage AS
WITH campaign_deliveries AS (
    SELECT campaign_id, SUM(successful_delivery_count) AS successful_delivery_count
    FROM dm_successful_deliveries_country
    GROUP BY campaign_id
)
SELECT
    t.campaign_id,
    COALESCE(d.successful_delivery_count, 0) AS successful_delivery_count,
    t.target_population,
    ROUND(
        COALESCE(d.successful_delivery_count, 0)::NUMERIC / NULLIF(t.target_population, 0) * 100, 2
    ) AS coverage_percentage
FROM dm_campaign_targets t
LEFT JOIN campaign_deliveries d ON t.campaign_id = d.campaign_id;

CREATE UNIQUE INDEX idx_campaign_coverage_pk ON dm_campaign_coverage (campaign_id);


-- ==========================================================================
-- KPI 3: HEALTH FACILITY COVERAGE RATE
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_health_facility_coverage AS
SELECT
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(
        COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2
    ), 100.00) AS coverage_percentage
FROM (
    SELECT campaign_id, COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM dm_enumerated_health_centers
    GROUP BY campaign_id
) enum_hf
LEFT JOIN (
    SELECT campaign_id, COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM dm_successful_deliveries_health_center
    WHERE health_center_code IS NOT NULL
    GROUP BY campaign_id
) succ_hf ON succ_hf.campaign_id = enum_hf.campaign_id;

CREATE UNIQUE INDEX idx_hf_coverage_pk ON dm_health_facility_coverage (campaign_id);


-- ==========================================================================
-- KPI 3B: INACTIVE HEALTH FACILITIES
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_inactive_health_facilities AS
SELECT
    enum_hf.campaign_id,
    enum_hf.health_center_code
FROM dm_enumerated_health_centers enum_hf
LEFT JOIN dm_successful_deliveries_health_center succ_hf
    ON enum_hf.campaign_id = succ_hf.campaign_id
   AND enum_hf.health_center_code = succ_hf.health_center_code
WHERE succ_hf.health_center_code IS NULL
   OR succ_hf.successful_delivery_count = 0
GROUP BY
    enum_hf.campaign_id,
    enum_hf.health_center_code;

CREATE UNIQUE INDEX idx_inactive_hf_pk ON dm_inactive_health_facilities (campaign_id, health_center_code);

-- ==========================================================================
-- KPI 4: HEALTH FACILITY COVERAGE RATE BY HIERARCHY
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_hf_coverage_country AS
SELECT
    enum_hf.country_code AS boundary_code,
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2), 100.00) AS coverage_percentage
FROM (
    SELECT country_code, campaign_id, COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM dm_enumerated_health_centers
    GROUP BY country_code, campaign_id
) enum_hf
LEFT JOIN (
    SELECT country_code, campaign_id, COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM dm_successful_deliveries_health_center
    WHERE health_center_code IS NOT NULL
    GROUP BY country_code, campaign_id
) succ_hf ON succ_hf.country_code = enum_hf.country_code AND succ_hf.campaign_id = enum_hf.campaign_id;

CREATE UNIQUE INDEX idx_hf_cov_country_pk ON dm_hf_coverage_country (boundary_code, campaign_id);

CREATE MATERIALIZED VIEW dm_hf_coverage_province AS
SELECT
    enum_hf.country_code,
    enum_hf.province_code AS boundary_code,
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2), 100.00) AS coverage_percentage
FROM (
    SELECT country_code, province_code, campaign_id, COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM dm_enumerated_health_centers
    GROUP BY country_code, province_code, campaign_id
) enum_hf
LEFT JOIN (
    SELECT country_code, province_code, campaign_id, COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM dm_successful_deliveries_health_center
    WHERE health_center_code IS NOT NULL
    GROUP BY country_code, province_code, campaign_id
) succ_hf ON succ_hf.country_code = enum_hf.country_code AND succ_hf.province_code = enum_hf.province_code AND succ_hf.campaign_id = enum_hf.campaign_id;

CREATE UNIQUE INDEX idx_hf_cov_province_pk ON dm_hf_coverage_province (country_code, boundary_code, campaign_id);
CREATE INDEX idx_hf_cov_prov_drill ON dm_hf_coverage_province (country_code, campaign_id);

CREATE MATERIALIZED VIEW dm_hf_coverage_district AS
SELECT
    enum_hf.country_code,
    enum_hf.province_code,
    enum_hf.district_code AS boundary_code,
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2), 100.00) AS coverage_percentage
FROM (
    SELECT country_code, province_code, district_code, campaign_id, COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM dm_enumerated_health_centers
    GROUP BY country_code, province_code, district_code, campaign_id
) enum_hf
LEFT JOIN (
    SELECT country_code, province_code, district_code, campaign_id, COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM dm_successful_deliveries_health_center
    WHERE health_center_code IS NOT NULL
    GROUP BY country_code, province_code, district_code, campaign_id
) succ_hf ON succ_hf.country_code = enum_hf.country_code AND succ_hf.province_code = enum_hf.province_code AND succ_hf.district_code = enum_hf.district_code AND succ_hf.campaign_id = enum_hf.campaign_id;

CREATE UNIQUE INDEX idx_hf_cov_district_pk ON dm_hf_coverage_district (country_code, province_code, boundary_code, campaign_id);
CREATE INDEX idx_hf_cov_dist_drill ON dm_hf_coverage_district (province_code, campaign_id);



-- ==========================================================================
-- KPI 5: DAILY COVERAGE RATE
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_latest_daily_deliveries AS
WITH max_dates AS (
    SELECT campaign_id, MAX(event_date) AS max_reporting_date
    FROM dm_successful_deliveries_base
    GROUP BY campaign_id
),
latest_two_days AS (
    SELECT
        b.campaign_id,
        b.event_date,
        SUM(b.successful_delivery_count) AS daily_delivery_count
    FROM dm_successful_deliveries_base b
    JOIN max_dates m ON b.campaign_id = m.campaign_id
    WHERE b.event_date IN (m.max_reporting_date, m.max_reporting_date - INTERVAL '1 day')
    GROUP BY b.campaign_id, b.event_date
)
SELECT
    curr.campaign_id,
    curr.event_date AS max_reporting_date,
    curr.daily_delivery_count AS kpi_value,
    COALESCE(prev.daily_delivery_count, 0) AS previous_day_value,
    curr.daily_delivery_count - COALESCE(prev.daily_delivery_count, 0) AS delta_from_yesterday
FROM latest_two_days curr
JOIN max_dates md ON curr.campaign_id = md.campaign_id AND curr.event_date = md.max_reporting_date
LEFT JOIN latest_two_days prev
    ON prev.campaign_id = curr.campaign_id
   AND prev.event_date = curr.event_date - INTERVAL '1 day';

CREATE UNIQUE INDEX idx_latest_daily_pk ON dm_latest_daily_deliveries (campaign_id);


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
        cov.successful_delivery_count,
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
    successful_delivery_count,
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


-- ==========================================================================
-- KPI 7: DISTRICT PERFORMANCE SUMMARY
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_district_performance AS
WITH district_deliveries AS (
    SELECT district_code, campaign_id, SUM(successful_delivery_count) AS delivery_count
    FROM dm_successful_deliveries_base
    GROUP BY district_code, campaign_id
),
district_metrics AS (
    SELECT
        dt.country_code,
        dt.province_code,
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
    province_code,
    district_code,
    campaign_id,
    delivery_count,
    target_population,
    actual_coverage,
    expected_coverage,
    ROUND(actual_coverage - expected_coverage, 2) AS variance,
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

CREATE UNIQUE INDEX idx_dist_perf_pk ON dm_district_performance (district_code, campaign_id);
CREATE INDEX idx_dist_perf_campaign ON dm_district_performance (campaign_id);
CREATE INDEX idx_dist_perf_province ON dm_district_performance (province_code, campaign_id);
CREATE INDEX idx_dist_perf_rank ON dm_district_performance (campaign_id, coverage_rank);
CREATE INDEX idx_dist_perf_variance ON dm_district_performance (campaign_id, variance DESC);


-- ==========================================================================
-- SECTION: KPI RETRIEVAL QUERIES
-- ==========================================================================

-- KPI 1: Total Children Vaccinated
SELECT campaign_id, successful_delivery_count FROM dm_successful_deliveries_country WHERE country_code = :country_code AND campaign_id = :campaign_id;
SELECT province_code, successful_delivery_count FROM dm_successful_deliveries_province WHERE country_code = :country_code AND campaign_id = :campaign_id ORDER BY successful_delivery_count DESC;
SELECT district_code, successful_delivery_count FROM dm_successful_deliveries_district WHERE province_code = :province_code AND campaign_id = :campaign_id ORDER BY successful_delivery_count DESC;
SELECT health_center_code, successful_delivery_count FROM dm_successful_deliveries_health_center WHERE district_code = :district_code AND campaign_id = :campaign_id ORDER BY successful_delivery_count DESC;
SELECT spp_code, successful_delivery_count FROM dm_successful_deliveries_spp WHERE health_center_code = :health_center_code AND campaign_id = :campaign_id ORDER BY successful_delivery_count DESC;
SELECT village_code, successful_delivery_count FROM dm_successful_deliveries_village WHERE spp_code = :spp_code AND campaign_id = :campaign_id ORDER BY successful_delivery_count DESC;

-- KPI 2: Overall Coverage Rate
SELECT campaign_id, successful_delivery_count, target_population, coverage_percentage FROM dm_campaign_coverage WHERE campaign_id = :campaign_id;
SELECT campaign_id, coverage_percentage, successful_delivery_count, target_population FROM dm_campaign_coverage ORDER BY campaign_id;

-- KPI 3: Health Facility Coverage Rate
SELECT campaign_id, enumerated_health_facilities, successful_health_facilities, coverage_percentage FROM dm_health_facility_coverage WHERE campaign_id = :campaign_id;

-- KPI 4: HF Coverage by Hierarchy
SELECT boundary_code AS country_code, enumerated_health_facilities, successful_health_facilities, coverage_percentage FROM dm_hf_coverage_country WHERE campaign_id = :campaign_id;
SELECT boundary_code AS province_code, enumerated_health_facilities, successful_health_facilities, coverage_percentage FROM dm_hf_coverage_province WHERE country_code = :country_code AND campaign_id = :campaign_id ORDER BY coverage_percentage DESC;
SELECT boundary_code AS district_code, enumerated_health_facilities, successful_health_facilities, coverage_percentage FROM dm_hf_coverage_district WHERE province_code = :province_code AND campaign_id = :campaign_id ORDER BY coverage_percentage DESC;


-- KPI 5: Daily Coverage Rate
SELECT campaign_id, max_reporting_date, kpi_value AS todays_delivery_count, previous_day_value AS yesterdays_delivery_count, delta_from_yesterday FROM dm_latest_daily_deliveries WHERE campaign_id = :campaign_id;
SELECT event_date, SUM(successful_delivery_count) AS daily_delivery_count FROM dm_successful_deliveries_base WHERE campaign_id = :campaign_id AND event_date >= CURRENT_DATE - INTERVAL '7 days' AND event_date IS NOT NULL GROUP BY event_date ORDER BY event_date;
SELECT event_date, SUM(successful_delivery_count) AS daily_delivery_count FROM dm_successful_deliveries_base WHERE campaign_id = :campaign_id AND event_date IS NOT NULL GROUP BY event_date ORDER BY event_date;

-- KPI 6: Campaign Completion Forecast
SELECT campaign_id, current_coverage_rate, days_elapsed, total_campaign_days, projected_coverage, on_track, campaign_start_date, campaign_end_date, total_campaign_days - days_elapsed AS days_remaining FROM dm_campaign_forecast WHERE campaign_id = :campaign_id;
SELECT campaign_id, current_coverage_rate, projected_coverage, on_track, total_campaign_days - days_elapsed AS days_remaining FROM dm_campaign_forecast ORDER BY on_track, projected_coverage DESC;
SELECT
    dc.event_date,
    SUM(SUM(dc.successful_delivery_count)) OVER (ORDER BY dc.event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_deliveries,
    ROUND(SUM(SUM(dc.successful_delivery_count)) OVER (ORDER BY dc.event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::NUMERIC / NULLIF(MAX(fc.target_population), 0) * 100, 2) AS cumulative_coverage_pct
FROM dm_successful_deliveries_base dc
JOIN dm_campaign_forecast fc ON fc.campaign_id = dc.campaign_id
WHERE dc.campaign_id = :campaign_id AND dc.event_date IS NOT NULL
GROUP BY dc.event_date
ORDER BY dc.event_date;

-- KPI 7: District Performance Summary
SELECT district_code, country_code, province_code, delivery_count, target_population, actual_coverage, expected_coverage, variance, coverage_rank FROM dm_district_performance WHERE campaign_id = :campaign_id ORDER BY coverage_rank;
SELECT district_code, actual_coverage, expected_coverage, variance FROM dm_district_performance WHERE campaign_id = :campaign_id ORDER BY actual_coverage DESC LIMIT 5;
SELECT district_code, actual_coverage, expected_coverage, variance FROM dm_district_performance WHERE campaign_id = :campaign_id ORDER BY actual_coverage ASC LIMIT 5;
SELECT district_code, actual_coverage, expected_coverage, variance, coverage_rank FROM dm_district_performance WHERE province_code = :province_code AND campaign_id = :campaign_id ORDER BY coverage_rank;
SELECT district_code, province_code, actual_coverage, expected_coverage, variance FROM dm_district_performance WHERE campaign_id = :campaign_id AND variance < 0 ORDER BY variance ASC;


-- ==========================================================================
-- SECTION: REFRESH STRATEGY
-- ==========================================================================

-- STEP 1: Independent base marts
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_successful_deliveries_base;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_enumerated_health_centers;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_campaign_coverage;

-- STEP 2: KPI 1 (Reads from dm_successful_deliveries_base)
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_successful_deliveries_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_successful_deliveries_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_successful_deliveries_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_successful_deliveries_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_successful_deliveries_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_successful_deliveries_village;

-- STEP 3: Dependent marts
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_health_facility_coverage;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_hf_coverage_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_hf_coverage_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_hf_coverage_district;

REFRESH MATERIALIZED VIEW CONCURRENTLY dm_latest_daily_deliveries;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_campaign_forecast;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_district_performance;
