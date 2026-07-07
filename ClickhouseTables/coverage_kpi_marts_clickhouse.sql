-- ==========================================================================
-- COVERAGE OVERVIEW KPI DATA MARTS - CLICKHOUSE IMPLEMENTATION
-- Architecture: Refreshable Materialized Views (ClickHouse 24.3+)
-- ==========================================================================

SET allow_experimental_refreshable_materialized_view = 1;

-- ==========================================================================
-- SECTION 1: FOUNDATIONAL BASE MARTS (Refreshable Materialized Views)
-- ==========================================================================

-- 1. dm_successful_deliveries_base
CREATE MATERIALIZED VIEW dm_successful_deliveries_base
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (tenant_id, campaign_number, product_name, region_code, district_code, health_facility_code, settlement_code, event_date)
AS 
SELECT
    tenant_id,
    campaign_number,
    region_code,
    district_code,
    health_facility_code,
    settlement_code,
    product_name,
    task_dates AS event_date,
    toUInt64(count()) AS total_vaccinated
FROM project_task_enriched
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
GROUP BY 
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code, 
    product_name, 
    task_dates;


-- 2. dm_targets_base
CREATE MATERIALIZED VIEW dm_targets_base
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (tenant_id, campaign_number, target_type, product_name, region_code, district_code, health_facility_code, settlement_code)
AS
SELECT
    tenant_id,
    campaign_number,
    target_type,
    region_code,
    district_code,
    health_facility_code,
    settlement_code,
    product_name,
    sum(toInt64(overall_target)) AS target_population,
    min(toDate(toDateTime(start_date / 1000))) AS start_date,
    max(toDate(toDateTime(end_date / 1000))) AS end_date,
    max(toInt32(campaign_duration_in_days)) AS total_days
FROM project_enriched
WHERE settlement_code IS NOT NULL 
  AND start_date IS NOT NULL 
  AND end_date IS NOT NULL
GROUP BY 
    tenant_id,
    campaign_number, 
    target_type, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code, 
    product_name;


-- ==========================================================================
-- SECTION 2: DEPENDENT MARTS (Logical Views)
-- ==========================================================================

-- KPI 2: OVERALL COVERAGE RATE
CREATE OR REPLACE VIEW dm_campaign_coverage AS
WITH campaign_deliveries AS (
    SELECT tenant_id, campaign_number, product_name, sum(total_vaccinated) AS total_vaccinated
    FROM dm_successful_deliveries_base
    GROUP BY tenant_id, campaign_number, product_name
),
campaign_targets_cte AS (
    SELECT tenant_id, campaign_number, product_name, sum(target_population) AS target_population
    FROM dm_targets_base
    WHERE target_type = 'INDIVIDUAL'
    GROUP BY tenant_id, campaign_number, product_name
)
SELECT
    t.tenant_id AS tenant_id,
    t.campaign_number AS campaign_number,
    t.product_name AS product_name,
    ifNull(d.total_vaccinated, 0) AS total_vaccinated,
    t.target_population,
    round(ifNull(d.total_vaccinated, 0) / nullIf(t.target_population, 0) * 100, 2) AS coverage_percentage
FROM campaign_targets_cte t
LEFT JOIN campaign_deliveries d ON t.tenant_id = d.tenant_id AND t.campaign_number = d.campaign_number AND t.product_name = d.product_name;


-- KPI 3: HEALTH FACILITY COVERAGE RATE
CREATE OR REPLACE VIEW dm_health_facility_status AS
WITH target_hfs AS (
    SELECT DISTINCT tenant_id, campaign_number, product_name, region_code, district_code, health_facility_code
    FROM dm_targets_base
    WHERE target_type = 'INDIVIDUAL'
      AND health_facility_code IS NOT NULL
),
delivered_hfs AS (
    SELECT DISTINCT tenant_id, campaign_number, product_name, region_code, district_code, health_facility_code
    FROM dm_successful_deliveries_base
    WHERE health_facility_code IS NOT NULL
)
SELECT 
    t.tenant_id AS tenant_id,
    t.campaign_number AS campaign_number,
    t.product_name AS product_name,
    t.region_code AS region_code,
    t.district_code AS district_code,
    t.health_facility_code AS health_facility_code,
    t.health_facility_code IS NOT NULL AS is_targeted,
    d.health_facility_code IS NOT NULL AS is_delivered
FROM target_hfs t
LEFT JOIN delivered_hfs d 
    ON t.tenant_id = d.tenant_id
   AND t.campaign_number = d.campaign_number 
   AND t.product_name = d.product_name
   AND t.region_code = d.region_code
   AND t.district_code = d.district_code
   AND t.health_facility_code = d.health_facility_code;


-- KPI 6: CAMPAIGN COMPLETION FORECAST
CREATE OR REPLACE VIEW dm_campaign_forecast AS
WITH campaign_dimensions AS (
    SELECT
        tenant_id,
        campaign_number,
        min(start_date) AS start_date,
        max(end_date)   AS end_date,
        max(total_days) AS total_days
    FROM dm_targets_base
    GROUP BY tenant_id, campaign_number
),
campaign_stats AS (
    SELECT
        cov.tenant_id,
        cov.campaign_number,
        cov.product_name,
        cov.total_vaccinated,
        cov.target_population,
        cov.coverage_percentage AS current_coverage_rate,
        cd.start_date           AS campaign_start_date,
        cd.end_date             AS campaign_end_date,
        cd.total_days           AS total_campaign_days,
        least(greatest(toInt32(dateDiff('day', cd.start_date, today()) + 1), 1), cd.total_days) AS days_elapsed
    FROM dm_campaign_coverage cov
    JOIN campaign_dimensions cd ON cd.tenant_id = cov.tenant_id AND cd.campaign_number = cov.campaign_number
)
SELECT
    tenant_id,
    campaign_number,
    product_name,
    total_vaccinated,
    target_population,
    current_coverage_rate,
    campaign_start_date,
    campaign_end_date,
    days_elapsed,
    total_campaign_days,
    round((current_coverage_rate / nullIf(days_elapsed, 0)) * total_campaign_days, 2) AS projected_coverage,
    (round((current_coverage_rate / nullIf(days_elapsed, 0)) * total_campaign_days, 2) >= 100.0) AS on_track
FROM campaign_stats;


-- KPI 7: DISTRICT PERFORMANCE SUMMARY
CREATE OR REPLACE VIEW dm_district_performance AS
WITH district_deliveries AS (
    SELECT tenant_id, region_code, district_code, campaign_number, product_name, sum(total_vaccinated) AS delivery_count
    FROM dm_successful_deliveries_base
    GROUP BY tenant_id, region_code, district_code, campaign_number, product_name
),
district_targets_cte AS (
    SELECT
        tenant_id,
        campaign_number,
        region_code,
        district_code,
        product_name,
        sum(target_population) AS target_population,
        min(start_date) AS start_date,
        max(end_date)   AS end_date,
        max(total_days) AS total_days
    FROM dm_targets_base
    WHERE target_type = 'INDIVIDUAL'
    GROUP BY tenant_id, campaign_number, region_code, district_code, product_name
),
district_metrics AS (
    SELECT
        dt.tenant_id AS tenant_id,
        dt.region_code AS region_code,
        dt.district_code AS district_code,
        dt.campaign_number AS campaign_number,
        dt.product_name AS product_name,
        ifNull(dd.delivery_count, 0) AS delivery_count,
        dt.target_population,
        round(ifNull(dd.delivery_count, 0) / nullIf(dt.target_population, 0) * 100, 2) AS actual_coverage,
        dt.start_date AS campaign_start_date,
        dt.end_date   AS campaign_end_date,
        dt.total_days AS total_campaign_days,
        least(greatest(toInt32(dateDiff('day', dt.start_date, today()) + 1), 1), dt.total_days) AS days_elapsed
    FROM district_targets_cte dt
    LEFT JOIN district_deliveries dd
        ON dd.tenant_id = dt.tenant_id
       AND dd.region_code   = dt.region_code
       AND dd.district_code = dt.district_code
       AND dd.campaign_number   = dt.campaign_number
       AND dd.product_name  = dt.product_name
),
district_metrics_computed AS (
    SELECT
        *,
        round(least(days_elapsed / nullIf(total_campaign_days, 0) * 100, 100.00), 2) AS expected_coverage
    FROM district_metrics
)
SELECT
    tenant_id,
    campaign_number,
    region_code,
    district_code,
    product_name,
    delivery_count,
    target_population,
    actual_coverage,
    expected_coverage,
    days_elapsed,
    total_campaign_days,
    campaign_start_date,
    campaign_end_date,
    today() AS snapshot_date,
    rank() OVER (
        PARTITION BY tenant_id, campaign_number, product_name
        ORDER BY actual_coverage DESC NULLS LAST
    ) AS coverage_rank
FROM district_metrics_computed;
