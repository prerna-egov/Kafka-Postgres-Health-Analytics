-- ==============================================================================
-- DATA QUALITY KPI MARTS - CLICKHOUSE IMPLEMENTATION
-- Architecture: Refreshable Materialized Views (ClickHouse 24.3+)
-- ==============================================================================

SET allow_experimental_refreshable_materialized_view = 1;

-- ==============================================================================
-- 1. BASE MATERIALIZED VIEW
-- ==============================================================================
-- Depends on dm_targets_base (ClickhouseTables/coverage_kpi_marts_clickhouse.sql)
CREATE MATERIALIZED VIEW mv_project_task_kpi_base
REFRESH EVERY 24 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, task_id)
AS
WITH campaign_dates AS (
    SELECT
        campaign_number,
        MIN(start_date) AS start_date,
        MAX(end_date)   AS end_date,
        MAX(total_days) AS campaign_duration_in_days
    FROM dm_targets_base
    GROUP BY campaign_number
),
task_enriched AS (
    SELECT 
        t.id AS task_id,
        t.campaign_number,
        ifNull(nullIf(t.country_code, ''), 'Unknown') AS country_code,
        ifNull(nullIf(t.health_facility_code, ''), 'Unknown') AS health_facility_code,
        ifNull(nullIf(t.region_code, ''), 'Unknown') AS region_code,
        ifNull(nullIf(t.district_code, ''), 'Unknown') AS district_code,
        ifNull(nullIf(t.settlement_code, ''), 'Unknown') AS settlement_code,
        t.latitude,
        t.longitude,
        t.location_accuracy AS task_location_accuracy,
        t.created_time,
        t.synced_time,
        toDate(toDateTime(t.created_time / 1000)) AS event_date,
        t.user_name AS team_id,
        b.beneficiary_id,
        cd.start_date AS project_start_date,
        cd.end_date AS project_end_date,
        cd.campaign_duration_in_days
    FROM project_task_enriched t
    LEFT JOIN project_beneficiary_enriched b
        ON t.project_beneficiary_client_reference_id = b.client_reference_id
    LEFT JOIN campaign_dates cd
        ON t.campaign_number = cd.campaign_number
)
SELECT 
    task_id,
    campaign_number,
    country_code,
    health_facility_code,
    region_code,
    district_code,
    settlement_code,
    latitude,
    longitude,
    task_location_accuracy,
    created_time,
    synced_time,
    event_date,
    project_start_date,
    project_end_date,
    campaign_duration_in_days,
    -- KPI 4 Duplicate Detection Logic
    multiIf(
        beneficiary_id IS NOT NULL AND count() OVER (PARTITION BY campaign_number, beneficiary_id) > 1, 1,
        0
    ) AS is_duplicate
FROM task_enriched;

-- ==============================================================================
-- 3. BOUNDARY LEVEL DATA MARTS
-- ==============================================================================

-- 3.1 Country Code Data Mart
CREATE MATERIALIZED VIEW datamart_country_code
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, boundary_hierarchy_code)
AS
SELECT 
    campaign_number,
    country_code AS boundary_hierarchy_code,
    
    countIf(latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180) * 100.0 / nullIf(count(), 0) AS gps_coverage_percentage,
    
    quantile(0.1)(task_location_accuracy) AS gps_accuracy_p10,
    quantile(0.5)(task_location_accuracy) AS gps_accuracy_p50,
    quantile(0.9)(task_location_accuracy) AS gps_accuracy_p90,
    countIf(task_location_accuracy > 50) AS gps_accuracy_gt_50m_count,
    
    countIf(created_time < synced_time AND event_date >= project_start_date AND event_date <= project_end_date) * 100.0 / nullIf(count(), 0) AS timestamp_consistency_rate,
    max(campaign_duration_in_days) AS campaign_duration_days,
    
    sum(is_duplicate) * 100.0 / nullIf(count(), 0) AS duplicate_percentage
    
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_number, 
    country_code;

-- 3.2 Health Center Code Data Mart
CREATE MATERIALIZED VIEW datamart_healthfacility_code
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, boundary_hierarchy_code)
AS
SELECT 
    campaign_number,
    health_facility_code AS boundary_hierarchy_code,
    countIf(latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180) * 100.0 / nullIf(count(), 0) AS gps_coverage_percentage,
    quantile(0.1)(task_location_accuracy) AS gps_accuracy_p10,
    quantile(0.5)(task_location_accuracy) AS gps_accuracy_p50,
    quantile(0.9)(task_location_accuracy) AS gps_accuracy_p90,
    countIf(task_location_accuracy > 50) AS gps_accuracy_gt_50m_count,
    countIf(created_time < synced_time AND event_date >= project_start_date AND event_date <= project_end_date) * 100.0 / nullIf(count(), 0) AS timestamp_consistency_rate,
    max(campaign_duration_in_days) AS campaign_duration_days,
    sum(is_duplicate) * 100.0 / nullIf(count(), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_number, 
    health_facility_code;

-- 3.3 Region Code Data Mart
CREATE MATERIALIZED VIEW datamart_region_code
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, boundary_hierarchy_code)
AS
SELECT 
    campaign_number,
    region_code AS boundary_hierarchy_code,
    countIf(latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180) * 100.0 / nullIf(count(), 0) AS gps_coverage_percentage,
    quantile(0.1)(task_location_accuracy) AS gps_accuracy_p10,
    quantile(0.5)(task_location_accuracy) AS gps_accuracy_p50,
    quantile(0.9)(task_location_accuracy) AS gps_accuracy_p90,
    countIf(task_location_accuracy > 50) AS gps_accuracy_gt_50m_count,
    countIf(created_time < synced_time AND event_date >= project_start_date AND event_date <= project_end_date) * 100.0 / nullIf(count(), 0) AS timestamp_consistency_rate,
    max(campaign_duration_in_days) AS campaign_duration_days,
    sum(is_duplicate) * 100.0 / nullIf(count(), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_number, 
    region_code;

-- 3.4 District Code Data Mart
CREATE MATERIALIZED VIEW datamart_district_code
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, boundary_hierarchy_code)
AS
SELECT 
    campaign_number,
    district_code AS boundary_hierarchy_code,
    countIf(latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180) * 100.0 / nullIf(count(), 0) AS gps_coverage_percentage,
    quantile(0.1)(task_location_accuracy) AS gps_accuracy_p10,
    quantile(0.5)(task_location_accuracy) AS gps_accuracy_p50,
    quantile(0.9)(task_location_accuracy) AS gps_accuracy_p90,
    countIf(task_location_accuracy > 50) AS gps_accuracy_gt_50m_count,
    countIf(created_time < synced_time AND event_date >= project_start_date AND event_date <= project_end_date) * 100.0 / nullIf(count(), 0) AS timestamp_consistency_rate,
    max(campaign_duration_in_days) AS campaign_duration_days,
    sum(is_duplicate) * 100.0 / nullIf(count(), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_number, 
    district_code;

-- 3.5 Settlement Code Data Mart
CREATE MATERIALIZED VIEW datamart_settlement_code
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, boundary_hierarchy_code)
AS
SELECT 
    campaign_number,
    settlement_code AS boundary_hierarchy_code,
    countIf(latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180) * 100.0 / nullIf(count(), 0) AS gps_coverage_percentage,
    quantile(0.1)(task_location_accuracy) AS gps_accuracy_p10,
    quantile(0.5)(task_location_accuracy) AS gps_accuracy_p50,
    quantile(0.9)(task_location_accuracy) AS gps_accuracy_p90,
    countIf(task_location_accuracy > 50) AS gps_accuracy_gt_50m_count,
    countIf(created_time < synced_time AND event_date >= project_start_date AND event_date <= project_end_date) * 100.0 / nullIf(count(), 0) AS timestamp_consistency_rate,
    max(campaign_duration_in_days) AS campaign_duration_days,
    sum(is_duplicate) * 100.0 / nullIf(count(), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_number, 
    settlement_code;
