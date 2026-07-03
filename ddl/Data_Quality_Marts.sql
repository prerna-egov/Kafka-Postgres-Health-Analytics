-- ==============================================================================
-- 1. BASE MATERIALIZED VIEW
-- ==============================================================================
-- Depends on dm_targets_base (ddl/coverage_kpi_data_marts.sql) — apply that file first.
CREATE MATERIALIZED VIEW mv_project_task_kpi_base AS
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
        t.country_code,
        t.healthfacility_code,
        t.region_code,
        t.district_code,
        t.settlement_code,
        t.latitude,
        t.longitude,
        t.location_accuracy AS task_location_accuracy,
        -- Raw timestamps for consistency check
        t.created_time,
        t.synced_time,
        -- Convert Epoch milliseconds to DATE for event_date
        TO_TIMESTAMP(t.created_time / 1000.0)::DATE AS event_date,
        -- Fallback to user_name as the team_id representation
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
),
spatial_clustering AS (
    SELECT 
        *,
        -- Spatial Duplicate Window Function
        -- Transforms GPS (EPSG:4326) to Web Mercator (EPSG:3857) to measure precisely in meters.
        -- Uses DBSCAN to assign a cluster ID if a task is within 10 meters of another task on the same day for the same team.
        -- Note: PostGIS ST_ClusterDBSCAN is disabled because the 'postgis' extension is not available.
        -- We temporarily output NULL for cluster_id until the extension is installed.
        NULL AS cluster_id
    FROM task_enriched
)
SELECT 
    task_id,
    campaign_number,
    country_code,
    healthfacility_code,
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
    CASE 
        -- Condition 1: Same beneficiary_id appears more than once IN THE SAME CAMPAIGN
        WHEN beneficiary_id IS NOT NULL 
             AND COUNT(beneficiary_id) OVER (PARTITION BY campaign_number, beneficiary_id) > 1 
        THEN 1
        -- Condition 2: Task belongs to a 10m spatial cluster for that day/team
        WHEN cluster_id IS NOT NULL 
        THEN 1
        ELSE 0
    END AS is_duplicate
FROM spatial_clustering;

-- ==============================================================================
-- 2. INDEXING
-- ==============================================================================
-- Unique index enables REFRESH MATERIALIZED VIEW CONCURRENTLY without blocking dashboard reads
CREATE UNIQUE INDEX idx_mv_task_kpi_base_unique_task ON mv_project_task_kpi_base (task_id);



-- ==============================================================================
-- 3. BOUNDARY LEVEL DATA MARTS
-- ==============================================================================

-- 3.1 Country Code Data Mart
DROP TABLE IF EXISTS datamart_country_code; -- removed
DROP MATERIALIZED VIEW IF EXISTS datamart_country_code; -- removed
CREATE MATERIALIZED VIEW datamart_country_code AS
SELECT 
    campaign_number,
    country_code AS boundary_hierarchy_code,
    
    -- KPI 1: GPS Coverage % (with strict coordinate validation)
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    
    -- KPI 2: GPS Accuracy Distribution
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN task_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    
    -- KPI 3: Timestamp Consistency Rate (Using parsed event_date correctly)
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    
    -- KPI 4: Possible Duplicate Beneficiaries
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
    
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_number, 
    country_code;

-- 3.2 Health Center Code Data Mart
DROP TABLE IF EXISTS datamart_healthfacility_code;
DROP MATERIALIZED VIEW IF EXISTS datamart_healthfacility_code;
CREATE MATERIALIZED VIEW datamart_healthfacility_code AS
SELECT 
    campaign_number,
    healthfacility_code AS boundary_hierarchy_code,
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN task_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_number, 
    healthfacility_code;

-- 3.3 Region Code Data Mart
DROP TABLE IF EXISTS datamart_region_code;
DROP MATERIALIZED VIEW IF EXISTS datamart_region_code;
CREATE MATERIALIZED VIEW datamart_region_code AS
SELECT 
    campaign_number,
    region_code AS boundary_hierarchy_code,
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN task_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_number, 
    region_code;

-- 3.4 District Code Data Mart
DROP TABLE IF EXISTS datamart_district_code;
DROP MATERIALIZED VIEW IF EXISTS datamart_district_code;
CREATE MATERIALIZED VIEW datamart_district_code AS
SELECT 
    campaign_number,
    district_code AS boundary_hierarchy_code,
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN task_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_number, 
    district_code;

-- 3.5 Settlement Code Data Mart
DROP TABLE IF EXISTS datamart_settlement_code;
DROP MATERIALIZED VIEW IF EXISTS datamart_settlement_code;
CREATE MATERIALIZED VIEW datamart_settlement_code AS
SELECT 
    campaign_number,
    settlement_code AS boundary_hierarchy_code,
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY task_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN task_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_number, 
    settlement_code;
