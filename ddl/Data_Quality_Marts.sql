-- ==============================================================================
-- 1. BASE MATERIALIZED VIEW
-- ==============================================================================
CREATE MATERIALIZED VIEW mv_project_task_kpi_base AS
WITH beneficiary_dedup AS (
    -- Prevent Join Explosion: Ensure 1:1 join by taking the latest beneficiary record if duplicates exist
    SELECT client_reference_id, beneficiary_id
    FROM (
        SELECT client_reference_id, beneficiary_id,
               ROW_NUMBER() OVER(PARTITION BY client_reference_id ORDER BY last_modified_time DESC) as rn
        FROM project_beneficiary_enriched
    ) sub
    WHERE rn = 1
),
task_enriched AS (
    SELECT 
        t.id AS task_id,
        t.campaign_id,
        t.country_code,
        t.health_center_code,
        t.spp_code,
        t.province_code,
        t.district_code,
        t.village_code,
        t.latitude,
        t.longitude,
        -- Use native location_accuracy column, with safe JSON extraction as fallback
        COALESCE(
            t.location_accuracy,
            CASE 
                WHEN (t.additional_details->>'locationAccuracy') ~ '^[0-9]+(\.[0-9]+)?$' 
                THEN (t.additional_details->>'locationAccuracy')::NUMERIC 
                ELSE NULL 
            END
        ) AS json_location_accuracy,
        -- Raw timestamps for consistency check
        t.created_time,
        t.synced_time,
        -- Convert Epoch milliseconds to DATE for event_date
        TO_TIMESTAMP(t.created_time / 1000.0)::DATE AS event_date,
        -- Fallback to created_by as the team_id representation
        t.created_by AS team_id,
        b.beneficiary_id,
        -- Semantically correct Project Dates (cast epoch ms to DATE)
        TO_TIMESTAMP(p.start_date / 1000.0)::DATE AS project_start_date,
        TO_TIMESTAMP(p.end_date / 1000.0)::DATE AS project_end_date,
        p.campaign_duration_in_days
    FROM project_task_enriched t
    LEFT JOIN beneficiary_dedup b
        ON t.project_beneficiary_client_reference_id = b.client_reference_id
    LEFT JOIN project_enriched p
        ON t.project_id = p.id
),
spatial_clustering AS (
    SELECT 
        *,
        -- Spatial Duplicate Window Function
        -- Transforms GPS (EPSG:4326) to Web Mercator (EPSG:3857) to measure precisely in meters.
        -- Uses DBSCAN to assign a cluster ID if a task is within 10 meters of another task on the same day for the same team.
        CASE 
            WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 AND team_id IS NOT NULL
            THEN ST_ClusterDBSCAN(
                     ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 3857), 
                     eps := 10, 
                     minpoints := 2
                 ) OVER (PARTITION BY campaign_id, event_date, team_id)
            ELSE NULL
        END AS cluster_id
    FROM task_enriched
)
SELECT 
    task_id,
    campaign_id,
    country_code,
    health_center_code,
    spp_code,
    province_code,
    district_code,
    village_code,
    latitude,
    longitude,
    json_location_accuracy,
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
             AND COUNT(beneficiary_id) OVER (PARTITION BY campaign_id, beneficiary_id) > 1 
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

CREATE INDEX idx_mv_task_kpi_base_country ON mv_project_task_kpi_base (campaign_id, country_code);
CREATE INDEX idx_mv_task_kpi_base_health_center ON mv_project_task_kpi_base (campaign_id, health_center_code);
CREATE INDEX idx_mv_task_kpi_base_spp ON mv_project_task_kpi_base (campaign_id, spp_code);
CREATE INDEX idx_mv_task_kpi_base_province ON mv_project_task_kpi_base (campaign_id, province_code);
CREATE INDEX idx_mv_task_kpi_base_district ON mv_project_task_kpi_base (campaign_id, district_code);
CREATE INDEX idx_mv_task_kpi_base_village ON mv_project_task_kpi_base (campaign_id, village_code);

-- ==============================================================================
-- 3. BOUNDARY LEVEL DATA MARTS
-- ==============================================================================

-- 3.1 Country Code Data Mart
DROP TABLE IF EXISTS datamart_country_code;
DROP MATERIALIZED VIEW IF EXISTS datamart_country_code;
CREATE MATERIALIZED VIEW datamart_country_code AS
SELECT 
    campaign_id,
    country_code AS boundary_hierarchy_code,
    
    -- KPI 1: GPS Coverage % (with strict coordinate validation)
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    
    -- KPI 2: GPS Accuracy Distribution
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN json_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    
    -- KPI 3: Timestamp Consistency Rate (Using parsed event_date correctly)
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    
    -- KPI 4: Possible Duplicate Beneficiaries
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
    
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_id, 
    country_code;

-- 3.2 Health Center Code Data Mart
DROP TABLE IF EXISTS datamart_health_center_code;
DROP MATERIALIZED VIEW IF EXISTS datamart_health_center_code;
CREATE MATERIALIZED VIEW datamart_health_center_code AS
SELECT 
    campaign_id,
    health_center_code AS boundary_hierarchy_code,
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN json_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_id, 
    health_center_code;

-- 3.3 SPP Code Data Mart
DROP TABLE IF EXISTS datamart_spp_code;
DROP MATERIALIZED VIEW IF EXISTS datamart_spp_code;
CREATE MATERIALIZED VIEW datamart_spp_code AS
SELECT 
    campaign_id,
    spp_code AS boundary_hierarchy_code,
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN json_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_id, 
    spp_code;

-- 3.4 Province Code Data Mart
DROP TABLE IF EXISTS datamart_province_code;
DROP MATERIALIZED VIEW IF EXISTS datamart_province_code;
CREATE MATERIALIZED VIEW datamart_province_code AS
SELECT 
    campaign_id,
    province_code AS boundary_hierarchy_code,
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN json_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_id, 
    province_code;

-- 3.5 District Code Data Mart
DROP TABLE IF EXISTS datamart_district_code;
DROP MATERIALIZED VIEW IF EXISTS datamart_district_code;
CREATE MATERIALIZED VIEW datamart_district_code AS
SELECT 
    campaign_id,
    district_code AS boundary_hierarchy_code,
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN json_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_id, 
    district_code;

-- 3.6 Village Code Data Mart
DROP TABLE IF EXISTS datamart_village_code;
DROP MATERIALIZED VIEW IF EXISTS datamart_village_code;
CREATE MATERIALIZED VIEW datamart_village_code AS
SELECT 
    campaign_id,
    village_code AS boundary_hierarchy_code,
    COUNT(CASE WHEN latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS gps_coverage_percentage,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p10,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY json_location_accuracy) AS gps_accuracy_p90,
    COUNT(CASE WHEN json_location_accuracy > 50 THEN 1 END) AS gps_accuracy_gt_50m_count,
    COUNT(CASE WHEN created_time < synced_time AND event_date BETWEEN project_start_date AND project_end_date THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS timestamp_consistency_rate,
    MAX(campaign_duration_in_days) AS campaign_duration_days,
    SUM(is_duplicate) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_percentage
FROM mv_project_task_kpi_base
GROUP BY 
    campaign_id, 
    village_code;

