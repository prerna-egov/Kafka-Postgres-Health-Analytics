-- =====================================================================
-- TEAM PERFORMANCE KPIs (MATERIALIZED VIEWS)
-- =====================================================================
-- Optimized with Base Mart Architecture
-- Database: PostgreSQL
--
-- Execution order:
--   1. Team Performance Base Mart
--   2. Dependent KPI Data Marts
--   3. Refresh script
-- =====================================================================

-- #############################################################################
-- SECTION 0: BASE MARTS
-- #############################################################################

CREATE MATERIALIZED VIEW dm_team_performance_base AS
SELECT
    pt.project_id,
    pt.campaign_id,
    pt.village_code,
    pt.user_name AS team_id,
    pt.task_dates AS task_date,
    COUNT(DISTINCT pt.id) AS total_submissions,
    COUNT(DISTINCT pb.beneficiary_id) FILTER (WHERE pt.administration_status IN ('VISITED', 'ADMINISTRATION_SUCCESS')) AS vaccinated_count,
    MIN(pt.created_time) AS min_created_time,
    MAX(pt.created_time) AS max_created_time,
    MAX(CASE WHEN pt.synced_date = pt.task_dates THEN 1 ELSE 0 END) AS is_synced_today,
    COUNT(CASE WHEN (pt.synced_time - pt.created_time) < 3600000 THEN 1 END) AS under_1hr_count,
    COUNT(CASE WHEN (pt.synced_time - pt.created_time) >= 3600000 AND (pt.synced_time - pt.created_time) < 21600000 THEN 1 END) AS one_to_6hr_count,
    COUNT(CASE WHEN (pt.synced_time - pt.created_time) >= 21600000 AND (pt.synced_time - pt.created_time) < 86400000 THEN 1 END) AS six_to_24hr_count,
    COUNT(CASE WHEN (pt.synced_time - pt.created_time) >= 86400000 THEN 1 END) AS over_24hr_count,
    SUM(pt.synced_time - pt.created_time) FILTER (WHERE pt.synced_time >= pt.created_time) AS total_sync_lag_ms,
    COUNT(*) FILTER (WHERE pt.synced_time IS NOT NULL AND pt.created_time IS NOT NULL AND pt.synced_time >= pt.created_time) AS valid_sync_count
FROM project_task_enriched pt
LEFT JOIN project_beneficiary_enriched pb 
    ON pt.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY
    pt.project_id, pt.campaign_id, pt.village_code, pt.user_name, pt.task_dates;

CREATE UNIQUE INDEX idx_dm_team_perf_base ON dm_team_performance_base (COALESCE(project_id, 'NONE'), COALESCE(campaign_id, 'NONE'), COALESCE(village_code, 'NONE'), COALESCE(team_id, 'NONE'), COALESCE(task_date, 'NONE'));


-- #############################################################################
-- SECTION 1: KPI VIEWS
-- #############################################################################

-- KPI 1: Campaign Vaccinations vs Target
-- Combines the target (from project_beneficiary_enriched) with the actuals (from base mart)
CREATE MATERIALIZED VIEW dm_team_vaccination_target AS
WITH target_data AS (
    SELECT 
        campaign_id,
        user_name AS team_id,
        COUNT(DISTINCT beneficiary_id) AS target
    FROM project_beneficiary_enriched
    WHERE is_deleted IS NOT TRUE
    GROUP BY campaign_id, user_name
),
vaccinated_data AS (
    SELECT 
        campaign_id,
        team_id,
        SUM(vaccinated_count) AS vaccinated
    FROM dm_team_performance_base
    GROUP BY campaign_id, team_id
)
SELECT
    COALESCE(t.campaign_id, v.campaign_id) AS campaign_id,
    COALESCE(t.team_id, v.team_id) AS team_id,
    COALESCE(v.vaccinated, 0) AS vaccinated,
    COALESCE(t.target, 0) AS target
FROM target_data t
FULL OUTER JOIN vaccinated_data v
    ON t.campaign_id = v.campaign_id
    AND t.team_id = v.team_id
ORDER BY vaccinated DESC;

CREATE UNIQUE INDEX idx_dm_team_vac_tgt ON dm_team_vaccination_target (COALESCE(campaign_id, 'NONE'), COALESCE(team_id, 'NONE'));


-- KPI 2: Daily Submission Velocity
CREATE MATERIALIZED VIEW dm_team_submission_velocity_village AS
SELECT
    tp.village_code,
    tp.campaign_id,
    tp.task_date,
    tp.team_id,
    SUM(tp.total_submissions) AS submissions_per_day
FROM dm_team_performance_base tp
JOIN project_enriched p ON tp.project_id = p.project_id
WHERE CAST(tp.task_date AS BIGINT) BETWEEN p.start_date AND p.end_date
GROUP BY tp.village_code, tp.campaign_id, tp.task_date, tp.team_id;

CREATE UNIQUE INDEX idx_dm_team_sub_vel_vil ON dm_team_submission_velocity_village (COALESCE(village_code, 'NONE'), COALESCE(campaign_id, 'NONE'), COALESCE(task_date, 'NONE'), COALESCE(team_id, 'NONE'));


-- KPI 3: Submission Rate per Hour (Flagging Outliers)
CREATE MATERIALIZED VIEW dm_team_submission_flags_village AS
SELECT DISTINCT team_id
FROM (
    SELECT 
        village_code, campaign_id, task_date, team_id,
        (SUM(total_submissions) / (GREATEST(MAX(max_created_time) - MIN(min_created_time), 1000) / 60000.0)) AS rate_per_minute
    FROM dm_team_performance_base
    GROUP BY village_code, campaign_id, task_date, team_id
    HAVING SUM(total_submissions) > 1
) AS flagged_events
WHERE rate_per_minute > 10.0;

CREATE UNIQUE INDEX idx_dm_team_sub_flags_vil ON dm_team_submission_flags_village (COALESCE(team_id, 'NONE'));


-- KPI 4: Sync Rate
CREATE MATERIALIZED VIEW dm_team_sync_rate_village AS
WITH team_daily AS (
    SELECT 
        village_code, campaign_id, task_date, team_id,
        MAX(is_synced_today) AS is_synced_today
    FROM dm_team_performance_base
    GROUP BY village_code, campaign_id, task_date, team_id
)
SELECT
    village_code,
    campaign_id,
    task_date,
    task_date AS "TODAY",
    COUNT(*) AS total_active_teams,
    SUM(is_synced_today) AS synced_teams_count,
    ROUND((SUM(is_synced_today) * 100.0) / NULLIF(COUNT(*), 0), 2) AS sync_rate_percentage
FROM team_daily
GROUP BY village_code, campaign_id, task_date;

CREATE UNIQUE INDEX idx_dm_team_sync_rate_vil ON dm_team_sync_rate_village (COALESCE(village_code, 'NONE'), COALESCE(campaign_id, 'NONE'), COALESCE(task_date, 'NONE'));


-- KPI 5: Sync Timing Distribution
CREATE MATERIALIZED VIEW dm_team_sync_timing_village AS
SELECT
    village_code,
    campaign_id,
    task_date,
    SUM(under_1hr_count) AS under_1hr_count,
    SUM(one_to_6hr_count) AS one_to_6hr_count,
    SUM(six_to_24hr_count) AS six_to_24hr_count,
    SUM(over_24hr_count) AS over_24hr_count
FROM dm_team_performance_base
GROUP BY
    village_code, campaign_id, task_date;

CREATE UNIQUE INDEX idx_dm_team_sync_tim_vil ON dm_team_sync_timing_village (COALESCE(village_code, 'NONE'), COALESCE(campaign_id, 'NONE'), COALESCE(task_date, 'NONE'));


-- KPI 6: Average Sync Lag per Team
CREATE MATERIALIZED VIEW dm_team_sync_lag_village AS
SELECT
    village_code,
    campaign_id,
    team_id,
    SUM(total_sync_lag_ms) / NULLIF(SUM(valid_sync_count), 0) AS avg_sync_lag
FROM dm_team_performance_base
GROUP BY
    village_code, campaign_id, team_id;

CREATE UNIQUE INDEX idx_dm_team_sync_lag_vil ON dm_team_sync_lag_village (COALESCE(village_code, 'NONE'), COALESCE(campaign_id, 'NONE'), COALESCE(team_id, 'NONE'));


-- #############################################################################
-- SECTION 2: REFRESH SCRIPT
-- #############################################################################

-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_performance_base;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_vaccination_target;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_submission_velocity_village;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_submission_flags_village;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_sync_rate_village;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_sync_timing_village;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_sync_lag_village;
