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
    pt.settlement_code,
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
    pt.project_id, pt.campaign_id, pt.settlement_code, pt.user_name, pt.task_dates;

CREATE UNIQUE INDEX idx_dm_team_perf_base ON dm_team_performance_base (COALESCE(project_id, 'NONE'), COALESCE(campaign_id, 'NONE'), COALESCE(settlement_code, 'NONE'), COALESCE(team_id, 'NONE'), COALESCE(task_date, 'NONE'));

-- KPI 1: Team Performance League Table (Vaccinations vs Target)
-- Provides a ranked list of all teams in a campaign based on their performance.

CREATE MATERIALIZED VIEW dm_team_performance_league AS
WITH target_data AS (
    SELECT campaign_id, user_name AS team_id, COUNT(DISTINCT beneficiary_id) AS target
    FROM project_beneficiary_enriched
    WHERE is_deleted IS NOT TRUE
    GROUP BY campaign_id, user_name
),
vaccinated_data AS (
    SELECT campaign_id, team_id, SUM(vaccinated_count) AS vaccinated
    FROM dm_team_performance_base
    GROUP BY campaign_id, team_id
)
SELECT
    COALESCE(v.campaign_id, t.campaign_id) AS campaign_id,
    COALESCE(v.team_id, t.team_id) AS team_id,
    COALESCE(v.vaccinated, 0) AS vaccinated,
    COALESCE(t.target, 0) AS target,
    ROUND((COALESCE(v.vaccinated, 0)::NUMERIC / NULLIF(COALESCE(t.target, 0), 0)) * 100, 2) AS performance_percentage,
    RANK() OVER (
        PARTITION BY COALESCE(v.campaign_id, t.campaign_id)
        ORDER BY COALESCE(v.vaccinated, 0) DESC NULLS LAST
    ) AS performance_rank
FROM target_data t
FULL OUTER JOIN vaccinated_data v 
  ON t.campaign_id = v.campaign_id AND t.team_id = v.team_id;

CREATE UNIQUE INDEX idx_dm_team_league_pk ON dm_team_performance_league (campaign_id, team_id);
CREATE INDEX idx_dm_team_league_rank ON dm_team_performance_league (campaign_id, performance_rank);


-- KPI 2: Daily Submission Velocity
-- Materialized view to pre-calculate daily submissions for all teams across all campaigns.
CREATE MATERIALIZED VIEW dm_team_daily_velocity AS
SELECT
    tp.campaign_id,
    tp.team_id,
    tp.task_date,
    SUM(tp.total_submissions) AS submissions_per_day
FROM dm_team_performance_base tp
JOIN project_enriched p ON tp.project_id = p.project_id
WHERE CAST(tp.task_date AS BIGINT) BETWEEN p.start_date AND p.end_date
GROUP BY tp.campaign_id, tp.team_id, tp.task_date;

CREATE UNIQUE INDEX idx_dm_team_daily_velocity ON dm_team_daily_velocity (COALESCE(campaign_id, 'NONE'), COALESCE(team_id, 'NONE'), COALESCE(task_date, 'NONE'));




-- KPI 3: Submission Rate per Hour (Flagging Outliers)
CREATE MATERIALIZED VIEW dm_team_submission_flags_village AS
SELECT DISTINCT campaign_id, team_id
FROM (
    SELECT 
        settlement_code, campaign_id, task_date, team_id,
        (SUM(total_submissions) / (GREATEST(MAX(max_created_time) - MIN(min_created_time), 1000) / 60000.0)) AS rate_per_minute
    FROM dm_team_performance_base
    GROUP BY settlement_code, campaign_id, task_date, team_id
    HAVING SUM(total_submissions) > 1
) AS flagged_events
WHERE rate_per_minute > 10.0;

CREATE UNIQUE INDEX idx_dm_team_sub_flags_vil ON dm_team_submission_flags_village (campaign_id, team_id);



-- KPI 4 & 5: Consolidated Sync Metrics (Rate & Timing)
CREATE MATERIALIZED VIEW dm_team_sync_metrics_village AS
WITH team_daily_sync AS (
    SELECT 
        settlement_code, campaign_id, task_date, team_id,
        MAX(is_synced_today) AS is_synced_today
    FROM dm_team_performance_base
    GROUP BY settlement_code, campaign_id, task_date, team_id
),
sync_rate AS (
    SELECT
        settlement_code,
        campaign_id,
        task_date,
        COUNT(*) AS total_active_teams,
        SUM(is_synced_today) AS synced_teams_count,
        ROUND((SUM(is_synced_today) * 100.0) / NULLIF(COUNT(*), 0), 2) AS sync_rate_percentage
    FROM team_daily_sync
    GROUP BY settlement_code, campaign_id, task_date
),
sync_timing AS (
    SELECT
        settlement_code,
        campaign_id,
        task_date,
        SUM(under_1hr_count) AS under_1hr_count,
        SUM(one_to_6hr_count) AS one_to_6hr_count,
        SUM(six_to_24hr_count) AS six_to_24hr_count,
        SUM(over_24hr_count) AS over_24hr_count
    FROM dm_team_performance_base
    GROUP BY settlement_code, campaign_id, task_date
)
SELECT
    COALESCE(r.settlement_code, t.settlement_code) AS settlement_code,
    COALESCE(r.campaign_id, t.campaign_id) AS campaign_id,
    COALESCE(r.task_date, t.task_date) AS task_date,
    COALESCE(r.total_active_teams, 0) AS total_active_teams,
    COALESCE(r.synced_teams_count, 0) AS synced_teams_count,
    COALESCE(r.sync_rate_percentage, 0.00) AS sync_rate_percentage,
    COALESCE(t.under_1hr_count, 0) AS under_1hr_count,
    COALESCE(t.one_to_6hr_count, 0) AS one_to_6hr_count,
    COALESCE(t.six_to_24hr_count, 0) AS six_to_24hr_count,
    COALESCE(t.over_24hr_count, 0) AS over_24hr_count
FROM sync_rate r
FULL OUTER JOIN sync_timing t 
  ON r.settlement_code = t.settlement_code 
 AND r.campaign_id = t.campaign_id 
 AND r.task_date = t.task_date;

CREATE UNIQUE INDEX idx_dm_team_sync_metrics_vil ON dm_team_sync_metrics_village (COALESCE(settlement_code, 'NONE'), COALESCE(campaign_id, 'NONE'), COALESCE(task_date, 'NONE'));


-- KPI 6: Average Sync Lag per Team (Ranked)
CREATE MATERIALIZED VIEW dm_team_sync_lag_campaign AS
SELECT
    campaign_id,
    team_id,
    SUM(total_sync_lag_ms) / NULLIF(SUM(valid_sync_count), 0) AS avg_sync_lag,
    RANK() OVER (
        PARTITION BY campaign_id
        ORDER BY SUM(total_sync_lag_ms) / NULLIF(SUM(valid_sync_count), 0) ASC NULLS LAST
    ) AS sync_lag_rank
FROM dm_team_performance_base
GROUP BY
    campaign_id, team_id;

CREATE UNIQUE INDEX idx_dm_team_sync_lag_camp ON dm_team_sync_lag_campaign (COALESCE(campaign_id, 'NONE'), COALESCE(team_id, 'NONE'));
CREATE INDEX idx_dm_team_sync_lag_rank ON dm_team_sync_lag_campaign (campaign_id, sync_lag_rank);

-- #############################################################################
-- SECTION 2: REFRESH SCRIPT
-- #############################################################################

-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_performance_base;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_performance_league;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_daily_velocity;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_submission_flags_village;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_sync_metrics_village;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_team_sync_lag_campaign;

