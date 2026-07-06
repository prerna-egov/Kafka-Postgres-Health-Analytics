-- =====================================================================
-- TEAM PERFORMANCE KPIs (MATERIALIZED VIEWS)
-- =====================================================================
-- Optimized with Base Mart Architecture
-- Database: ClickHouse (Refreshable Materialized Views)
-- =====================================================================

SET allow_experimental_refreshable_materialized_view = 1;

-- #############################################################################
-- SECTION 0: BASE MARTS
-- #############################################################################

CREATE MATERIALIZED VIEW dm_team_performance_base
REFRESH EVERY 24 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, team_id, task_date)
AS
SELECT
    pt.project_id,
    pt.campaign_number,
    ifNull(nullIf(pt.region_code, ''), 'Unknown') AS region_code,
    ifNull(nullIf(pt.district_code, ''), 'Unknown') AS district_code,
    ifNull(nullIf(pt.health_facility_code, ''), 'Unknown') AS health_facility_code,
    ifNull(nullIf(pt.settlement_code, ''), 'Unknown') AS settlement_code,
    pt.user_name AS team_id,
    pt.task_dates AS task_date,
    count(DISTINCT pt.id) AS total_submissions,
    count(DISTINCT pb.beneficiary_id) FILTER (WHERE pt.administration_status IN ('VISITED', 'ADMINISTRATION_SUCCESS')) AS vaccinated_count,
    min(pt.created_time) AS min_created_time,
    max(pt.created_time) AS max_created_time,
    max(if(pt.synced_date = pt.task_dates, 1, 0)) AS is_synced_today,
    countIf((pt.synced_time - pt.created_time) < 3600000) AS under_1hr_count,
    countIf((pt.synced_time - pt.created_time) >= 3600000 AND (pt.synced_time - pt.created_time) < 21600000) AS one_to_6hr_count,
    countIf((pt.synced_time - pt.created_time) >= 21600000 AND (pt.synced_time - pt.created_time) < 86400000) AS six_to_24hr_count,
    countIf((pt.synced_time - pt.created_time) >= 86400000) AS over_24hr_count,
    sumIf(pt.synced_time - pt.created_time, pt.synced_time >= pt.created_time) AS total_sync_lag_ms,
    countIf(pt.synced_time IS NOT NULL AND pt.created_time IS NOT NULL AND pt.synced_time >= pt.created_time) AS valid_sync_count
FROM project_task_enriched pt
LEFT JOIN project_beneficiary_enriched pb 
    ON pt.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY
    pt.project_id, pt.campaign_number, pt.region_code, pt.district_code, pt.health_facility_code, pt.settlement_code, pt.user_name, pt.task_dates;


-- #############################################################################
-- SECTION 1: KPI VIEWS
-- #############################################################################

-- KPI 1: Team Performance League Table (Vaccinations vs Target)
CREATE MATERIALIZED VIEW dm_team_performance_league
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, team_id)
AS
WITH target_data AS (
    SELECT campaign_number, user_name AS team_id, count(DISTINCT beneficiary_id) AS target
    FROM project_beneficiary_enriched
    WHERE is_deleted = false
    GROUP BY campaign_number, user_name
),
vaccinated_data AS (
    SELECT campaign_number, team_id, sum(vaccinated_count) AS vaccinated
    FROM dm_team_performance_base
    GROUP BY campaign_number, team_id
)
SELECT
    ifNull(v.campaign_number, t.campaign_number) AS campaign_number,
    ifNull(v.team_id, t.team_id) AS team_id,
    ifNull(v.vaccinated, 0) AS vaccinated,
    ifNull(t.target, 0) AS target,
    round((ifNull(v.vaccinated, 0) * 100.0) / nullIf(ifNull(t.target, 0), 0), 2) AS performance_percentage,
    RANK() OVER (
        PARTITION BY ifNull(v.campaign_number, t.campaign_number)
        ORDER BY ifNull(v.vaccinated, 0) DESC
    ) AS performance_rank
FROM target_data t
FULL OUTER JOIN vaccinated_data v 
  ON t.campaign_number = v.campaign_number AND t.team_id = v.team_id;

-- KPI 2: Daily Submission Velocity
CREATE MATERIALIZED VIEW dm_team_daily_velocity
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, team_id, task_date)
AS
SELECT
    tp.campaign_number,
    tp.team_id,
    tp.task_date,
    sum(tp.total_submissions) AS submissions_per_day
FROM dm_team_performance_base tp
JOIN project_enriched p ON tp.project_id = p.id AND p.settlement_code IS NOT NULL
WHERE toUnixTimestamp(toDateTime(tp.task_date)) * 1000 BETWEEN p.start_date AND p.end_date
GROUP BY tp.campaign_number, tp.team_id, tp.task_date;


-- KPI 4 & 5: Consolidated Sync Metrics (Rate & Timing)
CREATE MATERIALIZED VIEW dm_team_sync_metrics_base
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, task_date)
AS
WITH team_daily_sync AS (
    SELECT 
        campaign_number, region_code, district_code, health_facility_code, settlement_code, task_date, team_id,
        max(is_synced_today) AS is_synced_today
    FROM dm_team_performance_base
    GROUP BY campaign_number, region_code, district_code, health_facility_code, settlement_code, task_date, team_id
),
sync_rate AS (
    SELECT
        campaign_number, region_code, district_code, health_facility_code, settlement_code, task_date,
        count() AS total_active_teams,
        sum(is_synced_today) AS synced_teams_count,
        round((sum(is_synced_today) * 100.0) / nullIf(count(), 0), 2) AS sync_rate_percentage
    FROM team_daily_sync
    GROUP BY campaign_number, region_code, district_code, health_facility_code, settlement_code, task_date
),
sync_timing AS (
    SELECT
        campaign_number, region_code, district_code, health_facility_code, settlement_code, task_date,
        sum(under_1hr_count) AS under_1hr_count,
        sum(one_to_6hr_count) AS one_to_6hr_count,
        sum(six_to_24hr_count) AS six_to_24hr_count,
        sum(over_24hr_count) AS over_24hr_count
    FROM dm_team_performance_base
    GROUP BY campaign_number, region_code, district_code, health_facility_code, settlement_code, task_date
)
SELECT
    ifNull(r.campaign_number, t.campaign_number) AS campaign_number,
    ifNull(r.region_code, t.region_code) AS region_code,
    ifNull(r.district_code, t.district_code) AS district_code,
    ifNull(r.health_facility_code, t.health_facility_code) AS health_facility_code,
    ifNull(r.settlement_code, t.settlement_code) AS settlement_code,
    ifNull(r.task_date, t.task_date) AS task_date,
    ifNull(r.total_active_teams, 0) AS total_active_teams,
    ifNull(r.synced_teams_count, 0) AS synced_teams_count,
    ifNull(r.sync_rate_percentage, 0.00) AS sync_rate_percentage,
    ifNull(t.under_1hr_count, 0) AS under_1hr_count,
    ifNull(t.one_to_6hr_count, 0) AS one_to_6hr_count,
    ifNull(t.six_to_24hr_count, 0) AS six_to_24hr_count,
    ifNull(t.over_24hr_count, 0) AS over_24hr_count
FROM sync_rate r
FULL OUTER JOIN sync_timing t 
  ON r.campaign_number = t.campaign_number 
 AND r.region_code = t.region_code
 AND r.district_code = t.district_code
 AND r.health_facility_code = t.health_facility_code
 AND r.settlement_code = t.settlement_code 
 AND r.task_date = t.task_date;


-- KPI 6: Average Sync Lag per Team (Ranked)
CREATE MATERIALIZED VIEW dm_team_sync_lag_campaign
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, team_id)
AS
SELECT
    campaign_number,
    team_id,
    sum(total_sync_lag_ms) / nullIf(sum(valid_sync_count), 0) AS avg_sync_lag,
    RANK() OVER (
        PARTITION BY campaign_number
        ORDER BY sum(total_sync_lag_ms) / nullIf(sum(valid_sync_count), 0) ASC
    ) AS sync_lag_rank
FROM dm_team_performance_base
GROUP BY
    campaign_number, team_id;
