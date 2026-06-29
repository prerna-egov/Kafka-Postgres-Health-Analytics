-- =====================================================================
-- VACCINATION CAMPAIGN KPIs (MATERIALIZED VIEWS)
-- =====================================================================
-- Note: Replace [YOUR_RATE_THRESHOLD_PER_MINUTE] in KPI 3 before executing.

-- KPI 1: Campaign Vaccinations vs Target
CREATE MATERIALIZED VIEW dm_kpi1_vaccinations_village AS
WITH target_data AS (
    SELECT 
        village_code,
        campaign_id,
        user_name,
        COUNT(DISTINCT beneficiary_id) AS target
    FROM project_beneficiary_enriched
    GROUP BY village_code, campaign_id, user_name
),
vaccinated_data AS (
    SELECT 
        pt.village_code,
        pt.campaign_id,
        pt.user_name,
        COUNT(DISTINCT pb.beneficiary_id) AS vaccinated
    FROM project_task_enriched pt
    JOIN project_beneficiary_enriched pb 
        ON pt.project_beneficiary_client_reference_id = pb.client_reference_id
    WHERE pt.administration_status IN ('VISITED', 'ADMINISTRATION_SUCCESS')
    GROUP BY pt.village_code, pt.campaign_id, pt.user_name
)
SELECT
    COALESCE(t.village_code, v.village_code) AS village_code,
    COALESCE(t.campaign_id, v.campaign_id) AS campaign_id,
    COALESCE(t.user_name, v.user_name) AS team_id,
    COALESCE(v.vaccinated, 0) AS vaccinated,
    COALESCE(t.target, 0) AS target
FROM target_data t
         FULL OUTER JOIN vaccinated_data v
                         ON t.village_code = v.village_code
                             AND t.campaign_id = v.campaign_id
                             AND t.user_name = v.user_name;


-- KPI 2: Daily Submission Velocity
CREATE MATERIALIZED VIEW dm_kpi2_campaign_submissions_village AS
SELECT
    pt.village_code,
    pt.campaign_id,
    pt.task_dates AS task_date,
    pt.user_name AS team_id,
    COUNT(DISTINCT pt.id) AS submissions_per_day
FROM project_task_enriched pt
         JOIN project_enriched p ON pt.project_id = p.project_id
WHERE CAST(pt.task_dates AS BIGINT) BETWEEN p.start_date AND p.end_date
GROUP BY
    pt.village_code, pt.campaign_id, pt.task_dates, pt.user_name
ORDER BY
    task_date DESC;


-- KPI 3: Submission Rate per Hour (Flagging Outliers)
CREATE MATERIALIZED VIEW dm_kpi3_submission_flags_village AS
SELECT DISTINCT user_name AS team_id
FROM (
         SELECT user_name
         FROM project_task_enriched
         GROUP BY village_code, campaign_id, task_dates, user_name
         HAVING COUNT(DISTINCT id) > 1
            AND (COUNT(DISTINCT id) / (GREATEST(MAX(created_time) - MIN(created_time), 1000) / 60000.0)) > [YOUR_RATE_THRESHOLD_PER_MINUTE]
     ) AS flagged_events;


-- KPI 4: Sync Rate
CREATE MATERIALIZED VIEW dm_kpi4_sync_rate_village AS
SELECT
    village_code,
    campaign_id,
    task_dates AS task_date,
    task_dates AS "TODAY",
    COUNT(DISTINCT user_name) AS total_active_teams,
    COUNT(DISTINCT CASE WHEN synced_date = task_dates THEN user_name END) AS synced_teams_count,
    (COUNT(DISTINCT CASE WHEN synced_date = task_dates THEN user_name END) * 100.0)
        / NULLIF(COUNT(DISTINCT user_name), 0) AS sync_rate_percentage
FROM project_task_enriched
GROUP BY
    village_code, campaign_id, task_dates;


-- KPI 5: Sync Timing Distribution
CREATE MATERIALIZED VIEW dm_kpi5_sync_timing_village AS
SELECT
    village_code,
    campaign_id,
    task_dates AS task_date,
    COUNT(CASE WHEN (synced_time - created_time) < 3600000 THEN 1 END) AS under_1hr_count,
    COUNT(CASE WHEN (synced_time - created_time) >= 3600000 AND (synced_time - created_time) < 21600000 THEN 1 END) AS one_to_6hr_count,
    COUNT(CASE WHEN (synced_time - created_time) >= 21600000 AND (synced_time - created_time) < 86400000 THEN 1 END) AS six_to_24hr_count,
    COUNT(CASE WHEN (synced_time - created_time) >= 86400000 THEN 1 END) AS over_24hr_count
FROM project_task_enriched
WHERE synced_time IS NOT NULL
  AND created_time IS NOT NULL
  AND synced_time >= created_time
GROUP BY
    village_code, campaign_id, task_dates;


-- KPI 6: Average Sync Lag per Team
CREATE MATERIALIZED VIEW dm_kpi6_sync_lag_village AS
SELECT
    village_code,
    campaign_id,
    user_name AS team_id,
    AVG(synced_time - created_time) AS avg_sync_lag
FROM project_task_enriched
WHERE synced_time IS NOT NULL
  AND created_time IS NOT NULL
  AND synced_time >= created_time
GROUP BY
    village_code, campaign_id, user_name
ORDER BY
    avg_sync_lag DESC;
