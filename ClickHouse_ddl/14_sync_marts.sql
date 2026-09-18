-- 1. mv_dm_user_sync -> dm_user_sync                (KPIs 1-9)
--
-- One UNION ALL with a `src` marker instead of a join: CREATED comes from the
-- staff roster, SYNCED from the four record tables, and one GROUP BY builds
-- both. A FULL OUTER JOIN of two aggregates would give the same numbers but has
-- to cope with boundary/role combinations present on only one side -- and with
-- join_use_nulls = 0 the coalesce pattern silently loses keys.
--
-- THE TWO SIDES ARE KEYED DIFFERENTLY: CREATED counts staff user_id (uuid),
-- SYNCED counts user_name, because no record table carries a user id. The rate
-- is a ratio of two independently keyed populations, not a matched cohort.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_dm_user_sync
REFRESH EVERY 1 HOUR
TO analytics.dm_user_sync
EMPTY AS
SELECT
    tenant_id,
    campaign_number,
    hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
    level_six_code, level_seven_code, level_eight_code, level_nine_code,
    role,
    src,
    user_id,
    user_name,
    name_of_user,
    toUInt64(count()) AS records
FROM
(
    /* CREATED -- the staff roster. Carries BOTH user_id and user_name, so
       user_name is the key that joins this leg to the SYNCED legs. */
    SELECT
        tenant_id,
        toString(campaign_number) AS campaign_number,
        hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
        level_six_code, level_seven_code, level_eight_code, level_nine_code,
        toString(role) AS role,
        'CREATED' AS src,
        toString(user_id)      AS user_id,
        toString(user_name)    AS user_name,
        toString(name_of_user) AS name_of_user
    FROM analytics.project_staff_entity FINAL
    WHERE user_id != ''
      AND role IN ('DISTRIBUTOR','WAREHOUSE_MANAGER','NATIONAL_SUPERVISOR',
                   'PROVINCIAL_SUPERVISOR','DISTRICT_SUPERVISOR','TEAM_SUPERVISOR')

    UNION ALL
    /* SYNCED -- household */
    SELECT
        tenant_id,
        toString(campaign_number) AS campaign_number,
        hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
        level_six_code, level_seven_code, level_eight_code, level_nine_code,
        toString(role) AS role,
        'SYNCED' AS src,
        '' AS user_id,
        toString(user_name)    AS user_name,
        toString(name_of_user) AS name_of_user
    FROM analytics.household_entity FINAL
    WHERE user_name != ''

    UNION ALL
    /* SYNCED -- household_member */
    SELECT
        tenant_id,
        toString(campaign_number) AS campaign_number,
        hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
        level_six_code, level_seven_code, level_eight_code, level_nine_code,
        toString(role) AS role,
        'SYNCED' AS src,
        '' AS user_id,
        toString(user_name)    AS user_name,
        toString(name_of_user) AS name_of_user
    FROM analytics.household_member_entity FINAL
    WHERE user_name != ''

    UNION ALL
    /* SYNCED -- project_task */
    SELECT
        tenant_id,
        toString(campaign_number) AS campaign_number,
        hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
        level_six_code, level_seven_code, level_eight_code, level_nine_code,
        toString(role) AS role,
        'SYNCED' AS src,
        '' AS user_id,
        toString(user_name)    AS user_name,
        toString(name_of_user) AS name_of_user
    FROM analytics.project_task_entity FINAL
    WHERE user_name != ''

    UNION ALL
    /* SYNCED -- stock */
    SELECT
        tenant_id,
        toString(campaign_number) AS campaign_number,
        hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
        level_six_code, level_seven_code, level_eight_code, level_nine_code,
        toString(role) AS role,
        'SYNCED' AS src,
        '' AS user_id,
        toString(user_name)    AS user_name,
        toString(name_of_user) AS name_of_user
    FROM analytics.stock_entity FINAL
    WHERE user_name != ''
)
GROUP BY
    tenant_id, campaign_number, hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
    level_six_code, level_seven_code, level_eight_code, level_nine_code,
    role, src, user_id, user_name, name_of_user;


-- 2. mv_dm_cdd_sync_hourly -> dm_cdd_sync_hourly    (KPI 10)
--
-- role is a GRAIN COLUMN, not a WHERE filter: role is ~100% blank on stock,
-- household_member and project_task and 39.8% blank on household, so filtering
-- role = 'DISTRIBUTOR' in the MV reduced the whole instance to 3 CDDs / 247
-- rows. Stored as a dimension, the mart also serves the same histogram for any
-- other cadre.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_dm_cdd_sync_hourly
REFRESH EVERY 1 HOUR
TO analytics.dm_cdd_sync_hourly
EMPTY AS
SELECT
    tenant_id,
    toString(campaign_number) AS campaign_number,
    hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
    level_six_code, level_seven_code, level_eight_code, level_nine_code,
    toString(role)                   AS role,
    toStartOfHour(synced_time_stamp) AS synced_hour,
    toString(user_name)              AS user_name,
    toUInt64(count())                AS records
FROM
(
    SELECT tenant_id, campaign_number, hierarchy_type,
           level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
           level_six_code, level_seven_code, level_eight_code, level_nine_code,
           role, user_name, synced_time_stamp
    FROM analytics.household_entity FINAL
    WHERE user_name != '' AND synced_time_stamp > toDateTime64(0, 3)
    UNION ALL
    SELECT tenant_id, campaign_number, hierarchy_type,
           level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
           level_six_code, level_seven_code, level_eight_code, level_nine_code,
           role, user_name, synced_time_stamp
    FROM analytics.household_member_entity FINAL
    WHERE user_name != '' AND synced_time_stamp > toDateTime64(0, 3)
    UNION ALL
    SELECT tenant_id, campaign_number, hierarchy_type,
           level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
           level_six_code, level_seven_code, level_eight_code, level_nine_code,
           role, user_name, synced_time_stamp
    FROM analytics.project_task_entity FINAL
    WHERE user_name != '' AND synced_time_stamp > toDateTime64(0, 3)
    UNION ALL
    SELECT tenant_id, campaign_number, hierarchy_type,
           level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
           level_six_code, level_seven_code, level_eight_code, level_nine_code,
           role, user_name, synced_time_stamp
    FROM analytics.stock_entity FINAL
    WHERE user_name != '' AND synced_time_stamp > toDateTime64(0, 3)
)
GROUP BY
    tenant_id, campaign_number, hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
    level_six_code, level_seven_code, level_eight_code, level_nine_code,
    role, synced_hour, user_name;
