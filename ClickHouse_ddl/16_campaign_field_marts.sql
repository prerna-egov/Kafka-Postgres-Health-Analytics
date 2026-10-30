-- ==========================================================================
-- CAMPAIGN-FIELD MARTS -- LOGIC
--
-- The marts that read campaign-specific values, rebuilt to read them from the
-- <entity>_campaign_field child tables (05_silver_tables.sql) instead of the
-- silver columns / additional_details JSON. Each view here is a copy of the
-- original named in its header and writes dm_<name>_cf (the CAMPAIGN-FIELD
-- section at the end of 07_mart_tables.sql), so old and new outputs can be
-- compared before cutover. The originals stay untouched until then; at
-- cutover the dashboards move to the _cf tables (or these replace the
-- originals) and the silver columns go.
--
-- Direct readers swap the campaign columns for a pivot of the child table,
-- joined on the parent's version (child.last_modified_time = the parent's
-- version column), and keep the original query body. The rest are verbatim
-- copies reading the _cf marts. mv_dm_targets_base is shared: it reads no
-- campaign-specific value.
--
-- Same rules as the other mart files: every silver read uses FINAL, and the
-- views refresh only by hand (REFRESH EVERY 80 YEAR; see 08), parents first.
-- ==========================================================================

SET allow_experimental_refreshable_materialized_view = 1;

-- 1. mv_dm_smc_administered_base_cf -> dm_smc_administered_base_cf
--
-- Copy of mv_dm_smc_administered_base (10_overview_marts.sql):
-- child rows for cycleIndex, age and gender replace the silver columns.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_smc_administered_base_cf
REFRESH EVERY 80 YEAR
TO dm_smc_administered_base_cf
AS
WITH
-- The task's campaign-specific values, one row per task version. A key the
-- version doesn't carry stays 0 / '' (join_use_nulls = 0), as in silver.
task_campaign_fields AS (
    SELECT
        id,
        last_modified_time,
        toUInt8(anyIf(value_number, key = 'cycleIndex' AND value_type = 'number')) AS cycle_index,
        toUInt32(anyIf(value_number, key = 'age' AND value_type = 'number')) AS age,
        anyIf(value_string, key = 'gender' AND value_type = 'string') AS gender
    FROM project_task_campaign_field FINAL
    WHERE key IN ('cycleIndex', 'age', 'gender')
    GROUP BY id, last_modified_time
),
-- project_task_entity with those values under the silver column names, so the
-- query below is the original one. Joined on the version: keys a later version
-- dropped are left behind in the child table.
tasks_with_campaign_fields AS (
    SELECT
        task.* EXCEPT (`cycleIndex`, age, gender),
        fields.cycle_index AS `cycleIndex`,
        fields.age AS age,
        fields.gender AS gender
    FROM project_task_entity AS task FINAL
    LEFT JOIN task_campaign_fields AS fields
        ON  fields.id = task.id
        AND fields.last_modified_time = task.last_modified_time
)
SELECT
    tenant_id,
    campaign_number,
    hierarchy_type,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code,
    task_dates AS event_date,
    administration_status,
    delivered_to,
    is_delivered,
    gender,
    age,
    `cycleIndex` AS cycle_index,
    product_name,
    uniqExactState(
        toString(campaign_number),
        `cycleIndex`,
        project_beneficiary_client_reference_id,
        toString(administration_status)
    )                                  AS administered_uniq,
    -- Same measure for this row only, so the table reads without a Merge.
    toUInt64(uniqExact(
        toString(campaign_number),
        `cycleIndex`,
        project_beneficiary_client_reference_id,
        toString(administration_status)
    ))                                 AS administered_count,
    toUInt64(count())                  AS task_rows,
    sum(quantity)                      AS resource_quantity_sum
FROM tasks_with_campaign_fields
GROUP BY
    tenant_id,
    campaign_number,
    hierarchy_type,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code,
    event_date,
    administration_status,
    delivered_to,
    is_delivered,
    gender,
    age,
    cycle_index,
    product_name;

-- 2. mv_dm_smc_administered_by_beneficiary_cf -> dm_smc_administered_by_beneficiary_cf
--
-- Copy of mv_dm_smc_administered_by_beneficiary (10_overview_marts.sql):
-- verbatim, reading the _cf marts.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_smc_administered_by_beneficiary_cf
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_smc_administered_base_cf
TO dm_smc_administered_by_beneficiary_cf
AS
SELECT
    tenant_id,
    campaign_number,
    hierarchy_type,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code,
    event_date,
    administration_status,
    delivered_to,
    is_delivered,
    gender,
    age,
    cycle_index,
    -- The column is qualified with the table alias because the output column
    -- carries the SAME NAME as the input. Unqualified, the analyzer resolves
    -- `administered_uniq` to this SELECT's own alias and rejects the query with
    -- "aggregate function found inside another aggregate function".
    uniqExactMergeState(b.administered_uniq)      AS administered_uniq,
    -- Same measure for this row only, so the table reads without a Merge.
    toUInt64(uniqExactMerge(b.administered_uniq)) AS administered_count,
    sum(b.task_rows)                              AS task_rows,
    sum(b.resource_quantity_sum)                  AS resource_quantity_sum
FROM dm_smc_administered_base_cf AS b
GROUP BY
    tenant_id,
    campaign_number,
    hierarchy_type,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code,
    event_date,
    administration_status,
    delivered_to,
    is_delivered,
    gender,
    age,
    cycle_index;

-- 3. mv_dm_smc_administered_by_campaign_cf -> dm_smc_administered_by_campaign_cf
--
-- Copy of mv_dm_smc_administered_by_campaign (10_overview_marts.sql):
-- verbatim, reading the _cf marts.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_smc_administered_by_campaign_cf
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_smc_administered_by_beneficiary_cf
TO dm_smc_administered_by_campaign_cf
AS
SELECT
    tenant_id,
    campaign_number,
    hierarchy_type,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code,
    administration_status,
    delivered_to,
    is_delivered,
    gender,
    age,
    cycle_index,
    uniqExactMergeState(b.administered_uniq)      AS administered_uniq,
    toUInt64(uniqExactMerge(b.administered_uniq)) AS administered_count,
    sum(b.task_rows)                              AS task_rows,
    sum(b.resource_quantity_sum)                  AS resource_quantity_sum
FROM dm_smc_administered_by_beneficiary_cf AS b
GROUP BY
    tenant_id,
    campaign_number,
    hierarchy_type,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code,
    administration_status,
    delivered_to,
    is_delivered,
    gender,
    age,
    cycle_index;

-- 4. mv_dm_campaign_gender_breakdown_cf -> dm_campaign_gender_breakdown_cf
--
-- Copy of mv_dm_campaign_gender_breakdown (10_overview_marts.sql):
-- verbatim, reading the _cf marts.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign_gender_breakdown_cf
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_smc_administered_by_campaign_cf
TO dm_campaign_gender_breakdown_cf
AS
WITH
-- Each delivery row exploded to every ancestor node, keeping the path only down
-- to that node (the same pattern as 1d), then each node's SET of children per
-- gender.
children_per_node_per_gender AS (
    SELECT
        delivery.tenant_id AS tenant_id, delivery.campaign_number AS campaign_number,
        delivery.hierarchy_type AS hierarchy_type,
        toUInt8(ancestor.1) AS node_level, ancestor.2 AS code,
        if(node_level >= 1, delivery.level_one_code, '')   AS path_level_one_code,
        if(node_level >= 2, delivery.level_two_code, '')   AS path_level_two_code,
        if(node_level >= 3, delivery.level_three_code, '') AS path_level_three_code,
        if(node_level >= 4, delivery.level_four_code, '')  AS path_level_four_code,
        if(node_level >= 5, delivery.level_five_code, '')  AS path_level_five_code,
        if(node_level >= 6, delivery.level_six_code, '')   AS path_level_six_code,
        if(node_level >= 7, delivery.level_seven_code, '') AS path_level_seven_code,
        if(node_level >= 8, delivery.level_eight_code, '') AS path_level_eight_code,
        if(node_level >= 9, delivery.level_nine_code, '')  AS path_level_nine_code,
        delivery.gender AS gender,
        uniqExactMergeState(delivery.administered_uniq) AS children_state
    FROM dm_smc_administered_by_campaign_cf AS delivery
    ARRAY JOIN arrayFilter(level_and_code -> level_and_code.2 != '',
                   arrayZip(range(1, 10), [delivery.level_one_code, delivery.level_two_code,
                                           delivery.level_three_code, delivery.level_four_code,
                                           delivery.level_five_code, delivery.level_six_code,
                                           delivery.level_seven_code, delivery.level_eight_code,
                                           delivery.level_nine_code])) AS ancestor
    WHERE delivery.administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND delivery.delivered_to = 'INDIVIDUAL'
    GROUP BY tenant_id, campaign_number, hierarchy_type, node_level, code,
        path_level_one_code, path_level_two_code, path_level_three_code,
        path_level_four_code, path_level_five_code, path_level_six_code,
        path_level_seven_code, path_level_eight_code, path_level_nine_code, gender
),
-- The four buckets and the true total per node (buckets defined on item 26).
children_per_node AS (
    SELECT
        tenant_id, campaign_number, hierarchy_type, node_level, code,
        path_level_one_code, path_level_two_code, path_level_three_code,
        path_level_four_code, path_level_five_code, path_level_six_code,
        path_level_seven_code, path_level_eight_code, path_level_nine_code,
        toUInt64(uniqExactMergeIf(children_state, gender = 'MALE'))   AS boys_vaccinated,
        toUInt64(uniqExactMergeIf(children_state, gender = 'FEMALE')) AS girls_vaccinated,
        toUInt64(uniqExactMergeIf(children_state, gender = 'OTHER'))  AS other_gender_vaccinated,
        toUInt64(uniqExactMergeIf(children_state, gender NOT IN ('MALE', 'FEMALE', 'OTHER'))) AS unknown_gender_vaccinated,
        toUInt64(uniqExactMerge(children_state)) AS total_vaccinated
    FROM children_per_node_per_gender
    GROUP BY tenant_id, campaign_number, hierarchy_type, node_level, code,
        path_level_one_code, path_level_two_code, path_level_three_code,
        path_level_four_code, path_level_five_code, path_level_six_code,
        path_level_seven_code, path_level_eight_code, path_level_nine_code
),
-- Campaign-level: does ANY vaccinated child in the campaign have a known gender?
campaigns_collecting_gender AS (
    SELECT tenant_id, campaign_number, countIf(gender != '') > 0 AS campaign_collects_gender
    FROM dm_smc_administered_by_campaign_cf
    WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND delivered_to = 'INDIVIDUAL'
    GROUP BY tenant_id, campaign_number
)
SELECT
    node.tenant_id AS tenant_id, node.campaign_number AS campaign_number, node.hierarchy_type AS hierarchy_type,
    node.path_level_one_code   AS level_one_code,   node.path_level_two_code   AS level_two_code,
    node.path_level_three_code AS level_three_code, node.path_level_four_code  AS level_four_code,
    node.path_level_five_code  AS level_five_code,  node.path_level_six_code   AS level_six_code,
    node.path_level_seven_code AS level_seven_code, node.path_level_eight_code AS level_eight_code,
    node.path_level_nine_code  AS level_nine_code,
    node.node_level AS node_level, node.code AS code,
    node.boys_vaccinated AS boys_vaccinated,
    node.girls_vaccinated AS girls_vaccinated,
    node.other_gender_vaccinated AS other_gender_vaccinated,
    node.unknown_gender_vaccinated AS unknown_gender_vaccinated,
    node.total_vaccinated AS total_vaccinated,
    -- The split BETWEEN boys and girls, so the pair adds to 100; 0 when neither.
    if(node.boys_vaccinated + node.girls_vaccinated = 0, 0,
       node.boys_vaccinated * 100 / (node.boys_vaccinated + node.girls_vaccinated)) AS boys_percent,
    if(node.boys_vaccinated + node.girls_vaccinated = 0, 0,
       node.girls_vaccinated * 100 / (node.boys_vaccinated + node.girls_vaccinated)) AS girls_percent,
    campaign.campaign_collects_gender AS campaign_collects_gender,
    now() AS refreshed_at
FROM children_per_node AS node
LEFT JOIN campaigns_collecting_gender AS campaign
    ON  node.tenant_id = campaign.tenant_id
    AND node.campaign_number = campaign.campaign_number;

-- 5. mv_dm_campaign_daily_progress_cf -> dm_campaign_daily_progress_cf
--
-- Copy of mv_dm_campaign_daily_progress (10_overview_marts.sql):
-- verbatim, reading the _cf marts.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign_daily_progress_cf
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_smc_administered_by_beneficiary_cf, mv_dm_targets_base
TO dm_campaign_daily_progress_cf
AS
WITH
-- The PLANNED window per campaign (see item 25 in 07); bad or missing dates excluded.
campaign_window AS (
    SELECT tenant_id, campaign_number,
        min(toDate32(start_date)) AS window_start_date,
        least(max(toDate32(end_date)), toDate32(today())) AS window_end_date
    FROM dm_targets_base
    WHERE node_level > 0 AND start_date > toDate('1970-01-01') AND end_date >= start_date
    GROUP BY tenant_id, campaign_number
    HAVING window_end_date >= window_start_date
),
-- Each delivery row exploded to every ancestor node (the mv_dm_coverage_by_node
-- pattern), keeping the path only down to that node, then each node-day's SET
-- of children. arrayZip binds the absolute level before empty levels are
-- dropped, so a gap in the path cannot renumber the levels below it.
children_per_node_per_day AS (
    SELECT
        delivery.tenant_id AS tenant_id, delivery.campaign_number AS campaign_number,
        delivery.hierarchy_type AS hierarchy_type,
        toUInt8(ancestor.1) AS node_level, ancestor.2 AS code,
        if(node_level >= 1, delivery.level_one_code, '')   AS path_level_one_code,
        if(node_level >= 2, delivery.level_two_code, '')   AS path_level_two_code,
        if(node_level >= 3, delivery.level_three_code, '') AS path_level_three_code,
        if(node_level >= 4, delivery.level_four_code, '')  AS path_level_four_code,
        if(node_level >= 5, delivery.level_five_code, '')  AS path_level_five_code,
        if(node_level >= 6, delivery.level_six_code, '')   AS path_level_six_code,
        if(node_level >= 7, delivery.level_seven_code, '') AS path_level_seven_code,
        if(node_level >= 8, delivery.level_eight_code, '') AS path_level_eight_code,
        if(node_level >= 9, delivery.level_nine_code, '')  AS path_level_nine_code,
        delivery.event_date AS event_date,
        uniqExactMergeState(delivery.administered_uniq) AS children_on_day_state
    FROM dm_smc_administered_by_beneficiary_cf AS delivery
    INNER JOIN campaign_window
        ON  delivery.tenant_id = campaign_window.tenant_id
        AND delivery.campaign_number = campaign_window.campaign_number
    ARRAY JOIN arrayFilter(level_and_code -> level_and_code.2 != '',
                   arrayZip(range(1, 10), [delivery.level_one_code, delivery.level_two_code,
                                           delivery.level_three_code, delivery.level_four_code,
                                           delivery.level_five_code, delivery.level_six_code,
                                           delivery.level_seven_code, delivery.level_eight_code,
                                           delivery.level_nine_code])) AS ancestor
    WHERE delivery.administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND delivery.delivered_to = 'INDIVIDUAL'
      AND delivery.event_date BETWEEN campaign_window.window_start_date AND campaign_window.window_end_date
    GROUP BY tenant_id, campaign_number, hierarchy_type, node_level, code,
        path_level_one_code, path_level_two_code, path_level_three_code,
        path_level_four_code, path_level_five_code, path_level_six_code,
        path_level_seven_code, path_level_eight_code, path_level_nine_code, event_date
),
-- Only nodes with at least one vaccination in the window (see 07).
nodes_with_vaccinations AS (
    SELECT DISTINCT tenant_id, campaign_number, hierarchy_type, node_level, code,
        path_level_one_code, path_level_two_code, path_level_three_code,
        path_level_four_code, path_level_five_code, path_level_six_code,
        path_level_seven_code, path_level_eight_code, path_level_nine_code
    FROM children_per_node_per_day
),
-- Every calendar day of the window -- the spine that makes zero days visible.
calendar_days AS (
    SELECT tenant_id, campaign_number, window_start_date,
        arrayJoin(arrayMap(day_offset -> window_start_date + day_offset,
                           range(toUInt32(dateDiff('day', window_start_date, window_end_date)) + 1))) AS event_date
    FROM campaign_window
)
SELECT
    node.tenant_id AS tenant_id, node.campaign_number AS campaign_number, node.hierarchy_type AS hierarchy_type,
    node.path_level_one_code   AS level_one_code,   node.path_level_two_code   AS level_two_code,
    node.path_level_three_code AS level_three_code, node.path_level_four_code  AS level_four_code,
    node.path_level_five_code  AS level_five_code,  node.path_level_six_code   AS level_six_code,
    node.path_level_seven_code AS level_seven_code, node.path_level_eight_code AS level_eight_code,
    node.path_level_nine_code  AS level_nine_code,
    node.node_level AS node_level, node.code AS code,
    calendar_day.event_date AS event_date,
    toUInt16(dateDiff('day', calendar_day.window_start_date, calendar_day.event_date) + 1) AS campaign_day,
    -- A day with no match in children_per_node_per_day gets the column default --
    -- an EMPTY state -- so it finalizes to 0 and adds nothing to the running union.
    toUInt64(finalizeAggregation(day_children.children_on_day_state)) AS children_vaccinated,
    -- Running UNION of the daily sets, not a running sum of counts: a child seen
    -- on two days (SMC cycles) stays one child.
    toUInt64(uniqExactMerge(day_children.children_on_day_state) OVER (
        PARTITION BY node.tenant_id, node.campaign_number, node.hierarchy_type, node.node_level, node.code
        ORDER BY calendar_day.event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS children_vaccinated_cumulative,
    now() AS refreshed_at
FROM nodes_with_vaccinations AS node
INNER JOIN calendar_days AS calendar_day
    ON  node.tenant_id = calendar_day.tenant_id
    AND node.campaign_number = calendar_day.campaign_number
LEFT JOIN children_per_node_per_day AS day_children
    ON  node.tenant_id = day_children.tenant_id
    AND node.campaign_number = day_children.campaign_number
    AND node.hierarchy_type = day_children.hierarchy_type
    AND node.node_level = day_children.node_level
    AND node.code = day_children.code
    AND calendar_day.event_date = day_children.event_date;

-- 6. mv_dm_campaign_coverage_cf -> dm_campaign_coverage_cf
--
-- Copy of mv_dm_campaign_coverage (08_coverage_marts.sql):
-- verbatim, reading the _cf marts.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign_coverage_cf
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_smc_administered_by_beneficiary_cf, mv_dm_targets_base
TO dm_campaign_coverage_cf
AS
WITH campaign_administered AS (
    -- ADMINISTRATION_SUCCESS or VISITED, INDIVIDUAL only -- the same predicate
    -- as KPI r3/r28 (and the rest of the administered KPIs) in 13 and viz 714,

    -- so this mart agrees with every other coverage figure. is_delivered is
    -- deliberately NOT added: no other coverage query uses it, and adding it
    -- here alone would make this the one number that disagrees.
    SELECT
        tenant_id,
        campaign_number,
        uniqExactMergeIf(administered_uniq,
            administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
            AND delivered_to = 'INDIVIDUAL')     AS total_administered,
        sumIf(resource_quantity_sum,
            administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
            AND delivered_to = 'INDIVIDUAL')     AS total_product_administered
    FROM dm_smc_administered_by_beneficiary_cf
    GROUP BY tenant_id, campaign_number
),
campaign_targets_cte AS (
    SELECT
        f.tenant_id AS tenant_id,
        f.campaign_number AS campaign_number,
        sum(f.target_population) AS target_population
    FROM (
        -- The campaign-wide cut: the SHALLOWEST level this campaign+target_type
        -- actually has. Those rows are its own declared totals and are
        -- disjoint, so summing them counts the campaign exactly once.
        --
        -- min(node_level) rather than a fixed level, because campaigns start at
        -- different depths and a hardcoded level would silently return nothing
        -- for those that start deeper. node_level > 0 drops rows whose boundary
        -- never resolved.
        SELECT *
        FROM (
            SELECT *, min(node_level) OVER (PARTITION BY tenant_id, campaign_number, target_type) AS root_level
            FROM dm_targets_base
            WHERE node_level > 0
        )
        WHERE node_level = root_level
    ) AS f
    WHERE f.target_type = 'INDIVIDUAL'
      -- THE ROLL-UP CUT. A campaign carries a target row at every level of its
      -- tree -- parent totals plus child totals -- so summing across levels
      -- inflates this denominator by roughly the tree depth.
      --
      -- The shallowest level is used rather than the leaf level because it is
      -- the campaign's own declared total, and it is robust to a leaf project
      -- whose target row has not landed yet. It is relative to the campaign's
      -- ACTUAL node set for this target_type, so it can never be spuriously
      -- empty.
      --
      -- CORRECT HERE ONLY, because this mart is CAMPAIGN GRAIN -- one number
      -- per campaign, no boundary bucketing. DO NOT COPY IT INTO A
      -- BOUNDARY-BUCKETED QUERY: a chart that buckets by district must select
      -- node_level = 3 and the district code, or it gets the campaign's single
      -- top row. See the "NOT DIRECTLY SUMMABLE" block on item 2 in 07.
      --
      -- PARTITIONed by target_type, never taken over a union of all types: a
      -- campaign's HOUSEHOLD node set is usually a strict subset of its
      -- INDIVIDUAL one, so a union-based cut returns nothing for any type whose
      -- nodes exclude the union root.
    GROUP BY f.tenant_id, f.campaign_number
)
-- Targets drive the LEFT JOIN so a campaign with targets but zero deliveries
-- still appears, at 0%. ifNull/nullIf are meaningful here (unlike on the
-- non-Nullable silver columns): the join genuinely produces nulls, and
-- target_population can legitimately be 0.
SELECT
    t.tenant_id AS tenant_id,
    t.campaign_number AS campaign_number,
    ifNull(a.total_administered, 0) AS total_administered,
    ifNull(a.total_product_administered, 0) AS total_product_administered,
    t.target_population AS target_population,
    round(ifNull(a.total_administered, 0) / nullIf(t.target_population, 0) * 100, 2) AS coverage_percentage
FROM campaign_targets_cte t
LEFT JOIN campaign_administered a
    ON t.tenant_id = a.tenant_id
   AND t.campaign_number = a.campaign_number;

-- 7. mv_dm_zero_dose_daily_by_node_cf -> dm_zero_dose_daily_by_node_cf
--
-- Copy of mv_dm_zero_dose_daily_by_node (10_overview_marts.sql):
-- the receivedOPVBefore child row replaces the additional_details parse.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_zero_dose_daily_by_node_cf
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_targets_base
TO dm_zero_dose_daily_by_node_cf
AS
WITH
-- The task's campaign-specific values, one row per task version. A key the
-- version doesn't carry stays 0 / '' (join_use_nulls = 0), as in silver.
task_campaign_fields AS (
    SELECT
        id,
        last_modified_time,
        anyIf(value_string, key = 'receivedOPVBefore' AND value_type = 'string') AS received_opv_before
    FROM project_task_campaign_field FINAL
    WHERE key IN ('receivedOPVBefore')
    GROUP BY id, last_modified_time
),
-- project_task_entity with those values under the silver column names, so the
-- query below is the original one. Joined on the version: keys a later version
-- dropped are left behind in the child table.
tasks_with_campaign_fields AS (
    SELECT
        task.*,
        fields.received_opv_before AS received_opv_before
    FROM project_task_entity AS task FINAL
    LEFT JOIN task_campaign_fields AS fields
        ON  fields.id = task.id
        AND fields.last_modified_time = task.last_modified_time
),
-- The PLANNED window per campaign -- the same CTE as 1d (see item 25 in 07),
-- copied so the two views do not depend on each other.
campaign_window AS (
    SELECT tenant_id, campaign_number,
        min(toDate32(start_date)) AS window_start_date,
        least(max(toDate32(end_date)), toDate32(today())) AS window_end_date
    FROM dm_targets_base
    WHERE node_level > 0 AND start_date > toDate('1970-01-01') AND end_date >= start_date
    GROUP BY tenant_id, campaign_number
    HAVING window_end_date >= window_start_date
),
-- Every child task that answered receivedOPVBefore (YES or NO) inside the
-- window, from the task's receivedOPVBefore child row.
answered_tasks AS (
    SELECT
        task.tenant_id AS tenant_id, task.campaign_number AS campaign_number,
        task.hierarchy_type AS hierarchy_type,
        task.level_one_code AS level_one_code, task.level_two_code AS level_two_code,
        task.level_three_code AS level_three_code, task.level_four_code AS level_four_code,
        task.level_five_code AS level_five_code, task.level_six_code AS level_six_code,
        task.level_seven_code AS level_seven_code, task.level_eight_code AS level_eight_code,
        task.level_nine_code AS level_nine_code,
        task.task_dates AS event_date,
        task.project_beneficiary_client_reference_id AS beneficiary_reference_id,
        task.received_opv_before AS received_opv_before
    FROM tasks_with_campaign_fields AS task
    INNER JOIN campaign_window
        ON  task.tenant_id = campaign_window.tenant_id
        AND task.campaign_number = campaign_window.campaign_number
    WHERE task.task_dates BETWEEN campaign_window.window_start_date AND campaign_window.window_end_date
      AND task.project_beneficiary_client_reference_id != ''
      AND received_opv_before IN ('YES', 'NO')
),
-- Each answered task exploded to every ancestor node (the 1d pattern), then
-- each node-day's SETS of zero-dose and enumerated children.
children_per_node_per_day AS (
    SELECT
        tenant_id, campaign_number, hierarchy_type,
        toUInt8(ancestor.1) AS node_level, ancestor.2 AS code,
        if(node_level >= 1, level_one_code, '')   AS path_level_one_code,
        if(node_level >= 2, level_two_code, '')   AS path_level_two_code,
        if(node_level >= 3, level_three_code, '') AS path_level_three_code,
        if(node_level >= 4, level_four_code, '')  AS path_level_four_code,
        if(node_level >= 5, level_five_code, '')  AS path_level_five_code,
        if(node_level >= 6, level_six_code, '')   AS path_level_six_code,
        if(node_level >= 7, level_seven_code, '') AS path_level_seven_code,
        if(node_level >= 8, level_eight_code, '') AS path_level_eight_code,
        if(node_level >= 9, level_nine_code, '')  AS path_level_nine_code,
        event_date,
        uniqExactStateIf(campaign_number, beneficiary_reference_id, received_opv_before = 'NO') AS zero_dose_children_state,
        uniqExactState(campaign_number, beneficiary_reference_id) AS enumerated_children_state
    FROM answered_tasks
    ARRAY JOIN arrayFilter(level_and_code -> level_and_code.2 != '',
                   arrayZip(range(1, 10), [level_one_code, level_two_code, level_three_code,
                                           level_four_code, level_five_code, level_six_code,
                                           level_seven_code, level_eight_code, level_nine_code])) AS ancestor
    GROUP BY tenant_id, campaign_number, hierarchy_type, node_level, code,
        path_level_one_code, path_level_two_code, path_level_three_code,
        path_level_four_code, path_level_five_code, path_level_six_code,
        path_level_seven_code, path_level_eight_code, path_level_nine_code, event_date
),
-- Only nodes with at least one enumerated child in the window (see 07).
nodes_with_enumerated_children AS (
    SELECT DISTINCT tenant_id, campaign_number, hierarchy_type, node_level, code,
        path_level_one_code, path_level_two_code, path_level_three_code,
        path_level_four_code, path_level_five_code, path_level_six_code,
        path_level_seven_code, path_level_eight_code, path_level_nine_code
    FROM children_per_node_per_day
),
-- Every calendar day of the window, so a selected day always has a row.
calendar_days AS (
    SELECT tenant_id, campaign_number, window_start_date,
        arrayJoin(arrayMap(day_offset -> window_start_date + day_offset,
                           range(toUInt32(dateDiff('day', window_start_date, window_end_date)) + 1))) AS event_date
    FROM campaign_window
),
-- Running UNION of each day's sets: a child identified on two days stays one
-- child, and a child once seen as zero-dose stays zero-dose from that day on.
cumulative_per_node_per_day AS (
    SELECT
        node.tenant_id AS tenant_id, node.campaign_number AS campaign_number, node.hierarchy_type AS hierarchy_type,
        node.path_level_one_code   AS level_one_code,   node.path_level_two_code   AS level_two_code,
        node.path_level_three_code AS level_three_code, node.path_level_four_code  AS level_four_code,
        node.path_level_five_code  AS level_five_code,  node.path_level_six_code   AS level_six_code,
        node.path_level_seven_code AS level_seven_code, node.path_level_eight_code AS level_eight_code,
        node.path_level_nine_code  AS level_nine_code,
        node.node_level AS node_level, node.code AS code,
        calendar_day.event_date AS event_date,
        toUInt16(dateDiff('day', calendar_day.window_start_date, calendar_day.event_date) + 1) AS campaign_day,
        -- A day with no match gets EMPTY states, which add nothing to the union.
        toUInt64(uniqExactMerge(day_children.zero_dose_children_state) OVER (
            PARTITION BY node.tenant_id, node.campaign_number, node.hierarchy_type, node.node_level, node.code
            ORDER BY calendar_day.event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS zero_dose_children_cumulative,
        toUInt64(uniqExactMerge(day_children.enumerated_children_state) OVER (
            PARTITION BY node.tenant_id, node.campaign_number, node.hierarchy_type, node.node_level, node.code
            ORDER BY calendar_day.event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS children_enumerated_cumulative
    FROM nodes_with_enumerated_children AS node
    INNER JOIN calendar_days AS calendar_day
        ON  node.tenant_id = calendar_day.tenant_id
        AND node.campaign_number = calendar_day.campaign_number
    LEFT JOIN children_per_node_per_day AS day_children
        ON  node.tenant_id = day_children.tenant_id
        AND node.campaign_number = day_children.campaign_number
        AND node.hierarchy_type = day_children.hierarchy_type
        AND node.node_level = day_children.node_level
        AND node.code = day_children.code
        AND calendar_day.event_date = day_children.event_date
)
SELECT
    tenant_id, campaign_number, hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
    level_six_code, level_seven_code, level_eight_code, level_nine_code,
    node_level, code, event_date, campaign_day,
    zero_dose_children_cumulative,
    children_enumerated_cumulative,
    if(children_enumerated_cumulative = 0, 0,
       zero_dose_children_cumulative * 100 / children_enumerated_cumulative) AS zero_dose_percent,
    now() AS refreshed_at
FROM cumulative_per_node_per_day;

-- 8. mv_dm_missed_children_daily_by_node_cf -> dm_missed_children_daily_by_node_cf
--
-- Copy of mv_dm_missed_children_daily_by_node (10_overview_marts.sql):
-- child rows for reason and cycleIndex replace additional_details / the silver column.
-- The tasks are narrowed on silver columns first (miss statuses; successful
-- administrations of missed children) and the child table is read only for
-- those ids, instead of pivoting every task's cycleIndex twice.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_missed_children_daily_by_node_cf
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_targets_base
TO dm_missed_children_daily_by_node_cf
AS
WITH
-- The PLANNED window per campaign -- the same CTE as 1d (see item 25 in 07),
-- copied so the views do not depend on each other.
campaign_window AS (
    SELECT tenant_id, campaign_number,
        min(toDate32(start_date)) AS window_start_date,
        least(max(toDate32(end_date)), toDate32(today())) AS window_end_date
    FROM dm_targets_base
    WHERE node_level > 0 AND start_date > toDate('1970-01-01') AND end_date >= start_date
    GROUP BY tenant_id, campaign_number
    HAVING window_end_date >= window_start_date
),
-- Tasks inside the window whose STATUS can record a miss. Real SMC data uses
-- the statuses; the ES/polio shape is ADMINISTRATION_FAILED with a `reason`
-- child row, so those are kept as candidates until their reason is known.
candidate_missed_tasks AS (
    SELECT
        task.id AS id, task.last_modified_time AS last_modified_time,
        task.tenant_id AS tenant_id, task.campaign_number AS campaign_number,
        task.administration_status AS administration_status,
        task.project_beneficiary_client_reference_id AS beneficiary_reference_id,
        task.created_time AS task_time, task.task_dates AS event_date,
        (task.hierarchy_type, task.level_one_code, task.level_two_code, task.level_three_code,
         task.level_four_code, task.level_five_code, task.level_six_code, task.level_seven_code,
         task.level_eight_code, task.level_nine_code) AS boundary_path
    FROM project_task_entity AS task FINAL
    INNER JOIN campaign_window
        ON  task.tenant_id = campaign_window.tenant_id
        AND task.campaign_number = campaign_window.campaign_number
    WHERE task.task_dates BETWEEN campaign_window.window_start_date AND campaign_window.window_end_date
      AND task.project_beneficiary_client_reference_id != ''
      AND task.administration_status IN ('BENEFICIARY_REFUSED', 'BENEFICIARY_ABSENT', 'ADMINISTRATION_FAILED')
),
-- cycleIndex and reason of tasks with a miss status only: the child table is
-- ordered by (id, key), so the id filter reads just their rows. The id list is
-- a plain status scan (no FINAL, no window) -- a superset is harmless, the
-- join below is on the task version. A missing key stays 0 / ''
-- (join_use_nulls = 0), as in silver.
candidate_missed_task_fields AS (
    SELECT
        id,
        last_modified_time,
        toUInt8(anyIf(value_number, key = 'cycleIndex' AND value_type = 'number')) AS cycle_index,
        anyIf(value_string, key = 'reason' AND value_type = 'string') AS reason
    FROM project_task_campaign_field FINAL
    WHERE id IN (SELECT id FROM project_task_entity
                 WHERE administration_status IN ('BENEFICIARY_REFUSED', 'BENEFICIARY_ABSENT', 'ADMINISTRATION_FAILED'))
      AND key IN ('cycleIndex', 'reason')
    GROUP BY id, last_modified_time
),
-- Every task inside the window that records a child as absent or refused.
missed_tasks AS (
    SELECT
        candidate.tenant_id AS tenant_id, candidate.campaign_number AS campaign_number,
        fields.cycle_index AS cycle_index,
        candidate.beneficiary_reference_id AS beneficiary_reference_id,
        candidate.task_time AS task_time, candidate.event_date AS event_date,
        candidate.boundary_path AS boundary_path
    FROM candidate_missed_tasks AS candidate
    LEFT JOIN candidate_missed_task_fields AS fields
        ON  fields.id = candidate.id
        AND fields.last_modified_time = candidate.last_modified_time
    WHERE candidate.administration_status IN ('BENEFICIARY_REFUSED', 'BENEFICIARY_ABSENT')
       OR (candidate.administration_status = 'ADMINISTRATION_FAILED' AND fields.reason IN ('REFUSED', 'ABSENCE'))
),
-- One row per missed child PER CYCLE: when and where they were FIRST missed,
-- plus the dates of any further miss records (noted, never counted as new
-- misses). The cycle is in the key because an SMC child vaccinated in cycle 1
-- and absent in cycle 2 is genuinely missed for cycle 2.
missed_children AS (
    SELECT
        tenant_id, campaign_number, cycle_index, beneficiary_reference_id,
        min(task_time) AS first_missed_time,
        argMin(event_date, task_time) AS first_missed_date,
        argMin(boundary_path, task_time) AS boundary_path,
        arrayPopFront(arrayMap(time_and_date -> time_and_date.2,
                               arraySort(groupArray((task_time, event_date))))) AS repeat_miss_dates
    FROM missed_tasks
    GROUP BY tenant_id, campaign_number, cycle_index, beneficiary_reference_id
),
-- Successful administrations of children who have a miss-status task at all
-- (any cycle, any version: a cheap superset of the missed children). Which
-- (cycle, child) pairs were really missed is settled by child_outcomes' join.
candidate_administered_tasks AS (
    SELECT
        task.id AS id, task.last_modified_time AS last_modified_time,
        task.tenant_id AS tenant_id, task.campaign_number AS campaign_number,
        task.project_beneficiary_client_reference_id AS beneficiary_reference_id,
        task.created_time AS task_time, task.task_dates AS event_date
    FROM project_task_entity AS task FINAL
    INNER JOIN campaign_window
        ON  task.tenant_id = campaign_window.tenant_id
        AND task.campaign_number = campaign_window.campaign_number
    WHERE task.task_dates BETWEEN campaign_window.window_start_date AND campaign_window.window_end_date
      AND task.administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND task.delivered_to = 'INDIVIDUAL'
      AND task.project_beneficiary_client_reference_id IN (SELECT project_beneficiary_client_reference_id FROM project_task_entity
          WHERE administration_status IN ('BENEFICIARY_REFUSED', 'BENEFICIARY_ABSENT', 'ADMINISTRATION_FAILED'))
),
-- cycleIndex of those candidates only (the same id-filtered child read, the
-- id list again a plain scan of the same predicates).
candidate_administered_task_cycles AS (
    SELECT
        id,
        last_modified_time,
        toUInt8(anyIf(value_number, key = 'cycleIndex' AND value_type = 'number')) AS cycle_index
    FROM project_task_campaign_field FINAL
    WHERE id IN (SELECT id FROM project_task_entity
                 WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
                   AND delivered_to = 'INDIVIDUAL'
                   AND project_beneficiary_client_reference_id IN (SELECT project_beneficiary_client_reference_id FROM project_task_entity
                       WHERE administration_status IN ('BENEFICIARY_REFUSED', 'BENEFICIARY_ABSENT', 'ADMINISTRATION_FAILED')))
      AND key = 'cycleIndex'
    GROUP BY id, last_modified_time
),
-- Those administrations per (cycle, child); child_outcomes LEFT JOINs them on
-- the full key, so groups for a cycle the child wasn't missed in are unused.
administered_tasks_of_missed_children AS (
    SELECT
        candidate.tenant_id AS tenant_id, candidate.campaign_number AS campaign_number,
        cycles.cycle_index AS cycle_index,
        candidate.beneficiary_reference_id AS beneficiary_reference_id,
        groupArray((candidate.task_time, candidate.event_date)) AS administered_times_and_dates
    FROM candidate_administered_tasks AS candidate
    LEFT JOIN candidate_administered_task_cycles AS cycles
        ON  cycles.id = candidate.id
        AND cycles.last_modified_time = candidate.last_modified_time
    GROUP BY tenant_id, campaign_number, cycle_index, beneficiary_reference_id
),
-- Each missed child's outcome. A child vaccinated in the same cycle BEFORE
-- their first miss was never a missed child and is dropped. first_reached_date is the date of the
-- first administration AFTER the first miss (by task time, so a revisit later
-- the same day counts); 1970-01-01 means not reached.
child_outcomes AS (
    SELECT
        missed.tenant_id AS tenant_id, missed.campaign_number AS campaign_number,
        missed.boundary_path AS boundary_path,
        missed.first_missed_date AS first_missed_date,
        arraySort(arrayFilter(time_and_date -> time_and_date.1 > missed.first_missed_time,
                              administered.administered_times_and_dates)) AS administered_after_miss,
        if(empty(administered_after_miss), toDate32('1970-01-01'), administered_after_miss[1].2) AS first_reached_date,
        missed.repeat_miss_dates AS repeat_miss_dates
    FROM missed_children AS missed
    LEFT JOIN administered_tasks_of_missed_children AS administered
        ON  missed.tenant_id = administered.tenant_id
        AND missed.campaign_number = administered.campaign_number
        AND missed.cycle_index = administered.cycle_index
        AND missed.beneficiary_reference_id = administered.beneficiary_reference_id
    WHERE NOT arrayExists(time_and_date -> time_and_date.1 < missed.first_missed_time,
                          administered.administered_times_and_dates)
),
-- Each child as dated events: one 'missed', at most one 'reached', and one
-- 'repeat_miss' per further miss record -- then spread to every ancestor node
-- of the child's first-missed boundary path (the 1d pattern).
events_per_node_per_day AS (
    SELECT
        tenant_id, campaign_number, boundary_path.1 AS hierarchy_type,
        toUInt8(ancestor.1) AS node_level, ancestor.2 AS code,
        if(node_level >= 1, boundary_path.2, '')  AS path_level_one_code,
        if(node_level >= 2, boundary_path.3, '')  AS path_level_two_code,
        if(node_level >= 3, boundary_path.4, '')  AS path_level_three_code,
        if(node_level >= 4, boundary_path.5, '')  AS path_level_four_code,
        if(node_level >= 5, boundary_path.6, '')  AS path_level_five_code,
        if(node_level >= 6, boundary_path.7, '')  AS path_level_six_code,
        if(node_level >= 7, boundary_path.8, '')  AS path_level_seven_code,
        if(node_level >= 8, boundary_path.9, '')  AS path_level_eight_code,
        if(node_level >= 9, boundary_path.10, '') AS path_level_nine_code,
        child_event.1 AS event_date,
        countIf(child_event.2 = 'missed')      AS children_missed_on_day,
        countIf(child_event.2 = 'reached')     AS children_reached_on_day,
        countIf(child_event.2 = 'repeat_miss') AS repeat_miss_records_on_day
    FROM child_outcomes
    ARRAY JOIN arrayConcat(
                   [(first_missed_date, 'missed')],
                   if(first_reached_date > toDate32('1970-01-01'), [(first_reached_date, 'reached')], []),
                   arrayMap(repeat_date -> (repeat_date, 'repeat_miss'), repeat_miss_dates)) AS child_event
    ARRAY JOIN arrayFilter(level_and_code -> level_and_code.2 != '',
                   arrayZip(range(1, 10), [boundary_path.2, boundary_path.3, boundary_path.4,
                                           boundary_path.5, boundary_path.6, boundary_path.7,
                                           boundary_path.8, boundary_path.9, boundary_path.10])) AS ancestor
    GROUP BY tenant_id, campaign_number, hierarchy_type, node_level, code,
        path_level_one_code, path_level_two_code, path_level_three_code,
        path_level_four_code, path_level_five_code, path_level_six_code,
        path_level_seven_code, path_level_eight_code, path_level_nine_code, event_date
),
-- Only nodes with at least one missed child (see 07).
nodes_with_missed_children AS (
    SELECT DISTINCT tenant_id, campaign_number, hierarchy_type, node_level, code,
        path_level_one_code, path_level_two_code, path_level_three_code,
        path_level_four_code, path_level_five_code, path_level_six_code,
        path_level_seven_code, path_level_eight_code, path_level_nine_code
    FROM events_per_node_per_day
),
-- Every calendar day of the window, so the timeline has no gaps.
calendar_days AS (
    SELECT tenant_id, campaign_number, window_start_date,
        arrayJoin(arrayMap(day_offset -> window_start_date + day_offset,
                           range(toUInt32(dateDiff('day', window_start_date, window_end_date)) + 1))) AS event_date
    FROM campaign_window
),
-- Running sums. Each child-cycle contributes exactly one 'missed' and at most
-- one 'reached' event at each node, so plain sums are exact distinct counts.
cumulative_per_node_per_day AS (
    SELECT
        node.tenant_id AS tenant_id, node.campaign_number AS campaign_number, node.hierarchy_type AS hierarchy_type,
        node.path_level_one_code   AS level_one_code,   node.path_level_two_code   AS level_two_code,
        node.path_level_three_code AS level_three_code, node.path_level_four_code  AS level_four_code,
        node.path_level_five_code  AS level_five_code,  node.path_level_six_code   AS level_six_code,
        node.path_level_seven_code AS level_seven_code, node.path_level_eight_code AS level_eight_code,
        node.path_level_nine_code  AS level_nine_code,
        node.node_level AS node_level, node.code AS code,
        calendar_day.event_date AS event_date,
        toUInt16(dateDiff('day', calendar_day.window_start_date, calendar_day.event_date) + 1) AS campaign_day,
        toUInt64(day_events.children_missed_on_day)     AS children_missed_on_day,
        toUInt64(day_events.children_reached_on_day)    AS children_reached_on_day,
        toUInt64(day_events.repeat_miss_records_on_day) AS repeat_miss_records_on_day,
        toUInt64(sum(day_events.children_missed_on_day) OVER (
            PARTITION BY node.tenant_id, node.campaign_number, node.hierarchy_type, node.node_level, node.code
            ORDER BY calendar_day.event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS children_missed_cumulative,
        toUInt64(sum(day_events.children_reached_on_day) OVER (
            PARTITION BY node.tenant_id, node.campaign_number, node.hierarchy_type, node.node_level, node.code
            ORDER BY calendar_day.event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS children_reached_cumulative
    FROM nodes_with_missed_children AS node
    INNER JOIN calendar_days AS calendar_day
        ON  node.tenant_id = calendar_day.tenant_id
        AND node.campaign_number = calendar_day.campaign_number
    LEFT JOIN events_per_node_per_day AS day_events
        ON  node.tenant_id = day_events.tenant_id
        AND node.campaign_number = day_events.campaign_number
        AND node.hierarchy_type = day_events.hierarchy_type
        AND node.node_level = day_events.node_level
        AND node.code = day_events.code
        AND calendar_day.event_date = day_events.event_date
)
SELECT
    tenant_id, campaign_number, hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
    level_six_code, level_seven_code, level_eight_code, level_nine_code,
    node_level, code, event_date, campaign_day,
    children_missed_on_day,
    children_reached_on_day,
    children_missed_cumulative,
    children_reached_cumulative,
    children_missed_cumulative - children_reached_cumulative AS children_outstanding,
    repeat_miss_records_on_day,
    now() AS refreshed_at
FROM cumulative_per_node_per_day;

-- 9. mv_dm_stock_facility_points_cf -> dm_stock_facility_points_cf
--
-- Copy of mv_dm_stock_facility_points (11_inventory_marts.sql):
-- lat / lng child rows replace additional_details.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_stock_facility_points_cf
REFRESH EVERY 80 YEAR
TO dm_stock_facility_points_cf
AS
WITH
-- The stock row's coordinates, one row per stock version; only versions with
-- BOTH lat and lng as numbers (a null coordinate has no child row).
stock_coordinates AS (
    SELECT
        id,
        last_modified_time,
        anyIf(value_number, key = 'lat') AS lat,
        anyIf(value_number, key = 'lng') AS lng
    FROM stock_campaign_field FINAL
    WHERE key IN ('lat', 'lng') AND value_type = 'number'
    GROUP BY id, last_modified_time
    HAVING countIf(key = 'lat') = 1 AND countIf(key = 'lng') = 1
)
SELECT
    tenant_id,
    campaign_number,
    hierarchy_type,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code,
    facility_id,
    facility_name,
    facility_type,
    avg(coordinates.lat)                             AS latitude,
    avg(coordinates.lng)                             AS longitude,
    toUInt64(count())                                AS transaction_count
FROM stock_entity AS stock FINAL
INNER JOIN stock_coordinates AS coordinates
    ON  coordinates.id = stock.id
    AND coordinates.last_modified_time = stock.last_modified_time
GROUP BY
    tenant_id,
    campaign_number,
    hierarchy_type,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code,
    facility_id,
    facility_name,
    facility_type;
