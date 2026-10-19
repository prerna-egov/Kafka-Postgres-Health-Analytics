-- ==========================================================================
-- OVERVIEW MARTS -- LOGIC
--
-- Gold layer for the DSS_HEALTH_OVERVIEW tab of the
-- `provincial-health-dashboard-iccd` dashboard (viz ids 709-716 in
-- configs/egov-dss-dashboards/dashboard-analytics/MasterDashboardConfig.json).
-- Deliberately scoped to that one dashboard.
--
-- LOGIC only, mirroring the 07/08 split. The target tables
-- (dm_smc_administered_base, dm_smc_adverse_events, dm_stock_balance, dm_campaign_daily_progress,
-- dm_campaign_gender_breakdown, dm_zero_dose_daily_by_node, dm_missed_children_daily_by_node) are defined in
-- 07_mart_tables.sql as items 11, 13, 14, 25, 26, 27 and 28 and must be created BEFORE this file runs -- the
-- views below write into them via TO <table>.
--
-- Every silver source is ReplacingMergeTree, so every read uses FINAL.
--
-- THIS TAB IS NOT A PORT. See the OVERVIEW MARTS banner in 07 -- most tiles
-- read pre-aggregated ES summary indexes whose fields are defined in no repo,
-- so every metric below is a DEFINITION sourced from the product owner and the
-- KPI framework, not a translation of an existing query.
--
-- Field mapping from the ES aggregations to silver:
--     Data.deliveredTo                       -> project_task_entity.delivered_to
--     Data.administrationStatus              -> project_task_entity.administration_status
--     Data.projectBeneficiaryClientReferenceId
--                                            -> project_task_entity.project_beneficiary_client_reference_id
--     Data.additionalDetails.cycleIndex      -> project_task_entity.`cycleIndex`
--     Data.eventType / Data.physicalCount    -> stock_entity.event_type / physical_count
--     Data.targetType / Data.overallTarget / Data.targetPerDay
--                                            -> dm_targets_base (already built; see below)
--     Data.boundaryHierarchy.*               -> level_one_code .. level_nine_code
--     Data.campaignNumber                    -> campaign_number
--   Summary-index fields with NO repo definition, defined here instead:
--     total_households_visited      -> count() over dm_household_registry   (20)
--     total_population_administered -> uniqExactMerge(administered_uniq)    (11)
--     total_administered_resources  -> sumIf(resource_quantity_sum, is_delivered) (11)
--     total_population_refused      -> (11) WHERE administration_status = 'BENEFICIARY_REFUSED'
--     ineligible_population_total   -> (11) WHERE administration_status = 'INELIGIBLE'
--
-- BOUNDARY LEVELS. ES documents carry NAMED boundary fields; silver carries
-- the standard dense, left-packed level_one_code..level_nine_code block. For
-- the ICCD hierarchy those line up as:
--
--     level_one_code    -- Country
--     level_two_code    -- Province                 (the dashboard's fixed filter)
--     level_three_code  -- District                 (default bucket for every panel)
--     level_four_code   -- AdministrativeProvince   (drill 1)
--     level_five_code   -- Locality                 (drill 2)
--     level_six_code    -- Village                  (drill 3)
--
-- Documented here and NOT baked into column names: hierarchy depth is chosen
-- per campaign/hierarchy_type and varies, so a column named `district_code`
-- would be wrong for any campaign whose tree is a different shape. See
-- airflow_dags/CLAUDE.md, "Boundary hierarchy flattening". Marts bucket on
-- CODES, not display names -- egov_api_utils.get_boundary_hierarchy_levels_bulk
-- deliberately stores only raw codes; label resolution is a presentation
-- concern.
--
-- TARGETS COME FROM dm_targets_base (07, item 2), NOT from new logic.
-- Every ES target query filters `exists province` + `must_not exists district`.
-- That is a LEVEL SELECTOR -- "rows whose deepest populated level is exactly
-- province" -- and it translates to node_level, NOT to the root cut:
--
--     FROM dm_targets_base
--     WHERE target_type = 'HOUSEHOLD' | 'INDIVIDUAL' | 'PRODUCT'
--       AND node_level  = <the level the chart buckets at>
--       AND code        = <the boundary code being bucketed>
--
-- A target is declared at the hierarchy's lowest level and summed upward, so
-- the row at level N is already the complete total for its subtree. Picking one
-- level is what avoids counting the same target once per level of depth.
-- is_target_type_root is for the CAMPAIGN-WIDE total only and must not be used
-- here -- it returns one cut per campaign, not a row per boundary. Full rules
-- in the "NOT DIRECTLY SUMMABLE" block on item 2 in 07.
--
-- HOW EACH PANEL IS SERVED. All queries additionally carry the dashboard's own
-- filters: level_two_code = <province>, optionally campaign_number, and
-- event_date within the global date range (the tiles whose dateRefField is ""
-- -- the all-time "till today" halves of each pair, and 715 -- take no date
-- filter). A drill-down moves the bucket column one level down
-- (level_three -> level_four -> level_five -> level_six) and adds the parent
-- level as a predicate; the full nine-level path is kept on every fact so no
-- schema change is needed for any drill level.
--
--   709 DSS_HEALTH_HOUSEHOLDS (4 tiles)
--         visited (date range) : count() FROM dm_household_registry (20), dated
--                                -- one row per household, so count() is the
--                                -- distinct count
--         visited (till today) : same, undated
--         target               : sum(target_population) FROM dm_targets_base
--                                WHERE target_type='HOUSEHOLD' AND node_level=2
--                                  AND code = <province>        -- province tile
--         coverage %           : visited / target * 100
--   710 DSS_HEALTH_POPULATION_ADMINISTERED (4 tiles)
--         administered         : uniqExactMerge(administered_uniq) FROM (11)
--                                WHERE administration_status = 'ADMINISTRATION_SUCCESS'
--                                  AND delivered_to = 'INDIVIDUAL' AND is_delivered
--         target               : dm_targets_base, target_type='INDIVIDUAL',
--                                node_level=2 AND code=<province>
--         coverage %           : administered / target * 100
--   711 DSS_HEALTH_OVERVIEW_DRUG_USED (4 tiles)
--         resources used       : sumIf(resource_quantity_sum, is_delivered) FROM (11)
--         target               : dm_targets_base, target_type='PRODUCT',
--                                node_level=2 AND code=<province>
--         coverage %           : used / target * 100
--   712 populationNotAdministeredByReasonProvinceICCD (donut, 4 slices)
--         Beneficiary Refused  : uniqExactMerge(administered_uniq) FROM (11)
--                                WHERE administration_status = 'BENEFICIARY_REFUSED'
--         Total Ineligible     : same, WHERE administration_status = 'INELIGIBLE'
--         Total Side Effects   : sum(event_count) FROM (13) WHERE event_kind='SIDE_EFFECT'
--         Total Referrals      : sum(event_count) FROM (13) WHERE event_kind='REFERRAL'
--         (its drilldown is the same four measures GROUP BY level_three_code)
--   713 totalComplaintsRegisteredICCD
--         ALREADY SERVED by dm_complaints_base (09) -- no mart here:
--           SELECT application_status, sum(complaint_count) FROM dm_complaints_base
--           WHERE application_status IN ('PENDING_ASSIGNMENT','RESOLVED','REJECTED')
--           GROUP BY application_status
--         and its drilldown complaintsByTypeDrilldownICCD is GROUP BY service_code.
--   714 populationCoverageRankingBarchartByDistrictICCD
--         num : uniqExactMerge(administered_uniq) FROM (11)
--               WHERE administration_status='ADMINISTRATION_SUCCESS' AND delivered_to='INDIVIDUAL'
--               GROUP BY level_three_code
--         den : dm_targets_base, target_type='INDIVIDUAL',
--               node_level=3 AND code=level_three_code (drilldowns: 4/5/6)
--         -> percentage. Drilldowns move to level_four/five/six.
--   715 daysInventoryStockLastsProvinceICCD
--         sum(net_on_hand) FROM (14)
--           / nullIf(sum(target_per_day) FROM dm_targets_base
--                    WHERE target_type='PRODUCT' AND node_level=3
--                      AND code=level_three_code, 0)
--         GROUP BY level_three_code. Undated on both sides.
--   716 checklistCompletionRateDistrictICCD
--         NOT BUILT. No checklist data in silver yet, and despite the name the
--         config computes a bare count with no denominator -- it is not a rate.
--         Build it when checklist data lands and the denominator is decided.
--
-- KNOWN GAPS -- properties of the pipeline, not of this file, stated so these
-- marts are not read as more complete than they are:
--   * Target-dependent numbers resolve only once dm_targets_base is populated.
--     Until then every denominator, the coverage tiles, 714 and 715 are empty.
--   * campaign_number may legitimately be ''. Such rows vanish the moment the
--     dashboard filters by campaign. Deliberately NOT filtered out -- dropping
--     them would hide the gap rather than surface it.
--   * Boundary resolution is partial in silver. Unresolved rows fall into the
--     '' bucket and disappear under a province filter.
--   * cycleIndex = 0 coexists with real cycles in the same campaign, so 0 is
--     most likely "unset". It is kept in the uniqueness key as specified; if it
--     means unset, those rows behave as one pseudo-cycle.
--   * Some ES configs bucket with `terms` and no `size`, silently truncating.
--     That is a config bug, not a spec, and is not reproduced.
-- ==========================================================================

SET allow_experimental_refreshable_materialized_view = 1;


-- 1. mv_dm_smc_administered_base -> dm_smc_administered_base
--
-- NO WHERE CLAUSE AT ALL, and that is the point. administration_status,
-- delivered_to and is_delivered are all in the grain, so each panel picks its
-- own set at read time. This is what lets one mart serve the successful-
-- administration tiles and panel 712's refusal/ineligible slices at once.
-- Contrast mv_dm_successful_deliveries_base, which bakes
-- IN ('ADMINISTRATION_SUCCESS','VISITED') into the mart and so can only ever
-- answer the coverage question.
--
-- DO NOT re-add `delivered_to = 'INDIVIDUAL' AND is_delivered` here. A refused
-- or ineligible beneficiary was never delivered to, so that predicate empties
-- panel 712, viz 750/753, and the refusal/ineligible KPIs (r17/r18/r42/r43/
-- r60/r61 and three columns of the campaign summary) -- silently, as zeroes
-- rather than an error. It also makes both columns constant, which breaks
-- viz 719's `sumIf(resource_quantity_sum, delivered_to = 'INDIVIDUAL')` and
-- viz 722's `sumIf(resource_quantity_sum, is_delivered)`. Consumers that want
-- only successful individual administrations apply the predicate themselves;
-- every query in 13_kpi_queries.sql already does.
--
-- The uniqExactState argument list IS the SMC distinct-beneficiary key
-- documented on the table in 07. toString() casts the two LowCardinality
-- columns to plain String so the AggregateFunction signature matches the
-- column type exactly -- a LowCardinality mismatch here is a create-time type
-- error, not a silent wrong answer, but the cast keeps it unambiguous.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_smc_administered_base
REFRESH EVERY 80 YEAR
TO dm_smc_administered_base
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
FROM project_task_entity FINAL
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


-- 1b. mv_dm_smc_administered_by_beneficiary -> dm_smc_administered_by_beneficiary
--
-- The product-free twin of the mart above. Item 11 fans a child out across one
-- row per product; this collapses that fan-out so a per-CHILD question cannot
-- accidentally be answered by summing per-product rows.
--
-- WHY READING A PRODUCT-GRAINED MART DOES NOT INHERIT ITS FAN-OUT.
-- This looks wrong at first glance and is worth spelling out, because the
-- question keeps coming up: item 11 holds one row per product, so surely a
-- child dosed with several products is already over-counted there, and surely
-- that flows downstream?
--
-- It does not. administered_uniq is an AggregateFunction(uniqExact, ...) -- a
-- SET OF DISTINCT KEYS, not a number. The key is
--
--     (campaign_number, cycleIndex,
--      project_beneficiary_client_reference_id, administration_status)
--
-- and PRODUCT IS NOT IN IT. A child dosed with twenty products contributes the
-- SAME key to twenty different states. uniqExactMergeState takes the UNION of
-- those sets, so the key appears once and the child collapses back to one.
-- Union, not addition, is what makes this exact rather than approximate.
--
-- Worked example, re-runnable -- campaign CMP-2025-08-05-001223, cycleIndex 0,
-- ADMINISTRATION_SUCCESS. Four children, twenty products each:
--
--     rows in item 11                     20
--     sum(administered_count)             80   <- the wrong way
--     uniqExactMerge(administered_uniq)    4   <- what this view does
--     uniqExact(...) over silver           4   <- ground truth
--
-- Whole-dataset check: building this mart by collapsing item 11, versus
-- building it directly from project_task_entity FINAL at the same grain, gives
-- 2829 rows both ways with ZERO rows differing on administered_count or
-- task_rows. Deriving is not an approximation of the direct build; it is the
-- same answer, from 3204 rows instead of 11305 and with no FINAL.
--
-- THE ONLY WAY TO REINTRODUCE THE INFLATION is to SUM administered_count.
-- That is why administered_count is recomputed here with uniqExactMerge and
-- never summed -- summing item 11's per-product counts is the exact bug this
-- mart exists to prevent.
--
-- Deriving also keeps the two marts from drifting apart if a filter is ever
-- edited on one and not the other.
--
-- DEPENDS ON is load-bearing: without it this view can refresh against a
-- half-rebuilt item 11.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_smc_administered_by_beneficiary
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_smc_administered_base
TO dm_smc_administered_by_beneficiary
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
FROM dm_smc_administered_base AS b
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


-- 1c. mv_dm_smc_administered_by_campaign -> dm_smc_administered_by_campaign
--
-- The last step of the collapse: 1 keeps product and date, 1b drops product,
-- this drops date. At this grain a child dosed on two days is ONE row, so
-- administered_count is directly readable -- the correct campaign number stops
-- depending on the reader reaching for uniqExactMerge.
--
-- Derived from 1b rather than from 1 or from silver: one hop, smallest input,
-- and it cannot disagree with the mart it collapses. The union-of-sets argument
-- in 1b covers this second hop unchanged -- event_date is no more part of the
-- distinct-beneficiary key than product_name is, so merging the states across
-- collapsed dates dedupes a child back to one exactly the same way.
--
-- The `b.` qualifier is REQUIRED, not stylistic. The output column carries the
-- same name as the input, and unqualified the analyzer resolves
-- `administered_uniq` to this SELECT's own alias and fails the query with
-- "aggregate function found inside another aggregate function".
--
-- administered_count is recomputed with uniqExactMerge, NEVER summed -- summing
-- 1b's per-day counts is precisely the error this mart exists to prevent.
--
-- The boundary block stays in the grain. Collapsing it would silently pick the
-- id-aware reading over the boundary-aware one and throw away every geographic
-- cut; see the long note on item 24 in 07.
--
-- DEPENDS ON is load-bearing: without it this view can refresh against a
-- half-rebuilt 1b.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_smc_administered_by_campaign
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_smc_administered_by_beneficiary
TO dm_smc_administered_by_campaign
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
FROM dm_smc_administered_by_beneficiary AS b
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


-- 1d. mv_dm_campaign_daily_progress -> dm_campaign_daily_progress
--
-- Children vaccinated per campaign day per boundary node, zero-filled, for the
-- campaign progress bar chart (#2308). Grain, window and reading rules are on
-- item 25 in 07.
--
-- Built from 1b, not silver: the distinct-beneficiary states are already
-- there, so this is a small re-aggregation (tens of thousands of rows) rather
-- than another full scan of project_task_entity.
--
-- DEPENDS ON is load-bearing: without it this view can refresh against a
-- half-rebuilt 1b or targets mart.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign_daily_progress
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_smc_administered_by_beneficiary, mv_dm_targets_base
TO dm_campaign_daily_progress
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
    FROM dm_smc_administered_by_beneficiary AS delivery
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


-- 1e. mv_dm_campaign_gender_breakdown -> dm_campaign_gender_breakdown
--
-- Boys / girls / other / unknown children vaccinated per boundary node, over
-- the whole campaign, for the overview gender section (#2310). Grain, buckets,
-- percentages and the hide flag are on item 26 in 07.
--
-- Built from 1c, not silver: gender is already in its grain and its states
-- carry the distinct-beneficiary key, so this is a small re-aggregation.
--
-- DEPENDS ON is load-bearing: without it this view can refresh against a
-- half-rebuilt 1c.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign_gender_breakdown
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_smc_administered_by_campaign
TO dm_campaign_gender_breakdown
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
    FROM dm_smc_administered_by_campaign AS delivery
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
    FROM dm_smc_administered_by_campaign
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


-- 1f. mv_dm_zero_dose_daily_by_node -> dm_zero_dose_daily_by_node
--
-- Zero-dose children and children enumerated per boundary node, cumulative per
-- campaign day, for the zero-dose map aggregated per health facility (#2316).
-- Definitions, grain and reading rules are on item 27 in 07.
--
-- Read straight from silver: no mart carries receivedOPVBefore. The LIKE is a
-- cheap pre-filter, so the JSON is parsed only on the tasks that carry the key.
--
-- DEPENDS ON is load-bearing: without it this view can refresh against a
-- half-rebuilt targets mart.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_zero_dose_daily_by_node
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_targets_base
TO dm_zero_dose_daily_by_node
AS
WITH
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
-- window. additional_details is the raw {"fields":[{key,value}],...} blob.
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
        JSONExtractString(
            arrayFirst(field -> JSONExtractString(field, 'key') = 'receivedOPVBefore',
                       JSONExtractArrayRaw(task.additional_details, 'fields')),
            'value') AS received_opv_before
    FROM project_task_entity AS task FINAL
    INNER JOIN campaign_window
        ON  task.tenant_id = campaign_window.tenant_id
        AND task.campaign_number = campaign_window.campaign_number
    WHERE task.additional_details LIKE '%receivedOPVBefore%'
      AND task.task_dates BETWEEN campaign_window.window_start_date AND campaign_window.window_end_date
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


-- 1g. mv_dm_missed_children_daily_by_node -> dm_missed_children_daily_by_node
--
-- Children missed (absent / refused) and later reached, per boundary node per
-- campaign day, for the mop-up timeline (#2324). Definitions, grain and
-- reading rules are on item 28 in 07.
--
-- Read straight from silver: the miss reason and the task time are in no
-- mart. The work is bounded by the MISSED children, not by all tasks: the
-- administered tasks are read only for children who were missed (a semi-join
-- on that small set), and each child collapses to one first-missed date and at
-- most one first-reached date before anything is spread over nodes and days.
--
-- DEPENDS ON is load-bearing: without it this view can refresh against a
-- half-rebuilt targets mart.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_missed_children_daily_by_node
REFRESH EVERY 80 YEAR
    DEPENDS ON mv_dm_targets_base
TO dm_missed_children_daily_by_node
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
-- Every task inside the window that records a child as absent or refused.
-- Real SMC data uses the statuses; the ES/polio shape is ADMINISTRATION_FAILED
-- with a `reason` field in the raw additional_details.
missed_tasks AS (
    SELECT
        task.tenant_id AS tenant_id, task.campaign_number AS campaign_number,
        task.cycleIndex AS cycle_index,
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
      AND (task.administration_status IN ('BENEFICIARY_REFUSED', 'BENEFICIARY_ABSENT')
           OR (task.administration_status = 'ADMINISTRATION_FAILED'
               AND JSONExtractString(
                       arrayFirst(field -> JSONExtractString(field, 'key') = 'reason',
                                  JSONExtractArrayRaw(task.additional_details, 'fields')),
                       'value') IN ('REFUSED', 'ABSENCE')))
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
-- Successful administrations, read ONLY for children who were missed in that cycle.
administered_tasks_of_missed_children AS (
    SELECT
        task.tenant_id AS tenant_id, task.campaign_number AS campaign_number,
        task.cycleIndex AS cycle_index,
        task.project_beneficiary_client_reference_id AS beneficiary_reference_id,
        groupArray((task.created_time, task.task_dates)) AS administered_times_and_dates
    FROM project_task_entity AS task FINAL
    INNER JOIN campaign_window
        ON  task.tenant_id = campaign_window.tenant_id
        AND task.campaign_number = campaign_window.campaign_number
    WHERE task.task_dates BETWEEN campaign_window.window_start_date AND campaign_window.window_end_date
      AND task.administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND task.delivered_to = 'INDIVIDUAL'
      AND (task.tenant_id, task.campaign_number, task.cycleIndex, task.project_beneficiary_client_reference_id) IN
          (SELECT tenant_id, campaign_number, cycle_index, beneficiary_reference_id FROM missed_children)
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


-- 2. mv_dm_smc_adverse_events -> dm_smc_adverse_events
--
-- UNION ALL of the two entities, each tagged with its own event_kind. Both
-- silver tables carry an identical boundary/campaign/task_dates block, so the
-- two legs line up column-for-column with no reshaping. FINAL on both.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_smc_adverse_events
REFRESH EVERY 80 YEAR
TO dm_smc_adverse_events
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
    event_kind,
    toUInt64(count()) AS event_count
FROM
(
    SELECT
        tenant_id, campaign_number, hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code,
        level_five_code, level_six_code, level_seven_code, level_eight_code,
        level_nine_code,
        task_dates AS event_date,
        'SIDE_EFFECT' AS event_kind
    FROM side_effect_entity FINAL

    UNION ALL

    SELECT
        tenant_id, campaign_number, hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code,
        level_five_code, level_six_code, level_seven_code, level_eight_code,
        level_nine_code,
        task_dates AS event_date,
        'REFERRAL' AS event_kind
    FROM referral_entity FINAL
)
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
    event_kind;



-- 3. mv_dm_stock_balance -> dm_stock_balance
--
-- All-time cumulative, no date grain -- the ES query's dateRefField is "".
--
-- The RECEIVED/DISPATCHED pair is matched case-insensitively: stock_entity
-- .event_type is a straight copy of bronze stock.transaction_type, and
-- stock_transformation.py itself compares that column with
-- lower(...) = 'received' when deciding the facility/counterparty direction,
-- so the source vocabulary is not guaranteed to be upper-case here either.
--
-- net_on_hand is computed rather than left to the reader so that the sign
-- convention (received positive, dispatched negative) lives in exactly one
-- place. It is NOT clamped -- see the table comment in 07.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_stock_balance
REFRESH EVERY 80 YEAR
TO dm_stock_balance
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
    product_name,
    sumIf(toInt64(physical_count), upper(event_type) = 'RECEIVED')   AS received_qty,
    sumIf(toInt64(physical_count), upper(event_type) = 'DISPATCHED') AS dispatched_qty,
    received_qty - dispatched_qty                                   AS net_on_hand
FROM stock_entity FINAL
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
    product_name;
