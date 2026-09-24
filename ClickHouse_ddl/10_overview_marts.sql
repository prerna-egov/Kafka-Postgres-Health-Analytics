-- ==========================================================================
-- OVERVIEW MARTS -- LOGIC
--
-- Gold layer for the DSS_HEALTH_OVERVIEW tab of the
-- `provincial-health-dashboard-iccd` dashboard (viz ids 709-716 in
-- configs/egov-dss-dashboards/dashboard-analytics/MasterDashboardConfig.json).
-- Deliberately scoped to that one dashboard.
--
-- LOGIC only, mirroring the 07/08 split. The four target tables
-- (dm_smc_administered_base, dm_smc_adverse_events, dm_stock_balance) are defined in 07_mart_tables.sql as items 11, 13 and 14 and must
-- be created BEFORE this file runs -- the views below write into them via
-- TO <table>.
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
REFRESH EVERY 1 HOUR
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
REFRESH EVERY 1 HOUR
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
REFRESH EVERY 1 HOUR
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


-- 2. mv_dm_smc_adverse_events -> dm_smc_adverse_events
--
-- UNION ALL of the two entities, each tagged with its own event_kind. Both
-- silver tables carry an identical boundary/campaign/task_dates block, so the
-- two legs line up column-for-column with no reshaping. FINAL on both.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_smc_adverse_events
REFRESH EVERY 1 HOUR
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
REFRESH EVERY 1 HOUR
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
