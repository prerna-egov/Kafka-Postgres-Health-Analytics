-- ==========================================================================
-- INVENTORY MARTS -- LOGIC
--
-- Gold layer for the DSS_HEALTH_INVENTORY tab of the
-- `provincial-health-dashboard-iccd` dashboard (viz ids 717-723 in
-- configs/egov-dss-dashboards/dashboard-analytics/MasterDashboardConfig.json).
-- Deliberately scoped to that one dashboard.
--
-- LOGIC only, mirroring the 07/08 split. The four target tables
-- (dm_stock_transactions, dm_stock_facility_points, dm_stock_reconciliation,
-- dm_user_sync) are defined in 07_mart_tables.sql as items 15-18 and must be
-- created BEFORE this file runs -- the views below write into them via
-- TO <table>.
--
-- Each refresh rebuilds its target wholesale (a refreshable MV with a TO
-- target and no APPEND clause replaces the target's contents atomically), so
-- there are never multiple versions of a row to collapse -- which is why those
-- targets are plain MergeTree, not ReplacingMergeTree.
--
-- Every silver source here is ReplacingMergeTree, so every read uses FINAL:
-- without it, CDC row versions that background merges haven't collapsed yet
-- are counted more than once.
--
-- THIS TAB IS A STRAIGHT PORT, unlike the Overview tab. All six ES indexes it
-- reads are raw Data.-prefixed event indexes, so each metric below translates
-- an existing query rather than inventing a definition.
--
-- Field mapping from the ES aggregations to silver:
--     Data.eventType            -> stock_entity.event_type       (RECEIVED / DISPATCHED)
--     Data.reason               -> stock_entity.reason           (note: NOT `transactionReason`)
--     Data.physicalCount        -> stock_entity.physical_count   (there is no `quantity` column on stock)
--     Data.facilityId / Name / Type
--                               -> stock_entity.facility_id / facility_name / facility_type
--     Data.additionalDetails.lat / .lng
--                               -> JSONExtractFloat(stock_entity.additional_details, 'lat'/'lng')
--     Data.stockReconciliation.physicalCount
--                               -> stock_reconciliation_entity.physical_count
--     Data.role                 -> project_staff_entity.role / stock_entity.role
--     Data.userId               -> project_staff_entity.user_id
--     Data.syncedUserId         -> stock_entity.user_name  (see mv 4 below -- no user id on stock)
--     Data.boundaryHierarchy.*  -> level_one_code .. level_nine_code
--     Data.campaignNumber       -> campaign_number
--   Read from marts built earlier, not re-derived here:
--     Data.targetType / targetPerDay / overallTarget
--                               -> dm_targets_base (07 item 2), level selector
--     Data.quantity (project-task)
--                               -> dm_smc_administered_base.resource_quantity_sum (07 item 11)
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
-- per campaign/hierarchy_type and varies. Marts bucket on CODES, not display
-- names. See airflow_dags/CLAUDE.md, "Boundary hierarchy flattening".
--
-- TARGETS COME FROM dm_targets_base (07 item 2), NOT from new logic.
-- The ES target queries filter `exists district` + `must_not exists
-- administrativeProvince` -- a LEVEL SELECTOR meaning "rows whose deepest
-- populated level is exactly district", which translates to node_level, NOT to
-- the root cut. Read it as:
--     FROM dm_targets_base
--     WHERE target_type = 'PRODUCT' AND node_level = 3 AND code = level_three_code
-- A target is declared at the hierarchy's lowest level and summed upward, so the
-- level-N row is already the complete subtree total; picking one level is what
-- stops the same target being counted once per level of depth.
-- is_target_type_root is CAMPAIGN-WIDE only -- see the "HOW TO READ A TARGET AT
-- A LEVEL" block on item 6 in 07.
--
-- HOW EACH PANEL IS SERVED. All queries carry level_two_code = <province> and
-- optionally campaign_number. Every chart on this tab is ALL-TIME
-- (dateRefField "") except 720 (Data.createdTime) and 721
-- (Data.lastModifiedTime) -- which is why marts 15-17 have no date grain.
--
--   717 warehouseManagerUserSyncICCD
--         FROM dm_user_sync WHERE role = 'WAREHOUSE_MANAGER'
--         rate = uniqExactMerge(users_synced_uniq)
--                / nullIf(uniqExactMerge(users_created_uniq), 0) * 100
--   718 daysInventoryStockLastsProvinceICCD
--         ALREADY SERVED by dm_stock_balance (14) -- the same chart id also
--         appears on the Overview tab. Nothing is built for it here:
--           sum(net_on_hand) / nullIf(sum(target_per_day), 0)
--   719 inventoryStockInHandProvinceICCD            (3 series)
--         Available Stock : sumIf(quantity_sum, event_type='RECEIVED')
--                         - sumIf(quantity_sum, event_type='DISPATCHED')  FROM 15
--         Overall Target  : dm_targets_base, PRODUCT, node_level=3 + code
--         Required Stock  : Overall Target
--                         - sumIf(resource_quantity_sum, delivered_to='INDIVIDUAL') FROM 11
--   720 inventoryTheoreticalVsManualStockDistrictICCD
--         Theoretical : RECEIVED - DISPATCHED  FROM 15
--         Manual      : sum(physical_count)    FROM 17   (latest per facility; see 07)
--   721 stockTransactionsBreakdownProvincialICCD    (6 series, GROUP BY level_three_code)
--         StockReceived : sumIf(quantity_sum, event_type='RECEIVED'   AND reason='RECEIVED')
--         StockIssued   : sumIf(quantity_sum, event_type='DISPATCHED' AND reason='')
--         StockReturned : sumIf(quantity_sum, event_type='RECEIVED'   AND reason='RETURNED')
--         StockLost     : sumIf(quantity_sum, event_type='DISPATCHED' AND reason IN ('LOST_IN_TRANSIT','LOST_IN_STORAGE'))
--         StockDamaged  : sumIf(quantity_sum, event_type='DISPATCHED' AND reason IN ('DAMAGED_IN_TRANSIT','DAMAGED_IN_STORAGE'))
--         Stock in Hand : RECEIVED - DISPATCHED
--   722 warehouseDistributionLatLong* (six charts)
--         points          : FROM 16, one row per facility (lat/long already averaged)
--         AvlRcvdDisp district  : FROM 15 GROUP BY level_three_code
--         AvlRcvdDisp facilities: FROM 15 GROUP BY facility_name
--         DaysStockLasts  : same expression as 718
--         StockStatus     : (RECEIVED-DISPATCHED from 15)
--                           - (target_population from dm_targets_base PRODUCT,
--                              node_level=3 AND code=level_three_code)
--                           + sumIf(resource_quantity_sum, is_delivered) FROM 11
--                           -- a stock-sufficiency gap; the ES config bakes the
--                           -- signs into its queries and sums the three series
--         WarNum          : uniqExact(facility_id) FROM 15
--                           WHERE facility_type != 'Monitor Local', GROUP BY level_three_code
--   723 InventorySummaryByDistrictICCD               (xtable)
--         Total Incoming Stock : sumIf(quantity_sum, event_type='RECEIVED')
--         Total Outgoing Stock : sumIf(quantity_sum, event_type='DISPATCHED')
--         Total Returned Stock : sumIf(quantity_sum, event_type='RECEIVED' AND reason='RETURNED')
--         Total Stock Balance  : RECEIVED - DISPATCHED
--         Stock Target To Receive / Days To Last : vs dm_targets_base PRODUCT,
--                                                  node_level=3 AND code=level_three_code
--         drill InventorySummaryByWarehouseICCD : identical, GROUP BY facility_name
--
-- KNOWN GAPS (properties of the existing pipeline, not of this file):
--   * 717's DENOMINATOR IS 0 ON THIS INSTANCE. project_staff_entity.role is
--     empty on all 575 rows, so "warehouse managers created" counts nothing and
--     the rate is NULL. The numerator works (147 distinct WM users in
--     stock_entity). Both sides are stored as separate components precisely so
--     the rate starts resolving on its own once role is populated -- no mart
--     change will be needed then.
--   * TARGET-DEPENDENT PANELS RETURN EMPTY. dm_targets_base is empty
--     here (the deployed dm_targets_base predates the current 07 DDL, so the
--     node-model views were never created). Deploying 07/08 is out of scope by
--     instruction. This affects 719's Required Stock, 722's DaysStockLasts and
--     StockStatus, and 723's Stock Target To Receive / Days To Last.
--   * COORDINATES AND DISTRICTS NEVER CO-OCCUR: 651 stock rows carry lat/lng
--     and none of them also carries a district code, so viz 722's district-grain
--     points query yields nothing. Mart 16 is facility-grain for that reason.
--   * RECONCILIATION BOUNDARY COVERAGE IS 12 OF 174 ROWS, so panel 720's manual
--     side is nearly empty at district grain whatever the mart does.
--   * facility_type on this instance is WAREHOUSE / STAFF / ''. The ES
--     'Monitor Local' exclusion matches nothing here but is kept for fidelity.
--   * Several ES terms aggs on this tab omit `size` and silently truncate to 10
--     districts (721's five named series, 723). Deliberately NOT reproduced --
--     that is a config bug, not a spec.
--   * campaign_number may legitimately be ''. Not filtered out; dropping those
--     rows would hide the gap rather than surface it.
-- ==========================================================================

SET allow_experimental_refreshable_materialized_view = 1;


-- 1. mv_dm_stock_transactions -> dm_stock_transactions
--
-- No WHERE clause at all: stock_entity has no is_deleted column, and every
-- event_type/reason combination is meaningful to some panel. The five named
-- series of panel 721 are read-time filters over this grain, not a filter
-- baked in here.
--
-- upper(event_type) is NOT applied on the way in -- event_type is stored
-- verbatim so the mart stays a faithful image of the source vocabulary, and
-- consumers match on it directly. (dm_stock_balance does normalize case,
-- because it collapses the column away and has to decide.) On current data the
-- values are exactly 'RECEIVED' and 'DISPATCHED'.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_stock_transactions
REFRESH EVERY 1 HOUR
TO dm_stock_transactions
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
    facility_id,
    facility_name,
    facility_type,
    event_type,
    reason,
    sum(toInt64(physical_count)) AS quantity_sum,
    toUInt64(count())            AS transaction_count
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
    product_name,
    facility_id,
    facility_name,
    facility_type,
    event_type,
    reason;


-- 2. mv_dm_stock_facility_points -> dm_stock_facility_points
--
-- JSONHas on both keys mirrors the ES `exists` guards on
-- Data.additionalDetails.lat / .lng. Without it every facility lacking
-- coordinates would land at (0, 0) -- a real point in the Gulf of Guinea, and
-- a classic way to put phantom warehouses on a map.
--
-- avg() matches the ES `avg` aggregation. For a fixed warehouse every row
-- should carry the same coordinate, so this is de-duplication rather than a
-- genuine average; see the table comment in 07 for what happens if a
-- facility's rows disagree.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_stock_facility_points
REFRESH EVERY 1 HOUR
TO dm_stock_facility_points
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
    facility_id,
    facility_name,
    facility_type,
    avg(JSONExtractFloat(additional_details, 'lat')) AS latitude,
    avg(JSONExtractFloat(additional_details, 'lng')) AS longitude,
    toUInt64(count())                                AS transaction_count
FROM stock_entity FINAL
WHERE JSONHas(additional_details, 'lat')
  AND JSONHas(additional_details, 'lng')
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


-- 3. mv_dm_stock_reconciliation -> dm_stock_reconciliation
--
-- argMax on client_last_modified_time takes the LATEST reconciliation per
-- (facility, product) rather than summing every one of them -- the deliberate
-- divergence from the ES config's double-count documented on the table in 07.
-- client_last_modified_time is the right clock here: it is this table's own
-- ReplacingMergeTree version column, and it is what the ES top_hits sorts on.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_stock_reconciliation
REFRESH EVERY 1 HOUR
TO dm_stock_reconciliation
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
    facility_id,
    facility_name,
    product_name,
    toInt64(argMax(physical_count,   client_last_modified_time))   AS physical_count,
    toInt64(argMax(calculated_count, client_last_modified_time))   AS calculated_count,
    toDateTime(intDiv(max(client_last_modified_time), 1000))       AS reconciled_at,
    toUInt64(count())                                              AS reconciliation_count
FROM stock_reconciliation_entity FINAL
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
    product_name;


-- 4. mv_dm_user_sync -> dm_user_sync
--
-- Two legs with different sources, different user keys and different meanings,
-- unioned and then split back apart by uniqExactStateIf:
--
--   CREATED -- project_staff_entity.user_id. The campaign's staff roster: who
--              was set up, whether or not they ever transacted.
--   SYNCED  -- user_name across the RECORD tables. Per the product owner, the
--              upstream pipeline wrote one record to the domain index AND one
--              to the user-sync index for the same event, so a user appearing
--              in any record table IS a user who synced. stock alone is not
--              enough once other tabs are served: warehouse managers sync
--              through stock, but distributors sync through project_task (162
--              distinct users there vs 33 in stock) and supervisors through
--              user_action. All three are unioned. None of these tables carries
--              a user id at all, hence user_name throughout.
--
-- The `src` marker exists so one GROUP BY can build both states; a FULL JOIN
-- of two aggregates would produce the same numbers but would have to cope with
-- boundary/role combinations present on only one side.
--
-- Empty user keys are excluded on both legs -- '' is not a user, and it would
-- otherwise register as one extra distinct "person" in every cell.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_user_sync
REFRESH EVERY 1 HOUR
TO dm_user_sync
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
    role,
    uniqExactStateIf(user_key, src = 'CREATED') AS users_created_uniq,
    uniqExactStateIf(user_key, src = 'SYNCED')  AS users_synced_uniq
FROM
(
    SELECT
        tenant_id, toString(campaign_number) AS campaign_number, hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code,
        level_five_code, level_six_code, level_seven_code, level_eight_code,
        level_nine_code,
        toString(role)      AS role,
        'CREATED'           AS src,
        toString(user_id)   AS user_key
    FROM project_staff_entity FINAL
    WHERE user_id != ''

    UNION ALL

    SELECT
        tenant_id, toString(campaign_number) AS campaign_number, hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code,
        level_five_code, level_six_code, level_seven_code, level_eight_code,
        level_nine_code,
        toString(role)      AS role,
        'SYNCED'            AS src,
        toString(user_name) AS user_key
    FROM stock_entity FINAL
    WHERE user_name != ''

    UNION ALL

    SELECT
        tenant_id, toString(campaign_number) AS campaign_number, hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code,
        level_five_code, level_six_code, level_seven_code, level_eight_code,
        level_nine_code,
        toString(role)      AS role,
        'SYNCED'            AS src,
        toString(user_name) AS user_key
    FROM project_task_entity FINAL
    WHERE user_name != ''

    UNION ALL

    SELECT
        tenant_id, toString(campaign_number) AS campaign_number, hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code,
        level_five_code, level_six_code, level_seven_code, level_eight_code,
        level_nine_code,
        toString(role)      AS role,
        'SYNCED'            AS src,
        toString(user_name) AS user_key
    FROM user_action_entity FINAL
    WHERE user_name != ''
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
    role;
