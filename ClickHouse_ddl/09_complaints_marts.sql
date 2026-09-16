-- ==========================================================================
-- COMPLAINTS (PGR) MARTS -- provincial-health-dashboard-iccd
--
-- Gold layer for the DSS_HEALTH_COMPLAINTS tab of the
-- `provincial-health-dashboard-iccd` dashboard (viz ids 731-738 in
-- configs/egov-dss-dashboards/dashboard-analytics/MasterDashboardConfig.json),
-- reproducing from ClickHouse silver what those panels today read out of the
-- `transformer-pgr-services` Elasticsearch index. The per-panel ES
-- aggregations live in ChartApiConfig.json, keyed by the chart ids named in
-- the mapping table below.
--
-- Deliberately scoped to that one dashboard: the national-* and district-*
-- ICCD dashboards, and the Overview tab's totalComplaintsRegisteredICCD, are
-- not covered here.
--
-- LOGIC only, mirroring the 07/08 split. The two target tables
-- (dm_complaints_base, dm_complaints_open_ageing) are defined in
-- 07_mart_tables.sql as items 8 and 9 and must be created BEFORE this file
-- runs -- the views below write into them via TO <table>.
--
-- Each refresh rebuilds its target wholesale (a refreshable MV with a TO
-- target and no APPEND clause replaces the target's contents atomically), so
-- there are never multiple versions of a row to collapse -- which is why
-- those targets are plain MergeTree, not ReplacingMergeTree.
--
-- Single silver source: pgr_complaints_entity (05_silver_tables.sql), written
-- by airflow_dags/dags/pgr_transformation.py. It is
-- ReplacingMergeTree(last_modified_time), so every read below uses FINAL --
-- without it, CDC row versions that background merges haven't collapsed yet
-- are counted more than once.
--
-- Field mapping from the ES aggregations to silver:
--     Data.service.serviceCode                     -> service_code (the complaint TYPE)
--     Data.service.applicationStatus               -> application_status
--     Data.service.auditDetails.createdTime        -> created_time      (Int64 epoch-ms)
--     Data.service.auditDetails.lastModifiedTime   -> last_modified_time(Int64 epoch-ms)
--                                                     date-truncated as task_dates
--     Data.boundaryHierarchy.*                     -> level_one_code .. level_nine_code
--     Data.campaignNumber                          -> campaign_number
--
-- BOUNDARY LEVELS. The ES documents carry NAMED boundary fields (province,
-- district, administrativeProvince, locality, village); silver carries the
-- standard dense, left-packed level_one_code..level_nine_code block instead.
-- For the ICCD hierarchy those line up as:
--
--     level_one_code    -- Country
--     level_two_code    -- Province                 (the dashboard's fixed filter)
--     level_three_code  -- District                 (default bucket for every panel)
--     level_four_code   -- AdministrativeProvince   (drill 1)
--     level_five_code   -- Locality                 (drill 2)
--     level_six_code    -- Village                  (drill 3)
--
-- That mapping is documented here and NOT baked into column names, because
-- hierarchy depth is chosen per campaign/hierarchy_type and varies -- naming a
-- column `district_code` would be wrong for any campaign whose tree is not
-- this shape. See airflow_dags/CLAUDE.md, "Boundary hierarchy flattening" and
-- "Roll-up cut for hierarchical targets". Consumers pick the column for the
-- level they are bucketing at; the full nine-level path is kept on the fact so
-- every drill-down level works off the same table with no schema change.
--
-- CODES, NOT NAMES. ES buckets on boundary display names (BoundaryService.java
-- resolves them via MDMS + localization). egov_api_utils.
-- get_boundary_hierarchy_levels_bulk deliberately stores only raw boundary
-- codes, so these marts bucket on codes. Resolving a code to a label is a
-- presentation concern, not a mart concern.
--
-- HOW EACH PANEL IS SERVED. Every query below additionally carries the
-- dashboard's own filters: level_two_code = <province>, optionally
-- campaign_number, and event_date within the global date
-- range (except where the chart's dateRefField is "", i.e. panels 737/738,
-- which the UI does not date-filter). A drill-down just moves the bucket
-- column one level down (level_three -> level_four -> level_five -> level_six)
-- and adds the parent level as a predicate.
--
--   731 complaintsByDistrictTotalICCD
--         WHERE application_status IN ('PENDING_ASSIGNMENT','RESOLVED','REJECTED')
--         GROUP BY level_three_code -> sum(complaint_count)
--   732 totalComplaintsRegisteredByTypeICCD
--         GROUP BY service_code -> sum(complaint_count)
--         (its drill pivots to GROUP BY application_status for the picked type)
--   733 complaintsByStatusBreakdownProvincialICCD
--         GROUP BY level_three_code, application_status  (same three statuses)
--   734 complaintsByTypeBreakdownProvincialICCD
--         GROUP BY level_three_code, service_code
--   735 ComplaintsShareByDistrictICCD
--         numerator as 731; denominator sum(complaint_count) over the same
--         filter set unbucketed -> percentage
--   736 averageResolutionTimeProvinceICCD   -- dm_complaints_resolution
--         GROUP BY level_three_code
--         -> sum(resolved_duration_ms_sum) / nullIf(sum(resolved_count), 0) / 3600000
--         No status filter needed: the mart holds terminal complaints only.
--   737 complaintsByStatusSummaryProvinceICCD   (tab BOUNDARY)
--         GROUP BY level_three_code, with sumIf(complaint_count, application_status = ...)
--         per status; Registered = Open + Resolved + Rejected, matching the
--         panel's AdditiveComputedField block
--   737 complaintsByStatusSummaryDayICCD        (tab DAYWISE)
--         the same pivot, GROUP BY event_date
--   738 openComplaintsSummaryByDistrictICCD
--         dm_complaints_open_ageing, GROUP BY level_three_code, age_bucket
--
-- KNOWN SILVER GAPS these marts inherit (properties of the existing pipeline,
-- not of this file -- stated so the marts are not read as more complete than
-- they are):
--   * campaign_number may legitimately be ''. PGR resolves it through a
--     project_staff -> project bridge; when that lookup misses,
--     pgr_transformation._build_silver_row writes ''. Such rows vanish the
--     moment the dashboard filters by campaign. They are deliberately NOT
--     filtered out below -- dropping them would hide the gap rather than
--     surface it.
--   * project_type_id is deliberately NOT carried, even though the charts'
--     requestQueryMap offers it as a filter. campaign_number is guaranteed
--     present from the product side, so campaign is the cut these marts model
--     on. An implementation that genuinely needs a project-type cut should
--     ALTER the mart to add the column, rather than have every mart carry it
--     by default. This applies to future marts too, not just these two.
--   * campaign_id is always '' (needs project-factory integration; see the
--     TODO in pgr_transformation._build_silver_row).
--   * The two drill-down leaf listings (complaintsListICCD,
--     openComplaintsListICCD) are intentionally not materialized: they are
--     row-level selects over pgr_complaints_entity FINAL, not aggregates, and
--     the complainant mobile number and supervisor contact number they display
--     have no bronze source yet (Service.user is not modeled in bronze).
-- ==========================================================================


-- ==========================================================================
-- SECTION 2: LOGIC
--
-- REFRESH EVERY 1 HOUR with a TO target and no APPEND clause means each
-- refresh atomically replaces the target table's contents -- these are full
-- rebuilds, not incremental appends. No Airflow DAG drives this layer.
--
-- Each SELECT's column ORDER matches its target table's column order exactly:
-- a materialized view with a TO target binds by POSITION, not by name.
-- ==========================================================================

SET allow_experimental_refreshable_materialized_view = 1;


-- 1. mv_dm_complaints_base -> dm_complaints_base
--
-- The application_status != '' guard mirrors the must_not
-- applicationStatus:[""] filter that the ES configs use to exclude records
-- with no status. It is the only row filter: the individual panels narrow to
-- their own status set (the three-status charts) or to a type at query time,
-- and baking either narrowing in here would make the mart serve one panel
-- instead of eight.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_complaints_base
REFRESH EVERY 1 HOUR
TO dm_complaints_base
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
    service_code,
    application_status,
    task_dates AS event_date,
    toUInt64(count()) AS complaint_count
FROM pgr_complaints_entity FINAL
WHERE application_status != ''
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
    service_code,
    application_status,
    event_date;


-- 2. mv_dm_complaints_open_ageing -> dm_complaints_open_ageing
--
-- age_ms is computed in an inner SELECT and bucketed in the outer one so the
-- "now" reference and the subtraction are written once and both multiIfs
-- (label and sort order) read the same expression. now() is constant-folded
-- per query, so every row in a given refresh is aged against the same instant.
--
-- created_time > 0 excludes rows whose audit timestamp never made it through
-- to silver (_build_silver_row defaults a missing one to 0); ageing those
-- would put them ~55 years old, in the "more than 48h" bucket, silently.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_complaints_open_ageing
REFRESH EVERY 1 HOUR
TO dm_complaints_open_ageing
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
    multiIf(
        age_ms <  6 * 3600000, 'open since 6h',
        age_ms < 12 * 3600000, 'open since 6h-12h',
        age_ms < 24 * 3600000, 'open since 12h-24h',
        age_ms < 48 * 3600000, 'open since 24h-48h',
        'open for more than 48h'
    ) AS age_bucket,
    multiIf(
        age_ms <  6 * 3600000, toUInt8(1),
        age_ms < 12 * 3600000, toUInt8(2),
        age_ms < 24 * 3600000, toUInt8(3),
        age_ms < 48 * 3600000, toUInt8(4),
        toUInt8(5)
    ) AS age_bucket_order,
    toUInt64(count()) AS open_count,
    refreshed_at
FROM
(
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
        now() AS refreshed_at,
        toInt64(toUnixTimestamp(now())) * 1000 - created_time AS age_ms
    FROM pgr_complaints_entity FINAL
    WHERE application_status = 'PENDING_ASSIGNMENT'
      AND created_time > 0
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
    age_bucket,
    age_bucket_order,
    refreshed_at;


-- 3. mv_dm_complaints_resolution -> dm_complaints_resolution
--
-- The terminal-status filter lives HERE, in the WHERE clause that defines the
-- mart's grain, rather than being left to each consumer. That is the whole
-- point of the separate table: no row in the target can describe a complaint
-- that never finished, so panel 736 cannot be computed over open complaints
-- by accident.
--
-- The created_time/last_modified_time guards mirror the ES aggregation's own
-- denominator, `value_count(Data.service.auditDetails.createdTime)`, which in
-- Elasticsearch counts only documents where that field is PRESENT. Silver has
-- no absent state -- pgr_transformation._default_int coerces a missing value
-- to 0 on a non-Nullable Int64 -- so `> 0` is the equivalent sentinel test.
-- Guarding the sum and the count with the identical predicate keeps numerator
-- and denominator over exactly the same rows; guarding only one would dilute
-- or inflate the average. Neither timestamp is currently ever 0 (both are NOT
-- NULL upstream, and last_modified_time doubles as the ReplacingMergeTree
-- version column), so this is a guard against future drift, not a live fix.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_complaints_resolution
REFRESH EVERY 1 HOUR
TO dm_complaints_resolution
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
    service_code,
    task_dates AS event_date,
    toUInt64(count())                                  AS resolved_count,
    sum(last_modified_time - created_time)             AS resolved_duration_ms_sum
FROM pgr_complaints_entity FINAL
WHERE application_status IN ('RESOLVED', 'REJECTED')
  AND created_time > 0
  AND last_modified_time > 0
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
    service_code,
    event_date;
