-- ==========================================================================
-- COVERAGE MARTS -- LOGIC
--
-- Gold layer: aggregates over the silver entities in 05_silver_tables.sql.
-- Each materialized view here writes into its target table from
-- 07_mart_tables.sql, which must be created first.
--
-- REFRESH EVERY 1 HOUR with a TO target and no APPEND clause means each
-- refresh atomically replaces the target table's contents -- these are full
-- rebuilds, not incremental appends. No Airflow DAG drives this layer.
--
-- Both silver sources are ReplacingMergeTree(last_modified_time), so every
-- read here uses FINAL: without it, CDC row versions that background merges
-- haven't collapsed yet are counted more than once.
-- ==========================================================================

SET allow_experimental_refreshable_materialized_view = 1;


-- ==========================================================================
-- SECTION 1: FOUNDATIONAL BASE MARTS
-- ==========================================================================

-- 1. mv_dm_successful_deliveries_base -> dm_successful_deliveries_base
--
-- The status filter is deliberately narrow. project_task_entity carries other
-- statuses that may also represent a successful delivery (notably
-- ADMINISTERED_SUCCESS, an older vocabulary, plus DELIVERED); including them
-- would change what "coverage" means, so widening this is a metric-definition
-- decision rather than a bug fix.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_successful_deliveries_base
REFRESH EVERY 1 HOUR
TO dm_successful_deliveries_base
AS
SELECT
    tenant_id,
    campaign_number,
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
    task_dates AS event_date,
    toUInt64(count()) AS total_administered,
    sum(quantity) AS total_product_administered
FROM project_task_entity FINAL
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
GROUP BY
    tenant_id,
    campaign_number,
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
    task_dates;


-- 2. mv_dm_targets_base -> dm_targets_base
--
-- One row per boundary NODE, carrying that node's own declared target. The
-- GROUP BY is the full nine-level path, so sum() here only ever collapses
-- projects sitting on the SAME node -- it never adds a parent to its children.
-- The depth-multiplication hazard is a property of READING the table across
-- levels, not of building it; node_level below is what makes the safe read
-- available. See the table comment in 07.
--
-- FILTERS ARE DELIBERATELY MINIMAL, and three that were previously here are
-- gone because measurement showed they did nothing:
--   * `campaign_number != ''` removed 0 rows and 0 target.
--   * `boundary_code` and `hierarchy_type` in the GROUP BY split 0 nodes --
--     both are functionally dependent on the path (99 distinct keys either
--     way), so they only ever added columns, never grain.
--   * `boundary_code` as an output column existed solely to power
--     mv_dm_campaign_hierarchy's `code = boundary_code` guard, which matched
--     99 of 99 rows -- it has never once fired. The guard is now `node_level > 0`
--     alone, which is the half that does real work.
--
-- `level_one_code != ''` is also gone, and that one is a behaviour change worth
-- stating plainly: it used to drop 304 rows holding ~11.5M of target whose
-- boundary never resolved. Those rows are now kept with node_level = 0, which
-- excludes them from every level query anyway while leaving the unattributed
-- remainder measurable instead of silently discarded.
--
-- project_count / target_row_count are dropped as diagnostics that no consumer
-- read.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_targets_base
REFRESH EVERY 1 HOUR
TO dm_targets_base
AS
SELECT
    tenant_id,
    campaign_number,
    target_type,
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
    -- Position of the last non-empty level code. The block is dense and
    -- left-packed from the root, so this is the node's depth; 0 when the
    -- boundary path never resolved.
    toUInt8(length(arrayFilter(t -> t != '', [
        level_one_code, level_two_code, level_three_code,
        level_four_code, level_five_code, level_six_code,
        level_seven_code, level_eight_code, level_nine_code]))) AS node_level,
    sum(toInt64(overall_target))                  AS target_population,
    sum(toInt64(target_per_day))                  AS target_per_day,
    min(toDate(toDateTime(p.start_date / 1000)))  AS start_date,
    max(toDate(toDateTime(p.end_date / 1000)))    AS end_date,
    max(toInt32(campaign_duration_in_days))       AS total_days
FROM project_entity AS p FINAL
-- Qualified with `p.` because start_date/end_date are also output aliases above
-- (Date), and an unqualified name in WHERE resolves to the alias, not to the
-- source Int64 column.
WHERE p.boundary_code != ''
  AND NOT endsWith(p.id, '-NO_TARGET')
  AND p.start_date > 0
  AND p.end_date   > 0
GROUP BY
    tenant_id,
    campaign_number,
    target_type,
    hierarchy_type,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code;

-- 3. mv_dm_campaign_hierarchy -> dm_campaign_hierarchy
--
-- Emits ONE ROW PER LEAF PATH. The target table stores only the leaves and only
-- their level block, because that block is already the complete root-to-leaf
-- path -- every ancestor is readable from it. Navigation idioms are on item 4
-- in 07.
--
-- THE LEAF COMPUTATION STAYS HERE, and that is the point of the change: the
-- complexity moves out of the stored table into this view, it does not vanish.
-- A leaf is a node that is not an ANCESTOR of any other node in the campaign,
-- established by the ancestor-set self-join below -- NOT "a row at the
-- campaign's deepest level". On a ragged tree (nodes at M1 and M1/M2/M3 but not
-- M2) a max_level filter would discard the M1 branch entirely and make
-- everything beneath it unaddressable; ancestor-set membership keeps both
-- branches' leaves. Every node on current data is a leaf, so the two are
-- indistinguishable against this instance -- which is exactly why the
-- distinction is spelled out here rather than trusted to testing.
--
-- ifNull() on the LEFT JOIN result is deliberate. With the default
-- join_use_nulls = 0 an unmatched column is '' and the bare comparison would
-- work, but the leaf test would silently invert if a profile ever set it to 1.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign_hierarchy
REFRESH EVERY 1 HOUR
    DEPENDS ON mv_dm_targets_base
TO dm_campaign_hierarchy
AS
WITH
nodes AS (
    SELECT DISTINCT
        tenant_id, campaign_number, hierarchy_type,
        boundary_path, node_level, code, boundary_path_str, parent_code, ancestor_codes,
        level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
        level_six_code, level_seven_code, level_eight_code, level_nine_code
    FROM (
        SELECT
            tenant_id, campaign_number, hierarchy_type,
            level_one_code, level_two_code, level_three_code, level_four_code, level_five_code,
            level_six_code, level_seven_code, level_eight_code, level_nine_code,
            -- arrayZip(range(1,10), ...) binds the ABSOLUTE level number to each
            -- code BEFORE the empty tail is filtered off. The plain
            -- arrayFilter + length form would renumber every level below a gap
            -- (['A','','C'] -> C at level 2 instead of 3), and the
            -- `code = boundary_code` guard below would NOT catch it, because the
            -- compacted path still ends at the right node. range(1,10) is
            -- end-exclusive, so it is [1..9]; arrayZip yields
            -- Array(Tuple(UInt8, String)), accessed as .1 / .2.
            arrayFilter(t -> t.2 != '', arrayZip(range(1, 10),
                [level_one_code, level_two_code, level_three_code,
                 level_four_code, level_five_code, level_six_code,
                 level_seven_code, level_eight_code, level_nine_code])) AS pairs,
            arrayMap(t -> t.2, pairs)                              AS boundary_path,
            if(empty(pairs), toUInt8(0), toUInt8(pairs[-1].1))     AS node_level,
            if(empty(pairs), '', pairs[-1].2)                      AS code,
            arrayStringConcat(boundary_path, '/')                  AS boundary_path_str,
            if(length(pairs) >= 2, pairs[-2].2, '')                AS parent_code,
            arrayMap(t -> t.2, arraySlice(pairs, 1, length(pairs) - 1)) AS ancestor_codes
        FROM dm_targets_base
    )
    -- node_level > 0 keeps only rows whose boundary path actually resolved.
    -- This used to also assert `code = boundary_code`, guarding against a path
    -- truncated at >9 levels by egov_api_utils.get_boundary_hierarchy_levels_bulk.
    -- That half was measured against real data and matched 99 of 99 rows -- it has
    -- never fired -- and boundary_code is no longer carried on dm_targets_base, so
    -- it is gone. If a truncated path ever does appear it will surface as a node
    -- whose code is not its real leaf; catch it with a data-quality check rather
    -- than by re-widening this mart.
    WHERE node_level > 0
),
-- One row per (node, each of its ancestors); at most 8 rows per node.
-- Restricted to ancestors that are THEMSELVES nodes of the same campaign --
-- that restriction is what makes root/leaf relative to the actual node set.
hits AS (
    SELECT
        e.tenant_id AS tenant_id, e.campaign_number AS campaign_number,
        e.child_code AS child_code, e.ancestor_code AS ancestor_code, n.node_level AS ancestor_level
    FROM (
        SELECT tenant_id, campaign_number, code AS child_code, arrayJoin(ancestor_codes) AS ancestor_code
        FROM nodes
    ) AS e
    INNER JOIN nodes AS n
        ON e.tenant_id = n.tenant_id AND e.campaign_number = n.campaign_number AND e.ancestor_code = n.code
)
SELECT DISTINCT
    n.tenant_id       AS tenant_id,
    n.campaign_number AS campaign_number,
    n.hierarchy_type  AS hierarchy_type,
    n.level_one_code, n.level_two_code, n.level_three_code, n.level_four_code, n.level_five_code,
    n.level_six_code, n.level_seven_code, n.level_eight_code, n.level_nine_code,
    n.node_level      AS node_level
FROM nodes AS n
-- Any node that is an ancestor of another node is internal; no match => leaf.
LEFT JOIN (
    SELECT DISTINCT tenant_id, campaign_number, ancestor_code AS code FROM hits
) AS i
    ON n.tenant_id = i.tenant_id AND n.campaign_number = i.campaign_number AND n.code = i.code
WHERE ifNull(i.code, '') = '';

-- 6. mv_dm_coverage_by_node -> dm_coverage_by_node
-- COVERAGE AT EVERY LEVEL ("drilldown till the lowest level")
--
-- Each delivery row is exploded into one row per ANCESTOR of its boundary path,
-- so a delivery is counted once at every level above it. That is the roll-up,
-- and it cannot double-count: a given delivery contributes exactly one row per
-- level. range(1, 10) is [1..9], matching the nine level columns.
--
-- No coverage ratio is stored. It is computed at query time against
-- dm_targets_base, joined on (tenant_id, campaign_number, node_level, code), so
-- the choice of denominator cut is not frozen into storage.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_coverage_by_node
REFRESH EVERY 1 HOUR
    DEPENDS ON mv_dm_successful_deliveries_base
TO dm_coverage_by_node
AS
SELECT
    tenant_id,
    campaign_number,
    node_level,
    code,
    product_name,
    event_date,
    sum(total_administered)         AS total_administered,
    sum(total_product_administered) AS total_product_administered
FROM (
    SELECT
        tenant_id, campaign_number, product_name, event_date,
        total_administered, total_product_administered,
        toUInt8(ancestor.1) AS node_level,
        ancestor.2          AS code
    FROM dm_successful_deliveries_base
    ARRAY JOIN arrayFilter(t -> t.2 != '',
                   arrayZip(range(1, 10), [level_one_code, level_two_code, level_three_code,
                                           level_four_code, level_five_code, level_six_code,
                                           level_seven_code, level_eight_code, level_nine_code])) AS ancestor
)
GROUP BY tenant_id, campaign_number, node_level, code, product_name, event_date;


-- ==========================================================================
-- SECTION 3: DEPENDENT MARTS
-- ==========================================================================

-- 7. mv_dm_campaign_coverage -> dm_campaign_coverage
-- OVERALL COVERAGE RATE
--
-- DEPENDS ON is load-bearing: without it this view can refresh against a
-- half-rebuilt base mart. It names the node marts rather than mv_dm_targets_base
-- because the denominator now comes from dm_targets_base.
--
-- CAVEAT: the target_type = 'INDIVIDUAL' filter means a campaign whose targets
-- are all HOUSEHOLD produces no target row, and all of its deliveries drop out
-- of this mart entirely. If both types occur in practice, target_type should
-- become a grouping column here instead of a filter.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign_coverage
REFRESH EVERY 1 HOUR
    DEPENDS ON mv_dm_successful_deliveries_base, mv_dm_targets_base
TO dm_campaign_coverage
AS
WITH campaign_deliveries AS (
    SELECT
        tenant_id,
        campaign_number,
        product_name,
        sum(total_administered) AS total_administered,
        sum(total_product_administered) AS total_product_administered
    FROM dm_successful_deliveries_base
    GROUP BY tenant_id, campaign_number, product_name
),
campaign_targets_cte AS (
    SELECT
        f.tenant_id AS tenant_id,
        f.campaign_number AS campaign_number,
        sum(f.target_population) AS target_population
    FROM dm_targets_base AS f
    WHERE f.target_type = 'INDIVIDUAL'
      -- THE ROLL-UP CUT, and the fix for a real double-count. A campaign creates
      -- one project per boundary node, so it carries a target row at EVERY level
      -- of its tree -- parent totals plus child totals. The previous version
      -- summed all of them straight off dm_targets_base, inflating this
      -- denominator by roughly the tree depth and understating coverage by the
      -- same factor.
      --
      -- The ROOT cut is used rather than the leaf cut because it is the
      -- campaign's own declared total, and it is robust to a leaf project whose
      -- target row has not landed in bronze yet (which would silently shrink a
      -- leaf-cut denominator). is_target_type_root is relative to the campaign's
      -- ACTUAL node set for this target_type, so every campaign has at least one
      -- root and this can never be spuriously empty -- even for a campaign that
      -- starts below level 1, or one that is a forest of disjoint subtrees.
      --
      -- ROOT IS CORRECT HERE, AND ONLY HERE. This mart is CAMPAIGN GRAIN -- one
      -- number for the whole campaign, no boundary bucketing -- and the root
      -- nodes are disjoint subtrees, so summing them counts the campaign
      -- exactly once. It is also robust to campaigns that do not start at
      -- level 1 (observed root levels include 1, 3, 4, 5, 6, 7 and 8), where a
      -- hardcoded node_level = 1 would return nothing.
      --
      -- DO NOT COPY THIS PREDICATE INTO A BOUNDARY-BUCKETED QUERY. Targets are
      -- declared at the hierarchy's lowest level and summed upward, so a chart
      -- that buckets by district must select node_level = 3 AND code = <the
      -- district>, not the root. Root would hand every district the campaign's
      -- single top row, or nothing when the root sits above the bucket level.
      -- See the "NOT DIRECTLY SUMMABLE" block on item 2 in 07.
      --
      -- The flag is keyed on target_type, never on a union of all types: a
      -- campaign's HOUSEHOLD node set is usually a strict subset of its
      -- INDIVIDUAL one, so a union flag would return 0 for any type whose nodes
      -- exclude the union root. Verified failure mode, not a hypothetical.
      -- (dm_campaign_hierarchy carries no flags at all now -- it is leaf paths
      -- only -- so this is the one place the cut lives.)
      --
      -- If root and leaf cuts ever disagree the source targets do not roll up
      -- exactly; validation V1/V4 in the plan surface that rather than hiding it.
      AND f.is_target_type_root
    GROUP BY f.tenant_id, f.campaign_number
)
-- Targets drive the LEFT JOIN so a campaign with targets but zero deliveries
-- still appears, at 0%. ifNull/nullIf are meaningful here (unlike on the
-- non-Nullable silver columns): the join genuinely produces nulls, and
-- target_population can legitimately be 0.
SELECT
    t.tenant_id AS tenant_id,
    t.campaign_number AS campaign_number,
    ifNull(d.product_name, '') AS product_name,
    ifNull(d.total_administered, 0) AS total_administered,
    ifNull(d.total_product_administered, 0) AS total_product_administered,
    t.target_population AS target_population,
    round(ifNull(d.total_administered, 0) / nullIf(t.target_population, 0) * 100, 2) AS coverage_percentage
FROM campaign_targets_cte t
LEFT JOIN campaign_deliveries d
    ON t.tenant_id = d.tenant_id
   AND t.campaign_number = d.campaign_number;
