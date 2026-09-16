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
-- is_campaign_root / is_campaign_leaf come from ANCESTOR-set membership, not
-- immediate-parent membership. On a ragged tree -- campaign has nodes at M1 and
-- M1/M2/M3 but not M2 -- immediate-parent logic marks M3 root (parent M2 absent)
-- AND M1 leaf (M1 is nobody's immediate parent), so both cuts return M1 + M3 and
-- double-count. Ancestor logic gives root = {M1}, leaf = {M3}. Verified.
--
-- rollup_parent_code (nearest ancestor actually PRESENT in the campaign, via
-- argMax over ancestor_level) is what makes drill-down gapless on such a tree,
-- and rollup_parent_code = '' IS is_campaign_root -- so every campaign is
-- guaranteed at least one root and a root-cut denominator can never be
-- spuriously empty.
--
-- ifNull() on the LEFT JOIN results is deliberate. With the default
-- join_use_nulls = 0 an unmatched column is ''/0 and the bare comparison would
-- work, but the flags would silently invert if a profile ever set it to 1.
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
SELECT
    cityHash64(n.tenant_id, n.campaign_number, toString(n.node_level), n.code) AS hierarchy_sk,
    cityHash64(n.tenant_id, n.campaign_number)                                 AS campaign_sk,
    n.tenant_id                                AS tenant_id,
    n.campaign_number                          AS campaign_number,
    n.hierarchy_type                           AS hierarchy_type,
    n.node_level                               AS node_level,
    n.code                                     AS code,
    n.boundary_path                            AS boundary_path,
    n.boundary_path_str                        AS boundary_path_str,
    n.parent_code                              AS parent_code,
    if(ifNull(rp.rollup_parent_code, '') = '',
       toUInt64(0),
       cityHash64(n.tenant_id, n.campaign_number,
                  toString(ifNull(rp.rollup_parent_level, 0)),
                  ifNull(rp.rollup_parent_code, '')))  AS rollup_parent_sk,
    ifNull(rp.rollup_parent_code, '')          AS rollup_parent_code,
    toUInt8(ifNull(rp.rollup_parent_level, 0)) AS rollup_parent_level,
    ifNull(rp.rollup_parent_code, '') = ''     AS is_campaign_root,
    ifNull(i.code, '') = ''                    AS is_campaign_leaf,
    (n.parent_code != '') AND (ifNull(rp.rollup_parent_code, '') = n.parent_code) AS parent_in_campaign,
    n.level_one_code, n.level_two_code, n.level_three_code, n.level_four_code, n.level_five_code,
    n.level_six_code, n.level_seven_code, n.level_eight_code, n.level_nine_code
FROM nodes AS n
-- Nearest ancestor present in the campaign. No match at all <=> campaign root.
LEFT JOIN (
    SELECT tenant_id, campaign_number, child_code AS code,
           argMax(ancestor_code, ancestor_level) AS rollup_parent_code,
           toUInt8(max(ancestor_level))          AS rollup_parent_level
    FROM hits
    GROUP BY tenant_id, campaign_number, child_code
) AS rp
    ON n.tenant_id = rp.tenant_id AND n.campaign_number = rp.campaign_number AND n.code = rp.code
-- Any node that is an ancestor of another node is internal, i.e. not a leaf.
LEFT JOIN (
    SELECT DISTINCT tenant_id, campaign_number, ancestor_code AS code FROM hits
) AS i
    ON n.tenant_id = i.tenant_id AND n.campaign_number = i.campaign_number AND n.code = i.code;


-- 4. mv_dm_campaign -> dm_campaign
-- Aggregates the node dim into per-campaign structure, and joins project_entity
-- for the descriptive fields the node dim does not carry.
--
-- levels_present / level_node_counts are built from one sorted tuple array so
-- they stay index-aligned; they are a sorted LIST rather than being indexed by
-- absolute level, because a ragged campaign can skip a level entirely.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign
REFRESH EVERY 1 HOUR
    DEPENDS ON mv_dm_campaign_hierarchy
TO dm_campaign
AS
WITH per_level AS (
    SELECT
        tenant_id, campaign_number, hierarchy_type, node_level,
        toUInt32(count())         AS node_cnt,
        countIf(is_campaign_root) AS root_cnt,
        countIf(is_campaign_leaf) AS leaf_cnt,
        countIf(node_level > 1 AND parent_code != '' AND NOT parent_in_campaign) AS orphan_cnt
    FROM dm_campaign_hierarchy
    GROUP BY tenant_id, campaign_number, hierarchy_type, node_level
),
shape AS (
    SELECT
        tenant_id, campaign_number, hierarchy_type,
        toUInt8(min(node_level)) AS root_level,
        toUInt8(max(node_level)) AS max_level,
        arraySort(x -> x.1, groupArray((node_level, node_cnt))) AS lvl_pairs,
        arrayMap(x -> x.1, lvl_pairs) AS levels_present,
        arrayMap(x -> x.2, lvl_pairs) AS level_node_counts,
        toUInt8(length(lvl_pairs))    AS level_count,
        toUInt32(sum(node_cnt))       AS node_count,
        toUInt32(sum(root_cnt))       AS root_node_count,
        toUInt32(sum(leaf_cnt))       AS leaf_node_count,
        toUInt32(sum(orphan_cnt))     AS orphan_node_count
    FROM per_level
    GROUP BY tenant_id, campaign_number, hierarchy_type
),
-- Descriptive fields + the campaign window, straight from the target staging
-- mart (which already carries the same exclusions the node dim was built from).
descr AS (
    SELECT
        t.tenant_id AS tenant_id,
        t.campaign_number AS campaign_number,
        min(t.start_date) AS start_date,
        max(t.end_date)   AS end_date,
        max(t.total_days) AS total_days
    FROM dm_targets_base AS t
    GROUP BY t.tenant_id, t.campaign_number
),
names AS (
    SELECT
        tenant_id,
        campaign_number,
        -- anyHeavy, not any(): a campaign's projects all share a name/type in
        -- practice, but if they diverge report the dominant value rather than an
        -- arbitrary one.
        anyHeavy(project_name) AS campaign_name,
        anyHeavy(project_type) AS project_type
    FROM project_entity FINAL
    WHERE campaign_number != ''
    GROUP BY tenant_id, campaign_number
)
SELECT
    cityHash64(s.tenant_id, s.campaign_number) AS campaign_sk,
    s.tenant_id         AS tenant_id,
    s.campaign_number   AS campaign_number,
    s.hierarchy_type    AS hierarchy_type,
    ifNull(nm.campaign_name, '') AS campaign_name,
    ifNull(nm.project_type, '')  AS project_type,
    s.root_level        AS root_level,
    s.max_level         AS max_level,
    s.level_count       AS level_count,
    s.levels_present    AS levels_present,
    s.level_node_counts AS level_node_counts,
    s.node_count        AS node_count,
    s.root_node_count   AS root_node_count,
    s.leaf_node_count   AS leaf_node_count,
    s.orphan_node_count AS orphan_node_count,
    ifNull(d.start_date, toDate(0)) AS start_date,
    ifNull(d.end_date,   toDate(0)) AS end_date,
    toInt32(ifNull(d.total_days, 0)) AS total_days
FROM shape AS s
LEFT JOIN descr AS d  ON s.tenant_id = d.tenant_id  AND s.campaign_number = d.campaign_number
LEFT JOIN names AS nm ON s.tenant_id = nm.tenant_id AND s.campaign_number = nm.campaign_number;


-- 5. mv_dm_campaign_target_fact -> dm_campaign_target_fact
--
-- hierarchy_sk is NOT joined for -- it is the identical deterministic hash
-- expression the dim uses, over (tenant, campaign, node_level, code), all of
-- which come from a single dm_targets_base row. The keys line up by construction.
--
-- The root/leaf machinery below mirrors mv_dm_campaign_hierarchy's exactly,
-- except every join is additionally keyed on target_type. That is the whole
-- point: a campaign's HOUSEHOLD node set is typically a strict subset of its
-- INDIVIDUAL one (218 of 302 projects in table_dumps/project_target.csv carry
-- INDIVIDUAL only), so the union flags on the dim return 0 for a HOUSEHOLD
-- root cut whenever the union root carries no HOUSEHOLD row.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign_target_fact
REFRESH EVERY 1 HOUR
    DEPENDS ON mv_dm_targets_base
TO dm_campaign_target_fact
AS
WITH
tt_nodes AS (
    SELECT
        tenant_id, campaign_number, target_type,
        node_level, code, ancestor_codes,
        target_population, target_per_day,
        start_date, end_date, total_days
    FROM (
        SELECT
            tenant_id, campaign_number, target_type,
            target_population, target_per_day,
            start_date, end_date, total_days,
            -- Same arrayZip idiom as the dim: the ABSOLUTE level is bound to each
            -- code before the empty tail is filtered off, so a gap cannot renumber
            -- anything below it.
            arrayFilter(t -> t.2 != '', arrayZip(range(1, 10),
                [level_one_code, level_two_code, level_three_code,
                 level_four_code, level_five_code, level_six_code,
                 level_seven_code, level_eight_code, level_nine_code])) AS pairs,
            if(empty(pairs), toUInt8(0), toUInt8(pairs[-1].1))     AS node_level,
            if(empty(pairs), '', pairs[-1].2)                      AS code,
            arrayMap(t -> t.2, arraySlice(pairs, 1, length(pairs) - 1)) AS ancestor_codes
        FROM dm_targets_base
    )
    -- Same >9-level truncation guard as the dim, so the fact can never reference
    -- a hierarchy_sk the dim does not contain.
    WHERE node_level > 0
),
tt_hits AS (
    SELECT
        e.tenant_id AS tenant_id, e.campaign_number AS campaign_number,
        e.target_type AS target_type, e.child_code AS child_code, e.ancestor_code AS ancestor_code
    FROM (
        SELECT tenant_id, campaign_number, target_type, code AS child_code,
               arrayJoin(ancestor_codes) AS ancestor_code
        FROM tt_nodes
    ) AS e
    INNER JOIN tt_nodes AS n
        ON e.tenant_id = n.tenant_id AND e.campaign_number = n.campaign_number
       AND e.target_type = n.target_type AND e.ancestor_code = n.code
)
SELECT
    cityHash64(n.tenant_id, n.campaign_number)                                    AS campaign_sk,
    cityHash64(n.tenant_id, n.campaign_number, toString(n.node_level), n.code)    AS hierarchy_sk,
    n.tenant_id        AS tenant_id,
    n.campaign_number  AS campaign_number,
    n.target_type      AS target_type,
    n.node_level       AS node_level,
    n.code             AS code,
    ifNull(nr.code, '') = '' AS is_target_type_root,
    ifNull(nl.code, '') = '' AS is_target_type_leaf,
    n.target_population, n.target_per_day,
    n.start_date, n.end_date, n.total_days
FROM tt_nodes AS n
-- A node with a target-bearing ancestor OF THE SAME target_type is not a root.
LEFT JOIN (
    SELECT DISTINCT tenant_id, campaign_number, target_type, child_code AS code FROM tt_hits
) AS nr
    ON n.tenant_id = nr.tenant_id AND n.campaign_number = nr.campaign_number
   AND n.target_type = nr.target_type AND n.code = nr.code
-- A node that is an ancestor of another node of the same target_type is not a leaf.
LEFT JOIN (
    SELECT DISTINCT tenant_id, campaign_number, target_type, ancestor_code AS code FROM tt_hits
) AS nl
    ON n.tenant_id = nl.tenant_id AND n.campaign_number = nl.campaign_number
   AND n.target_type = nl.target_type AND n.code = nl.code;


-- 6. mv_dm_coverage_by_node -> dm_coverage_by_node
-- COVERAGE AT EVERY LEVEL ("drilldown till the lowest level")
--
-- Each delivery row is exploded into one row per ANCESTOR of its boundary path,
-- so a delivery is counted once at every level above it. That is the roll-up,
-- and it cannot double-count: a given delivery contributes exactly one row per
-- level. range(1, 10) is [1..9], matching the nine level columns.
--
-- No coverage ratio is stored. It is computed at query time against
-- dm_campaign_target_fact on hierarchy_sk, so the choice of denominator cut is
-- not frozen into storage.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_coverage_by_node
REFRESH EVERY 1 HOUR
    DEPENDS ON mv_dm_successful_deliveries_base
TO dm_coverage_by_node
AS
SELECT
    cityHash64(tenant_id, campaign_number)                              AS campaign_sk,
    cityHash64(tenant_id, campaign_number, toString(node_level), code)  AS hierarchy_sk,
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
-- because the denominator now comes from them.
--
-- CAVEAT: the target_type = 'INDIVIDUAL' filter means a campaign whose targets
-- are all HOUSEHOLD produces no target row, and all of its deliveries drop out
-- of this mart entirely. If both types occur in practice, target_type should
-- become a grouping column here instead of a filter.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign_coverage
REFRESH EVERY 1 HOUR
    DEPENDS ON mv_dm_successful_deliveries_base, mv_dm_campaign_target_fact
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
    FROM dm_campaign_target_fact AS f
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
      -- leaf-cut denominator). is_campaign_root is relative to the campaign's
      -- ACTUAL node set, so every campaign has at least one root and this can
      -- never be spuriously empty -- even for a campaign that starts below
      -- level 1, or one that is a forest of disjoint subtrees.
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
      -- See "HOW TO READ A TARGET AT A LEVEL" on item 6 in 07.
      --
      -- is_target_type_root, NOT the dim's is_campaign_root: the dim's flags are
      -- computed over the UNION of all target types' nodes, and a campaign's
      -- HOUSEHOLD node set is usually a strict subset of its INDIVIDUAL one, so
      -- the union flag would return 0 for any type whose nodes exclude the union
      -- root. Verified failure mode, not a hypothetical.
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
