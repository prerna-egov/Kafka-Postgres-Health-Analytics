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
-- statuses that may also represent a successful delivery; including them would
-- change what "coverage" means, so widening this is a metric decision rather
-- than a bug fix.
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
-- GROUP BY is the full nine-level path, so sum() here only collapses projects
-- sitting on the SAME node -- it never adds a parent to its children. The
-- depth-multiplication hazard is a property of READING across levels, not of
-- building this table; node_level is what makes the safe read available.
--
-- Rows whose boundary never resolved are kept with node_level = 0, which
-- excludes them from every level query while leaving the unattributed remainder
-- measurable rather than silently dropped.
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
-- Emits one row per LEAF path. The stored table keeps only the leaves and only
-- their level block, because that block is already the complete root-to-leaf
-- path.
--
-- THE LEAF COMPUTATION STAYS HERE -- the complexity moves out of the stored
-- table into this view, it does not vanish. A leaf is a node that is not an
-- ancestor of any other node in the campaign, established by the ancestor-set
-- join below. NOT "a row at the deepest level": on a ragged tree that would
-- discard the shallow branch and everything under it.
--
-- ifNull() on the join result is deliberate -- the leaf test would invert if a
-- profile ever set join_use_nulls = 1.
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
    -- Keeps only rows whose boundary path actually resolved.
    WHERE node_level > 0
),
-- One row per (node, each of its ancestors), restricted to ancestors that are
-- THEMSELVES nodes of the same campaign -- that restriction is what makes leaf
-- relative to the campaign's actual node set.
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

-- 4. mv_dm_coverage_by_node -> dm_coverage_by_node
--
-- Each delivery is exploded into one row per ANCESTOR of its boundary path, so
-- it is counted once at every level above it. This cannot double-count: a given
-- delivery contributes exactly one row per level.
--
-- No coverage ratio is stored; it is computed at query time against
-- dm_targets_base on (tenant_id, campaign_number, node_level, code).
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

-- 5. mv_dm_campaign_coverage -> dm_campaign_coverage
--
-- DEPENDS ON is load-bearing: without it this view can refresh against a
-- half-rebuilt base mart. mv_dm_smc_administered_by_beneficiary is defined
-- later, in 10 -- a forward reference is fine, the view simply sits in
-- WaitingForDependencies until that MV exists.
--
-- THE NUMERATOR IS DISTINCT CHILDREN, NOT DOSES. This read used to come from
-- dm_successful_deliveries_base (1), whose total_administered counts
-- task-resource ROWS. The denominator below is an INDIVIDUAL target, a count of
-- PEOPLE, so that division was doses-over-people and overstated coverage by
-- roughly the number of products each child receives. Measured on real data it
-- read 7,168 where the correct answer was 3,384.
--
-- dm_successful_deliveries_base is still the right mart for a PRODUCT question,
-- which is what it was built for; it is simply not the right one here. The
-- product-free mart (23) is read rather than item 11 so that no product fan-out
-- can reach this calculation at all.
--
-- CAVEAT: the target_type = 'INDIVIDUAL' filter means a campaign whose targets
-- are all HOUSEHOLD produces no target row, and all of its deliveries drop out
-- of this mart. If both types occur in practice, target_type should become a
-- grouping column here instead of a filter.
--
-- KNOWN GAP, not a defect in this query: a campaign only appears with a
-- numerator if its INDIVIDUAL target rows have a RESOLVED boundary
-- (node_level > 0). Where boundary enrichment has not landed, targets sit at
-- node_level = 0, the cut below drops them, and the campaign shows no coverage.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_campaign_coverage
REFRESH EVERY 1 HOUR
    DEPENDS ON mv_dm_smc_administered_by_beneficiary, mv_dm_targets_base
TO dm_campaign_coverage
AS
WITH campaign_administered AS (
    -- ADMINISTRATION_SUCCESS + INDIVIDUAL matches KPI r3/r28 in 13 and viz 714,
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
    FROM dm_smc_administered_by_beneficiary
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
