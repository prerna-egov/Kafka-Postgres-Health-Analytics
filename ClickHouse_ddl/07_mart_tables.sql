-- ==========================================================================
-- MART TABLES
--
-- Storage for the gold layer. These are the target tables only -- the query
-- logic that populates them lives in 08_coverage_marts.sql (coverage, tables
-- 1-7), 09_complaints_marts.sql (complaints, tables 8-10) and
-- 10_overview_marts.sql (overview, tables 11-14), 11_inventory_marts.sql
-- (inventory, tables 15-18) and 12_dashboard_marts.sql (the remaining tabs,
-- tables 19-22), as refreshable materialized views that write here via
-- TO <table>.
--
-- Engine choice: plain MergeTree, not ReplacingMergeTree. Each refresh
-- rebuilds the table wholesale (a refreshable MV with a TO target and no
-- APPEND clause replaces the target's contents atomically), so there are
-- never multiple versions of a row to collapse.
--
-- Create these BEFORE 08-12 -- the materialized views there reference them.
-- ==========================================================================


-- 1. dm_successful_deliveries_base
-- Grain: one row per (tenant, campaign, boundary path, product, delivery date).
-- project_task_entity holds one row per task *resource*, so a task delivering
-- two products contributes two rows -- total_administered is doses/products
-- administered, not distinct people.
CREATE TABLE IF NOT EXISTS dm_successful_deliveries_base (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    product_name                String,                 -- project_task_entity.product_name (product_variant.sku), a single SKU
    event_date                  Date32,                 -- project_task_entity.task_dates (CLIENT audit last-modified date)

    total_administered          UInt64,                 -- count of successful task-resource rows
    total_product_administered  Int64,                  -- sum of project_task_entity.quantity

    INDEX idx_dm_sdb_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, event_date, product_name)
SETTINGS index_granularity = 8192;


-- 2. dm_targets_base
-- Grain: one row per (tenant, campaign, target_type, boundary NODE).
--
-- The level_*_code block is a dense, left-packed root-to-node path, so the path
-- IS the node identity -- this is already node grain, not path grain. It is the
-- staging layer the node model in 4/5/6 below is derived from; it exists as its
-- own table (rather than a CTE inside mv_dm_campaign_hierarchy) because
-- ClickHouse INLINES `WITH ... AS (subquery)` at every reference, and that MV
-- reads its input three times.
--
-- NOT DIRECTLY SUMMABLE -- and node_level is how you avoid it. A target is
-- declared at the boundary hierarchy's LOWEST level and then SUMMED UPWARD, so
-- a parent node's project carries a target equal to the sum of its children's.
-- Every level's row is a complete, correct total for its own subtree, and the
-- same target is therefore represented once at every depth. A bare
-- SUM(target_population) over this table multiplies the true total by roughly
-- the depth of the tree -- on a 3-level tree carrying 1000, it returns 3000.
--
-- Pick exactly one of:
--
--   (1) TARGET AT A REPORTING LEVEL N -- and because the full path is on every
--       row, a CASCADING filter chain works here, which is the whole reason
--       this mart replaced the node-grain fact:
--
--           WHERE target_type = '<t>' AND node_level = N
--             AND level_two_code   = :province
--             AND level_three_code = :district      -- and so on down the chain
--
--   (2) CAMPAIGN-WIDE TOTAL, no boundary bucketing:
--
--           WHERE target_type = '<t>' AND is_target_type_root
--
-- Never combine them, and never SUM without one of them.
--
-- node_level lives here rather than only on (6) deliberately: without it the
-- only way to select a level from this table was to re-derive the whole
-- arrayFilter/arrayZip expression by hand, which made the wrong (summing) query
-- the path of least resistance.
CREATE TABLE IF NOT EXISTS dm_targets_base (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    target_type                 LowCardinality(String), -- project_target.beneficiary_type; may legitimately be '' (see 08)
    hierarchy_type              LowCardinality(String), -- project_entity.hierarchy_type

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    node_level                  UInt8,

    -- Per-target_type cuts, moved here from the retired dm_campaign_target_fact
    -- (6). Keyed on target_type, NOT on a union of all types: a campaign's
    -- HOUSEHOLD node set is typically a strict subset of its INDIVIDUAL one
    -- (218 of 302 projects in table_dumps/project_target.csv carry INDIVIDUAL
    -- only), so a union flag returns 0 for a HOUSEHOLD root cut whenever the
    -- union root carries no HOUSEHOLD row.
    --
    -- Both are FALSE on node_level = 0 rows: an unresolved path is neither a
    -- root nor a leaf of anything, and this is what keeps the ~11.5M of
    -- unattributed target out of every root-cut total.
    is_target_type_root         Bool,
    is_target_type_leaf         Bool,

    target_population           Int64,                  -- sum of project_entity.overall_target
    target_per_day              Int64,                  -- sum of project_entity.target_per_day; the "planned" series for 715/718/723/754

    start_date                  Date,                   -- earliest project start in the group
    end_date                    Date,                   -- latest project end in the group
    total_days                  Int32,                  -- max project_entity.campaign_duration_in_days

    INDEX idx_dm_tb_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
-- node_level third so "the level-N total" is a contiguous primary-key range.
ORDER BY (tenant_id, campaign_number, target_type, node_level)
SETTINGS index_granularity = 8192;


-- 3. dm_campaign_coverage
-- Grain: one row per (tenant, campaign, product).
-- Deliveries are product-level; the target denominator is campaign-level, so
-- each product is measured against the same campaign target population.
CREATE TABLE IF NOT EXISTS dm_campaign_coverage (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    product_name                String,

    total_administered          UInt64,
    total_product_administered  Int64,
    target_population           Int64,
    coverage_percentage         Nullable(Float64) -- NULL when target_population is 0: no denominator means no rate, not 0%
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, product_name)
SETTINGS index_granularity = 8192;


-- ==========================================================================
-- NODE-BASED CAMPAIGN TARGET MODEL (4-7)
--
-- Why this exists: a campaign creates one project per boundary, so targets live
-- at EVERY level of its tree. Summing them double-counts. You must take exactly
-- one cut -- the roots, or the leaves.
--
-- Why it can't be done with a fixed level: hierarchy depth is chosen per
-- campaign / hierarchy_type and varies (depths of 1, 3 and 6 all occur in
-- table_dumps/project_dump_1.csv). Nothing here hardcodes a level; the
-- discriminator is the per-row node_level, derived in SQL from the fact that
-- the level_*_code block is dense and left-packed from the root.
--
-- NO SURROGATE KEYS. These marts previously carried campaign_sk / hierarchy_sk
-- as deterministic cityHash64 values. They were removed: nothing ever joined on
-- them. Both facts carry (tenant_id, campaign_number, node_level, code)
-- denormalized, which is the natural key every query actually filters and joins
-- on, and dm_campaign_hierarchy (4) no longer carries a key to join TO. Leaving
-- them would have advertised a star-join that does not exist.
--
-- Join facts to each other, and to the hierarchy dim, on:
--     (tenant_id, campaign_number, node_level, code)
-- ==========================================================================


-- 4. dm_campaign_hierarchy
-- Grain: one row per LEAF boundary path, per (tenant, campaign, hierarchy_type).
--
-- THE LOAD-BEARING PROPERTY: each row's level_one_code..level_nine_code block is
-- a complete, dense, left-packed ROOT-TO-LEAF path. Every ancestor of that leaf
-- is therefore already readable off the same row, which is why storing only the
-- leaves loses nothing -- 58 stored rows here expose 249 distinct
-- (campaign, level, code) nodes. Navigation needs no recursion, no surrogate
-- keys and no parent pointers:
--
--     -- districts in a province:
--     SELECT DISTINCT level_three_code FROM dm_campaign_hierarchy
--     WHERE level_two_code = :province AND level_three_code != ''
--
--     -- every province in a campaign:
--     SELECT DISTINCT level_two_code FROM dm_campaign_hierarchy
--     WHERE campaign_number = :campaign AND level_two_code != ''
--
-- This replaces a much wider table that carried hierarchy_sk, campaign_sk,
-- code, boundary_path, boundary_path_str, parent_code, rollup_parent_sk/code/
-- level, is_campaign_root, is_campaign_leaf and parent_in_campaign. All of it
-- was derivable from the level block or unused: its only consumer was
-- mv_dm_campaign (now retired), and hierarchy_sk was never joined on -- it is a
-- deterministic hash that dm_campaign_target_fact and dm_coverage_by_node
-- recompute identically, and those two already carry node_level + code
-- denormalized, which is what queries actually filter on.
--
-- LEAF, NOT max_level. A leaf is a node that is not an ancestor of any other
-- node in the campaign -- computed in mv_dm_campaign_hierarchy by ancestor-set
-- membership. It is NOT "the rows at the campaign's deepest level": on a ragged
-- tree (one branch stopping at level 3 while another runs to level 6) a
-- max_level filter silently discards the shallow branch and everything under it
-- becomes unaddressable. On current data every node is a leaf, so the two are
-- indistinguishable here -- which is precisely why this cannot be caught by
-- testing against this instance and has to be stated.
--
-- Target-type-AGNOSTIC: a campaign's boundary tree does not depend on
-- beneficiary type (the one project on a boundary carries both its HOUSEHOLD
-- and INDIVIDUAL target rows). Slice by target_type on
-- dm_campaign_target_fact (6) instead, which carries it.
CREATE TABLE IF NOT EXISTS dm_campaign_hierarchy (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    -- The full root-to-leaf path. Dense and left-packed, so level_N_code is
    -- populated for every N <= node_level and empty above it.
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    -- Depth of the leaf = position of its last non-empty level code. Kept for
    -- symmetry with dm_targets_base (2) and dm_campaign_target_fact (6), so
    -- `node_level = N` means the same thing on all three.
    node_level                  UInt8,

    INDEX idx_dm_ch_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, node_level)
SETTINGS index_granularity = 8192;


-- 5. dm_campaign -- RETIRED, intentionally absent.
--
-- Held per-campaign tree shape: root_level, max_level, level_count,
-- levels_present, level_node_counts, node_count, root/leaf/orphan node counts,
-- campaign_name, project_type and the date window. It was computed entirely
-- from dm_campaign_hierarchy's root/leaf/parent flags, which item 4 above no
-- longer stores.
--
-- Dropped rather than rebuilt because NOTHING read it -- no view in 08-12, no
-- documented dashboard query. Its diagnostics were also degenerate on real
-- data: every node being simultaneously root and leaf made
-- root_node_count = leaf_node_count = node_count. Campaign dates remain
-- available on dm_targets_base (2).
--
-- Item numbers 6-22 are deliberately NOT renumbered -- 09 through 12 reference
-- them in prose. This gap is intentional.

-- 6. dm_campaign_target_fact -- RETIRED, intentionally absent.
--
-- Held one row per (tenant, campaign, target_type, boundary node):
-- node_level, code, is_target_type_root, is_target_type_leaf, target_population,
-- target_per_day and the campaign window.
--
-- Dropped because it was NOT DENSE ENOUGH TO FILTER. It carried node_level +
-- code -- the node's own identity -- and not one level_*_code column, so a
-- level-5 target row had no level-2 code on it. A dashboard applies its filters
-- as a CASCADE (country, then province, then district, then AP, then locality),
-- and `WHERE level_two_code = :province AND level_three_code = :district` was
-- simply not expressible against this table. Every real consumer would have had
-- to fall back to dm_targets_base (2) anyway.
--
-- Everything it held that was not derivable now lives on dm_targets_base (2),
-- which already carried target_type in its grain and all nine level codes:
--   * is_target_type_root / is_target_type_leaf  -> moved there verbatim
--   * code                                       -> derivable; it is the last
--                                                   non-empty level code
--   * everything else                            -> was already there
--
-- Its one SQL consumer, mv_dm_campaign_coverage, now reads dm_targets_base with
-- the identical predicate.
--
-- Item numbers 7-22 are deliberately NOT renumbered -- 09 through 12 reference
-- them in prose. This gap is intentional.


-- 7. dm_coverage_by_node
-- Grain: one row per (tenant, campaign, boundary node, product, delivery date).
--
-- Every delivery is counted once at EVERY ancestor level of its boundary path,
-- which is what makes this a roll-up. It cannot double-count, because a given
-- delivery contributes exactly one row per level.
--
-- Daily grain so both point-in-time coverage and the cumulative-pace KPI are
-- servable from one table. The coverage RATIO is deliberately not stored: it is
-- computed at query time against dm_campaign_target_fact, joined on
-- (tenant_id, campaign_number, node_level, code), so the choice of denominator
-- cut is not frozen into storage.
CREATE TABLE IF NOT EXISTS dm_coverage_by_node (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    node_level                  UInt8,
    code                        LowCardinality(String),
    product_name                String,
    event_date                  Date32,

    total_administered          UInt64,
    total_product_administered  Int64
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, node_level, code, event_date, product_name)
SETTINGS index_granularity = 8192;


-- ==========================================================================
-- COMPLAINTS MARTS (8-10)
--
-- Storage for the DSS_HEALTH_COMPLAINTS panels of the
-- `provincial-health-dashboard-iccd` dashboard. The refreshable materialized
-- views that populate these three live in 09_complaints_marts.sql, which also
-- carries the full ES-to-silver field mapping, the ICCD boundary-level
-- mapping, and the per-panel query for each of the nine charts.
--
-- Source: pgr_complaints_entity (silver), not project_task_entity -- these
-- share no lineage with the coverage marts in 1-7 above.
--
-- NOTE ON project_type_id: deliberately absent. campaign_number is guaranteed
-- present from the product side, so campaign is the filter these marts model
-- on. An implementation that genuinely needs a project-type cut should ALTER
-- the mart to add the column rather than have every mart carry it.
-- ==========================================================================


-- 8. dm_complaints_base
-- Grain: one row per (tenant, campaign, boundary path,
-- complaint type, status, event date).
--
-- The foundational complaints fact: seven of the nine panels are a GROUP BY
-- over this one table. service_code and application_status are both IN the
-- grain rather than pivoted into columns, so the by-type, by-status and
-- combined breakdowns all fall out of the same rows without a second mart
-- holding no new information.
--
-- COUNTS ONLY -- deliberately no duration measure. A resolution time is only
-- meaningful for a complaint that reached a terminal state, but this grain
-- spans every status, so such a column would be populated on rows where it
-- means nothing (PENDING_ASSIGNMENT, ASSIGNED, and whatever a richer PGR
-- workflow adds later). That is not just untidy: a reader who aggregates it
-- without remembering to filter by status gets open complaints' mere AGE
-- averaged in, which silently drags the answer down rather than producing an
-- obviously broken one. Duration lives in dm_complaints_resolution (10),
-- which cannot contain a non-terminal row at all.
--
-- event_date is task_dates, i.e. the DATE of last_modified_time -- which is
-- what dateRefField "Data.service.auditDetails.lastModifiedTime" selects in
-- every date-filtered complaints chart.
CREATE TABLE IF NOT EXISTS dm_complaints_base (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String), -- may be ''; see header
    hierarchy_type              LowCardinality(String), -- pgr_complaints_entity.hierarchy_type (direct bronze column for PGR)

    -- Flattened Boundary Hierarchy Fields (ICCD level mapping in the header)
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    service_code                LowCardinality(String), -- Service.serviceCode -- the complaint TYPE, raw code (not MDMS-localized)
    application_status          LowCardinality(String), -- Service.applicationStatus, raw code
    event_date                  Date32,                 -- pgr_complaints_entity.task_dates (date of last_modified_time)

    complaint_count             UInt64,                 -- count of complaints in this cell -- the ONLY measure here

    INDEX idx_dm_cb_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
-- (tenant_id, campaign_number) first matches every other mart and is the
-- universal filter. event_date third makes the global date-range filter -- on
-- every chart that has one -- a contiguous primary-key range rather than a
-- scan; status and type follow because the three-status and eight-type
-- predicates are the next most common narrowing.
ORDER BY (tenant_id, campaign_number, event_date, application_status, service_code)
SETTINGS index_granularity = 8192;


-- 9. dm_complaints_open_ageing
-- Grain: one row per (tenant, campaign, boundary path, age
-- bucket). Open (PENDING_ASSIGNMENT) complaints only.
--
-- Why this is a separate table rather than a cut of dm_complaints_base: the
-- buckets are measured relative to WALL-CLOCK time, so a complaint moves
-- between buckets as it ages even though nothing about it changed. That
-- cannot be derived from a stored fact -- it has to be recomputed on each
-- refresh, which is exactly what this mart does.
--
-- AGEING BASIS. ES ages on Data.@timestamp, which is the ES *indexing* time,
-- not a domain field -- there is no silver column for it, and nothing should
-- be invented to imitate one. The KPI framework sheet defines this KPI as
-- "summary of open complaints based on time filed", so this ages on
-- created_time. refreshed_at records the instant the bucketing was computed
-- against, so a consumer can always see how stale it is (at most one hour).
CREATE TABLE IF NOT EXISTS dm_complaints_open_ageing (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String), -- may be ''; see header
    hierarchy_type              LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields (ICCD level mapping in the header)
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    -- Verbatim ES date_range bucket keys, so the panel's aggregationPaths
    -- match with no remapping on the way out.
    age_bucket                  LowCardinality(String),
    age_bucket_order            UInt8,                  -- 1..5, so the panel orders its columns without sorting on the label
    open_count                  UInt64,
    refreshed_at                DateTime,               -- as-of instant the buckets were computed against

    INDEX idx_dm_coa_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, age_bucket_order)
SETTINGS index_granularity = 8192;


-- 10. dm_complaints_resolution
-- Grain: one row per (tenant, campaign, boundary path, complaint type,
-- resolution date). RESOLVED and REJECTED complaints ONLY.
--
-- Why this is its own table rather than two more columns on
-- dm_complaints_base: a duration is only meaningful once a complaint has
-- reached a terminal state, so the terminal-status filter belongs in the
-- GRAIN, not in the consumer's WHERE clause. Because a non-terminal row
-- cannot exist here at all, panel 736 is correct whether or not the caller
-- remembers to filter -- the mistake is unrepresentable instead of merely
-- documented. application_status is deliberately NOT a column: every row
-- satisfies the same predicate, so carrying it would only invite someone to
-- filter on a column that has no discriminating power left.
--
-- WHAT "RESOLUTION TIME" MEANS HERE. PGR has no resolved_time/closed_time
-- column -- pgr_complaints_entity carries only created_time and
-- last_modified_time -- so the duration is last_modified_time - created_time
-- for a row already in a terminal state, i.e. "time from filing to the last
-- write", which for a terminal complaint is its resolution. This is exactly
-- what the ES bucket_script on averageResolutionTimeProvinceICCD computes. It
-- is an approximation in one respect: any edit made AFTER resolution pushes
-- last_modified_time out and inflates the duration.
--
-- The RESOLVED/REJECTED pair is a metric definition, not an oversight -- it
-- mirrors the ES chart's own `terms` filter. If a richer PGR workflow adds
-- another terminal state (CLOSED, WITHDRAWN, ...), this filter must be
-- widened deliberately; until then it fails CLOSED, undercounting rather than
-- silently averaging in complaints that never finished. Same reasoning as the
-- narrow status filter on mv_dm_successful_deliveries_base.
CREATE TABLE IF NOT EXISTS dm_complaints_resolution (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String), -- may be ''; see 09 header
    hierarchy_type              LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields (ICCD level mapping in 09's header)
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    service_code                LowCardinality(String), -- kept so "average resolution time BY complaint type" needs no extra mart
    event_date                  Date32,                 -- task_dates: the date the complaint reached its terminal state

    -- Named for what they are: a duration over terminal complaints. NOT a
    -- count of complaints in general -- dm_complaints_base is where counts
    -- live. resolved_count is the denominator that pairs with the sum, and is
    -- guarded identically to it, so the two can never disagree about which
    -- rows contributed.
    resolved_count              UInt64,                 -- terminal complaints with usable timestamps
    resolved_duration_ms_sum    Int64,                  -- sum(last_modified_time - created_time) over those rows

    INDEX idx_dm_cr_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, event_date, service_code)
SETTINGS index_granularity = 8192;


-- ==========================================================================
-- OVERVIEW MARTS (11-14)
--
-- Storage for the DSS_HEALTH_OVERVIEW panels (viz 709-715) of the
-- `provincial-health-dashboard-iccd` dashboard. The refreshable materialized
-- views that populate these live in 10_overview_marts.sql, which also carries
-- the ES-field mapping, the ICCD boundary-level mapping, and the per-panel
-- query for each tile.
--
-- These are NOT a port. The complaints marts (8-10) translate a raw ES index
-- one-for-one; most Overview tiles instead read PRE-AGGREGATED ES summary
-- indexes (household-coverage-summary-iccd-v1, population-coverage-summary-v1,
-- population-coverage-summary-datewise-v4, ineligible-summary-v3) whose fields
-- -- total_households_visited, total_population_administered,
-- total_administered_resources, total_population_refused,
-- ineligible_population_total -- are referenced in ChartApiConfig.json and
-- DEFINED NOWHERE IN ANY REPO. Those indexes are themselves marts. Every
-- metric below is therefore a DEFINITION, sourced from the product owner and
-- KPI/SMC_Campaign_KPI_Framework_updated.xlsx, and stated explicitly so the
-- choice stays reviewable rather than buried in a query.
--
-- TARGETS ARE NOT REDEFINED HERE. Every ES target query filters
-- `exists province` + `must_not exists district`, which is exactly the ROOT
-- CUT that dm_campaign_target_fact (6) already implements per target_type as
-- is_target_type_root. All three target tiles and every coverage denominator
-- read that mart with target_type IN ('HOUSEHOLD','INDIVIDUAL','PRODUCT') --
-- no new target logic, per airflow_dags/CLAUDE.md's rule that a mart takes
-- exactly one cut and never infers a level.
--
-- No project_type_id, per the standing convention: campaign_number is
-- guaranteed present from the product side and is the cut marts model on.
-- ==========================================================================


-- 11. dm_smc_administered_base
-- Grain: one row per (tenant, campaign, boundary path, event date,
-- administration status, delivered-to, product).
--
-- The core Overview fact. administration_status is IN the grain rather than
-- filtered, so one table serves the successful-administration tiles (710/714)
-- AND panel 712's refusal/ineligible slices, without a second mart holding no
-- new information.
--
-- THE SMC DISTINCT-BENEFICIARY KEY. "Population administered" is a count of
-- DISTINCT beneficiaries, not of task rows, matching both the ES drilldowns
-- (cardinality on projectBeneficiaryClientReferenceId) and Coverage KPI 1.0
-- ("beneficiaries treated"). A beneficiary is counted once per:
--
--     (campaign_number, cycleIndex, project_beneficiary_client_reference_id,
--      administration_status)
--
-- so the same id appearing twice with the same cycle, campaign and status
-- counts ONCE. On current data: 7,105 task rows -> 4,559 distinct ids ->
-- 4,569 under this key.
--
-- cycleIndex IS IN THE KEY BECAUSE THIS IS AN SMC MART. SMC runs in multiple
-- cycles and the same beneficiary should legitimately receive a task in EVERY
-- cycle; leaving cycle out of the key would collapse those genuine repeat
-- treatments into one and undercount coverage. This is SMC-specific -- a
-- campaign type that does not run in cycles needs this key revisited before
-- these marts are reused for it.
--
-- WHY AN AGGREGATE STATE, NOT A UInt64. A distinct count is NOT additive. The
-- dashboard filters by province, drills district -> AP -> locality -> village,
-- and sums over date ranges; a stored integer would over-count every
-- beneficiary who appears in two cells on every one of those roll-ups.
-- uniqExactState defers the dedup to read time, so uniqExactMerge is correct
-- at any level. uniqExact (not uniq) because it is exact and the state is the
-- same order of size as the source; if volume ever makes that costly, uniq
-- (HyperLogLog, ~0.5% error) is a drop-in swap on both the State and Merge
-- sides. Note `cycleIndex` is camelCase in ClickHouse and needs backticks.
CREATE TABLE IF NOT EXISTS dm_smc_administered_base (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String), -- may be ''; see 10's header
    hierarchy_type              LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields (ICCD level mapping in 10's header)
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    event_date                  Date32,                 -- project_task_entity.task_dates (CLIENT audit last-modified date)
    administration_status       LowCardinality(String), -- project_task_entity.administration_status, raw code
    delivered_to                LowCardinality(String), -- Data.deliveredTo: INDIVIDUAL / HOUSEHOLD / '' -- panel 714 filters INDIVIDUAL
    -- Data.isDelivered. NOT redundant with delivered_to, which names the
    -- RECIPIENT TYPE while this records whether the delivery actually
    -- happened: 3,459 rows are delivered_to='INDIVIDUAL' with
    -- is_delivered=false. Viz 722's StockStatus filters on this one, so using
    -- delivered_to as a proxy would be wrong.
    is_delivered                Bool,
    -- Beneficiary demographics, added for the Registration/Administration and
    -- Specific-KPIs tabs. gender is enumerated MALE/FEMALE/OTHER/'' in silver;
    -- the ES charts hard-filter only MALE and FEMALE via two `term` clauses, so
    -- a third value silently vanishes from those charts -- reproducing that
    -- filter is the CONSUMER's choice, and keeping gender in the grain here
    -- means the mart itself does not lose those rows.
    gender                      LowCardinality(String),
    -- Age in MONTHS (project_task_entity.age). Stored raw, NOT pre-banded: the
    -- ES age charts use 3-11, 12-59 and 3-59 (the last being the union, not a
    -- third disjoint band), and baking those boundaries into storage would
    -- freeze a metric definition that the KPI framework already states
    -- differently per campaign. Bands are a read-time range predicate.
    age                         UInt32,
    -- cycleIndex. Already inside the distinct-beneficiary key below, but also a
    -- grain column because the across-cycles Venn (viz 806) needs per-cycle
    -- beneficiary SETS, not just a per-cycle count.
    cycle_index                 UInt8,
    product_name                String,                 -- a single SKU; one task delivering two products contributes two rows

    -- Distinct beneficiaries under the SMC key above. Read with
    -- uniqExactMerge(administered_uniq) -- NEVER sum() this column.
    administered_uniq           AggregateFunction(uniqExact, String, UInt8, String, String),
    task_rows                   UInt64,                 -- raw task-resource row count; additive, and what dm_successful_deliveries_base counts
    resource_quantity_sum       Int64,                  -- sum(quantity): doses/resources, the "total_administered_resources" tile (711)

    INDEX idx_dm_sab_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
-- (tenant_id, campaign_number) first matches every other mart. event_date
-- third makes the global date-range filter a contiguous primary-key range;
-- status follows because every panel narrows on it first.
ORDER BY (tenant_id, campaign_number, event_date, administration_status, product_name)
SETTINGS index_granularity = 8192;


-- 12. dm_smc_household_visited
-- Grain: one row per (tenant, campaign, boundary path, event date).
-- Serves panel 709's numerator ("total_households_visited" in the ES summary
-- index, which has no definition in any repo).
--
-- Definition from Coverage KPI 4.0, Household Visit Rate: "unique households
-- visited (with at least one beneficiary assessed)". Numerator is therefore
-- distinct project_task_entity.household_id; the denominator is
-- dm_campaign_target_fact with target_type = 'HOUSEHOLD' AND
-- is_target_type_root -- target rows that already exist and are currently
-- unused, because mv_dm_campaign_coverage hard-filters to INDIVIDUAL.
--
-- DELIBERATE ASYMMETRY WITH 11, FLAGGED FOR REVIEW: this key has NO cycle
-- component, because KPI 4.0 says "visited AT LEAST ONCE during the campaign"
-- -- a household visited in three cycles is one visited household, whereas a
-- beneficiary treated in three cycles is three treatments. That is the
-- intended reading of the KPI, but it is the one place the SMC cycle rule in
-- 11 is not applied, so it is called out here rather than left implicit. If
-- households should also be counted per-cycle, add `cycleIndex` to the
-- uniqExactState below and this comment goes away.
CREATE TABLE IF NOT EXISTS dm_smc_household_visited (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    event_date                  Date32,

    -- Distinct households. Read with uniqExactMerge -- never sum().
    households_uniq             AggregateFunction(uniqExact, String),
    task_rows                   UInt64,                 -- tasks contributing; additive, makes the dedup ratio visible

    INDEX idx_dm_shv_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, event_date)
SETTINGS index_granularity = 8192;


-- 13. dm_smc_adverse_events
-- Grain: one row per (tenant, campaign, boundary path, event date, kind).
--
-- Two of panel 712's four donut slices. The ES config synthesises them with
-- constant-script terms aggs ("'SideEffect'" / "'Referral'") over two separate
-- indexes -- there is no shared "reason" field anywhere; the slice label IS
-- the aggregation name. This mart makes that explicit as an event_kind
-- dimension over a UNION ALL of the two silver entities.
--
-- The other two slices (Beneficiary Refused, Total Ineligible) are NOT here:
-- they are beneficiary counts, not event counts, and fall out of
-- dm_smc_administered_base by administration_status. Splitting them that way
-- keeps "distinct people" and "document count" from being summed together by
-- accident.
CREATE TABLE IF NOT EXISTS dm_smc_adverse_events (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    event_date                  Date32,
    event_kind                  LowCardinality(String), -- 'SIDE_EFFECT' | 'REFERRAL'
    event_count                 UInt64,                 -- document count, NOT distinct beneficiaries

    INDEX idx_dm_sae_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, event_date, event_kind)
SETTINGS index_granularity = 8192;


-- 14. dm_stock_balance
-- Grain: one row per (tenant, campaign, boundary path, product).
-- Serves panel 715, "days inventory can last".
--
-- COMPONENTS, NOT THE RATIO -- deliberate. The ES chart computes
-- (RECEIVED - DISPATCHED) / sum(targetPerDay) per district via action:
-- "division" across two separate index queries. Storing the quotient here
-- would be wrong: the dashboard's province filter re-aggregates, and a ratio
-- cannot be re-aggregated. Consumers divide at read time against
-- dm_campaign_target_fact (target_type = 'PRODUCT' AND is_target_type_root):
--
--     sum(net_on_hand) / nullIf(sum(target_per_day), 0)
--
-- net_on_hand IS DELIBERATELY NOT CLAMPED AT ZERO. On the current instance
-- DISPATCHED (1.06bn units) is roughly 7x RECEIVED (150m), so balances come
-- out strongly negative. That is a real data problem -- an incomplete receipt
-- feed, most likely -- and flooring it at 0 would render a plausible-looking
-- dashboard over broken data. A negative balance should be visible and
-- investigated, not smoothed away.
CREATE TABLE IF NOT EXISTS dm_stock_balance (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields. Resolved from the FACILITY side's
    -- address only -- stock_entity never resolves the transacting party's.
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    product_name                LowCardinality(String),

    received_qty                Int64,                  -- sum(physical_count) where event_type = 'RECEIVED'
    dispatched_qty              Int64,                  -- sum(physical_count) where event_type = 'DISPATCHED'
    net_on_hand                 Int64,                  -- received_qty - dispatched_qty; may be negative, see above

    INDEX idx_dm_sb_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
-- No event_date: the ES query has dateRefField "" -- this is an all-time
-- cumulative balance, not a dated series. Adding a date would change what the
-- number means.
ORDER BY (tenant_id, campaign_number, product_name)
SETTINGS index_granularity = 8192;


-- ==========================================================================
-- INVENTORY MARTS (15-18)
--
-- Storage for the DSS_HEALTH_INVENTORY panels (viz 717, 719-723) of the
-- `provincial-health-dashboard-iccd` dashboard. The refreshable materialized
-- views that populate these live in 11_inventory_marts.sql, which carries the
-- ES-field mapping, the ICCD boundary-level mapping, and the per-panel query
-- for each tile.
--
-- Unlike the Overview marts (11-14), this tab IS a straight port: all six ES
-- indexes it reads (stock-index-v1, project-index-v1, project-task-index-v1,
-- stock-reconciliation-index-v1, project-staff-index-v1, user-sync-index-v1)
-- are raw Data.-prefixed event indexes, not pre-aggregated summaries, so every
-- metric here has a real query to translate rather than a definition to invent.
--
-- NOT COVERED HERE, deliberately: viz 718
-- (daysInventoryStockLastsProvinceICCD) is the SAME chart id already served by
-- dm_stock_balance (14) -- the chart appears on both the Overview and
-- Inventory tabs. Nothing new is built for it.
--
-- RELATIONSHIP TO dm_stock_balance (14). These are siblings over the same
-- silver table, not a replacement. dm_stock_balance keeps its product-grain
-- net-on-hand; dm_stock_transactions (15) below carries the raw per-
-- event_type/reason quantities and DELIBERATELY HAS NO NET COLUMN, so the
-- definition of "net on hand" lives in exactly one place and the two marts
-- cannot drift apart on it.
--
-- No project_type_id, per the standing convention.
-- ==========================================================================


-- 15. dm_stock_transactions
-- Grain: one row per (tenant, campaign, boundary path, product, facility,
-- event type, reason).
--
-- The workhorse of this tab: panels 721, 723 and four of viz 722's six charts
-- are all GROUP BYs over this one table.
--
-- WHY reason IS IN THE GRAIN. Panel 721 breaks stock movement into five series
-- that are each an (event_type, reason) PAIR, not an event_type alone:
--     StockReceived  = RECEIVED   + reason 'RECEIVED'
--     StockIssued    = DISPATCHED + reason ''            (ES: must_not exists)
--     StockReturned  = RECEIVED   + reason 'RETURNED'
--     StockLost      = DISPATCHED + reason IN ('LOST_IN_TRANSIT','LOST_IN_STORAGE')
--     StockDamaged   = DISPATCHED + reason IN ('DAMAGED_IN_TRANSIT','DAMAGED_IN_STORAGE')
-- dm_stock_balance cannot answer any of these -- it aggregates reason away.
--
-- NOTE the five series do NOT partition the data: on current data 98 rows
-- (109,508 units) are RECEIVED with a blank reason, so they land in no named
-- series while still counting toward Stock in Hand. That is faithful to the ES
-- config, which has the same hole; it is not a bug in this mart.
--
-- TWO COMPETING BALANCE FORMULAS, both derivable from this grain:
--     dashboard:     RECEIVED - DISPATCHED                         (what 14 stores)
--     KPI framework: Received + Returned(Unused) - Issued          (Existing-KPI, Inventory page)
-- They differ by ~100M units on current data because RECEIVED/RETURNED alone
-- is 100,006,932. Neither is materialized here -- both are one-line read-time
-- expressions over these rows, and picking one in storage would bury a
-- metric-definition decision inside a mart.
CREATE TABLE IF NOT EXISTS dm_stock_transactions (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String), -- may be ''; see 11's header
    hierarchy_type              LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields. Resolved from the FACILITY side's
    -- address only -- stock_entity never resolves the transacting party's.
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    product_name                LowCardinality(String),
    facility_id                 LowCardinality(String), -- the derived facility (receiver on RECEIVED, sender on DISPATCHED)
    facility_name               LowCardinality(String), -- viz 722's facility tables and 723's drill key on NAME, not id
    facility_type               LowCardinality(String), -- WarNum excludes 'Monitor Local' at read time
    event_type                  LowCardinality(String), -- stock.transaction_type: RECEIVED / DISPATCHED
    reason                      LowCardinality(String), -- stock.transaction_reason; '' is a real, common value

    quantity_sum                Int64,                  -- sum(physical_count) -- NOT a "quantity" column; stock_entity has none
    transaction_count           UInt64,

    INDEX idx_dm_stx_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1,
    INDEX idx_dm_stx_cat (facility_type, event_type, reason) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
-- No event_date: every Inventory chart except 720/721 has dateRefField "" --
-- these are all-time cumulative positions, not a dated series.
ORDER BY (tenant_id, campaign_number, product_name, event_type, reason)
SETTINGS index_granularity = 8192;


-- 16. dm_stock_facility_points
-- Grain: one row per (tenant, campaign, boundary path, facility).
-- Serves viz 722's map-points chart.
--
-- WHERE THE COORDINATES COME FROM. There is no facility master in silver at
-- all -- no facility_entity, and bronze stg_facility carries an address_id but
-- no lat/long of its own. The ES chart reads Data.additionalDetails.lat/.lng,
-- which is the SAME free-form JSON blob that lands in
-- stock_entity.additional_details, so the coordinates are extracted from there
-- with JSONExtractFloat. This is not a workaround for a missing join: it is
-- exactly the field the dashboard reads today.
--
-- FACILITY GRAIN ONLY, and that is a data fact rather than a preference. The
-- ES points chart issues TWO queries -- one bucketed by district, one by
-- facility name. On current data 651 stock rows carry lat/lng and ZERO of them
-- also carry a district code, so the district-grain variant produces nothing
-- whatsoever. Only the facility grain (33 facilities) yields points, so that
-- is what is modeled. If boundary resolution improves for coordinate-bearing
-- rows, the district roll-up is a GROUP BY over this same table.
--
-- latitude/longitude are MEANS over the facility's stock rows, matching the ES
-- `avg` aggregations. For a fixed warehouse every row should carry the same
-- coordinate, so the mean is a de-duplication rather than a real average; a
-- facility whose rows disagree will silently land between them.
CREATE TABLE IF NOT EXISTS dm_stock_facility_points (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields (empty in practice -- see above)
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    facility_id                 LowCardinality(String),
    facility_name               LowCardinality(String), -- the ES plotLabel is "Warehouse Name"
    facility_type               LowCardinality(String),

    latitude                    Float64,                -- avg(JSONExtractFloat(additional_details,'lat'))
    longitude                   Float64,                -- avg(JSONExtractFloat(additional_details,'lng'))
    transaction_count           UInt64,                 -- rows behind the point; the UI needs one numeric plot per marker

    INDEX idx_dm_sfp_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, facility_id)
SETTINGS index_granularity = 8192;


-- 17. dm_stock_reconciliation
-- Grain: one row per (tenant, campaign, boundary path, facility, product).
-- Serves the "manual stock" half of panel 720.
--
-- LATEST PER FACILITY, NOT A SUM -- this deliberately DIVERGES from the ES
-- config, which has a latent double-count. That aggregation builds a
-- `top_hits` sub-agg named `latest` (size 1, sorted by lastModifiedTime desc)
-- per facility, and then never reads it: the enclosing `sum_bucket` sums
-- `sum_calculatedCount`, which is a plain sum over EVERY reconciliation
-- document for that facility. So a facility reconciled three times contributes
-- three physical counts to its district total. The unused `top_hits` makes the
-- intent unambiguous -- a physical stock count is a snapshot, not something
-- you add up -- so this mart takes argMax on the client audit timestamp.
-- reconciliation_count is kept precisely so the size of that divergence stays
-- measurable rather than invisible.
--
-- (The ES agg is also misnamed: `sum_calculatedCount` sums
-- Data.stockReconciliation.physicalCount, not calculatedCount.)
--
-- calculated_count is carried even though no chart on this tab reads it:
-- Stock KPI 10.0, "Stock Balance Accuracy (Physical vs System)", is exactly
-- abs(physical_count - calculated_count) / calculated_count * 100, and at this
-- grain it costs one more argMax.
CREATE TABLE IF NOT EXISTS dm_stock_reconciliation (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    facility_id                 String,                 -- plain String here: stock_reconciliation_entity types it String, unlike stock_entity's LowCardinality
    facility_name               String,
    product_name                String,

    physical_count              Int64,                  -- LATEST physical count (argMax on client_last_modified_time)
    calculated_count            Int64,                  -- LATEST system balance, same row
    reconciled_at               DateTime,               -- when that latest reconciliation happened
    reconciliation_count        UInt64,                 -- how many reconciliations were collapsed; >1 is where ES over-counts

    INDEX idx_dm_srec_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, facility_id, product_name)
SETTINGS index_granularity = 8192;


-- 18. dm_user_sync
-- Grain: one row per (tenant, campaign, boundary path, role).
-- Serves panel 717's warehouse-manager sync rate.
--
-- WHY stock_entity IS THE SYNC SIGNAL. The ES chart reads user-sync-index-v1
-- for its numerator, and there is no such entity in silver. There does not
-- need to be: the upstream pipeline wrote one record to the stock index and
-- one to the user-sync index for the same underlying event, so a user
-- appearing in stock_entity IS a user who synced. The original table is used
-- directly rather than reconstructing a parallel sync feed.
--
-- role IS A GRAIN COLUMN, NOT A FILTER. The chart only asks about
-- WAREHOUSE_MANAGER, but the same shape answers the KPI framework's CDD and
-- supervisor sync rows (Existing-KPI `Sync` page, rows 81-89) for free, and a
-- filter baked into the mart would have forced a second near-identical table.
--
-- AGGREGATE STATES, NOT COUNTS -- same reasoning as the SMC beneficiary key on
-- mart 11. A user active in two districts is one user; a stored integer would
-- be double-counted by every roll-up to province. uniqExactMerge at read time
-- is correct at any level.
--
-- THE TWO SIDES ARE KEYED DIFFERENTLY, and this cannot be fixed here:
--   denominator -> project_staff_entity.user_id   (the campaign's staff roster)
--   numerator   -> stock_entity.user_name         (stock_entity has NO user_id column at all)
-- So the rate is a ratio of two independently-counted populations, not a
-- per-user matched cohort; no user-level "did this person sync" join is
-- possible until stock_entity carries a user id. Note also that
-- egov_api_utils._get_staff_role collapses a multi-role user to their
-- highest-ranked role only, so someone who is both a warehouse manager and
-- something more senior will not appear under WAREHOUSE_MANAGER at all.
CREATE TABLE IF NOT EXISTS dm_user_sync (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    -- Flattened Boundary Hierarchy Fields
    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    role                        LowCardinality(String), -- WAREHOUSE_MANAGER / DISTRIBUTOR / DISTRICT_SUPERVISOR / ...

    -- Read with uniqExactMerge -- never sum(). See the note above.
    users_created_uniq          AggregateFunction(uniqExact, String), -- project_staff_entity.user_id
    users_synced_uniq           AggregateFunction(uniqExact, String), -- stock_entity.user_name

    INDEX idx_dm_us_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, role)
SETTINGS index_granularity = 8192;


-- ==========================================================================
-- REMAINING-TAB MARTS (19-22)
--
-- Storage for the DSS_HEALTH_TEAM_PERFORMANCE (viz 782-783),
-- DSS_HEALTH_DATA_QUALITY (viz 789, 793-794) and
-- DSS_HEALTH_REFERRAL_AND_SIDE_EFFECTS (viz 802-803) tabs. Logic lives in
-- 12_dashboard_marts.sql.
--
-- The DSS_HEALTH_REGISTRATION_ADMINISTRATION tab (viz 747-755, 40 charts)
-- needs NO new table: administration coverage, the coverage heatmap,
-- not-administered-by-reason, administered-by-gender, actual-vs-planned and
-- both delivery-summary tables are all GROUP BYs over dm_smc_administered_base
-- (11) -- which is why gender, age and cycle_index were added to its grain --
-- joined to dm_smc_household_visited (12), dm_smc_adverse_events (13) and
-- dm_campaign_target_fact (6).
--
-- NOT BUILT, and why (each is a source gap, not a modelling choice):
--   * viz 726-730, the six Supervision "checklist completion rate" charts, and
--     viz 809 (malaria screening): all read service_task_entity, which has 0
--     rows. Every one of the six is also a value_count with action:"" and no
--     denominator anywhere -- the "Fill Rate" column names are
--     AdditiveComputedField over a single count, i.e. relabelling. Same call as
--     viz 716.
--   * viz 808, administered-by-height: the ES chart ranges over
--     Data.additionalDetails.height; NO row in project_task_entity carries a
--     height key in additional_details. (Its buckets are also broken: 90-119,
--     120-139, 140-159, then "169 and above" -- 160-168cm falls in no bucket.)
--   * viz 760-762, DSS_KIBANA_MAPS: not DSS charts at all. The three ids appear
--     nowhere in ChartApiConfig.json; MasterDashboardConfig gives them
--     chartType "kibanaComponent" plus moduleName/pageName routing keys, so
--     they are front-end embeds with no backing query to port.
-- ==========================================================================


-- 19. dm_attendance
-- Grain: one row per (tenant, campaign, boundary path, event date, individual).
-- Serves viz 782 (FLW attendance) and the attendance half of viz 783.
--
-- PRESENCE IS AN EXIT LOG. The ES charts filter
-- attendanceLog.type = 'EXIT' AND attendanceLog.status = 'ACTIVE' -- presence
-- is inferred from someone clocking OUT, not in. That is the source's
-- convention, not an oversight, and it is preserved here.
--
-- Individual grain, one row per person per day, is deliberate: every consumer
-- of this mart wants "distinct people present", and the ES version computes
-- that with a terms agg on individualId that carries NO `size` -- silently
-- truncating to 10 individuals per day per boundary. Keeping the individual in
-- the grain makes the distinct count exact and the truncation unreproducible.
--
-- ON THIS INSTANCE THIS MART IS EMPTY, and correctly so: all 10 rows in
-- attendance_log_entity are type='ENTRY'; not one is 'EXIT'. The ES charts
-- would render zero against the same data. Do not "fix" this by widening the
-- filter to ENTRY -- that would silently change the metric from "clocked out"
-- to "clocked in" and make this instance's numbers incomparable with any
-- deployment that has real exit logs.
--
-- NOTE the ES per-day average divides by `per_day._bucket_count`, i.e. only
-- days that HAD logs. A boundary that reported on 1 of 10 campaign days scores
-- as if it were fully staffed. That denominator choice is left to the consumer
-- rather than baked in.
CREATE TABLE IF NOT EXISTS dm_attendance (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    event_date                  Date32,
    individual_id               String,
    role                        LowCardinality(String),
    log_count                   UInt64,                 -- EXIT+ACTIVE logs for this person on this day; >1 means duplicate clock-outs

    INDEX idx_dm_att_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, event_date)
SETTINGS index_granularity = 8192;


-- 20. dm_household_registry
-- Grain: one row per (tenant, campaign, boundary path, household).
-- Serves viz 789 (households with >20 members) and the household-registry
-- columns of viz 794.
--
-- member_count is carried per household rather than pre-filtered to >20, so the
-- threshold stays a read-time predicate. The ES chart uses `gt: 20` -- strictly
-- greater, so a household of exactly 20 is NOT counted despite the label
-- "more than twenty members" (which is, to be fair, correct English for it).
-- Keeping the raw count also makes the KPI framework's other household-size
-- questions answerable from the same rows.
CREATE TABLE IF NOT EXISTS dm_household_registry (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    household_id                String,
    member_count                Int32,                  -- household_entity.member_count; viz 789 filters > 20
    event_date                  Date32,

    INDEX idx_dm_hhr_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, household_id)
SETTINGS index_granularity = 8192;


-- 21. dm_referral_summary
-- Grain: one row per (tenant, campaign, boundary path, event date, source).
-- Serves viz 802 and 803.
--
-- TWO SOURCES, TWO DIFFERENT COUNTING RULES, kept as separate measures rather
-- than one column with a source dimension, because they are NOT the same unit:
--   referred_children_uniq -- referral_entity, DISTINCT beneficiaries
--                             (ES: cardinality on the beneficiary ref id)
--   hf_referral_records    -- hf_referral_entity, RECORD COUNT
--                             (ES: value_count on the hfReferral id)
-- The ES chart divides the second by the first to get "% children present at
-- health facility". That ratio divides a record count by a distinct-child
-- count, so one child attending twice pushes it ABOVE 100%. The two measures
-- are stored separately and unreconciled precisely so that asymmetry stays
-- visible to whoever writes the ratio, instead of being frozen into a column.
--
-- Note the tab is called REFERRAL_AND_SIDE_EFFECTS but neither of its two viz
-- queries the side-effect index at all -- side effects live only on the
-- Overview tab's panel 712 (dm_smc_adverse_events, 13).
CREATE TABLE IF NOT EXISTS dm_referral_summary (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    event_date                  Date32,

    -- Distinct children referred by field teams. Read with uniqExactMerge.
    referred_children_uniq      AggregateFunction(uniqExact, String),
    hf_referral_records         UInt64,                 -- health-facility referral RECORDS, not distinct children

    INDEX idx_dm_ref_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, event_date)
SETTINGS index_granularity = 8192;


-- 22. dm_suspected_fraud
-- Grain: one row per (tenant, campaign, boundary path, user, minute).
-- Serves viz 793 and the fraud column of viz 794.
--
-- WHAT "SUSPECTED FRAUD" MEANS. There is no fraud flag in any source. The ES
-- chart is a THROUGHPUT HEURISTIC: a user is suspect if there exists at least
-- one wall-clock minute in which they recorded 4 or more successful individual
-- administrations. This mart stores the offending (user, minute) buckets that
-- clear that bar; the per-boundary chart counts DISTINCT USERS over them, and
-- the distributor-level drill counts the BUCKETS themselves -- two different
-- numbers the ES config computes from the same shape, both derivable here.
--
-- The threshold is a metric definition, not a constant of nature: min_doc_count
-- 4 appears three times in the ES agg (boundary, user, minute). Only the minute
-- one is semantically meaningful and it is the one reproduced. Widening or
-- narrowing it is a product decision.
--
-- CLOCK CAVEAT: the ES version filters on Data.createdTime but buckets minutes
-- on Data.@timestamp -- the ES ingest clock, which is not a domain field and
-- has no silver equivalent. This uses created_time for both, so a burst is
-- measured against when the device recorded the work rather than when the
-- pipeline happened to index it. That is the more defensible clock, and on a
-- backfilled load it is the only one that means anything at all.
CREATE TABLE IF NOT EXISTS dm_suspected_fraud (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),
    hierarchy_type              LowCardinality(String),

    level_one_code                      LowCardinality(String),
    level_two_code                      LowCardinality(String),
    level_three_code                    LowCardinality(String),
    level_four_code                     LowCardinality(String),
    level_five_code                     LowCardinality(String),
    level_six_code                      LowCardinality(String),
    level_seven_code                    LowCardinality(String),
    level_eight_code                    LowCardinality(String),
    level_nine_code                     LowCardinality(String),

    created_by                  String,                 -- the user whose throughput tripped the heuristic
    user_name                   LowCardinality(String), -- the distributor-level drill keys on name, not id
    minute_bucket               DateTime,               -- the offending wall-clock minute
    task_count                  UInt64,                 -- administrations in that minute; always >= the threshold

    INDEX idx_dm_fraud_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, created_by, minute_bucket)
SETTINGS index_granularity = 8192;
