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
-- One row per task RESOURCE, so a task delivering two products contributes two
-- rows: total_administered is doses, not distinct people.
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
-- Grain: one row per (tenant, campaign, target_type, boundary node).
--
-- The level_*_code block is a dense, left-packed root-to-node path, so the path
-- IS the node identity. Kept as its own table rather than a CTE because
-- ClickHouse inlines `WITH ... AS (subquery)` at every reference.
--
-- NOT DIRECTLY SUMMABLE. A target is declared at the hierarchy's LOWEST level
-- and summed upward, so a parent carries the sum of its children and the same
-- target appears once at every depth. A bare SUM multiplies the true total by
-- the depth of the tree. Pick exactly one:
--
--   (1) At a reporting level -- the full path is on the row, so a cascading
--       filter chain works:
--           WHERE target_type = '<t>' AND node_level = N
--             AND level_two_code = :province AND level_three_code = :district
--
--   (2) Campaign-wide -- the shallowest level the campaign has, which is its
--       own declared total. Not a fixed level: campaigns start at different
--       depths. See mv_dm_campaign_coverage in 08.
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
-- Grain: one row per (tenant, campaign).
--
-- CAMPAIGN GRAIN, MATCHING ITS OWN DENOMINATOR. The target is declared once per
-- campaign, so the numerator must be too. product_name was in this grain while
-- the numerator came from dm_successful_deliveries_base (1), which counts
-- task-resource rows -- DOSES. Dividing doses by an INDIVIDUAL target (PEOPLE)
-- overstates coverage by roughly the number of products each child receives,
-- and a per-product row measured against the whole-campaign target invited
-- summing the column, which double-counts any child given two products.
--
-- total_administered is now DISTINCT CHILDREN, read from
-- dm_smc_administered_by_beneficiary (23) -- the product-free mart that exists
-- precisely so this number cannot be got wrong. See the MV in 08.
CREATE TABLE IF NOT EXISTS dm_campaign_coverage (
    tenant_id                   LowCardinality(String),
    campaign_number             LowCardinality(String),

    total_administered          UInt64,                 -- DISTINCT CHILDREN successfully administered, campaign-wide
    total_product_administered  Int64,                  -- product units behind those administrations
    target_population           Int64,                  -- the campaign's own declared INDIVIDUAL target
    coverage_percentage         Nullable(Float64) -- NULL when target_population is 0: no denominator means no rate, not 0%
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number)
SETTINGS index_granularity = 8192;


-- ==========================================================================
-- NODE-BASED CAMPAIGN TARGET MODEL (4-7)
--
-- A campaign creates one project per boundary, so targets exist at every level
-- of its tree and summing across levels double-counts. Nothing here hardcodes a
-- level: the discriminator is the per-row node_level, derived from the fact
-- that the level_*_code block is dense and left-packed from the root.
--
-- NO SURROGATE KEYS. Join on the natural key:
--     (tenant_id, campaign_number, node_level, code)
-- ==========================================================================


-- 4. dm_campaign_hierarchy
-- Grain: one row per LEAF boundary path, per (tenant, campaign, hierarchy_type).
--
-- Each row's level block is a complete root-to-leaf path, so every ancestor is
-- readable from it and storing only leaves loses nothing. Navigation needs no
-- recursion or parent pointers:
--
--     SELECT DISTINCT level_three_code WHERE level_two_code = :province
--
-- LEAF, NOT deepest-level. A leaf is a node that is not an ancestor of any
-- other node in the campaign. On a ragged tree -- one branch ending shallow,
-- another running deep -- a max-level filter would discard the shallow branch
-- entirely.
--
-- Target-type agnostic: a campaign's tree does not depend on beneficiary type.
-- Slice by target_type on dm_targets_base (2).
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
-- Held per-campaign tree shape derived from the root/leaf flags that item 4 no
-- longer stores. Nothing read it. Campaign dates live on dm_targets_base (2).
-- Items 6-22 are deliberately not renumbered; 09-12 reference them by number.


-- 7. dm_coverage_by_node
-- Grain: one row per (tenant, campaign, boundary node, product, delivery date).
--
-- Every delivery is counted once at EVERY ancestor level of its boundary path,
-- which is the roll-up. Daily grain, so both point-in-time coverage and
-- cumulative pace are servable. The coverage RATIO is deliberately not stored:
-- computed at query time against dm_targets_base, so the choice of denominator
-- is not frozen into storage.
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
-- Storage for the complaints panels. Logic, field mapping and the per-panel
-- queries live in 09_complaints_marts.sql.
--
-- Source: pgr_complaints_entity -- no lineage with the coverage marts above.
--
-- NOTE ON project_type_id: deliberately absent from every mart here and below.
-- campaign_number is guaranteed present from the product side, so campaign is
-- the cut these marts model on. An implementation that needs a project-type cut
-- should ALTER that one mart rather than have every mart carry the column.
-- ==========================================================================


-- 8. dm_complaints_base
-- Grain: one row per (tenant, campaign, boundary path, complaint type, status,
-- event date).
--
-- service_code and application_status are IN the grain rather than pivoted, so
-- one table serves the by-type, by-status and combined breakdowns.
--
-- COUNTS ONLY -- no duration measure. A resolution time is meaningful only for
-- a complaint that reached a terminal state, but this grain spans every status,
-- so such a column would be populated where it means nothing and would be
-- averaged in by anyone who forgot to filter. Duration lives in
-- dm_complaints_resolution (10), which cannot contain a non-terminal row.
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
-- Grain: one row per (tenant, campaign, boundary path, age bucket). Open
-- complaints only.
--
-- Separate from (8) because the buckets are relative to wall-clock time: a
-- complaint moves between them as it ages even though nothing about it changed,
-- so it must be recomputed each refresh rather than derived from a stored fact.
--
-- Ages on created_time -- "time filed", per the KPI definition. refreshed_at
-- records the instant the bucketing was computed against.
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
-- resolution date). RESOLVED and REJECTED only.
--
-- Its own table rather than columns on (8) so the terminal-status filter lives
-- in the GRAIN: a non-terminal row cannot exist here, which makes the metric
-- correct whether or not the caller remembers to filter. application_status is
-- deliberately not a column -- every row satisfies the same predicate.
--
-- PGR has no resolved_time, so the duration is last_modified_time -
-- created_time for an already-terminal row. An edit made after resolution
-- inflates it.
--
-- The RESOLVED/REJECTED pair is a metric definition. If the workflow gains
-- another terminal state it must be widened deliberately; until then it fails
-- closed, undercounting rather than averaging in unfinished complaints.
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
-- `exists province` + `must_not exists district` -- a LEVEL SELECTOR, meaning
-- "rows whose deepest populated level is exactly province". Read
-- dm_targets_base (2) with target_type IN ('HOUSEHOLD','INDIVIDUAL','PRODUCT')
-- AND node_level = <the level the chart buckets at>, plus the parent level
-- codes for a cascading filter. No new target logic, per
-- airflow_dags/CLAUDE.md's rule that a mart takes exactly one cut and never
-- infers a level.
--
-- No project_type_id, per the standing convention: campaign_number is
-- guaranteed present from the product side and is the cut marts model on.
-- ==========================================================================


-- 11. dm_smc_administered_base
-- Grain: one row per (tenant, campaign, boundary path, event date,
-- administration status, delivered-to, delivered flag, demographics, product).
--
-- The core fact. administration_status is in the grain rather than filtered, so
-- one table serves the successful-administration panels and the
-- refusal/ineligible breakdowns alike.
--
-- THE SMC DISTINCT-BENEFICIARY KEY. "Population administered" counts distinct
-- beneficiaries, not task rows:
--
--     (campaign_number, cycleIndex, project_beneficiary_client_reference_id,
--      administration_status)
--
-- cycleIndex is in the key because SMC runs in cycles and the same beneficiary
-- should receive a task in every cycle; without it those genuine repeat
-- treatments collapse into one and undercount. This is SMC-specific -- a
-- campaign type that does not run in cycles needs the key revisited.
--
-- STORED AS AN AGGREGATE STATE, not an integer, because a distinct count is not
-- additive: the dashboard filters by province, drills down, and sums over date
-- ranges, and a stored count would double-count anyone appearing in two cells.
-- uniqExactMerge is correct at any level; swap uniqExact for uniq if the volume
-- ever makes exactness costly.
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
    -- happened. A row can name an INDIVIDUAL recipient and still be
    -- undelivered, so delivered_to is not a proxy for this.
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

    -- Aggregate STATE, not a number: a raw SELECT shows a binary blob, which is
    -- expected. Roll it up with uniqExactMerge() -- correct at any level,
    -- because a distinct count is not additive and summing per-cell counts
    -- double-counts anything appearing in two cells. The plain column beside it
    -- is the same measure for THIS ROW ONLY, so the table is readable without
    -- losing the correct path.
    administered_uniq           AggregateFunction(uniqExact, String, UInt8, String, String),
    administered_count          UInt64,                 -- distinct beneficiaries in THIS row; do not SUM across rows
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


-- 23. dm_smc_administered_by_beneficiary
-- Grain: exactly item 11's grain MINUS product_name -- one row per (tenant,
-- campaign, boundary path, event date, administration status, delivered-to,
-- delivered flag, demographics, cycle).
--
-- WHY THIS EXISTS. Item 11 carries product_name in its grain, so a child given
-- two products occupies two rows there. That is correct for a per-product
-- question and a TRAP for a per-child one: summing administered_count across
-- those rows counts the child twice. uniqExactMerge over item 11 gets the right
-- answer anyway -- the state is what makes that safe -- but it relies on every
-- reader knowing to use it, and it pays for the product fan-out on every scan.
--
-- This mart removes the fan-out at the source. Read it for any question about
-- CHILDREN; read item 11 when the question is about PRODUCTS. Its first
-- consumer is mv_dm_campaign_coverage (3), whose denominator is an INDIVIDUAL
-- target -- a count of people -- and which previously divided by a count of
-- doses taken from dm_successful_deliveries_base (1).
--
-- Built by collapsing item 11 rather than re-reading project_task_entity, so
-- the two marts cannot drift apart: uniqExactMergeState merges the per-product
-- states back into one, which is exact, not an approximation.
--
-- Exact BECAUSE product_name is not part of the distinct-beneficiary key. The
-- state is a SET of those keys, so a child dosed with several products carries
-- the same key in every product's state, and merging UNIONS the sets rather
-- than adding the counts -- the child dedupes back to one. Verified: collapsing
-- item 11 and building straight from silver give identical rows, zero
-- differences. The full argument and the worked example are on the MV in 10.
--
-- Same caveat as item 11: administration_status is IN THE GRAIN, not filtered.
-- Every consumer states its own predicate.
CREATE TABLE IF NOT EXISTS dm_smc_administered_by_beneficiary (
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
    administration_status       LowCardinality(String), -- raw code, in the grain not filtered
    delivered_to                LowCardinality(String), -- Data.deliveredTo: INDIVIDUAL / HOUSEHOLD / ''
    is_delivered                Bool,                   -- Data.isDelivered; NOT a proxy for delivered_to
    gender                      LowCardinality(String), -- MALE/FEMALE/OTHER/''
    age                         UInt32,                 -- MONTHS, stored raw; bands are a read-time predicate
    cycle_index                 UInt8,

    -- Same state type and same distinct-beneficiary key as item 11, merged
    -- across that mart's product rows. uniqExactMerge() here returns exactly
    -- what uniqExactMerge() over item 11 returns for the same predicate.
    administered_uniq           AggregateFunction(uniqExact, String, UInt8, String, String),
    administered_count          UInt64,                 -- distinct beneficiaries in THIS row; do not SUM across rows
    task_rows                   UInt64,                 -- additive
    resource_quantity_sum       Int64,                  -- sum(quantity) across every product in this cell

    INDEX idx_dm_sabb_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
-- Item 11's ORDER BY minus its product_name tail, so the two marts share a
-- prefix and a query moving between them keeps the same access pattern.
ORDER BY (tenant_id, campaign_number, event_date, administration_status)
SETTINGS index_granularity = 8192;


-- 24. dm_smc_administered_by_campaign
-- Grain: item 23's grain MINUS event_date -- one row per (tenant, campaign,
-- boundary path, administration status, delivered-to, delivered flag,
-- demographics, cycle).
--
-- THE CAMPAIGN-LEVEL SOURCE OF TRUTH. The family reads as a progression, each
-- step removing one way a single child can occupy more than one row:
--
--     11. dm_smc_administered_base            + product  + date
--     23. dm_smc_administered_by_beneficiary  - product  + date
--     24. this mart                           - product  - date
--
-- With no date dimension, a child dosed on two days inside the same campaign,
-- cycle, status and place collapses to one row, so administered_count can be
-- READ DIRECTLY here without a Merge. That is the point: the correct number
-- stops depending on the reader knowing to reach for uniqExactMerge.
--
-- WHAT THIS STILL DOES NOT SETTLE, and deliberately so. A beneficiary id
-- recorded under two boundary paths is still two rows, because the platform
-- cannot tell which reading is right:
--
--   sum(administered_count)  is BOUNDARY-AWARE -- treats the two as two
--                            children. Correct if two distributors in sibling
--                            boundaries each searched up a child by name and
--                            landed on the same backend id while genuinely
--                            dosing different children.
--   uniqExactMerge(...)      is ID-AWARE -- treats them as one child. Correct
--                            if the id really is one person.
--
-- Both are kept so a consumer can pick. They differ by ~0.2% in practice, and
-- almost all of that is one malformed household reference rather than the real
-- same-name case. Do NOT "fix" this by dropping the boundary block: that would
-- silently choose the id-aware answer and lose every geographic cut with it.
--
-- WHEN TO USE A SIBLING INSTEAD: item 11 for anything per-product; item 23
-- when the answer needs a date.
CREATE TABLE IF NOT EXISTS dm_smc_administered_by_campaign (
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

    administration_status       LowCardinality(String), -- raw code, in the grain not filtered
    delivered_to                LowCardinality(String), -- Data.deliveredTo: INDIVIDUAL / HOUSEHOLD / ''
    is_delivered                Bool,                   -- Data.isDelivered; NOT a proxy for delivered_to
    gender                      LowCardinality(String), -- MALE/FEMALE/OTHER/''
    age                         UInt32,                 -- MONTHS, stored raw; bands are a read-time predicate
    cycle_index                 UInt8,

    -- Same state type and same distinct-beneficiary key as items 11 and 23,
    -- merged across this mart's collapsed date rows. uniqExactMerge() here
    -- returns exactly what it returns over either sibling for the same
    -- predicate -- verified identical, not approximately equal.
    administered_uniq           AggregateFunction(uniqExact, String, UInt8, String, String),
    -- Unlike its siblings, this column IS safe to read directly at this grain.
    -- It is still a per-row figure: see the boundary note above before summing
    -- across boundary paths.
    administered_count          UInt64,
    task_rows                   UInt64,                 -- additive
    resource_quantity_sum       Int64,                  -- sum(quantity) across every product and date in this cell

    INDEX idx_dm_sabc_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
-- Item 23's ORDER BY with the event_date term dropped, so all three siblings
-- share a prefix and a query moving between them keeps the same access pattern.
ORDER BY (tenant_id, campaign_number, administration_status)
SETTINGS index_granularity = 8192;


-- 12. dm_smc_household_visited -- RETIRED, intentionally absent.
--
-- Held distinct households visited per (campaign, boundary path, date). It was
-- sourced from project_task_entity, which was wrong: households visited comes
-- from the household registry, not from the task feed.
--
-- Once corrected to that source it was a pure aggregation of
-- dm_household_registry (20), which already reads household_entity at HOUSEHOLD
-- grain -- so one row there IS one household and count() is already the
-- distinct count, with no aggregate state required:
--
--     SELECT count() FROM dm_household_registry
--     WHERE level_two_code = :province AND event_date BETWEEN :from AND :to
--
-- Items 13-22 are deliberately not renumbered; 09-12 reference them by number.


-- 13. dm_smc_adverse_events
-- Grain: one row per (tenant, campaign, boundary path, event date, kind).
--
-- Side effects and referrals, unioned with an event_kind dimension. They come
-- from separate entities and have no shared "reason" field; the kind IS the
-- distinction.
--
-- Refusals and ineligibles are NOT here: they are beneficiary counts, not event
-- counts, and fall out of (11) by administration_status. Keeping them apart
-- stops "distinct people" and "document count" being summed together.
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
--
-- COMPONENTS, NOT THE RATIO. Days-of-stock is (received - dispatched) / daily
-- consumption, but a ratio cannot be re-aggregated and the dashboard's province
-- filter re-aggregates. Consumers divide at read time against dm_targets_base.
--
-- net_on_hand is NOT clamped at zero. A negative balance means the receipt feed
-- is incomplete; flooring it would render a plausible dashboard over broken
-- data.
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
-- WHY reason IS IN THE GRAIN. The transaction breakdown splits stock movement
-- into series that are each an (event_type, reason) PAIR -- received, issued,
-- returned, lost, damaged -- which dm_stock_balance (14) cannot answer because
-- it aggregates reason away.
--
-- Those series do not partition the data: rows with an unrecognised reason land
-- in no named series while still counting toward stock in hand. That is
-- faithful to the source definition, not a gap here.
--
-- TWO COMPETING BALANCE FORMULAS, both derivable from this grain and neither
-- materialized, because picking one in storage would bury a metric decision:
--     dashboard:     received - dispatched
--     KPI framework: received + returned(unused) - issued
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
--
-- There is no facility master in silver; the coordinates come from the stock
-- record's own additional_details JSON, which is the same field the map reads.
--
-- FACILITY GRAIN ONLY. Coordinate-bearing rows do not reliably carry a resolved
-- boundary, so a district-grain variant has nothing to group by. If boundary
-- resolution improves, a district roll-up is a GROUP BY over this table.
--
-- latitude/longitude are MEANS over the facility's rows. For a fixed warehouse
-- every row should carry the same coordinate, so this de-duplicates rather than
-- averages; a facility whose rows disagree lands between them.
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
--
-- LATEST PER FACILITY, NOT A SUM. A physical stock count is a snapshot, not
-- something to add up -- a facility reconciled three times must contribute its
-- most recent count once, not three counts. reconciliation_count keeps the
-- collapse visible.
--
-- calculated_count is carried although no chart reads it: stock balance
-- accuracy is |physical - calculated| / calculated, and at this grain it costs
-- one more argMax.
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
-- Sync marts. Split by grain, not one per KPI:
--   dm_user_sync          -> KPIs 1-9  (created / synced / % per cadre)
--   dm_cdd_sync_hourly    -> KPI 10    (CDD sync by hour, last 24h)
--   dm_cdd_sync_facility  -> KPI 11    (records per CDD per health facility)
--
-- All three keep the USER in the grain and leave the distinct count to the read
-- query. A pre-aggregated distinct count is not additive: stored per boundary
-- cell, sum() double-counts any user present in two cells and no other
-- aggregate recovers the true total. Measured on unified-dev, the
-- pre-aggregated form reported 9 synced distributors against a true 3.
-- Keeping the user in the grain makes uniqExact() exact at EVERY level with
-- plain MergeTree and no AggregateFunction states.
--
-- All nine boundary levels are carried because the level that represents a
-- health facility differs per hierarchy_type -- observed as level_four for one
-- tenant and a ward at the same depth for another. hierarchy_type is stored
-- alongside so the read can resolve the level via boundary_hierarchy_dim (06).
--
-- campaign_id and project_id are deliberately ABSENT: campaign_id is hardcoded
-- '' by the transformation DAGs and project_id is blank on ~76% of household
-- rows, so both only invite filters that silently match nothing.

CREATE TABLE IF NOT EXISTS analytics.dm_user_sync
(
    tenant_id            LowCardinality(String),
    campaign_number      LowCardinality(String),

    hierarchy_type       LowCardinality(String),
    level_one_code       LowCardinality(String),
    level_two_code       LowCardinality(String),
    level_three_code     LowCardinality(String),
    level_four_code      LowCardinality(String),
    level_five_code      LowCardinality(String),
    level_six_code       LowCardinality(String),
    level_seven_code     LowCardinality(String),
    level_eight_code     LowCardinality(String),
    level_nine_code      LowCardinality(String),

    role                 LowCardinality(String),

    src                  LowCardinality(String),   -- CREATED | SYNCED

    user_id              String,   -- roster uuid; '' on SYNCED (record tables carry no user id)
    user_name            String,   -- present on BOTH legs: the key that joins them
    name_of_user         String,   -- blank on many SYNCED rows; CREATED is authoritative

    records              UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, role, src, level_two_code, level_three_code, user_name)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS analytics.dm_cdd_sync_hourly
(
    tenant_id            LowCardinality(String),
    campaign_number      LowCardinality(String),
    hierarchy_type       LowCardinality(String),

    level_one_code       LowCardinality(String),
    level_two_code       LowCardinality(String),
    level_three_code     LowCardinality(String),
    level_four_code      LowCardinality(String),
    level_five_code      LowCardinality(String),
    level_six_code       LowCardinality(String),
    level_seven_code     LowCardinality(String),
    level_eight_code     LowCardinality(String),
    level_nine_code      LowCardinality(String),

    role                 LowCardinality(String),
    synced_hour          DateTime,
    user_name            String,
    records              UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, role, synced_hour, hierarchy_type,
          level_one_code, level_two_code, level_three_code, user_name)
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
--
-- PRESENCE IS AN EXIT LOG -- presence is inferred from clocking OUT, which is
-- the source's convention.
--
-- Individual grain because every consumer wants distinct people present, and
-- keeping the individual in the grain makes that count exact.
--
-- A per-day average over only the days that HAD logs flatters a boundary that
-- reported rarely; that denominator choice is left to the consumer.
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
--
-- member_count is carried raw rather than pre-filtered, so the
-- oversized-household threshold stays a read-time predicate and the same rows
-- serve as the registered-household denominator.
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
--
-- TWO SOURCES, TWO COUNTING RULES, kept as separate measures because they are
-- not the same unit: field referrals are counted as DISTINCT beneficiaries,
-- health-facility referrals as RECORDS. Dividing the second by the first can
-- therefore exceed 100% when one child attends twice. They are stored
-- unreconciled so that asymmetry stays visible to whoever writes the ratio.
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

    -- Aggregate STATE, not a number: a raw SELECT shows a binary blob, which is
    -- expected. Roll it up with uniqExactMerge() -- correct at any level,
    -- because a distinct count is not additive and summing per-cell counts
    -- double-counts anything appearing in two cells. The plain column beside it
    -- is the same measure for THIS ROW ONLY, so the table is readable without
    -- losing the correct path.
    referred_children_uniq      AggregateFunction(uniqExact, String),
    referred_children_count     UInt64,                 -- distinct children in THIS row; do not SUM across rows
    hf_referral_records         UInt64,                 -- health-facility referral RECORDS, not distinct children

    INDEX idx_dm_ref_geo (level_two_code, level_three_code, level_four_code, level_five_code, level_six_code) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, campaign_number, event_date)
SETTINGS index_granularity = 8192;


-- 22. dm_suspected_fraud
-- Grain: one row per (tenant, campaign, boundary path, user, minute).
--
-- A THROUGHPUT HEURISTIC, not a fraud flag -- no source carries one. A user is
-- suspect if some wall-clock minute holds an implausible number of successful
-- administrations. This stores the offending (user, minute) buckets; counting
-- the users and counting the buckets are both derivable.
--
-- The threshold is a metric definition, not a constant of nature.
--
-- Buckets on the device clock rather than an ingest timestamp, so a burst is
-- measured against when the work was recorded -- the only clock that means
-- anything on a backfilled load.
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
