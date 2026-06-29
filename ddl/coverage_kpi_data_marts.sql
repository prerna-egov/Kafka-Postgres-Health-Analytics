-- ==========================================================================
-- COVERAGE OVERVIEW KPI DATA MARTS
-- ==========================================================================
-- Generated: 2026-06-25
-- Total Materialized Views: 18
-- Source Tables: project_task_enriched, project_enriched, project_beneficiary_enriched
-- Join Key: project_task_enriched.project_id = project_enriched.id
-- Delivery Filter: administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
-- ==========================================================================


-- ==========================================================================
-- SECTION 0: SOURCE TABLE INDEXES
-- ==========================================================================
-- These indexes optimize the materialized view refresh queries.
-- Create these ONCE on the source tables.
-- ==========================================================================

-- project_task_enriched indexes
CREATE INDEX IF NOT EXISTS idx_pte_admin_status
    ON project_task_enriched (administration_status);
CREATE INDEX IF NOT EXISTS idx_pte_campaign
    ON project_task_enriched (campaign_id);
CREATE INDEX IF NOT EXISTS idx_pte_admin_campaign
    ON project_task_enriched (administration_status, campaign_id);
CREATE INDEX IF NOT EXISTS idx_pte_project_id
    ON project_task_enriched (project_id);
CREATE INDEX IF NOT EXISTS idx_pte_country
    ON project_task_enriched (country_code);
CREATE INDEX IF NOT EXISTS idx_pte_province
    ON project_task_enriched (province_code);
CREATE INDEX IF NOT EXISTS idx_pte_district
    ON project_task_enriched (district_code);
CREATE INDEX IF NOT EXISTS idx_pte_hc
    ON project_task_enriched (health_center_code);
CREATE INDEX IF NOT EXISTS idx_pte_spp
    ON project_task_enriched (spp_code);
CREATE INDEX IF NOT EXISTS idx_pte_village
    ON project_task_enriched (village_code);
CREATE INDEX IF NOT EXISTS idx_pte_task_dates
    ON project_task_enriched (task_dates);
CREATE INDEX IF NOT EXISTS idx_pte_admin_task_dates
    ON project_task_enriched (administration_status, task_dates, campaign_id);

-- project_enriched indexes
CREATE INDEX IF NOT EXISTS idx_pe_campaign
    ON project_enriched (campaign_id);
CREATE INDEX IF NOT EXISTS idx_pe_village_campaign
    ON project_enriched (campaign_id) WHERE village_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pe_hc_campaign
    ON project_enriched (campaign_id, health_center_code) WHERE health_center_code IS NOT NULL;

-- project_beneficiary_enriched indexes
CREATE INDEX IF NOT EXISTS idx_pbe_campaign_hc
    ON project_beneficiary_enriched (campaign_id, health_center_code)
    WHERE health_center_code IS NOT NULL;


-- ==========================================================================
-- ==========================================================================
--
--  KPI 1: TOTAL CHILDREN VACCINATED
--
--  Business Definition:
--    COUNT(*) of successful delivery records
--    WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
--    GROUP BY campaign_id, boundary_hierarchy_code
--
--  Visualization: KPI Card
--  Type: Non-derived
--  Marts: 6 (one per hierarchy level)
--
-- ==========================================================================
-- ==========================================================================

-- --------------------------------------------------------------------------
-- KPI 1 — Country Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi1_country AS
SELECT
    country_code,
    campaign_id,
    COUNT(*) AS successful_delivery_count
FROM project_task_enriched
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
GROUP BY country_code, campaign_id;

-- --------------------------------------------------------------------------
-- KPI 1 — Province Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi1_province AS
SELECT
    country_code,
    province_code,
    campaign_id,
    COUNT(*) AS successful_delivery_count
FROM project_task_enriched
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
GROUP BY country_code, province_code, campaign_id;

-- --------------------------------------------------------------------------
-- KPI 1 — District Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi1_district AS
SELECT
    country_code,
    province_code,
    district_code,
    campaign_id,
    COUNT(*) AS successful_delivery_count
FROM project_task_enriched
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
GROUP BY country_code, province_code, district_code, campaign_id;

-- --------------------------------------------------------------------------
-- KPI 1 — Health Center Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi1_health_center AS
SELECT
    country_code,
    province_code,
    district_code,
    health_center_code,
    campaign_id,
    COUNT(*) AS successful_delivery_count
FROM project_task_enriched
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
GROUP BY country_code, province_code, district_code, health_center_code, campaign_id;

-- --------------------------------------------------------------------------
-- KPI 1 — SPP Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi1_spp AS
SELECT
    country_code,
    province_code,
    district_code,
    health_center_code,
    spp_code,
    campaign_id,
    COUNT(*) AS successful_delivery_count
FROM project_task_enriched
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
GROUP BY country_code, province_code, district_code, health_center_code, spp_code, campaign_id;

-- --------------------------------------------------------------------------
-- KPI 1 — Village Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi1_village AS
SELECT
    country_code,
    province_code,
    district_code,
    health_center_code,
    spp_code,
    village_code,
    campaign_id,
    COUNT(*) AS successful_delivery_count
FROM project_task_enriched
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
GROUP BY country_code, province_code, district_code, health_center_code, spp_code, village_code, campaign_id;

-- --------------------------------------------------------------------------
-- KPI 1 — Indexes
-- --------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_kpi1_country_pk  ON dm_kpi1_country  (country_code, campaign_id);
CREATE UNIQUE INDEX idx_kpi1_province_pk ON dm_kpi1_province (country_code, province_code, campaign_id);
CREATE UNIQUE INDEX idx_kpi1_district_pk ON dm_kpi1_district (country_code, province_code, district_code, campaign_id);
CREATE UNIQUE INDEX idx_kpi1_hc_pk       ON dm_kpi1_health_center (country_code, province_code, district_code, health_center_code, campaign_id);
CREATE UNIQUE INDEX idx_kpi1_spp_pk      ON dm_kpi1_spp (country_code, province_code, district_code, health_center_code, spp_code, campaign_id);
CREATE UNIQUE INDEX idx_kpi1_village_pk  ON dm_kpi1_village (country_code, province_code, district_code, health_center_code, spp_code, village_code, campaign_id);

-- Drill-down filter indexes
CREATE INDEX idx_kpi1_province_drill ON dm_kpi1_province (country_code, campaign_id);
CREATE INDEX idx_kpi1_district_drill ON dm_kpi1_district (province_code, campaign_id);
CREATE INDEX idx_kpi1_hc_drill       ON dm_kpi1_health_center (district_code, campaign_id);
CREATE INDEX idx_kpi1_spp_drill      ON dm_kpi1_spp (health_center_code, campaign_id);
CREATE INDEX idx_kpi1_village_drill   ON dm_kpi1_village (spp_code, campaign_id);


-- ==========================================================================
-- ==========================================================================
--
--  KPI 2: OVERALL COVERAGE RATE
--
--  Business Definition:
--    Coverage % = SUM(successful_deliveries) / SUM(project_target) × 100
--    Applicable only to Village-Level Projects (village_code IS NOT NULL)
--
--  Join Strategy:
--    project_task_enriched.project_id = project_enriched.id
--    AND project_task_enriched.village_code = project_enriched.village_code
--    (Each project operates on a specific village)
--
--  Visualization: Gauge + KPI Card
--  Type: Derived
--  Marts: 1 (campaign-level)
--
-- ==========================================================================
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_kpi2_coverage AS
WITH campaign_targets AS (
    -- Sum targets per campaign (Village-level projects only)
    SELECT
        campaign_id,
        SUM(overall_target) AS target_population
    FROM project_enriched
    WHERE village_code IS NOT NULL
    GROUP BY campaign_id
),
campaign_deliveries AS (
    -- Count successful deliveries per campaign, ensuring village matches project
    SELECT
        pt.campaign_id,
        COUNT(*) AS successful_delivery_count
    FROM project_task_enriched pt
    INNER JOIN project_enriched pe
        ON pt.project_id = pe.id
       AND pt.village_code = pe.village_code
    WHERE pt.administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND pe.village_code IS NOT NULL
    GROUP BY pt.campaign_id
)
SELECT
    t.campaign_id,
    COALESCE(d.successful_delivery_count, 0) AS successful_delivery_count,
    t.target_population,
    ROUND(
        COALESCE(d.successful_delivery_count, 0)::NUMERIC
        / NULLIF(t.target_population, 0) * 100, 2
    ) AS coverage_percentage
FROM campaign_targets t
LEFT JOIN campaign_deliveries d
    ON t.campaign_id = d.campaign_id;

-- --------------------------------------------------------------------------
-- KPI 2 — Indexes
-- --------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_kpi2_pk ON dm_kpi2_coverage (campaign_id);


-- ==========================================================================
-- ==========================================================================
--
--  KPI 3: HEALTH FACILITY COVERAGE RATE
--
--  Business Definition:
--    Numerator:  Distinct health_center_code with ≥1 successful delivery
--    Denominator: Distinct health_center_code from project_beneficiary_enriched
--                 (health facilities where people have been enumerated)
--
--    HF Coverage % = Numerator / Denominator × 100
--
--  Visualization: KPI Card + Progress Bar
--  Type: Derived
--  Marts: 1 (campaign-level)
--
-- ==========================================================================
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_kpi3_hf_coverage AS
SELECT
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(
        COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC
        / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2
    ), 100.00) AS coverage_percentage
FROM (
    -- Denominator: Distinct health centers with enumerated people
    -- (from project_beneficiary_enriched)
    SELECT
        campaign_id,
        COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM project_beneficiary_enriched
    WHERE health_center_code IS NOT NULL
      AND is_deleted IS NOT TRUE
    GROUP BY campaign_id
) enum_hf
LEFT JOIN (
    -- Numerator: Distinct health centers with at least 1 successful delivery
    SELECT
        campaign_id,
        COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM project_task_enriched
    WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND health_center_code IS NOT NULL
    GROUP BY campaign_id
) succ_hf ON succ_hf.campaign_id = enum_hf.campaign_id;

-- --------------------------------------------------------------------------
-- KPI 3 — Indexes
-- --------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_kpi3_pk ON dm_kpi3_hf_coverage (campaign_id);


-- ==========================================================================
-- ==========================================================================
--
--  KPI 4: HEALTH FACILITY COVERAGE RATE BY HIERARCHY (Drillable KPI 3)
--
--  Same calculation as KPI 3 but drillable by boundary hierarchy.
--  Uses project_beneficiary_enriched for the denominator (enumerated HFs).
--
--  Visualization: Drillable progress bars
--  Type: Derived
--  Marts: 6 (one per hierarchy level)
--
-- ==========================================================================
-- ==========================================================================

-- --------------------------------------------------------------------------
-- KPI 4 — Country Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi4_country AS
SELECT
    enum_hf.country_code    AS boundary_code,
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(
        COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC
        / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2
    ), 100.00) AS coverage_percentage
FROM (
    SELECT
        country_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM project_beneficiary_enriched
    WHERE health_center_code IS NOT NULL
      AND is_deleted IS NOT TRUE
    GROUP BY country_code, campaign_id
) enum_hf
LEFT JOIN (
    SELECT
        country_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM project_task_enriched
    WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND health_center_code IS NOT NULL
    GROUP BY country_code, campaign_id
) succ_hf ON succ_hf.country_code = enum_hf.country_code
         AND succ_hf.campaign_id  = enum_hf.campaign_id;

-- --------------------------------------------------------------------------
-- KPI 4 — Province Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi4_province AS
SELECT
    enum_hf.country_code,
    enum_hf.province_code   AS boundary_code,
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(
        COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC
        / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2
    ), 100.00) AS coverage_percentage
FROM (
    SELECT
        country_code,
        province_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM project_beneficiary_enriched
    WHERE health_center_code IS NOT NULL
      AND is_deleted IS NOT TRUE
    GROUP BY country_code, province_code, campaign_id
) enum_hf
LEFT JOIN (
    SELECT
        country_code,
        province_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM project_task_enriched
    WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND health_center_code IS NOT NULL
    GROUP BY country_code, province_code, campaign_id
) succ_hf ON succ_hf.country_code  = enum_hf.country_code
         AND succ_hf.province_code = enum_hf.province_code
         AND succ_hf.campaign_id   = enum_hf.campaign_id;

-- --------------------------------------------------------------------------
-- KPI 4 — District Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi4_district AS
SELECT
    enum_hf.country_code,
    enum_hf.province_code,
    enum_hf.district_code   AS boundary_code,
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(
        COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC
        / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2
    ), 100.00) AS coverage_percentage
FROM (
    SELECT
        country_code,
        province_code,
        district_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM project_beneficiary_enriched
    WHERE health_center_code IS NOT NULL
      AND is_deleted IS NOT TRUE
    GROUP BY country_code, province_code, district_code, campaign_id
) enum_hf
LEFT JOIN (
    SELECT
        country_code,
        province_code,
        district_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM project_task_enriched
    WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND health_center_code IS NOT NULL
    GROUP BY country_code, province_code, district_code, campaign_id
) succ_hf ON succ_hf.country_code  = enum_hf.country_code
         AND succ_hf.province_code = enum_hf.province_code
         AND succ_hf.district_code = enum_hf.district_code
         AND succ_hf.campaign_id   = enum_hf.campaign_id;

-- --------------------------------------------------------------------------
-- KPI 4 — Health Center Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi4_health_center AS
SELECT
    enum_hf.country_code,
    enum_hf.province_code,
    enum_hf.district_code,
    enum_hf.health_center_code AS boundary_code,
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(
        COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC
        / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2
    ), 100.00) AS coverage_percentage
FROM (
    SELECT
        country_code,
        province_code,
        district_code,
        health_center_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM project_beneficiary_enriched
    WHERE health_center_code IS NOT NULL
      AND is_deleted IS NOT TRUE
    GROUP BY country_code, province_code, district_code, health_center_code, campaign_id
) enum_hf
LEFT JOIN (
    SELECT
        country_code,
        province_code,
        district_code,
        health_center_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM project_task_enriched
    WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND health_center_code IS NOT NULL
    GROUP BY country_code, province_code, district_code, health_center_code, campaign_id
) succ_hf ON succ_hf.country_code       = enum_hf.country_code
         AND succ_hf.province_code      = enum_hf.province_code
         AND succ_hf.district_code      = enum_hf.district_code
         AND succ_hf.health_center_code = enum_hf.health_center_code
         AND succ_hf.campaign_id        = enum_hf.campaign_id;

-- --------------------------------------------------------------------------
-- KPI 4 — SPP Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi4_spp AS
SELECT
    enum_hf.country_code,
    enum_hf.province_code,
    enum_hf.district_code,
    enum_hf.health_center_code,
    enum_hf.spp_code        AS boundary_code,
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(
        COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC
        / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2
    ), 100.00) AS coverage_percentage
FROM (
    SELECT
        country_code,
        province_code,
        district_code,
        health_center_code,
        spp_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM project_beneficiary_enriched
    WHERE health_center_code IS NOT NULL
      AND is_deleted IS NOT TRUE
    GROUP BY country_code, province_code, district_code, health_center_code, spp_code, campaign_id
) enum_hf
LEFT JOIN (
    SELECT
        country_code,
        province_code,
        district_code,
        health_center_code,
        spp_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM project_task_enriched
    WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND health_center_code IS NOT NULL
    GROUP BY country_code, province_code, district_code, health_center_code, spp_code, campaign_id
) succ_hf ON succ_hf.country_code       = enum_hf.country_code
         AND succ_hf.province_code      = enum_hf.province_code
         AND succ_hf.district_code      = enum_hf.district_code
         AND succ_hf.health_center_code = enum_hf.health_center_code
         AND succ_hf.spp_code           = enum_hf.spp_code
         AND succ_hf.campaign_id        = enum_hf.campaign_id;

-- --------------------------------------------------------------------------
-- KPI 4 — Village Level
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi4_village AS
SELECT
    enum_hf.country_code,
    enum_hf.province_code,
    enum_hf.district_code,
    enum_hf.health_center_code,
    enum_hf.spp_code,
    enum_hf.village_code    AS boundary_code,
    enum_hf.campaign_id,
    enum_hf.enumerated_health_facilities,
    COALESCE(succ_hf.successful_health_facilities, 0) AS successful_health_facilities,
    LEAST(ROUND(
        COALESCE(succ_hf.successful_health_facilities, 0)::NUMERIC
        / NULLIF(enum_hf.enumerated_health_facilities, 0) * 100, 2
    ), 100.00) AS coverage_percentage
FROM (
    SELECT
        country_code,
        province_code,
        district_code,
        health_center_code,
        spp_code,
        village_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS enumerated_health_facilities
    FROM project_beneficiary_enriched
    WHERE health_center_code IS NOT NULL
      AND is_deleted IS NOT TRUE
    GROUP BY country_code, province_code, district_code, health_center_code, spp_code, village_code, campaign_id
) enum_hf
LEFT JOIN (
    SELECT
        country_code,
        province_code,
        district_code,
        health_center_code,
        spp_code,
        village_code,
        campaign_id,
        COUNT(DISTINCT health_center_code) AS successful_health_facilities
    FROM project_task_enriched
    WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
      AND health_center_code IS NOT NULL
    GROUP BY country_code, province_code, district_code, health_center_code, spp_code, village_code, campaign_id
) succ_hf ON succ_hf.country_code       = enum_hf.country_code
         AND succ_hf.province_code      = enum_hf.province_code
         AND succ_hf.district_code      = enum_hf.district_code
         AND succ_hf.health_center_code = enum_hf.health_center_code
         AND succ_hf.spp_code           = enum_hf.spp_code
         AND succ_hf.village_code       = enum_hf.village_code
         AND succ_hf.campaign_id        = enum_hf.campaign_id;

-- --------------------------------------------------------------------------
-- KPI 4 — Indexes
-- --------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_kpi4_country_pk  ON dm_kpi4_country (boundary_code, campaign_id);
CREATE UNIQUE INDEX idx_kpi4_province_pk ON dm_kpi4_province (country_code, boundary_code, campaign_id);
CREATE UNIQUE INDEX idx_kpi4_district_pk ON dm_kpi4_district (country_code, province_code, boundary_code, campaign_id);
CREATE UNIQUE INDEX idx_kpi4_hc_pk       ON dm_kpi4_health_center (country_code, province_code, district_code, boundary_code, campaign_id);
CREATE UNIQUE INDEX idx_kpi4_spp_pk      ON dm_kpi4_spp (country_code, province_code, district_code, health_center_code, boundary_code, campaign_id);
CREATE UNIQUE INDEX idx_kpi4_village_pk  ON dm_kpi4_village (country_code, province_code, district_code, health_center_code, spp_code, boundary_code, campaign_id);

-- Drill-down filter indexes
CREATE INDEX idx_kpi4_province_drill ON dm_kpi4_province (country_code, campaign_id);
CREATE INDEX idx_kpi4_district_drill ON dm_kpi4_district (province_code, campaign_id);
CREATE INDEX idx_kpi4_hc_drill       ON dm_kpi4_health_center (district_code, campaign_id);
CREATE INDEX idx_kpi4_spp_drill      ON dm_kpi4_spp (health_center_code, campaign_id);
CREATE INDEX idx_kpi4_village_drill   ON dm_kpi4_village (spp_code, campaign_id);


-- ==========================================================================
-- ==========================================================================
--
--  KPI 5: DAILY COVERAGE RATE
--
--  Business Definition:
--    How many children were vaccinated today?
--    COUNT of delivery records where event_date = today.
--    KPI card with delta from yesterday.
--
--  Date Field: task_dates (VARCHAR parsed as DATE)
--
--  Visualization: KPI Card with delta from yesterday
--  Type: Non-derived
--  Marts: 2 (daily snapshot + latest with delta)
--
-- ==========================================================================
-- ==========================================================================

-- --------------------------------------------------------------------------
-- KPI 5 — Daily Snapshot Mart
-- Grain: One row per campaign_id × event_date
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi5_daily_coverage AS
SELECT
    campaign_id,
    TO_DATE(task_dates, 'YYYY-MM-DD') AS event_date,
    COUNT(*) AS daily_delivery_count
FROM project_task_enriched
WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
  AND task_dates IS NOT NULL
GROUP BY campaign_id, TO_DATE(task_dates, 'YYYY-MM-DD');

-- --------------------------------------------------------------------------
-- KPI 5 — Latest Snapshot with Delta from Yesterday
-- Grain: One row per campaign_id (latest date only)
-- --------------------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_kpi5_latest AS
SELECT
    dc.campaign_id,
    dc.event_date                                                   AS max_reporting_date,
    dc.daily_delivery_count                                         AS kpi_value,
    COALESCE(prev.daily_delivery_count, 0)                          AS previous_day_value,
    dc.daily_delivery_count - COALESCE(prev.daily_delivery_count, 0) AS delta_from_yesterday
FROM dm_kpi5_daily_coverage dc
LEFT JOIN dm_kpi5_daily_coverage prev
    ON prev.campaign_id = dc.campaign_id
   AND prev.event_date  = dc.event_date - INTERVAL '1 day'
WHERE dc.event_date = (
    SELECT MAX(sub.event_date)
    FROM dm_kpi5_daily_coverage sub
    WHERE sub.campaign_id = dc.campaign_id
);

-- --------------------------------------------------------------------------
-- KPI 5 — Indexes
-- --------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_kpi5_daily_pk    ON dm_kpi5_daily_coverage (campaign_id, event_date);
CREATE INDEX idx_kpi5_daily_campaign     ON dm_kpi5_daily_coverage (campaign_id);
CREATE UNIQUE INDEX idx_kpi5_latest_pk   ON dm_kpi5_latest (campaign_id);


-- ==========================================================================
-- ==========================================================================
--
--  KPI 6: CAMPAIGN COMPLETION FORECAST
--
--  Business Definition:
--    Is the campaign on track to meet its target by end date?
--    Projected Coverage = (current_coverage_rate / days_elapsed) × total_campaign_days
--
--  Dependencies: dm_kpi2_coverage (KPI 2)
--  Campaign Dates: project_enriched.start_date / end_date (BIGINT epoch millis)
--    All projects in a campaign share the same start/end dates.
--
--  Visualization: Trend line + forecast
--  Type: Derived
--  Marts: 1 (campaign-level)
--
-- ==========================================================================
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_kpi6_campaign_forecast AS

-- Step 1: Prepare the clean dimensions first (The CTE)
WITH campaign_dimensions AS (
    SELECT
        campaign_id,
        TO_TIMESTAMP(MIN(start_date) / 1000)::DATE AS start_date,
        TO_TIMESTAMP(MAX(end_date) / 1000)::DATE   AS end_date,

        -- Use provided campaign duration
        MAX(campaign_duration_in_days) AS total_days
    FROM project_enriched
    WHERE start_date IS NOT NULL
      AND end_date   IS NOT NULL
    GROUP BY campaign_id
)

-- Step 2: Fused data and fixed math
SELECT
    cov.campaign_id,
    cov.successful_delivery_count,
    cov.target_population,
    cov.coverage_percentage AS current_coverage_rate,

    cd.start_date AS campaign_start_date,
    cd.end_date   AS campaign_end_date,

    -- FIXED BUG: Calculate Elapsed Days, capped at the Total Days max
    LEAST(
            GREATEST((CURRENT_DATE - cd.start_date) + 1, 1),
            cd.total_days
    ) AS days_elapsed,

    cd.total_days AS total_campaign_days,

    -- Cleaned up Forecast Math
    ROUND(
            (cov.coverage_percentage
                / NULLIF(
                     LEAST(GREATEST((CURRENT_DATE - cd.start_date) + 1, 1), cd.total_days)
                 , 0)
                ) * cd.total_days
        , 2) AS projected_coverage,

    -- Cleaned up On-Track Flag
    CASE
        WHEN ROUND(
                     (cov.coverage_percentage
                         / NULLIF(
                              LEAST(GREATEST((CURRENT_DATE - cd.start_date) + 1, 1), cd.total_days)
                          , 0)
                         ) * cd.total_days
                 , 2) >= 100.0 THEN TRUE
        ELSE FALSE
        END AS on_track

FROM dm_kpi2_coverage cov
         JOIN campaign_dimensions cd
              ON cd.campaign_id = cov.campaign_id;
-- --------------------------------------------------------------------------
-- KPI 6 — Indexes
-- --------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_kpi6_pk ON dm_kpi6_campaign_forecast (campaign_id);


-- ==========================================================================
-- ==========================================================================
--
--  KPI 7: DISTRICT PERFORMANCE SUMMARY
--
--  Business Definition:
--    Coverage rate per district vs expected rate at current campaign day.
--    League table ranked by coverage %.
--
--  Formula:
--    actual_coverage   = delivery_count / target_population × 100
--    expected_coverage = (days_elapsed / total_campaign_days) × 100
--    variance          = actual_coverage - expected_coverage
--
--  Visualization: League table — ranked by coverage %
--  Type: Derived
--  Marts: 1 (district-level)
--
-- ==========================================================================
-- ==========================================================================

CREATE MATERIALIZED VIEW dm_kpi7_district_performance AS

WITH district_targets AS (
    -- Keep dimensional columns for the final UI output,
    -- but group at the unique district level
    SELECT
        country_code,
        province_code,
        district_code,
        campaign_id,
        SUM(overall_target) AS target_population,

        -- Clean dates once
        TO_TIMESTAMP(MIN(start_date) / 1000)::DATE AS start_date,
        TO_TIMESTAMP(MAX(end_date)   / 1000)::DATE AS end_date,

        -- Use provided campaign duration
        MAX(campaign_duration_in_days) AS total_days

    FROM project_enriched
    WHERE district_code IS NOT NULL
      AND start_date IS NOT NULL
      AND end_date IS NOT NULL
    GROUP BY country_code, province_code, district_code, campaign_id
),

district_deliveries AS (
    -- Stripped out country/province to make the aggregation lightning fast
    SELECT
        district_code,
        campaign_id,
        COUNT(*) AS delivery_count
    FROM project_task_enriched
    WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
    GROUP BY district_code, campaign_id
),

district_metrics AS (
    SELECT
        dt.country_code,
        dt.province_code,
        dt.district_code,
        dt.campaign_id,
        COALESCE(dd.delivery_count, 0) AS delivery_count,
        dt.target_population,

        -- Actual coverage %
        ROUND(
            COALESCE(dd.delivery_count, 0)::NUMERIC
            / NULLIF(dt.target_population, 0) * 100,
            2
        ) AS actual_coverage,

        -- Cleaned campaign time dimensions
        dt.start_date AS campaign_start_date,
        dt.end_date   AS campaign_end_date,
        dt.total_days AS total_campaign_days,

        -- FIXED: Days elapsed, capped at total days
        LEAST(
            GREATEST((CURRENT_DATE - dt.start_date) + 1, 1),
            dt.total_days
        ) AS days_elapsed,

        -- FIXED: Expected coverage %, strictly capped at 100%
        ROUND(
            LEAST(GREATEST((CURRENT_DATE - dt.start_date) + 1, 1), dt.total_days)::NUMERIC
            / NULLIF(dt.total_days, 0) * 100,
            2
        ) AS expected_coverage

    FROM district_targets dt

    -- Stripped out the heavy geographical join conditions
    LEFT JOIN district_deliveries dd
        ON dd.district_code = dt.district_code
       AND dd.campaign_id   = dt.campaign_id
)

SELECT
    dm.country_code,
    dm.province_code,
    dm.district_code,
    dm.campaign_id,
    dm.delivery_count,
    dm.target_population,
    dm.actual_coverage,
    dm.expected_coverage,

    -- Variance is now safe from infinite growth
    ROUND(dm.actual_coverage - dm.expected_coverage, 2) AS variance,

    dm.days_elapsed,
    dm.total_campaign_days,
    dm.campaign_start_date,
    dm.campaign_end_date,
    CURRENT_DATE AS snapshot_date,

    -- League Table Rank
    RANK() OVER (
        PARTITION BY dm.campaign_id
        ORDER BY dm.actual_coverage DESC NULLS LAST
    ) AS coverage_rank

FROM district_metrics dm;
-- --------------------------------------------------------------------------
-- KPI 7 — Indexes
-- --------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_kpi7_pk        ON dm_kpi7_district_performance (district_code, campaign_id);
CREATE INDEX idx_kpi7_campaign         ON dm_kpi7_district_performance (campaign_id);
CREATE INDEX idx_kpi7_province         ON dm_kpi7_district_performance (province_code, campaign_id);
CREATE INDEX idx_kpi7_rank             ON dm_kpi7_district_performance (campaign_id, coverage_rank);
CREATE INDEX idx_kpi7_variance         ON dm_kpi7_district_performance (campaign_id, variance DESC);


-- ==========================================================================
-- ==========================================================================
--
--  SECTION: KPI RETRIEVAL QUERIES
--
--  These are the optimized queries dashboards should use.
--  They read ONLY from data marts — never from source tables.
--
-- ==========================================================================
-- ==========================================================================


-- =========================================
-- KPI 1: Total Children Vaccinated
-- =========================================

-- National total for a campaign
SELECT campaign_id, successful_delivery_count
FROM dm_kpi1_country
WHERE country_code = :country_code
  AND campaign_id = :campaign_id;

-- Province drill-down
SELECT province_code, successful_delivery_count
FROM dm_kpi1_province
WHERE country_code = :country_code
  AND campaign_id = :campaign_id
ORDER BY successful_delivery_count DESC;

-- District drill-down (within a province)
SELECT district_code, successful_delivery_count
FROM dm_kpi1_district
WHERE province_code = :province_code
  AND campaign_id = :campaign_id
ORDER BY successful_delivery_count DESC;

-- Health center drill-down (within a district)
SELECT health_center_code, successful_delivery_count
FROM dm_kpi1_health_center
WHERE district_code = :district_code
  AND campaign_id = :campaign_id
ORDER BY successful_delivery_count DESC;

-- SPP drill-down (within a health center)
SELECT spp_code, successful_delivery_count
FROM dm_kpi1_spp
WHERE health_center_code = :health_center_code
  AND campaign_id = :campaign_id
ORDER BY successful_delivery_count DESC;

-- Village drill-down (within an SPP)
SELECT village_code, successful_delivery_count
FROM dm_kpi1_village
WHERE spp_code = :spp_code
  AND campaign_id = :campaign_id
ORDER BY successful_delivery_count DESC;


-- =========================================
-- KPI 2: Overall Coverage Rate
-- =========================================

-- KPI Card: Overall coverage for a campaign
SELECT
    campaign_id,
    successful_delivery_count,
    target_population,
    coverage_percentage
FROM dm_kpi2_coverage
WHERE campaign_id = :campaign_id;

-- All campaigns comparison
SELECT
    campaign_id,
    coverage_percentage,
    successful_delivery_count,
    target_population
FROM dm_kpi2_coverage
ORDER BY campaign_id;


-- =========================================
-- KPI 3: Health Facility Coverage Rate
-- =========================================

-- KPI Card + Progress Bar
SELECT
    campaign_id,
    enumerated_health_facilities,
    successful_health_facilities,
    coverage_percentage
FROM dm_kpi3_hf_coverage
WHERE campaign_id = :campaign_id;


-- =========================================
-- KPI 4: HF Coverage by Hierarchy
-- =========================================

-- Country level
SELECT
    boundary_code AS country_code,
    enumerated_health_facilities,
    successful_health_facilities,
    coverage_percentage
FROM dm_kpi4_country
WHERE campaign_id = :campaign_id;

-- Province drill-down
SELECT
    boundary_code AS province_code,
    enumerated_health_facilities,
    successful_health_facilities,
    coverage_percentage
FROM dm_kpi4_province
WHERE country_code = :country_code
  AND campaign_id = :campaign_id
ORDER BY coverage_percentage DESC;

-- District drill-down (within a province)
SELECT
    boundary_code AS district_code,
    enumerated_health_facilities,
    successful_health_facilities,
    coverage_percentage
FROM dm_kpi4_district
WHERE province_code = :province_code
  AND campaign_id = :campaign_id
ORDER BY coverage_percentage DESC;

-- Health center drill-down (within a district)
SELECT
    boundary_code AS health_center_code,
    enumerated_health_facilities,
    successful_health_facilities,
    coverage_percentage
FROM dm_kpi4_health_center
WHERE district_code = :district_code
  AND campaign_id = :campaign_id
ORDER BY coverage_percentage DESC;


-- =========================================
-- KPI 5: Daily Coverage Rate
-- =========================================

-- KPI Card: Today's count with delta from yesterday
SELECT
    campaign_id,
    max_reporting_date,
    kpi_value                AS todays_delivery_count,
    previous_day_value       AS yesterdays_delivery_count,
    delta_from_yesterday
FROM dm_kpi5_latest
WHERE campaign_id = :campaign_id;

-- Trend: Daily counts over last 7 days
SELECT
    event_date,
    daily_delivery_count
FROM dm_kpi5_daily_coverage
WHERE campaign_id = :campaign_id
  AND event_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY event_date;

-- Full daily trend (sparkline / chart)
SELECT
    event_date,
    daily_delivery_count
FROM dm_kpi5_daily_coverage
WHERE campaign_id = :campaign_id
ORDER BY event_date;


-- =========================================
-- KPI 6: Campaign Completion Forecast
-- =========================================

-- KPI Card: Forecast for a specific campaign
SELECT
    campaign_id,
    current_coverage_rate,
    days_elapsed,
    total_campaign_days,
    projected_coverage,
    on_track,
    campaign_start_date,
    campaign_end_date,
    total_campaign_days - days_elapsed AS days_remaining
FROM dm_kpi6_campaign_forecast
WHERE campaign_id = :campaign_id;

-- All campaigns forecast dashboard
SELECT
    campaign_id,
    current_coverage_rate,
    projected_coverage,
    on_track,
    total_campaign_days - days_elapsed AS days_remaining
FROM dm_kpi6_campaign_forecast
ORDER BY on_track, projected_coverage DESC;

-- Trend line data (cumulative coverage over time)
SELECT
    dc.event_date,
    SUM(dc.daily_delivery_count) OVER (
        ORDER BY dc.event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                               AS cumulative_deliveries,
    ROUND(
        SUM(dc.daily_delivery_count) OVER (
            ORDER BY dc.event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )::NUMERIC / NULLIF(fc.target_population, 0) * 100, 2
    )                                                               AS cumulative_coverage_pct
FROM dm_kpi5_daily_coverage dc
JOIN dm_kpi6_campaign_forecast fc
    ON fc.campaign_id = dc.campaign_id
WHERE dc.campaign_id = :campaign_id
ORDER BY dc.event_date;


-- =========================================
-- KPI 7: District Performance Summary
-- =========================================

-- League table: All districts ranked by coverage
SELECT
    district_code,
    country_code,
    province_code,
    delivery_count,
    target_population,
    actual_coverage,
    expected_coverage,
    variance,
    coverage_rank
FROM dm_kpi7_district_performance
WHERE campaign_id = :campaign_id
ORDER BY coverage_rank;

-- Top 5 performing districts
SELECT
    district_code,
    actual_coverage,
    expected_coverage,
    variance
FROM dm_kpi7_district_performance
WHERE campaign_id = :campaign_id
ORDER BY actual_coverage DESC
LIMIT 5;

-- Bottom 5 (lagging) districts
SELECT
    district_code,
    actual_coverage,
    expected_coverage,
    variance
FROM dm_kpi7_district_performance
WHERE campaign_id = :campaign_id
ORDER BY actual_coverage ASC
LIMIT 5;

-- Districts within a specific province
SELECT
    district_code,
    actual_coverage,
    expected_coverage,
    variance,
    coverage_rank
FROM dm_kpi7_district_performance
WHERE province_code = :province_code
  AND campaign_id = :campaign_id
ORDER BY coverage_rank;

-- Districts falling behind (negative variance)
SELECT
    district_code,
    province_code,
    actual_coverage,
    expected_coverage,
    variance
FROM dm_kpi7_district_performance
WHERE campaign_id = :campaign_id
  AND variance < 0
ORDER BY variance ASC;


-- ==========================================================================
-- ==========================================================================
--
--  SECTION: REFRESH STRATEGY
--
--  Dependency Order:
--    Step 1: All independent marts (KPI 1, 2, 3, 4, 5-daily, 7) — parallel
--    Step 2: Dependent marts (KPI 5-latest → KPI 5-daily, KPI 6 → KPI 2)
--
-- ==========================================================================
-- ==========================================================================

-- ============================================
-- STEP 1: Independent base marts (run in parallel)
-- ============================================

-- KPI 1: Total Children Vaccinated (6 hierarchy levels)
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi1_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi1_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi1_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi1_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi1_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi1_village;

-- KPI 2: Overall Coverage Rate
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi2_coverage;

-- KPI 3: Health Facility Coverage Rate
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi3_hf_coverage;

-- KPI 4: HF Coverage by Hierarchy (6 levels)
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi4_country;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi4_province;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi4_district;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi4_health_center;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi4_spp;
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi4_village;

-- KPI 5: Daily Coverage Rate (base snapshot)
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi5_daily_coverage;

-- KPI 7: District Performance Summary
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi7_district_performance;

-- ============================================
-- STEP 2: Dependent marts (run AFTER step 1)
-- ============================================

-- KPI 5: Latest snapshot with delta (depends on dm_kpi5_daily_coverage)
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi5_latest;

-- KPI 6: Campaign Forecast (depends on dm_kpi2_coverage)
REFRESH MATERIALIZED VIEW CONCURRENTLY dm_kpi6_campaign_forecast;


-- ==========================================================================
-- ==========================================================================
--
--  SECTION: INCREMENTAL REFRESH (KPI 5 — ALTERNATIVE)
--
--  For KPI 5, if using a regular table instead of materialized view,
--  this ON CONFLICT upsert pattern allows incremental daily updates.
--
-- ==========================================================================
-- ==========================================================================

-- Alternative: Use a regular table with upsert for incremental refresh
-- (Uncomment to use instead of materialized view)

-- CREATE TABLE dm_kpi5_daily_coverage_incr (
--     campaign_id         VARCHAR(128) NOT NULL,
--     event_date          DATE         NOT NULL,
--     daily_delivery_count BIGINT      NOT NULL DEFAULT 0,
--     PRIMARY KEY (campaign_id, event_date)
-- );
--
-- -- Incremental upsert: refresh only recent days
-- INSERT INTO dm_kpi5_daily_coverage_incr (campaign_id, event_date, daily_delivery_count)
-- SELECT
--     campaign_id,
--     TO_DATE(task_dates, 'YYYY-MM-DD') AS event_date,
--     COUNT(*) AS daily_delivery_count
-- FROM project_task_enriched
-- WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
--   AND task_dates IS NOT NULL
--   AND TO_DATE(task_dates, 'YYYY-MM-DD') >= CURRENT_DATE - INTERVAL '2 days'
-- GROUP BY campaign_id, TO_DATE(task_dates, 'YYYY-MM-DD')
-- ON CONFLICT (campaign_id, event_date)
-- DO UPDATE SET daily_delivery_count = EXCLUDED.daily_delivery_count;


-- ==========================================================================
-- ==========================================================================
--
--  SECTION: VERIFICATION QUERIES
--
--  Use these to validate mart data against source tables.
--
-- ==========================================================================
-- ==========================================================================

-- Verify KPI 1: Mart total should match direct source query
-- SELECT
--     (SELECT SUM(successful_delivery_count)
--      FROM dm_kpi1_country
--      WHERE campaign_id = :campaign_id)
--     =
--     (SELECT COUNT(*)
--      FROM project_task_enriched
--      WHERE administration_status IN ('ADMINISTRATION_SUCCESS', 'VISITED')
--        AND campaign_id = :campaign_id)
-- AS kpi1_matches;

-- Verify KPI 2: Coverage should be between 0 and a reasonable upper bound
-- SELECT campaign_id, coverage_percentage
-- FROM dm_kpi2_coverage
-- WHERE coverage_percentage < 0 OR coverage_percentage > 200;
-- Expected: 0 rows (may slightly exceed 100 if deliveries > target)

-- Verify KPI 3: Successful facilities should never exceed enumerated
-- SELECT campaign_id
-- FROM dm_kpi3_hf_coverage
-- WHERE successful_health_facilities > enumerated_health_facilities;
-- Expected: 0 rows

-- Verify KPI 4: Roll-up — sum across provinces should ≈ country total
-- SELECT
--     (SELECT SUM(successful_health_facilities)
--      FROM dm_kpi4_province
--      WHERE campaign_id = :campaign_id)
--     AS province_sum,
--     (SELECT successful_health_facilities
--      FROM dm_kpi4_country
--      WHERE campaign_id = :campaign_id)
--     AS country_total;
-- Note: These may differ due to COUNT DISTINCT across boundaries

-- Verify KPI 5: Sum of daily counts should match KPI 1 national total
-- SELECT
--     (SELECT SUM(daily_delivery_count)
--      FROM dm_kpi5_daily_coverage
--      WHERE campaign_id = :campaign_id)
--     =
--     (SELECT successful_delivery_count
--      FROM dm_kpi1_country
--      WHERE campaign_id = :campaign_id
--        AND country_code = :country_code)
-- AS kpi5_matches;

-- Verify KPI 7: All districts should have non-null coverage_rank
-- SELECT district_code, campaign_id
-- FROM dm_kpi7_district_performance
-- WHERE coverage_rank IS NULL;
-- Expected: 0 rows
