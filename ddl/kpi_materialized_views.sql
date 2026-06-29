-- ================================================================
-- VACCINATION CAMPAIGN KPI - MATERIALIZED VIEWS
-- ================================================================

-- ---------------------------------------------------------------
-- KPI 1: Failed Visit Count
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_kpi1_country AS
SELECT campaign_id, country_code, COUNT(*) AS failed_visit_count
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')
GROUP BY campaign_id, country_code;

CREATE MATERIALIZED VIEW mv_kpi1_province AS
SELECT campaign_id, province_code, COUNT(*) AS failed_visit_count
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')
GROUP BY campaign_id, province_code;

CREATE MATERIALIZED VIEW mv_kpi1_district AS
SELECT campaign_id, district_code, COUNT(*) AS failed_visit_count
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')
GROUP BY campaign_id, district_code;

CREATE MATERIALIZED VIEW mv_kpi1_health_center AS
SELECT campaign_id, health_center_code, COUNT(*) AS failed_visit_count
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')
GROUP BY campaign_id, health_center_code;

CREATE MATERIALIZED VIEW mv_kpi1_spp AS
SELECT campaign_id, spp_code, COUNT(*) AS failed_visit_count
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')
GROUP BY campaign_id, spp_code;

CREATE MATERIALIZED VIEW mv_kpi1_village AS
SELECT campaign_id, village_code, COUNT(*) AS failed_visit_count
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')
GROUP BY campaign_id, village_code;

-- ---------------------------------------------------------------
-- KPI 2: Refusal Rate
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_kpi2_country AS
SELECT
    t.campaign_id,
    t.country_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                  AND t.additional_details ->> 'reason' = 'REFUSED') AS refused_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                      AND t.additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.country_code;

CREATE MATERIALIZED VIEW mv_kpi2_province AS
SELECT
    t.campaign_id,
    t.province_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                  AND t.additional_details ->> 'reason' = 'REFUSED') AS refused_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                      AND t.additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.province_code;

CREATE MATERIALIZED VIEW mv_kpi2_district AS
SELECT
    t.campaign_id,
    t.district_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                  AND t.additional_details ->> 'reason' = 'REFUSED') AS refused_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                      AND t.additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.district_code;

CREATE MATERIALIZED VIEW mv_kpi2_health_center AS
SELECT
    t.campaign_id,
    t.health_center_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                  AND t.additional_details ->> 'reason' = 'REFUSED') AS refused_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                      AND t.additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.health_center_code;

CREATE MATERIALIZED VIEW mv_kpi2_spp AS
SELECT
    t.campaign_id,
    t.spp_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                  AND t.additional_details ->> 'reason' = 'REFUSED') AS refused_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                      AND t.additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.spp_code;

CREATE MATERIALIZED VIEW mv_kpi2_village AS
SELECT
    t.campaign_id,
    t.village_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                  AND t.additional_details ->> 'reason' = 'REFUSED') AS refused_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED'
                      AND t.additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.village_code;

-- ---------------------------------------------------------------
-- KPI 3: Absence Rate
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_kpi3_country AS
SELECT
    t.campaign_id,
    t.country_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                       AND t.additional_details ->> 'reason' = 'ABSENCE')
                   OR t.administration_status = 'CLOSED_HOUSEHOLD') AS absent_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                           AND t.additional_details ->> 'reason' = 'ABSENCE')
                       OR t.administration_status = 'CLOSED_HOUSEHOLD') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS absence_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.country_code;

CREATE MATERIALIZED VIEW mv_kpi3_province AS
SELECT
    t.campaign_id,
    t.province_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                       AND t.additional_details ->> 'reason' = 'ABSENCE')
                   OR t.administration_status = 'CLOSED_HOUSEHOLD') AS absent_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                           AND t.additional_details ->> 'reason' = 'ABSENCE')
                       OR t.administration_status = 'CLOSED_HOUSEHOLD') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS absence_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.province_code;

CREATE MATERIALIZED VIEW mv_kpi3_district AS
SELECT
    t.campaign_id,
    t.district_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                       AND t.additional_details ->> 'reason' = 'ABSENCE')
                   OR t.administration_status = 'CLOSED_HOUSEHOLD') AS absent_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                           AND t.additional_details ->> 'reason' = 'ABSENCE')
                       OR t.administration_status = 'CLOSED_HOUSEHOLD') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS absence_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.district_code;

CREATE MATERIALIZED VIEW mv_kpi3_health_center AS
SELECT
    t.campaign_id,
    t.health_center_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                       AND t.additional_details ->> 'reason' = 'ABSENCE')
                   OR t.administration_status = 'CLOSED_HOUSEHOLD') AS absent_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                           AND t.additional_details ->> 'reason' = 'ABSENCE')
                       OR t.administration_status = 'CLOSED_HOUSEHOLD') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS absence_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.health_center_code;

CREATE MATERIALIZED VIEW mv_kpi3_spp AS
SELECT
    t.campaign_id,
    t.spp_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                       AND t.additional_details ->> 'reason' = 'ABSENCE')
                   OR t.administration_status = 'CLOSED_HOUSEHOLD') AS absent_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                           AND t.additional_details ->> 'reason' = 'ABSENCE')
                       OR t.administration_status = 'CLOSED_HOUSEHOLD') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS absence_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.spp_code;

CREATE MATERIALIZED VIEW mv_kpi3_village AS
SELECT
    t.campaign_id,
    t.village_code,
    COUNT(DISTINCT pb.beneficiary_id)
        FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                       AND t.additional_details ->> 'reason' = 'ABSENCE')
                   OR t.administration_status = 'CLOSED_HOUSEHOLD') AS absent_beneficiaries,
    COUNT(DISTINCT pb.beneficiary_id) AS total_beneficiaries,
    ROUND(
        COUNT(DISTINCT pb.beneficiary_id)
            FILTER (WHERE (t.administration_status = 'ADMINISTRATION_FAILED'
                           AND t.additional_details ->> 'reason' = 'ABSENCE')
                       OR t.administration_status = 'CLOSED_HOUSEHOLD') * 100.0
        / NULLIF(COUNT(DISTINCT pb.beneficiary_id), 0), 2
    ) AS absence_rate_pct
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.village_code;

-- ---------------------------------------------------------------
-- KPI 4: Refusal Breakdown
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_kpi4_country AS
SELECT
    campaign_id,
    country_code,
    COALESCE(additional_details ->> 'refusalReason', 'Unknown') AS refusal_reason,
    COUNT(*) AS refusal_count
FROM project_task_enriched
WHERE administration_status = 'ADMINISTRATION_FAILED'
  AND additional_details ->> 'reason' = 'REFUSED'
GROUP BY campaign_id, country_code, COALESCE(additional_details ->> 'refusalReason', 'Unknown');

CREATE MATERIALIZED VIEW mv_kpi4_province AS
SELECT
    campaign_id,
    province_code,
    COALESCE(additional_details ->> 'refusalReason', 'Unknown') AS refusal_reason,
    COUNT(*) AS refusal_count
FROM project_task_enriched
WHERE administration_status = 'ADMINISTRATION_FAILED'
  AND additional_details ->> 'reason' = 'REFUSED'
GROUP BY campaign_id, province_code, COALESCE(additional_details ->> 'refusalReason', 'Unknown');

CREATE MATERIALIZED VIEW mv_kpi4_district AS
SELECT
    campaign_id,
    district_code,
    COALESCE(additional_details ->> 'refusalReason', 'Unknown') AS refusal_reason,
    COUNT(*) AS refusal_count
FROM project_task_enriched
WHERE administration_status = 'ADMINISTRATION_FAILED'
  AND additional_details ->> 'reason' = 'REFUSED'
GROUP BY campaign_id, district_code, COALESCE(additional_details ->> 'refusalReason', 'Unknown');

CREATE MATERIALIZED VIEW mv_kpi4_health_center AS
SELECT
    campaign_id,
    health_center_code,
    COALESCE(additional_details ->> 'refusalReason', 'Unknown') AS refusal_reason,
    COUNT(*) AS refusal_count
FROM project_task_enriched
WHERE administration_status = 'ADMINISTRATION_FAILED'
  AND additional_details ->> 'reason' = 'REFUSED'
GROUP BY campaign_id, health_center_code, COALESCE(additional_details ->> 'refusalReason', 'Unknown');

CREATE MATERIALIZED VIEW mv_kpi4_spp AS
SELECT
    campaign_id,
    spp_code,
    COALESCE(additional_details ->> 'refusalReason', 'Unknown') AS refusal_reason,
    COUNT(*) AS refusal_count
FROM project_task_enriched
WHERE administration_status = 'ADMINISTRATION_FAILED'
  AND additional_details ->> 'reason' = 'REFUSED'
GROUP BY campaign_id, spp_code, COALESCE(additional_details ->> 'refusalReason', 'Unknown');

CREATE MATERIALIZED VIEW mv_kpi4_village AS
SELECT
    campaign_id,
    village_code,
    COALESCE(additional_details ->> 'refusalReason', 'Unknown') AS refusal_reason,
    COUNT(*) AS refusal_count
FROM project_task_enriched
WHERE administration_status = 'ADMINISTRATION_FAILED'
  AND additional_details ->> 'reason' = 'REFUSED'
GROUP BY campaign_id, village_code, COALESCE(additional_details ->> 'refusalReason', 'Unknown');

-- ---------------------------------------------------------------
-- KPI 5: Absence Breakdown
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_kpi5_country AS
SELECT
    campaign_id,
    country_code,
    CASE
        WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
        ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED')
    END AS absence_category,
    COUNT(*) AS absence_count
FROM project_task_enriched
WHERE additional_details ->> 'reason' = 'ABSENCE'
   OR administration_status = 'CLOSED_HOUSEHOLD'
GROUP BY campaign_id, country_code,
    CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
         ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END;

CREATE MATERIALIZED VIEW mv_kpi5_province AS
SELECT
    campaign_id,
    province_code,
    CASE
        WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
        ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED')
    END AS absence_category,
    COUNT(*) AS absence_count
FROM project_task_enriched
WHERE additional_details ->> 'reason' = 'ABSENCE'
   OR administration_status = 'CLOSED_HOUSEHOLD'
GROUP BY campaign_id, province_code,
    CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
         ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END;

CREATE MATERIALIZED VIEW mv_kpi5_district AS
SELECT
    campaign_id,
    district_code,
    CASE
        WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
        ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED')
    END AS absence_category,
    COUNT(*) AS absence_count
FROM project_task_enriched
WHERE additional_details ->> 'reason' = 'ABSENCE'
   OR administration_status = 'CLOSED_HOUSEHOLD'
GROUP BY campaign_id, district_code,
    CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
         ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END;

CREATE MATERIALIZED VIEW mv_kpi5_health_center AS
SELECT
    campaign_id,
    health_center_code,
    CASE
        WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
        ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED')
    END AS absence_category,
    COUNT(*) AS absence_count
FROM project_task_enriched
WHERE additional_details ->> 'reason' = 'ABSENCE'
   OR administration_status = 'CLOSED_HOUSEHOLD'
GROUP BY campaign_id, health_center_code,
    CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
         ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END;

CREATE MATERIALIZED VIEW mv_kpi5_spp AS
SELECT
    campaign_id,
    spp_code,
    CASE
        WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
        ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED')
    END AS absence_category,
    COUNT(*) AS absence_count
FROM project_task_enriched
WHERE additional_details ->> 'reason' = 'ABSENCE'
   OR administration_status = 'CLOSED_HOUSEHOLD'
GROUP BY campaign_id, spp_code,
    CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
         ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END;

CREATE MATERIALIZED VIEW mv_kpi5_village AS
SELECT
    campaign_id,
    village_code,
    CASE
        WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
        ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED')
    END AS absence_category,
    COUNT(*) AS absence_count
FROM project_task_enriched
WHERE additional_details ->> 'reason' = 'ABSENCE'
   OR administration_status = 'CLOSED_HOUSEHOLD'
GROUP BY campaign_id, village_code,
    CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD'
         ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END;

-- ---------------------------------------------------------------
-- KPI 6: Refusal Rate by District
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_kpi6_district AS
SELECT
    campaign_id,
    district_code,
    COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                       AND additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) AS total_records
FROM project_task_enriched
WHERE district_code IS NOT NULL
GROUP BY campaign_id, district_code;

-- ---------------------------------------------------------------
-- KPI 7: Refusal Rate by Settlement Type
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_kpi7_country AS
SELECT
    campaign_id,
    country_code,
    additional_details ->> 'settlementType' AS settlement_type,
    COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                       AND additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) AS total_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                           AND additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched
WHERE additional_details ->> 'settlementType' IS NOT NULL
GROUP BY campaign_id, country_code, additional_details ->> 'settlementType';

CREATE MATERIALIZED VIEW mv_kpi7_province AS
SELECT
    campaign_id,
    province_code,
    additional_details ->> 'settlementType' AS settlement_type,
    COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                       AND additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) AS total_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                           AND additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched
WHERE additional_details ->> 'settlementType' IS NOT NULL
GROUP BY campaign_id, province_code, additional_details ->> 'settlementType';

CREATE MATERIALIZED VIEW mv_kpi7_district AS
SELECT
    campaign_id,
    district_code,
    additional_details ->> 'settlementType' AS settlement_type,
    COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                       AND additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) AS total_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                           AND additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched
WHERE additional_details ->> 'settlementType' IS NOT NULL
GROUP BY campaign_id, district_code, additional_details ->> 'settlementType';

CREATE MATERIALIZED VIEW mv_kpi7_health_center AS
SELECT
    campaign_id,
    health_center_code,
    additional_details ->> 'settlementType' AS settlement_type,
    COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                       AND additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) AS total_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                           AND additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched
WHERE additional_details ->> 'settlementType' IS NOT NULL
GROUP BY campaign_id, health_center_code, additional_details ->> 'settlementType';

CREATE MATERIALIZED VIEW mv_kpi7_spp AS
SELECT
    campaign_id,
    spp_code,
    additional_details ->> 'settlementType' AS settlement_type,
    COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                       AND additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) AS total_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                           AND additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched
WHERE additional_details ->> 'settlementType' IS NOT NULL
GROUP BY campaign_id, spp_code, additional_details ->> 'settlementType';

CREATE MATERIALIZED VIEW mv_kpi7_village AS
SELECT
    campaign_id,
    village_code,
    additional_details ->> 'settlementType' AS settlement_type,
    COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                       AND additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) AS total_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED'
                           AND additional_details ->> 'reason' = 'REFUSED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS refusal_rate_pct
FROM project_task_enriched
WHERE additional_details ->> 'settlementType' IS NOT NULL
GROUP BY campaign_id, village_code, additional_details ->> 'settlementType';

-- ---------------------------------------------------------------
-- KPI 8: Revisit Success Rate
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_kpi8_country AS
SELECT
    campaign_id,
    country_code,
    COUNT(*) FILTER (WHERE administration_status = 'VISITED') AS visited_count,
    COUNT(*) FILTER (WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS failed_count,
    COUNT(*) AS total_revisit_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'VISITED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS revisit_success_rate_pct
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED', 'VISITED')
GROUP BY campaign_id, country_code;

CREATE MATERIALIZED VIEW mv_kpi8_province AS
SELECT
    campaign_id,
    province_code,
    COUNT(*) FILTER (WHERE administration_status = 'VISITED') AS visited_count,
    COUNT(*) FILTER (WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS failed_count,
    COUNT(*) AS total_revisit_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'VISITED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS revisit_success_rate_pct
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED', 'VISITED')
GROUP BY campaign_id, province_code;

CREATE MATERIALIZED VIEW mv_kpi8_district AS
SELECT
    campaign_id,
    district_code,
    COUNT(*) FILTER (WHERE administration_status = 'VISITED') AS visited_count,
    COUNT(*) FILTER (WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS failed_count,
    COUNT(*) AS total_revisit_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'VISITED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS revisit_success_rate_pct
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED', 'VISITED')
GROUP BY campaign_id, district_code;

CREATE MATERIALIZED VIEW mv_kpi8_health_center AS
SELECT
    campaign_id,
    health_center_code,
    COUNT(*) FILTER (WHERE administration_status = 'VISITED') AS visited_count,
    COUNT(*) FILTER (WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS failed_count,
    COUNT(*) AS total_revisit_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'VISITED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS revisit_success_rate_pct
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED', 'VISITED')
GROUP BY campaign_id, health_center_code;

CREATE MATERIALIZED VIEW mv_kpi8_spp AS
SELECT
    campaign_id,
    spp_code,
    COUNT(*) FILTER (WHERE administration_status = 'VISITED') AS visited_count,
    COUNT(*) FILTER (WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS failed_count,
    COUNT(*) AS total_revisit_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'VISITED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS revisit_success_rate_pct
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED', 'VISITED')
GROUP BY campaign_id, spp_code;

CREATE MATERIALIZED VIEW mv_kpi8_village AS
SELECT
    campaign_id,
    village_code,
    COUNT(*) FILTER (WHERE administration_status = 'VISITED') AS visited_count,
    COUNT(*) FILTER (WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS failed_count,
    COUNT(*) AS total_revisit_records,
    ROUND(
        COUNT(*) FILTER (WHERE administration_status = 'VISITED') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS revisit_success_rate_pct
FROM project_task_enriched
WHERE administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED', 'VISITED')
GROUP BY campaign_id, village_code;

-- ---------------------------------------------------------------
-- KPI 9: Multi-Unsuccessful Revisit Beneficiaries
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_kpi9_unsuccessful_base AS
SELECT pb.beneficiary_id
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY pb.beneficiary_id
HAVING COUNT(*) > 1
   AND COUNT(*) FILTER (WHERE t.administration_status = 'VISITED') = 0
   AND COUNT(*) FILTER (WHERE t.administration_status = 'ADMINISTRATION_SUCCESS') = 0;

CREATE MATERIALIZED VIEW mv_kpi9_country AS
SELECT
    t.campaign_id,
    t.country_code,
    COUNT(DISTINCT pb.beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
WHERE pb.beneficiary_id IN (SELECT beneficiary_id FROM mv_kpi9_unsuccessful_base)
GROUP BY t.campaign_id, t.country_code;

CREATE MATERIALIZED VIEW mv_kpi9_province AS
SELECT
    t.campaign_id,
    t.province_code,
    COUNT(DISTINCT pb.beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
WHERE pb.beneficiary_id IN (SELECT beneficiary_id FROM mv_kpi9_unsuccessful_base)
GROUP BY t.campaign_id, t.province_code;

CREATE MATERIALIZED VIEW mv_kpi9_district AS
SELECT
    t.campaign_id,
    t.district_code,
    COUNT(DISTINCT pb.beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
WHERE pb.beneficiary_id IN (SELECT beneficiary_id FROM mv_kpi9_unsuccessful_base)
GROUP BY t.campaign_id, t.district_code;

CREATE MATERIALIZED VIEW mv_kpi9_health_center AS
SELECT
    t.campaign_id,
    t.health_center_code,
    COUNT(DISTINCT pb.beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
WHERE pb.beneficiary_id IN (SELECT beneficiary_id FROM mv_kpi9_unsuccessful_base)
GROUP BY t.campaign_id, t.health_center_code;

CREATE MATERIALIZED VIEW mv_kpi9_spp AS
SELECT
    t.campaign_id,
    t.spp_code,
    COUNT(DISTINCT pb.beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
WHERE pb.beneficiary_id IN (SELECT beneficiary_id FROM mv_kpi9_unsuccessful_base)
GROUP BY t.campaign_id, t.spp_code;

CREATE MATERIALIZED VIEW mv_kpi9_village AS
SELECT
    t.campaign_id,
    t.village_code,
    COUNT(DISTINCT pb.beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_task_enriched t
JOIN project_beneficiary_enriched pb
  ON t.project_beneficiary_client_reference_id = pb.client_reference_id
WHERE pb.beneficiary_id IN (SELECT beneficiary_id FROM mv_kpi9_unsuccessful_base)
GROUP BY t.campaign_id, t.village_code;

-- ---------------------------------------------------------------
-- OPTIMIZATION & INDEXES
-- ---------------------------------------------------------------
CREATE INDEX idx_task_admin_status ON project_task_enriched (administration_status);
CREATE INDEX idx_task_reason ON project_task_enriched ((additional_details ->> 'reason'));
CREATE INDEX idx_task_refusal_reason ON project_task_enriched ((additional_details ->> 'refusalReason')) WHERE additional_details ->> 'reason' = 'REFUSED';
CREATE INDEX idx_task_absence_reason ON project_task_enriched ((additional_details ->> 'absenceReason')) WHERE additional_details ->> 'reason' = 'ABSENCE';
CREATE INDEX idx_task_settlement_type ON project_task_enriched ((additional_details ->> 'settlementType'));
CREATE INDEX idx_task_beneficiary_ref ON project_task_enriched (project_beneficiary_client_reference_id);
CREATE INDEX idx_beneficiary_client_ref ON project_beneficiary_enriched (client_reference_id);
CREATE INDEX idx_task_hierarchy ON project_task_enriched (campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code);

-- ---------------------------------------------------------------
-- REFRESH STRATEGY (pg_cron)
-- ---------------------------------------------------------------
/*
SELECT cron.schedule('refresh_kpi_views', '*/30 * * * *', $$
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi1_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi1_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi1_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi1_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi1_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi1_village;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi2_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi2_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi2_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi2_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi2_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi2_village;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi3_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi3_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi3_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi3_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi3_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi3_village;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi4_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi4_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi4_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi4_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi4_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi4_village;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi5_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi5_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi5_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi5_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi5_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi5_village;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi6_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi7_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi7_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi7_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi7_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi7_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi7_village;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi8_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi8_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi8_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi8_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi8_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi8_village;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi9_unsuccessful_base;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi9_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi9_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi9_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi9_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi9_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi9_village;
$$);
*/
