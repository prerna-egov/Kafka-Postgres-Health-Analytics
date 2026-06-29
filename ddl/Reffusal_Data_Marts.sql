-- ================================================================
-- VACCINATION CAMPAIGN KPI - MATERIALIZED VIEWS
-- ================================================================

-- ---------------------------------------------------------------
-- FOUNDATIONAL BASE MARTS
-- ---------------------------------------------------------------

CREATE MATERIALIZED VIEW dm_beneficiary_status_base AS
SELECT
    pb.campaign_id,
    pb.country_code,
    pb.province_code,
    pb.district_code,
    pb.health_center_code,
    pb.spp_code,
    pb.village_code,
    pb.beneficiary_id,
    MAX(CASE WHEN t.administration_status = 'ADMINISTRATION_FAILED' AND t.additional_details ->> 'reason' = 'REFUSED' THEN 1 ELSE 0 END) AS is_refused,
    MAX(CASE WHEN (t.administration_status = 'ADMINISTRATION_FAILED' AND t.additional_details ->> 'reason' = 'ABSENCE') OR t.administration_status = 'CLOSED_HOUSEHOLD' THEN 1 ELSE 0 END) AS is_absent
FROM project_beneficiary_enriched pb
         JOIN project_task_enriched t ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY pb.campaign_id, pb.country_code, pb.province_code, pb.district_code, pb.health_center_code, pb.spp_code, pb.village_code, pb.beneficiary_id;

CREATE UNIQUE INDEX idx_dm_beneficiary_status_base ON dm_beneficiary_status_base (beneficiary_id);

CREATE MATERIALIZED VIEW dm_task_status_base AS
SELECT
    t.campaign_id, t.country_code, t.province_code, t.district_code, t.health_center_code, t.spp_code, t.village_code,
    COUNT(*) FILTER (WHERE t.administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS failed_visit_count,
    COUNT(*) FILTER (WHERE t.administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED', 'VISITED')) - COUNT(DISTINCT pb.beneficiary_id) FILTER (WHERE t.administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS total_revisit_records,
    COUNT(*) FILTER (WHERE t.administration_status = 'VISITED') AS visited_count,
    COUNT(*) FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED' AND t.additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) AS total_records
FROM project_task_enriched t
         LEFT JOIN project_beneficiary_enriched pb ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.country_code, t.province_code, t.district_code, t.health_center_code, t.spp_code, t.village_code;

CREATE UNIQUE INDEX idx_dm_task_status_base ON dm_task_status_base (campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code);

CREATE MATERIALIZED VIEW dm_task_breakdown_base AS
SELECT
    campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code,
    COALESCE(additional_details ->> 'refusalReason', 'Unknown') AS refusal_reason,
    CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD' ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END AS absence_category,
    COALESCE(additional_details ->> 'settlementType', 'Unknown') AS settlement_type,
    COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED' AND additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) FILTER (WHERE additional_details ->> 'reason' = 'ABSENCE' OR administration_status = 'CLOSED_HOUSEHOLD') AS absence_count,
    COUNT(*) AS total_records
FROM project_task_enriched
GROUP BY campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code,
         COALESCE(additional_details ->> 'refusalReason', 'Unknown'),
         CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD' ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END,
         COALESCE(additional_details ->> 'settlementType', 'Unknown');

CREATE UNIQUE INDEX idx_dm_task_breakdown_base ON dm_task_breakdown_base (campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code, refusal_reason, absence_category, settlement_type);

CREATE MATERIALIZED VIEW dm_multi_unsuccessful_base AS
SELECT pb.beneficiary_id
FROM project_task_enriched t
         JOIN project_beneficiary_enriched pb ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY pb.beneficiary_id
HAVING COUNT(*) > 1
   AND COUNT(*) FILTER (WHERE t.administration_status = 'VISITED') = 0
   AND COUNT(*) FILTER (WHERE t.administration_status = 'ADMINISTRATION_SUCCESS') = 0;

CREATE UNIQUE INDEX idx_dm_multi_unsuccessful_base ON dm_multi_unsuccessful_base (beneficiary_id);

-- ---------------------------------------------------------------
-- KPI 1: Failed Visit Count
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_failed_visits_country AS
SELECT campaign_id, country_code, SUM(failed_visit_count) AS failed_visit_count FROM dm_task_status_base GROUP BY campaign_id, country_code;
CREATE UNIQUE INDEX idx_dm_failed_visits_country ON dm_failed_visits_country (campaign_id, country_code);

CREATE MATERIALIZED VIEW dm_failed_visits_province AS
SELECT campaign_id, province_code, SUM(failed_visit_count) AS failed_visit_count FROM dm_task_status_base GROUP BY campaign_id, province_code;
CREATE UNIQUE INDEX idx_dm_failed_visits_province ON dm_failed_visits_province (campaign_id, province_code);

CREATE MATERIALIZED VIEW dm_failed_visits_district AS
SELECT campaign_id, district_code, SUM(failed_visit_count) AS failed_visit_count FROM dm_task_status_base GROUP BY campaign_id, district_code;
CREATE UNIQUE INDEX idx_dm_failed_visits_district ON dm_failed_visits_district (campaign_id, district_code);

CREATE MATERIALIZED VIEW dm_failed_visits_health_center AS
SELECT campaign_id, health_center_code, SUM(failed_visit_count) AS failed_visit_count FROM dm_task_status_base GROUP BY campaign_id, health_center_code;
CREATE UNIQUE INDEX idx_dm_failed_visits_health_center ON dm_failed_visits_health_center (campaign_id, health_center_code);

CREATE MATERIALIZED VIEW dm_failed_visits_spp AS
SELECT campaign_id, spp_code, SUM(failed_visit_count) AS failed_visit_count FROM dm_task_status_base GROUP BY campaign_id, spp_code;
CREATE UNIQUE INDEX idx_dm_failed_visits_spp ON dm_failed_visits_spp (campaign_id, spp_code);

CREATE MATERIALIZED VIEW dm_failed_visits_village AS
SELECT campaign_id, village_code, SUM(failed_visit_count) AS failed_visit_count FROM dm_task_status_base GROUP BY campaign_id, village_code;
CREATE UNIQUE INDEX idx_dm_failed_visits_village ON dm_failed_visits_village (campaign_id, village_code);

-- ---------------------------------------------------------------
-- KPI 2: Refusal Rate
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_refusal_rate_country AS
SELECT campaign_id, country_code, SUM(is_refused) AS refused_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_refused) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS refusal_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, country_code;
CREATE UNIQUE INDEX idx_dm_refusal_rate_country ON dm_refusal_rate_country (campaign_id, country_code);

CREATE MATERIALIZED VIEW dm_refusal_rate_province AS
SELECT campaign_id, province_code, SUM(is_refused) AS refused_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_refused) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS refusal_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, province_code;
CREATE UNIQUE INDEX idx_dm_refusal_rate_province ON dm_refusal_rate_province (campaign_id, province_code);

CREATE MATERIALIZED VIEW dm_refusal_rate_district AS
SELECT campaign_id, district_code, SUM(is_refused) AS refused_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_refused) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS refusal_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, district_code;
CREATE UNIQUE INDEX idx_dm_refusal_rate_district ON dm_refusal_rate_district (campaign_id, district_code);

CREATE MATERIALIZED VIEW dm_refusal_rate_health_center AS
SELECT campaign_id, health_center_code, SUM(is_refused) AS refused_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_refused) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS refusal_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, health_center_code;
CREATE UNIQUE INDEX idx_dm_refusal_rate_health_center ON dm_refusal_rate_health_center (campaign_id, health_center_code);

CREATE MATERIALIZED VIEW dm_refusal_rate_spp AS
SELECT campaign_id, spp_code, SUM(is_refused) AS refused_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_refused) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS refusal_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, spp_code;
CREATE UNIQUE INDEX idx_dm_refusal_rate_spp ON dm_refusal_rate_spp (campaign_id, spp_code);

CREATE MATERIALIZED VIEW dm_refusal_rate_village AS
SELECT campaign_id, village_code, SUM(is_refused) AS refused_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_refused) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS refusal_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, village_code;
CREATE UNIQUE INDEX idx_dm_refusal_rate_village ON dm_refusal_rate_village (campaign_id, village_code);

-- ---------------------------------------------------------------
-- KPI 3: Absence Rate
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_absence_rate_country AS
SELECT campaign_id, country_code, SUM(is_absent) AS absent_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_absent) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS absence_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, country_code;
CREATE UNIQUE INDEX idx_dm_absence_rate_country ON dm_absence_rate_country (campaign_id, country_code);

CREATE MATERIALIZED VIEW dm_absence_rate_province AS
SELECT campaign_id, province_code, SUM(is_absent) AS absent_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_absent) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS absence_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, province_code;
CREATE UNIQUE INDEX idx_dm_absence_rate_province ON dm_absence_rate_province (campaign_id, province_code);

CREATE MATERIALIZED VIEW dm_absence_rate_district AS
SELECT campaign_id, district_code, SUM(is_absent) AS absent_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_absent) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS absence_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, district_code;
CREATE UNIQUE INDEX idx_dm_absence_rate_district ON dm_absence_rate_district (campaign_id, district_code);

CREATE MATERIALIZED VIEW dm_absence_rate_health_center AS
SELECT campaign_id, health_center_code, SUM(is_absent) AS absent_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_absent) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS absence_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, health_center_code;
CREATE UNIQUE INDEX idx_dm_absence_rate_health_center ON dm_absence_rate_health_center (campaign_id, health_center_code);

CREATE MATERIALIZED VIEW dm_absence_rate_spp AS
SELECT campaign_id, spp_code, SUM(is_absent) AS absent_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_absent) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS absence_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, spp_code;
CREATE UNIQUE INDEX idx_dm_absence_rate_spp ON dm_absence_rate_spp (campaign_id, spp_code);

CREATE MATERIALIZED VIEW dm_absence_rate_village AS
SELECT campaign_id, village_code, SUM(is_absent) AS absent_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_absent) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS absence_rate_pct FROM dm_beneficiary_status_base GROUP BY campaign_id, village_code;
CREATE UNIQUE INDEX idx_dm_absence_rate_village ON dm_absence_rate_village (campaign_id, village_code);

-- ---------------------------------------------------------------
-- KPI 4: Refusal Breakdown
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_refusal_breakdown_country AS
SELECT campaign_id, country_code, refusal_reason, SUM(refusal_count) AS refusal_count
FROM dm_task_breakdown_base WHERE refusal_count > 0 GROUP BY campaign_id, country_code, refusal_reason;
CREATE UNIQUE INDEX idx_dm_refusal_breakdown_country ON dm_refusal_breakdown_country (campaign_id, country_code, refusal_reason);

CREATE MATERIALIZED VIEW dm_refusal_breakdown_province AS
SELECT campaign_id, province_code, refusal_reason, SUM(refusal_count) AS refusal_count
FROM dm_task_breakdown_base WHERE refusal_count > 0 GROUP BY campaign_id, province_code, refusal_reason;
CREATE UNIQUE INDEX idx_dm_refusal_breakdown_province ON dm_refusal_breakdown_province (campaign_id, province_code, refusal_reason);

CREATE MATERIALIZED VIEW dm_refusal_breakdown_district AS
SELECT campaign_id, district_code, refusal_reason, SUM(refusal_count) AS refusal_count
FROM dm_task_breakdown_base WHERE refusal_count > 0 GROUP BY campaign_id, district_code, refusal_reason;
CREATE UNIQUE INDEX idx_dm_refusal_breakdown_district ON dm_refusal_breakdown_district (campaign_id, district_code, refusal_reason);

CREATE MATERIALIZED VIEW dm_refusal_breakdown_health_center AS
SELECT campaign_id, health_center_code, refusal_reason, SUM(refusal_count) AS refusal_count
FROM dm_task_breakdown_base WHERE refusal_count > 0 GROUP BY campaign_id, health_center_code, refusal_reason;
CREATE UNIQUE INDEX idx_dm_refusal_breakdown_health_center ON dm_refusal_breakdown_health_center (campaign_id, health_center_code, refusal_reason);

CREATE MATERIALIZED VIEW dm_refusal_breakdown_spp AS
SELECT campaign_id, spp_code, refusal_reason, SUM(refusal_count) AS refusal_count
FROM dm_task_breakdown_base WHERE refusal_count > 0 GROUP BY campaign_id, spp_code, refusal_reason;
CREATE UNIQUE INDEX idx_dm_refusal_breakdown_spp ON dm_refusal_breakdown_spp (campaign_id, spp_code, refusal_reason);

CREATE MATERIALIZED VIEW dm_refusal_breakdown_village AS
SELECT campaign_id, village_code, refusal_reason, SUM(refusal_count) AS refusal_count
FROM dm_task_breakdown_base WHERE refusal_count > 0 GROUP BY campaign_id, village_code, refusal_reason;
CREATE UNIQUE INDEX idx_dm_refusal_breakdown_village ON dm_refusal_breakdown_village (campaign_id, village_code, refusal_reason);

-- ---------------------------------------------------------------
-- KPI 5: Absence Breakdown
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_absence_breakdown_country AS
SELECT campaign_id, country_code, absence_category, SUM(absence_count) AS absence_count
FROM dm_task_breakdown_base WHERE absence_count > 0 GROUP BY campaign_id, country_code, absence_category;
CREATE UNIQUE INDEX idx_dm_absence_breakdown_country ON dm_absence_breakdown_country (campaign_id, country_code, absence_category);

CREATE MATERIALIZED VIEW dm_absence_breakdown_province AS
SELECT campaign_id, province_code, absence_category, SUM(absence_count) AS absence_count
FROM dm_task_breakdown_base WHERE absence_count > 0 GROUP BY campaign_id, province_code, absence_category;
CREATE UNIQUE INDEX idx_dm_absence_breakdown_province ON dm_absence_breakdown_province (campaign_id, province_code, absence_category);

CREATE MATERIALIZED VIEW dm_absence_breakdown_district AS
SELECT campaign_id, district_code, absence_category, SUM(absence_count) AS absence_count
FROM dm_task_breakdown_base WHERE absence_count > 0 GROUP BY campaign_id, district_code, absence_category;
CREATE UNIQUE INDEX idx_dm_absence_breakdown_district ON dm_absence_breakdown_district (campaign_id, district_code, absence_category);

CREATE MATERIALIZED VIEW dm_absence_breakdown_health_center AS
SELECT campaign_id, health_center_code, absence_category, SUM(absence_count) AS absence_count
FROM dm_task_breakdown_base WHERE absence_count > 0 GROUP BY campaign_id, health_center_code, absence_category;
CREATE UNIQUE INDEX idx_dm_absence_breakdown_health_center ON dm_absence_breakdown_health_center (campaign_id, health_center_code, absence_category);

CREATE MATERIALIZED VIEW dm_absence_breakdown_spp AS
SELECT campaign_id, spp_code, absence_category, SUM(absence_count) AS absence_count
FROM dm_task_breakdown_base WHERE absence_count > 0 GROUP BY campaign_id, spp_code, absence_category;
CREATE UNIQUE INDEX idx_dm_absence_breakdown_spp ON dm_absence_breakdown_spp (campaign_id, spp_code, absence_category);

CREATE MATERIALIZED VIEW dm_absence_breakdown_village AS
SELECT campaign_id, village_code, absence_category, SUM(absence_count) AS absence_count
FROM dm_task_breakdown_base WHERE absence_count > 0 GROUP BY campaign_id, village_code, absence_category;
CREATE UNIQUE INDEX idx_dm_absence_breakdown_village ON dm_absence_breakdown_village (campaign_id, village_code, absence_category);

-- ---------------------------------------------------------------
-- KPI 6: Refusal Rate by District
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_refusal_rate_by_district AS
SELECT
    campaign_id,
    district_code,
    SUM(refusal_count) AS refusal_count,
    SUM(total_records) AS total_records
FROM dm_task_status_base
WHERE district_code IS NOT NULL
GROUP BY campaign_id, district_code;

CREATE UNIQUE INDEX idx_dm_refusal_rate_by_district ON dm_refusal_rate_by_district (campaign_id, district_code);

-- ---------------------------------------------------------------
-- KPI 7: Refusal Rate by Settlement Type
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_refusal_rate_settlement_country AS
SELECT
    campaign_id, country_code, settlement_type,
    SUM(refusal_count) AS refusal_count,
    SUM(total_records) AS total_records,
    ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct
FROM dm_task_breakdown_base
WHERE settlement_type IS NOT NULL
GROUP BY campaign_id, country_code, settlement_type;
CREATE UNIQUE INDEX idx_dm_refusal_rate_settlement_country ON dm_refusal_rate_settlement_country (campaign_id, country_code, settlement_type);

CREATE MATERIALIZED VIEW dm_refusal_rate_settlement_province AS
SELECT
    campaign_id, province_code, settlement_type,
    SUM(refusal_count) AS refusal_count,
    SUM(total_records) AS total_records,
    ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct
FROM dm_task_breakdown_base
WHERE settlement_type IS NOT NULL
GROUP BY campaign_id, province_code, settlement_type;
CREATE UNIQUE INDEX idx_dm_refusal_rate_settlement_province ON dm_refusal_rate_settlement_province (campaign_id, province_code, settlement_type);

CREATE MATERIALIZED VIEW dm_refusal_rate_settlement_district AS
SELECT
    campaign_id, district_code, settlement_type,
    SUM(refusal_count) AS refusal_count,
    SUM(total_records) AS total_records,
    ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct
FROM dm_task_breakdown_base
WHERE settlement_type IS NOT NULL
GROUP BY campaign_id, district_code, settlement_type;
CREATE UNIQUE INDEX idx_dm_refusal_rate_settlement_district ON dm_refusal_rate_settlement_district (campaign_id, district_code, settlement_type);

CREATE MATERIALIZED VIEW dm_refusal_rate_settlement_health_center AS
SELECT
    campaign_id, health_center_code, settlement_type,
    SUM(refusal_count) AS refusal_count,
    SUM(total_records) AS total_records,
    ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct
FROM dm_task_breakdown_base
WHERE settlement_type IS NOT NULL
GROUP BY campaign_id, health_center_code, settlement_type;
CREATE UNIQUE INDEX idx_dm_refusal_rate_settlement_health_center ON dm_refusal_rate_settlement_health_center (campaign_id, health_center_code, settlement_type);

CREATE MATERIALIZED VIEW dm_refusal_rate_settlement_spp AS
SELECT
    campaign_id, spp_code, settlement_type,
    SUM(refusal_count) AS refusal_count,
    SUM(total_records) AS total_records,
    ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct
FROM dm_task_breakdown_base
WHERE settlement_type IS NOT NULL
GROUP BY campaign_id, spp_code, settlement_type;
CREATE UNIQUE INDEX idx_dm_refusal_rate_settlement_spp ON dm_refusal_rate_settlement_spp (campaign_id, spp_code, settlement_type);

CREATE MATERIALIZED VIEW dm_refusal_rate_settlement_village AS
SELECT
    campaign_id, village_code, settlement_type,
    SUM(refusal_count) AS refusal_count,
    SUM(total_records) AS total_records,
    ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct
FROM dm_task_breakdown_base
WHERE settlement_type IS NOT NULL
GROUP BY campaign_id, village_code, settlement_type;
CREATE UNIQUE INDEX idx_dm_refusal_rate_settlement_village ON dm_refusal_rate_settlement_village (campaign_id, village_code, settlement_type);

-- ---------------------------------------------------------------
-- KPI 8: Revisit Success Rate
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_revisit_success_country AS
SELECT
    campaign_id, country_code,
    SUM(visited_count) AS visited_count,
    SUM(failed_visit_count) AS failed_total_count,
    SUM(total_revisit_records) AS total_revisit_records,
    ROUND(SUM(visited_count) * 100.0 / NULLIF(SUM(total_revisit_records), 0), 2) AS revisit_success_rate_pct
FROM dm_task_status_base
GROUP BY campaign_id, country_code;
CREATE UNIQUE INDEX idx_dm_revisit_success_country ON dm_revisit_success_country (campaign_id, country_code);

CREATE MATERIALIZED VIEW dm_revisit_success_province AS
SELECT
    campaign_id, province_code,
    SUM(visited_count) AS visited_count,
    SUM(failed_visit_count) AS failed_total_count,
    SUM(total_revisit_records) AS total_revisit_records,
    ROUND(SUM(visited_count) * 100.0 / NULLIF(SUM(total_revisit_records), 0), 2) AS revisit_success_rate_pct
FROM dm_task_status_base
GROUP BY campaign_id, province_code;
CREATE UNIQUE INDEX idx_dm_revisit_success_province ON dm_revisit_success_province (campaign_id, province_code);

CREATE MATERIALIZED VIEW dm_revisit_success_district AS
SELECT
    campaign_id, district_code,
    SUM(visited_count) AS visited_count,
    SUM(failed_visit_count) AS failed_total_count,
    SUM(total_revisit_records) AS total_revisit_records,
    ROUND(SUM(visited_count) * 100.0 / NULLIF(SUM(total_revisit_records), 0), 2) AS revisit_success_rate_pct
FROM dm_task_status_base
GROUP BY campaign_id, district_code;
CREATE UNIQUE INDEX idx_dm_revisit_success_district ON dm_revisit_success_district (campaign_id, district_code);

CREATE MATERIALIZED VIEW dm_revisit_success_health_center AS
SELECT
    campaign_id, health_center_code,
    SUM(visited_count) AS visited_count,
    SUM(failed_visit_count) AS failed_total_count,
    SUM(total_revisit_records) AS total_revisit_records,
    ROUND(SUM(visited_count) * 100.0 / NULLIF(SUM(total_revisit_records), 0), 2) AS revisit_success_rate_pct
FROM dm_task_status_base
GROUP BY campaign_id, health_center_code;
CREATE UNIQUE INDEX idx_dm_revisit_success_health_center ON dm_revisit_success_health_center (campaign_id, health_center_code);

CREATE MATERIALIZED VIEW dm_revisit_success_spp AS
SELECT
    campaign_id, spp_code,
    SUM(visited_count) AS visited_count,
    SUM(failed_visit_count) AS failed_total_count,
    SUM(total_revisit_records) AS total_revisit_records,
    ROUND(SUM(visited_count) * 100.0 / NULLIF(SUM(total_revisit_records), 0), 2) AS revisit_success_rate_pct
FROM dm_task_status_base
GROUP BY campaign_id, spp_code;
CREATE UNIQUE INDEX idx_dm_revisit_success_spp ON dm_revisit_success_spp (campaign_id, spp_code);

CREATE MATERIALIZED VIEW dm_revisit_success_village AS
SELECT
    campaign_id, village_code,
    SUM(visited_count) AS visited_count,
    SUM(failed_visit_count) AS failed_total_count,
    SUM(total_revisit_records) AS total_revisit_records,
    ROUND(SUM(visited_count) * 100.0 / NULLIF(SUM(total_revisit_records), 0), 2) AS revisit_success_rate_pct
FROM dm_task_status_base
GROUP BY campaign_id, village_code;
CREATE UNIQUE INDEX idx_dm_revisit_success_village ON dm_revisit_success_village (campaign_id, village_code);

-- ---------------------------------------------------------------
-- KPI 9: Multi-Unsuccessful Revisit Beneficiaries
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW dm_multi_unsuccessful_country AS
SELECT
    campaign_id, country_code,
    COUNT(DISTINCT beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_beneficiary_enriched
WHERE beneficiary_id IN (SELECT beneficiary_id FROM dm_multi_unsuccessful_base)
GROUP BY campaign_id, country_code;
CREATE UNIQUE INDEX idx_dm_multi_unsuccessful_country ON dm_multi_unsuccessful_country (campaign_id, country_code);

CREATE MATERIALIZED VIEW dm_multi_unsuccessful_province AS
SELECT
    campaign_id, province_code,
    COUNT(DISTINCT beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_beneficiary_enriched
WHERE beneficiary_id IN (SELECT beneficiary_id FROM dm_multi_unsuccessful_base)
GROUP BY campaign_id, province_code;
CREATE UNIQUE INDEX idx_dm_multi_unsuccessful_province ON dm_multi_unsuccessful_province (campaign_id, province_code);

CREATE MATERIALIZED VIEW dm_multi_unsuccessful_district AS
SELECT
    campaign_id, district_code,
    COUNT(DISTINCT beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_beneficiary_enriched
WHERE beneficiary_id IN (SELECT beneficiary_id FROM dm_multi_unsuccessful_base)
GROUP BY campaign_id, district_code;
CREATE UNIQUE INDEX idx_dm_multi_unsuccessful_district ON dm_multi_unsuccessful_district (campaign_id, district_code);

CREATE MATERIALIZED VIEW dm_multi_unsuccessful_health_center AS
SELECT
    campaign_id, health_center_code,
    COUNT(DISTINCT beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_beneficiary_enriched
WHERE beneficiary_id IN (SELECT beneficiary_id FROM dm_multi_unsuccessful_base)
GROUP BY campaign_id, health_center_code;
CREATE UNIQUE INDEX idx_dm_multi_unsuccessful_health_center ON dm_multi_unsuccessful_health_center (campaign_id, health_center_code);

CREATE MATERIALIZED VIEW dm_multi_unsuccessful_spp AS
SELECT
    campaign_id, spp_code,
    COUNT(DISTINCT beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_beneficiary_enriched
WHERE beneficiary_id IN (SELECT beneficiary_id FROM dm_multi_unsuccessful_base)
GROUP BY campaign_id, spp_code;
CREATE UNIQUE INDEX idx_dm_multi_unsuccessful_spp ON dm_multi_unsuccessful_spp (campaign_id, spp_code);

CREATE MATERIALIZED VIEW dm_multi_unsuccessful_village AS
SELECT
    campaign_id, village_code,
    COUNT(DISTINCT beneficiary_id) AS multi_unsuccessful_beneficiaries
FROM project_beneficiary_enriched
WHERE beneficiary_id IN (SELECT beneficiary_id FROM dm_multi_unsuccessful_base)
GROUP BY campaign_id, village_code;
CREATE UNIQUE INDEX idx_dm_multi_unsuccessful_village ON dm_multi_unsuccessful_village (campaign_id, village_code);

-- ---------------------------------------------------------------
-- OPTIMIZATION & INDEXES
-- ---------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_task_admin_status ON project_task_enriched (administration_status);
CREATE INDEX IF NOT EXISTS idx_task_reason ON project_task_enriched ((additional_details ->> 'reason'));
CREATE INDEX IF NOT EXISTS idx_task_refusal_reason ON project_task_enriched ((additional_details ->> 'refusalReason')) WHERE additional_details ->> 'reason' = 'REFUSED';
CREATE INDEX IF NOT EXISTS idx_task_absence_reason ON project_task_enriched ((additional_details ->> 'absenceReason')) WHERE additional_details ->> 'reason' = 'ABSENCE';
CREATE INDEX IF NOT EXISTS idx_task_settlement_type ON project_task_enriched ((additional_details ->> 'settlementType'));
CREATE INDEX IF NOT EXISTS idx_task_beneficiary_ref ON project_task_enriched (project_beneficiary_client_reference_id);
CREATE INDEX IF NOT EXISTS idx_beneficiary_client_ref ON project_beneficiary_enriched (client_reference_id);
CREATE INDEX IF NOT EXISTS idx_task_hierarchy ON project_task_enriched (campaign_id, country_code, province_code, district_code, health_center_code, spp_code, village_code);

-- ---------------------------------------------------------------
-- REFRESH STRATEGY (pg_cron)
-- ---------------------------------------------------------------
/*
SELECT cron.schedule('refresh_kpi_views', '*/30 * * * *', $$
    -- 1. Refresh Base Marts
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_beneficiary_status_base;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_task_status_base;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_task_breakdown_base;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_multi_unsuccessful_base;

    -- 2. Refresh Dependent Marts
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_failed_visits_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_failed_visits_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_failed_visits_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_failed_visits_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_failed_visits_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_failed_visits_village;

    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_village;

    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_rate_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_rate_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_rate_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_rate_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_rate_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_rate_village;

    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_breakdown_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_breakdown_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_breakdown_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_breakdown_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_breakdown_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_breakdown_village;

    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_breakdown_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_breakdown_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_breakdown_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_breakdown_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_breakdown_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_absence_breakdown_village;

    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_by_district;

    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_settlement_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_settlement_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_settlement_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_settlement_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_settlement_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_refusal_rate_settlement_village;

    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_revisit_success_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_revisit_success_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_revisit_success_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_revisit_success_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_revisit_success_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_revisit_success_village;

    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_multi_unsuccessful_country;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_multi_unsuccessful_province;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_multi_unsuccessful_district;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_multi_unsuccessful_health_center;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_multi_unsuccessful_spp;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_multi_unsuccessful_village;
$$);
*/
