-- VACCINATION CAMPAIGN KPI - MATERIALIZED VIEWS
-- ================================================================

-- ---------------------------------------------------------------
-- FOUNDATIONAL BASE MARTS
-- ---------------------------------------------------------------

CREATE MATERIALIZED VIEW dm_beneficiary_status_base AS -- For each beneficiary we check if he/she ever be refused or absent 
SELECT 
    pb.campaign_id,
    pb.country_code,
    pb.region_code,
    pb.district_code,
    pb.healthfacility_code,
    pb.settlement_code,
    pb.beneficiary_id,
    MAX(CASE WHEN t.administration_status = 'ADMINISTRATION_FAILED' AND t.additional_details ->> 'reason' = 'REFUSED' THEN 1 ELSE 0 END) AS is_refused, -- being used in the KPI 2 (Refusal Rate) 
    MAX(CASE WHEN (t.administration_status = 'ADMINISTRATION_FAILED' AND t.additional_details ->> 'reason' = 'ABSENCE') OR t.administration_status = 'CLOSED_HOUSEHOLD' THEN 1 ELSE 0 END) AS is_absent, -- being used in the KPI 3 (Absence )
    CASE 
        WHEN COUNT(*) > 1 
         AND COUNT(*) FILTER (WHERE t.administration_status = 'VISITED') = 0 
         AND COUNT(*) FILTER (WHERE t.administration_status = 'ADMINISTRATION_SUCCESS') = 0 
        THEN 1 ELSE 0 
    END AS is_multi_unsuccessful -- being used in the KPI 9 (Multi-Unsuccessful)
FROM project_beneficiary_enriched pb
         JOIN project_task_enriched t ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY pb.campaign_id, pb.country_code, pb.region_code, pb.district_code, pb.healthfacility_code, pb.settlement_code, pb.beneficiary_id;

CREATE UNIQUE INDEX idx_dm_beneficiary_status_base ON dm_beneficiary_status_base (
    campaign_id, country_code, region_code, district_code, healthfacility_code, settlement_code, beneficiary_id
);



-- Indexes to optimize dynamic geographic filtering for KPIs 2, 3, and 9
CREATE INDEX IF NOT EXISTS idx_ben_status_campaign_province ON dm_beneficiary_status_base (campaign_id, region_code); -- Used by the 2nd query in KPIs 2, 3, 9
CREATE INDEX IF NOT EXISTS idx_ben_status_campaign_district ON dm_beneficiary_status_base (campaign_id, district_code); -- Used by the 3rd query in KPIs 2, 3, 9
CREATE INDEX IF NOT EXISTS idx_ben_status_campaign_hc ON dm_beneficiary_status_base (campaign_id, healthfacility_code); -- Used by the 4th query in KPIs 2, 3, 9
CREATE INDEX IF NOT EXISTS idx_ben_status_campaign_village ON dm_beneficiary_status_base (campaign_id, settlement_code); -- Used if drilling down to settlement level

-- Note: The raw table indexes (idx_raw_ben_campaign_...) were deleted here because our recent optimization made KPI 9 use the base mart instead!
-- ================================================================


CREATE MATERIALIZED VIEW dm_task_status_base AS -- aggregation for different fields required by KPI's 
SELECT
    t.campaign_id, t.country_code, t.region_code, t.district_code, t.healthfacility_code, t.settlement_code,
    COUNT(*) FILTER (WHERE t.administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS failed_visit_count, -- being used in the KPI 1 (Failed Visit Count )
    COUNT(*) FILTER (WHERE t.administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED', 'VISITED')) - COUNT(DISTINCT pb.beneficiary_id) FILTER (WHERE t.administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS total_revisit_records, -- being used in the KPI 8 (Revisit Success Rate) 
    COUNT(*) FILTER (WHERE t.administration_status = 'VISITED') AS revisit_successful_count, -- being used in the KPI 8 (Revisit Success Rate) 
    COUNT(*) FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED' AND t.additional_details ->> 'reason' = 'REFUSED') AS refusal_count, -- being used in the refusal rate by district (KPI 6)
    COUNT(*) AS total_records
FROM project_task_enriched t    
         LEFT JOIN project_beneficiary_enriched pb ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_id, t.country_code, t.region_code, t.district_code, t.healthfacility_code, t.settlement_code;

CREATE UNIQUE INDEX idx_dm_task_status_base ON dm_task_status_base (campaign_id, country_code, region_code, district_code, healthfacility_code, settlement_code);
-- Indices to optimize dynamic geographic filtering with skipped hierarchy
CREATE INDEX IF NOT EXISTS idx_task_status_camp_province ON dm_task_status_base (campaign_id, region_code);
CREATE INDEX IF NOT EXISTS idx_task_status_camp_district ON dm_task_status_base (campaign_id, district_code);
CREATE INDEX IF NOT EXISTS idx_task_status_camp_hc ON dm_task_status_base (campaign_id, healthfacility_code);
-- Unique is used for concurrent refreshing
CREATE MATERIALIZED VIEW dm_task_breakdown_base AS
SELECT
    campaign_id, country_code, region_code, district_code, healthfacility_code, settlement_code, 
    COALESCE(additional_details ->> 'refusalReason', 'Unknown') AS refusal_reason,
    CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD' ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END AS absence_category,
    COALESCE(additional_details ->> 'settlementType', 'Unknown') AS settlement_type,
    COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED' AND additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) FILTER (WHERE additional_details ->> 'reason' = 'ABSENCE' OR administration_status = 'CLOSED_HOUSEHOLD') AS absence_count,
    COUNT(*) AS total_records
FROM project_task_enriched
GROUP BY campaign_id, country_code, region_code, district_code, healthfacility_code, settlement_code,
         COALESCE(additional_details ->> 'refusalReason', 'Unknown'), -- refusal reason breakdown (KPI 4)
         CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD' ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END, -- absence reason breakdown (KPI 5)

-- ---------------------------------------------------------------
-- REFRESH STRATEGY (pg_cron)
-- ---------------------------------------------------------------
/*
SELECT cron.schedule('refresh_kpi_views', '*/30 * * * *', $$
    -- 1. Refresh Base Marts
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_beneficiary_status_base;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_task_status_base;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dm_task_breakdown_base;
