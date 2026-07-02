-- VACCINATION CAMPAIGN KPI - MATERIALIZED VIEWS
-- ================================================================

-- ---------------------------------------------------------------
-- FOUNDATIONAL BASE MARTS
-- ---------------------------------------------------------------

CREATE MATERIALIZED VIEW dm_beneficiary_status_base AS -- For each beneficiary we check if he/she ever be refused or absent 
WITH ben_level AS (
    SELECT 
        pb.campaign_number, pb.region_code, pb.district_code, pb.healthfacility_code, pb.settlement_code, pb.beneficiary_id,
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
    GROUP BY pb.campaign_number, pb.region_code, pb.district_code, pb.healthfacility_code, pb.settlement_code, pb.beneficiary_id
)
SELECT 
    campaign_number, region_code, district_code, healthfacility_code, settlement_code,
    SUM(is_refused) AS refused_beneficiaries,
    SUM(is_absent) AS absent_beneficiaries,
    SUM(is_multi_unsuccessful) AS multi_unsuccessful_beneficiaries,
    COUNT(beneficiary_id) AS total_beneficiaries
FROM ben_level
GROUP BY campaign_number, region_code, district_code, healthfacility_code, settlement_code;

CREATE UNIQUE INDEX idx_dm_beneficiary_status_base ON dm_beneficiary_status_base (
    campaign_number, region_code, district_code, healthfacility_code, settlement_code
);



-- Indices removed based on audit report (redundant to UNIQUE INDEX)

-- Note: The raw table indexes (idx_raw_ben_campaign_...) were deleted here because our recent optimization made KPI 9 use the base mart instead!
-- ================================================================


CREATE MATERIALIZED VIEW dm_task_status_base AS -- aggregation for different fields required by KPI's 
SELECT
    t.campaign_number, t.region_code, t.district_code, t.healthfacility_code, t.settlement_code,
    COUNT(*) FILTER (WHERE t.administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS failed_visit_count, -- being used in the KPI 1 (Failed Visit Count )
    COUNT(*) FILTER (WHERE t.administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED', 'VISITED')) - COUNT(DISTINCT pb.beneficiary_id) FILTER (WHERE t.administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED')) AS total_revisit_records, -- being used in the KPI 8 (Revisit Success Rate) 
    COUNT(*) FILTER (WHERE t.administration_status = 'VISITED') AS revisit_successful_count, -- being used in the KPI 8 (Revisit Success Rate) 
    COUNT(*) FILTER (WHERE t.administration_status = 'ADMINISTRATION_FAILED' AND t.additional_details ->> 'reason' = 'REFUSED') AS refusal_count, -- being used in the refusal rate by district (KPI 6)
    COUNT(*) AS total_records
FROM project_task_enriched t    
         LEFT JOIN project_beneficiary_enriched pb ON t.project_beneficiary_client_reference_id = pb.client_reference_id
GROUP BY t.campaign_number, t.region_code, t.district_code, t.healthfacility_code, t.settlement_code;

CREATE UNIQUE INDEX idx_dm_task_status_base ON dm_task_status_base (campaign_number, region_code, district_code, healthfacility_code, settlement_code);
-- Indices removed based on audit report (redundant to UNIQUE INDEX)
-- Unique is used for concurrent refreshing
CREATE MATERIALIZED VIEW dm_refusal_breakdown AS
SELECT
    campaign_number, region_code, district_code, healthfacility_code, settlement_code, 
    COALESCE(additional_details ->> 'refusalReason', 'Unknown') AS refusal_reason,
    COUNT(*) AS refusal_count
FROM project_task_enriched
WHERE administration_status = 'ADMINISTRATION_FAILED' AND additional_details ->> 'reason' = 'REFUSED'
GROUP BY campaign_number, region_code, district_code, healthfacility_code, settlement_code,
         COALESCE(additional_details ->> 'refusalReason', 'Unknown');

CREATE UNIQUE INDEX idx_dm_refusal_breakdown ON dm_refusal_breakdown (campaign_number, region_code, district_code, healthfacility_code, settlement_code, refusal_reason);

CREATE MATERIALIZED VIEW dm_absence_breakdown AS
SELECT
    campaign_number, region_code, district_code, healthfacility_code, settlement_code, 
    CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD' ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END AS absence_category,
    COUNT(*) AS absence_count
FROM project_task_enriched
WHERE (administration_status = 'ADMINISTRATION_FAILED' AND additional_details ->> 'reason' = 'ABSENCE') OR administration_status = 'CLOSED_HOUSEHOLD'
GROUP BY campaign_number, region_code, district_code, healthfacility_code, settlement_code,
         CASE WHEN administration_status = 'CLOSED_HOUSEHOLD' THEN 'CLOSED_HOUSEHOLD' ELSE COALESCE(additional_details ->> 'absenceReason', 'UNSPECIFIED') END;

CREATE UNIQUE INDEX idx_dm_absence_breakdown ON dm_absence_breakdown (campaign_number, region_code, district_code, healthfacility_code, settlement_code, absence_category);

CREATE MATERIALIZED VIEW dm_settlement_refusal_rate AS
SELECT
    campaign_number, region_code, district_code, healthfacility_code, settlement_code, 
    COALESCE(additional_details ->> 'settlementType', 'Unknown') AS settlement_type,
    COUNT(*) FILTER (WHERE administration_status = 'ADMINISTRATION_FAILED' AND additional_details ->> 'reason' = 'REFUSED') AS refusal_count,
    COUNT(*) AS total_records
FROM project_task_enriched
GROUP BY campaign_number, region_code, district_code, healthfacility_code, settlement_code,
         COALESCE(additional_details ->> 'settlementType', 'Unknown');

CREATE UNIQUE INDEX idx_dm_settlement_refusal_rate ON dm_settlement_refusal_rate (campaign_number, region_code, district_code, healthfacility_code, settlement_code, settlement_type);




-- 1. Refresh Base Marts
--REFRESH MATERIALIZED VIEW CONCURRENTLY dm_beneficiary_status_base;
--REFRESH MATERIALIZED VIEW CONCURRENTLY dm_task_status_base;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dm_task_breakdown_base;
