-- ==========================================================================
-- REFUSAL OVERVIEW KPI DATA MARTS - CLICKHOUSE IMPLEMENTATION
-- Architecture: Refreshable Materialized Views (ClickHouse 24.3+)
-- ==========================================================================

SET allow_experimental_refreshable_materialized_view = 1;

-- ==========================================================================
-- SECTION 1: FOUNDATIONAL BASE MARTS
-- ==========================================================================

-- 1. dm_beneficiary_status
CREATE MATERIALIZED VIEW dm_beneficiary_status_base
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (tenant_id, campaign_number, region_code, district_code, health_facility_code, settlement_code)
AS
WITH beneficiary_visits AS (
    SELECT
        pb.tenant_id,
        pb.campaign_number,
        pb.region_code,
        pb.district_code,
        pb.health_facility_code,
        pb.settlement_code,
        pb.beneficiary_id,
        max(if(t.administration_status = 'ADMINISTRATION_FAILED' AND JSONExtractString(t.additional_details, 'reason') = 'REFUSED', 1, 0)) AS is_refused,
        max(if((t.administration_status = 'ADMINISTRATION_FAILED' AND JSONExtractString(t.additional_details, 'reason') = 'ABSENCE') OR t.administration_status = 'CLOSED_HOUSEHOLD', 1, 0)) AS is_absent,
        sum(if(t.id IS NOT NULL, 1, 0)) AS total_visits,
        sum(if(t.administration_status = 'VISITED', 1, 0)) AS visited_count,
        sum(if(t.administration_status = 'ADMINISTRATION_SUCCESS', 1, 0)) AS success_count
    FROM project_beneficiary_enriched pb
    LEFT JOIN project_task_enriched t ON t.project_beneficiary_client_reference_id = pb.client_reference_id AND t.tenant_id = pb.tenant_id
    GROUP BY pb.tenant_id, pb.campaign_number, pb.region_code, pb.district_code, pb.health_facility_code, pb.settlement_code, pb.beneficiary_id
)
SELECT
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code,
    sum(is_refused) AS refused_beneficiaries,
    sum(is_absent) AS absent_beneficiaries,
    sum(if(total_visits > 1 AND visited_count = 0 AND success_count = 0, 1, 0)) AS multi_unsuccessful_beneficiaries,
    toUInt64(count(beneficiary_id)) AS total_beneficiaries
FROM beneficiary_visits
GROUP BY 
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code;


-- 2. dm_task_status
CREATE MATERIALIZED VIEW dm_task_status_base
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (tenant_id, campaign_number, region_code, district_code, health_facility_code, settlement_code)
AS
SELECT
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code,
    sum(if(administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED'), 1, 0)) AS failed_visit_count,
    
    -- total_revisit_records = failed_or_visited_count - unique_failed_beneficiaries
    sum(if(administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED', 'VISITED'), 1, 0)) 
    - count(DISTINCT if(administration_status IN ('CLOSED_HOUSEHOLD', 'ADMINISTRATION_FAILED'), project_beneficiary_client_reference_id, NULL)) AS total_revisit_records,
    
    sum(if(administration_status = 'VISITED', 1, 0)) AS revisit_successful_count,
    sum(if(administration_status = 'ADMINISTRATION_FAILED' AND JSONExtractString(additional_details, 'reason') = 'REFUSED', 1, 0)) AS refusal_count,
    toUInt64(count()) AS total_records
FROM project_task_enriched    
GROUP BY 
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code;


-- ==========================================================================
-- SECTION 2: BREAKDOWN MARTS
-- ==========================================================================

-- 3. dm_refusal_breakdown
CREATE MATERIALIZED VIEW dm_refusal_breakdown
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (tenant_id, campaign_number, region_code, district_code, health_facility_code, settlement_code, refusal_reason)
AS
SELECT
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code, 
    ifNull(nullIf(JSONExtractString(additional_details, 'refusalReason'), ''), 'Unknown') AS refusal_reason,
    toUInt64(count()) AS refusal_count
FROM project_task_enriched
WHERE administration_status = 'ADMINISTRATION_FAILED' AND JSONExtractString(additional_details, 'reason') = 'REFUSED'
GROUP BY 
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code,
    refusal_reason;


-- 4. dm_absence_breakdown
CREATE MATERIALIZED VIEW dm_absence_breakdown
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (tenant_id, campaign_number, region_code, district_code, health_facility_code, settlement_code, absence_category)
AS
SELECT
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code, 
    if(administration_status = 'CLOSED_HOUSEHOLD', 'CLOSED_HOUSEHOLD', ifNull(nullIf(JSONExtractString(additional_details, 'absenceReason'), ''), 'UNSPECIFIED')) AS absence_category,
    toUInt64(count()) AS absence_count
FROM project_task_enriched
WHERE (administration_status = 'ADMINISTRATION_FAILED' AND JSONExtractString(additional_details, 'reason') = 'ABSENCE') OR administration_status = 'CLOSED_HOUSEHOLD'
GROUP BY 
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code,
    absence_category;


-- 5. dm_settlement_refusal_rate
CREATE MATERIALIZED VIEW dm_settlement_refusal_rate
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (tenant_id, campaign_number, region_code, district_code, health_facility_code, settlement_code, settlement_type)
AS
SELECT
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code, 
    ifNull(nullIf(JSONExtractString(additional_details, 'settlementType'), ''), 'Unknown') AS settlement_type,
    sum(if(administration_status = 'ADMINISTRATION_FAILED' AND JSONExtractString(additional_details, 'reason') = 'REFUSED', 1, 0)) AS refusal_count,
    toUInt64(count()) AS total_records
FROM project_task_enriched
GROUP BY 
    tenant_id,
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code,
    settlement_type;
