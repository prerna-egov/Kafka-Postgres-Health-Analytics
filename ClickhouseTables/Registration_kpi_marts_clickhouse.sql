-- ==========================================================================
-- REGISTRATION & HOUSEHOLD KPI DATA MARTS - CLICKHOUSE IMPLEMENTATION
-- Architecture: Refreshable Materialized Views (ClickHouse 24.3+)
-- ==========================================================================

SET allow_experimental_refreshable_materialized_view = 1;

-- ==========================================================================
-- 1. REGISTRATION METRICS BASE MART
-- ==========================================================================
CREATE MATERIALIZED VIEW dm_registration_metrics_base
REFRESH EVERY 24 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, region_code, district_code, health_facility_code, settlement_code, age_band, gender, settlement_type)
AS 
SELECT
    pb.campaign_number, 
    pb.region_code, 
    pb.district_code, 
    pb.health_facility_code, 
    pb.settlement_code,
    multiIf(
        toFloat64OrNull(JSONExtractString(pb.beneficiary_additional_fields, 'ageMonths')) BETWEEN 0 AND 11, '0-11m',
        toFloat64OrNull(JSONExtractString(pb.beneficiary_additional_fields, 'ageMonths')) BETWEEN 12 AND 23, '12-23m',
        toFloat64OrNull(JSONExtractString(pb.beneficiary_additional_fields, 'ageMonths')) BETWEEN 24 AND 59, '24-59m',
        'Other'
    ) AS age_band,
    ifNull(nullIf(JSONExtractString(pb.additional_details, 'gender'), ''), 'Unknown') AS gender,
    ifNull(nullIf(JSONExtractString(pb.beneficiary_additional_fields, 'settlementType'), ''), 'Unknown') AS settlement_type,
    
    count() AS total_enumerated_all,
    sum(if(toFloat64OrNull(JSONExtractString(pb.beneficiary_additional_fields, 'ageMonths')) <= 59, 1, 0)) AS enumerated_u5_count,
    sum(if(lower(JSONExtractString(pb.additional_details, 'guestMember')) = 'yes', 1, 0)) AS guest_member_count,
    
    sum(if(pt.administration_status = 'ADMINISTRATION_SUCCESS' AND pt.product_variant = 'zero_dose', 1, 0)) AS zero_dose_count,
    
    sum(if(toFloat64OrNull(JSONExtractString(pb.beneficiary_additional_fields, 'ageMonths')) <= 59 AND 
           pt.administration_status = 'ADMINISTRATION_SUCCESS' AND 
           pt.product_variant != 'zero_dose', 1, 0)) AS vaccinated_u5_count,
           
    sum(if(toFloat64OrNull(JSONExtractString(pb.beneficiary_additional_fields, 'ageMonths')) <= 59 AND 
           pt.administration_status IN ('ADMINISTRATION_SUCCESS', 'ADMINISTRATION_FAILED', 'VISITED'), 1, 0)) AS delivered_u5_count

FROM project_beneficiary_enriched pb
LEFT JOIN project_task_enriched pt 
    ON pt.project_beneficiary_client_reference_id = pb.client_reference_id
WHERE pb.is_deleted = 0
GROUP BY 
    pb.campaign_number, 
    pb.region_code, 
    pb.district_code, 
    pb.health_facility_code, 
    pb.settlement_code,
    age_band, 
    gender, 
    settlement_type;


-- ==========================================================================
-- 2. HOUSEHOLD METRICS BASE MART
-- ==========================================================================
CREATE MATERIALIZED VIEW dm_household_metrics_base
REFRESH EVERY 1 HOUR
ENGINE = MergeTree()
ORDER BY (campaign_number, region_code, district_code, health_facility_code, settlement_code)
AS
SELECT
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code,
    count(DISTINCT id) AS total_households_registered
FROM household_enriched
WHERE is_deleted = 0
GROUP BY 
    campaign_number, 
    region_code, 
    district_code, 
    health_facility_code, 
    settlement_code;
