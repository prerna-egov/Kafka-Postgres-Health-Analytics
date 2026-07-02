-- ==============================================================================
-- KPI RETRIEVAL QUERIES FOR Data Quality 
-- ==============================================================================

-- Country Level Data Quality KPIs
SELECT boundary_hierarchy_code AS country_code, gps_coverage_percentage, gps_accuracy_p10, gps_accuracy_p50, gps_accuracy_p90, gps_accuracy_gt_50m_count, timestamp_consistency_rate, duplicate_percentage
FROM datamart_country_code
WHERE campaign_id = :campaign_id;

-- Region Level Data Quality KPIs
SELECT boundary_hierarchy_code AS region_code, gps_coverage_percentage, gps_accuracy_p10, gps_accuracy_p50, gps_accuracy_p90, gps_accuracy_gt_50m_count, timestamp_consistency_rate, duplicate_percentage
FROM datamart_region_code
WHERE campaign_id = :campaign_id;

-- District Level Data Quality KPIs
SELECT boundary_hierarchy_code AS district_code, gps_coverage_percentage, gps_accuracy_p10, gps_accuracy_p50, gps_accuracy_p90, gps_accuracy_gt_50m_count, timestamp_consistency_rate, duplicate_percentage
FROM datamart_district_code
WHERE campaign_id = :campaign_id;

-- Health Facility Level Data Quality KPIs
SELECT boundary_hierarchy_code AS healthfacility_code, gps_coverage_percentage, gps_accuracy_p10, gps_accuracy_p50, gps_accuracy_p90, gps_accuracy_gt_50m_count, timestamp_consistency_rate, duplicate_percentage
FROM datamart_healthfacility_code
WHERE campaign_id = :campaign_id;

-- Settlement Level Data Quality KPIs
SELECT boundary_hierarchy_code AS settlement_code, gps_coverage_percentage, gps_accuracy_p10, gps_accuracy_p50, gps_accuracy_p90, gps_accuracy_gt_50m_count, timestamp_consistency_rate, duplicate_percentage
FROM datamart_settlement_code
WHERE campaign_id = :campaign_id;


-- ================================================================
--  KPI RETRIEVAL QUERIES FOR Reffusal
-- ================================================================

-- ---------------------------------------------------------------
-- KPI 1: Failed Visit Count (Query)
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT campaign_id, SUM(failed_visit_count) AS failed_visit_count FROM dm_task_status_base WHERE campaign_id = :campaign_id GROUP BY campaign_id;
SELECT region_code, SUM(failed_visit_count) AS failed_visit_count FROM dm_task_status_base WHERE campaign_id = :campaign_id GROUP BY region_code ORDER BY failed_visit_count DESC;
SELECT district_code, SUM(failed_visit_count) AS failed_visit_count FROM dm_task_status_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code ORDER BY failed_visit_count DESC;
SELECT healthfacility_code, SUM(failed_visit_count) AS failed_visit_count FROM dm_task_status_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code ORDER BY failed_visit_count DESC;

-- ---------------------------------------------------------------
-- KPI 2: Refusal Rate (query)
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT campaign_id, SUM(is_refused) AS refused_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_refused) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS refusal_rate_pct FROM dm_beneficiary_status_base WHERE campaign_id = :campaign_id GROUP BY campaign_id;
SELECT region_code, SUM(is_refused) AS refused_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_refused) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS refusal_rate_pct FROM dm_beneficiary_status_base WHERE campaign_id = :campaign_id GROUP BY region_code ORDER BY refusal_rate_pct DESC;
SELECT district_code, SUM(is_refused) AS refused_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_refused) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS refusal_rate_pct FROM dm_beneficiary_status_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code ORDER BY refusal_rate_pct DESC;
SELECT healthfacility_code, SUM(is_refused) AS refused_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_refused) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS refusal_rate_pct FROM dm_beneficiary_status_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code ORDER BY refusal_rate_pct DESC;

-- ---------------------------------------------------------------
-- KPI 3: Absence Rate (query)
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT campaign_id, SUM(is_absent) AS absent_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_absent) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS absence_rate_pct FROM dm_beneficiary_status_base WHERE campaign_id = :campaign_id GROUP BY campaign_id;
SELECT region_code, SUM(is_absent) AS absent_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_absent) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS absence_rate_pct FROM dm_beneficiary_status_base WHERE campaign_id = :campaign_id GROUP BY region_code ORDER BY absence_rate_pct DESC;
SELECT district_code, SUM(is_absent) AS absent_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_absent) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS absence_rate_pct FROM dm_beneficiary_status_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code ORDER BY absence_rate_pct DESC;
SELECT healthfacility_code, SUM(is_absent) AS absent_beneficiaries, COUNT(beneficiary_id) AS total_beneficiaries, ROUND(SUM(is_absent) * 100.0 / NULLIF(COUNT(beneficiary_id), 0), 2) AS absence_rate_pct FROM dm_beneficiary_status_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code ORDER BY absence_rate_pct DESC;

-- ---------------------------------------------------------------
-- KPI 4: Refusal Breakdown (query)
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT campaign_id, refusal_reason, SUM(refusal_count) AS refusal_count FROM dm_refusal_breakdown WHERE campaign_id = :campaign_id AND refusal_count > 0 GROUP BY campaign_id, refusal_reason ORDER BY refusal_count DESC;
SELECT region_code, refusal_reason, SUM(refusal_count) AS refusal_count FROM dm_refusal_breakdown WHERE campaign_id = :campaign_id AND refusal_count > 0 GROUP BY region_code, refusal_reason ORDER BY refusal_count DESC;
SELECT district_code, refusal_reason, SUM(refusal_count) AS refusal_count FROM dm_refusal_breakdown WHERE region_code = :region_code AND campaign_id = :campaign_id AND refusal_count > 0 GROUP BY district_code, refusal_reason ORDER BY refusal_count DESC;
SELECT healthfacility_code, refusal_reason, SUM(refusal_count) AS refusal_count FROM dm_refusal_breakdown WHERE district_code = :district_code AND campaign_id = :campaign_id AND refusal_count > 0 GROUP BY healthfacility_code, refusal_reason ORDER BY refusal_count DESC;
SELECT  refusal_reason, SUM(refusal_count) AS refusal_count FROM dm_refusal_breakdown WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id AND refusal_count > 0 GROUP BY  refusal_reason ORDER BY refusal_count DESC;

-- ---------------------------------------------------------------
-- KPI 5: Absence Breakdown (query)
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT campaign_id, absence_category, SUM(absence_count) AS absence_count FROM dm_absence_breakdown WHERE campaign_id = :campaign_id AND absence_count > 0 GROUP BY campaign_id, absence_category ORDER BY absence_count DESC;
SELECT region_code, absence_category, SUM(absence_count) AS absence_count FROM dm_absence_breakdown WHERE campaign_id = :campaign_id AND absence_count > 0 GROUP BY region_code, absence_category ORDER BY absence_count DESC;
SELECT district_code, absence_category, SUM(absence_count) AS absence_count FROM dm_absence_breakdown WHERE region_code = :region_code AND campaign_id = :campaign_id AND absence_count > 0 GROUP BY district_code, absence_category ORDER BY absence_count DESC;
SELECT healthfacility_code, absence_category, SUM(absence_count) AS absence_count FROM dm_absence_breakdown WHERE district_code = :district_code AND campaign_id = :campaign_id AND absence_count > 0 GROUP BY healthfacility_code, absence_category ORDER BY absence_count DESC;
SELECT  absence_category, SUM(absence_count) AS absence_count FROM dm_absence_breakdown WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id AND absence_count > 0 GROUP BY  absence_category ORDER BY absence_count DESC;

-- ---------------------------------------------------------------
-- KPI 6: Refusal Rate by District (Query)
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT
    district_code,
    SUM(refusal_count) AS refusal_count,
    SUM(total_records) AS total_records,
    ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct
FROM dm_task_status_base
WHERE campaign_id = :campaign_id
  AND district_code IS NOT NULL
GROUP BY district_code
ORDER BY refusal_rate_pct DESC;

-- ---------------------------------------------------------------
-- KPI 7: Refusal Rate by Settlement Type
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT campaign_id, settlement_type, SUM(refusal_count) AS refusal_count, SUM(total_records) AS total_records, ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct FROM dm_settlement_refusal_rate WHERE campaign_id = :campaign_id AND settlement_type IS NOT NULL GROUP BY campaign_id, settlement_type ORDER BY refusal_rate_pct DESC;
SELECT region_code, settlement_type, SUM(refusal_count) AS refusal_count, SUM(total_records) AS total_records, ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct FROM dm_settlement_refusal_rate WHERE campaign_id = :campaign_id AND settlement_type IS NOT NULL GROUP BY region_code, settlement_type ORDER BY refusal_rate_pct DESC;
SELECT district_code, settlement_type, SUM(refusal_count) AS refusal_count, SUM(total_records) AS total_records, ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct FROM dm_settlement_refusal_rate WHERE region_code = :region_code AND campaign_id = :campaign_id AND settlement_type IS NOT NULL GROUP BY district_code, settlement_type ORDER BY refusal_rate_pct DESC;
SELECT healthfacility_code, settlement_type, SUM(refusal_count) AS refusal_count, SUM(total_records) AS total_records, ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct FROM dm_settlement_refusal_rate WHERE district_code = :district_code AND campaign_id = :campaign_id AND settlement_type IS NOT NULL GROUP BY healthfacility_code, settlement_type ORDER BY refusal_rate_pct DESC;
SELECT  settlement_type, SUM(refusal_count) AS refusal_count, SUM(total_records) AS total_records, ROUND(SUM(refusal_count) * 100.0 / NULLIF(SUM(total_records), 0), 2) AS refusal_rate_pct FROM dm_settlement_refusal_rate WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id AND settlement_type IS NOT NULL GROUP BY  settlement_type ORDER BY refusal_rate_pct DESC;

-- ---------------------------------------------------------------
-- KPI 8: Revisit Success Rate
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT campaign_id, SUM(revisit_successful_count) AS revisit_successful_count, SUM(failed_visit_count) AS failed_total_count, SUM(total_revisit_records) AS total_revisit_records, ROUND(SUM(revisit_successful_count) * 100.0 / NULLIF(SUM(total_revisit_records), 0), 2) AS revisit_success_rate_pct FROM dm_task_status_base WHERE campaign_id = :campaign_id GROUP BY campaign_id;
SELECT region_code, SUM(revisit_successful_count) AS revisit_successful_count, SUM(failed_visit_count) AS failed_total_count, SUM(total_revisit_records) AS total_revisit_records, ROUND(SUM(revisit_successful_count) * 100.0 / NULLIF(SUM(total_revisit_records), 0), 2) AS revisit_success_rate_pct FROM dm_task_status_base WHERE campaign_id = :campaign_id GROUP BY region_code ORDER BY revisit_success_rate_pct DESC;
SELECT district_code, SUM(revisit_successful_count) AS revisit_successful_count, SUM(failed_visit_count) AS failed_total_count, SUM(total_revisit_records) AS total_revisit_records, ROUND(SUM(revisit_successful_count) * 100.0 / NULLIF(SUM(total_revisit_records), 0), 2) AS revisit_success_rate_pct FROM dm_task_status_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code ORDER BY revisit_success_rate_pct DESC;
SELECT healthfacility_code, SUM(revisit_successful_count) AS revisit_successful_count, SUM(failed_visit_count) AS failed_total_count, SUM(total_revisit_records) AS total_revisit_records, ROUND(SUM(revisit_successful_count) * 100.0 / NULLIF(SUM(total_revisit_records), 0), 2) AS revisit_success_rate_pct FROM dm_task_status_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code ORDER BY revisit_success_rate_pct DESC;

-- ---------------------------------------------------------------
-- KPI 9: Multi-Unsuccessful Revisit Beneficiaries
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT campaign_id, SUM(multi_unsuccessful_beneficiaries) AS multi_unsuccessful_beneficiaries FROM dm_beneficiary_status_base WHERE campaign_id = :campaign_id GROUP BY campaign_id;
SELECT region_code, SUM(multi_unsuccessful_beneficiaries) AS multi_unsuccessful_beneficiaries FROM dm_beneficiary_status_base WHERE campaign_id = :campaign_id GROUP BY region_code ORDER BY multi_unsuccessful_beneficiaries DESC;
SELECT district_code, SUM(multi_unsuccessful_beneficiaries) AS multi_unsuccessful_beneficiaries FROM dm_beneficiary_status_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code ORDER BY multi_unsuccessful_beneficiaries DESC;
SELECT healthfacility_code, SUM(multi_unsuccessful_beneficiaries) AS multi_unsuccessful_beneficiaries FROM dm_beneficiary_status_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code ORDER BY multi_unsuccessful_beneficiaries DESC;


-- ==========================================================================
-- SECTION: KPI RETRIEVAL QUERIES FOR Coverage
-- ==========================================================================

-- KPI 1: Total Children Vaccinated (Dynamic)
SELECT campaign_id, product_name, SUM(total_vaccinated) AS total_vaccinated FROM dm_successful_deliveries_base WHERE campaign_id = :campaign_id AND product_name = :product_name GROUP BY campaign_id, product_name;
SELECT region_code, product_name, SUM(total_vaccinated) AS total_vaccinated FROM dm_successful_deliveries_base WHERE campaign_id = :campaign_id AND product_name = :product_name GROUP BY region_code, product_name ORDER BY total_vaccinated DESC;
SELECT district_code, product_name, SUM(total_vaccinated) AS total_vaccinated FROM dm_successful_deliveries_base WHERE region_code = :region_code AND campaign_id = :campaign_id AND product_name = :product_name GROUP BY district_code, product_name ORDER BY total_vaccinated DESC;
SELECT healthfacility_code, product_name, SUM(total_vaccinated) AS total_vaccinated FROM dm_successful_deliveries_base WHERE district_code = :district_code AND campaign_id = :campaign_id AND product_name = :product_name GROUP BY healthfacility_code, product_name ORDER BY total_vaccinated DESC;

-- KPI 2: Overall Coverage Rate
SELECT campaign_id, product_name, total_vaccinated, target_population, coverage_percentage FROM dm_campaign_coverage WHERE campaign_id = :campaign_id AND product_name = :product_name;
SELECT campaign_id, product_name, coverage_percentage, total_vaccinated, target_population FROM dm_campaign_coverage WHERE product_name = :product_name ORDER BY campaign_id;

-- KPI 3 & 4: Health Facility Coverage Rate & Hierarchy (Dynamic)
-- Campaign overall
SELECT campaign_id, product_name, 
       COUNT(healthfacility_code) FILTER (WHERE is_targeted) AS target_health_facilities,
       COUNT(healthfacility_code) FILTER (WHERE is_delivered AND is_targeted) AS covered_health_facilities,
       ROUND(COUNT(healthfacility_code) FILTER (WHERE is_delivered AND is_targeted)::NUMERIC / NULLIF(COUNT(healthfacility_code) FILTER (WHERE is_targeted), 0) * 100, 2) AS coverage_percentage
FROM dm_health_facility_status WHERE campaign_id = :campaign_id AND product_name = :product_name GROUP BY campaign_id, product_name;

-- By Country
SELECT product_name, 
       COUNT(healthfacility_code) FILTER (WHERE is_targeted) AS target_health_facilities,
       COUNT(healthfacility_code) FILTER (WHERE is_delivered AND is_targeted) AS covered_health_facilities,
       ROUND(COUNT(healthfacility_code) FILTER (WHERE is_delivered AND is_targeted)::NUMERIC / NULLIF(COUNT(healthfacility_code) FILTER (WHERE is_targeted), 0) * 100, 2) AS coverage_percentage
FROM dm_health_facility_status WHERE campaign_id = :campaign_id AND product_name = :product_name GROUP BY product_name;

-- By Province
SELECT region_code, product_name, 
       COUNT(healthfacility_code) FILTER (WHERE is_targeted) AS target_health_facilities,
       COUNT(healthfacility_code) FILTER (WHERE is_delivered AND is_targeted) AS covered_health_facilities,
       ROUND(COUNT(healthfacility_code) FILTER (WHERE is_delivered AND is_targeted)::NUMERIC / NULLIF(COUNT(healthfacility_code) FILTER (WHERE is_targeted), 0) * 100, 2) AS coverage_percentage
FROM dm_health_facility_status WHERE campaign_id = :campaign_id AND product_name = :product_name GROUP BY region_code, product_name ORDER BY coverage_percentage DESC;

-- By District
SELECT district_code, product_name, 
       COUNT(healthfacility_code) FILTER (WHERE is_targeted) AS target_health_facilities,
       COUNT(healthfacility_code) FILTER (WHERE is_delivered AND is_targeted) AS covered_health_facilities,
       ROUND(COUNT(healthfacility_code) FILTER (WHERE is_delivered AND is_targeted)::NUMERIC / NULLIF(COUNT(healthfacility_code) FILTER (WHERE is_targeted), 0) * 100, 2) AS coverage_percentage
FROM dm_health_facility_status WHERE region_code = :region_code AND campaign_id = :campaign_id AND product_name = :product_name GROUP BY district_code, product_name ORDER BY coverage_percentage DESC;

-- KPI 3B: Inactive Health Facilities
SELECT healthfacility_code, product_name FROM dm_health_facility_status WHERE campaign_id = :campaign_id AND product_name = :product_name AND is_targeted = TRUE AND is_delivered = FALSE;


-- KPI 5: Daily Coverage Rate (Dynamic - Skip Holidays)
WITH last_two_days AS (
    SELECT event_date, product_name, SUM(total_vaccinated) AS delivery_count
    FROM dm_successful_deliveries_base
    WHERE campaign_id = :campaign_id AND product_name = :product_name AND event_date IS NOT NULL
    GROUP BY event_date, product_name
    ORDER BY event_date DESC
    LIMIT 2
),
ranked_days AS (
    SELECT event_date, product_name, delivery_count, ROW_NUMBER() OVER (PARTITION BY product_name ORDER BY event_date DESC) as rn
    FROM last_two_days
)
SELECT 
    :campaign_id AS campaign_id,
    product_name,
    MAX(CASE WHEN rn = 1 THEN event_date END) AS max_reporting_date,
    COALESCE(MAX(CASE WHEN rn = 1 THEN delivery_count END), 0) AS todays_delivery_count,
    COALESCE(MAX(CASE WHEN rn = 2 THEN delivery_count END), 0) AS yesterdays_delivery_count,
    COALESCE(MAX(CASE WHEN rn = 1 THEN delivery_count END), 0) - COALESCE(MAX(CASE WHEN rn = 2 THEN delivery_count END), 0) AS delta_from_yesterday
FROM ranked_days
GROUP BY product_name;
SELECT event_date, product_name, SUM(total_vaccinated) AS daily_delivery_count FROM dm_successful_deliveries_base WHERE campaign_id = :campaign_id AND product_name = :product_name AND event_date >= CURRENT_DATE - INTERVAL '7 days' AND event_date IS NOT NULL GROUP BY event_date, product_name ORDER BY event_date;
SELECT event_date, product_name, SUM(total_vaccinated) AS daily_delivery_count FROM dm_successful_deliveries_base WHERE campaign_id = :campaign_id AND product_name = :product_name AND event_date IS NOT NULL GROUP BY event_date, product_name ORDER BY event_date;

-- KPI 6: Campaign Completion Forecast
SELECT campaign_id, product_name, current_coverage_rate, days_elapsed, total_campaign_days, projected_coverage, on_track, campaign_start_date, campaign_end_date, total_campaign_days - days_elapsed AS days_remaining FROM dm_campaign_forecast WHERE campaign_id = :campaign_id AND product_name = :product_name;
SELECT campaign_id, product_name, current_coverage_rate, projected_coverage, on_track, total_campaign_days - days_elapsed AS days_remaining FROM dm_campaign_forecast WHERE product_name = :product_name ORDER BY on_track, projected_coverage DESC;
SELECT
    dc.event_date,
    SUM(SUM(dc.total_vaccinated)) OVER (ORDER BY dc.event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_deliveries,
    ROUND(SUM(SUM(dc.total_vaccinated)) OVER (ORDER BY dc.event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::NUMERIC / NULLIF(MAX(fc.target_population), 0) * 100, 2) AS cumulative_coverage_pct
FROM dm_successful_deliveries_base dc
JOIN dm_campaign_forecast fc ON fc.campaign_id = dc.campaign_id AND fc.product_name = dc.product_name
WHERE dc.campaign_id = :campaign_id AND dc.product_name = :product_name AND dc.event_date IS NOT NULL
GROUP BY dc.event_date
ORDER BY dc.event_date;

-- KPI 7: District Performance Summary
SELECT district_code, region_code, product_name, delivery_count, target_population, actual_coverage, expected_coverage, coverage_rank FROM dm_district_performance WHERE campaign_id = :campaign_id AND product_name = :product_name ORDER BY coverage_rank;
SELECT district_code, product_name, actual_coverage, expected_coverage FROM dm_district_performance WHERE campaign_id = :campaign_id AND product_name = :product_name ORDER BY actual_coverage DESC LIMIT 5;
SELECT district_code, product_name, actual_coverage, expected_coverage FROM dm_district_performance WHERE campaign_id = :campaign_id AND product_name = :product_name ORDER BY actual_coverage ASC LIMIT 5;
SELECT district_code, product_name, actual_coverage, expected_coverage, coverage_rank FROM dm_district_performance WHERE region_code = :region_code AND campaign_id = :campaign_id AND product_name = :product_name ORDER BY coverage_rank;
SELECT district_code, region_code, product_name, actual_coverage, expected_coverage FROM dm_district_performance WHERE campaign_id = :campaign_id AND product_name = :product_name AND actual_coverage < expected_coverage ORDER BY actual_coverage - expected_coverage ASC;

-- ================================================================
--  KPI RETRIEVAL QUERIES FOR Registration
-- ================================================================
-- ---------------------------------------------------------------
-- KPI 1: Total Children Enumerated
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT region_code, SUM(enumerated_u5_count) AS total_children_enumerated FROM dm_registration_metrics_base WHERE campaign_id = :campaign_id GROUP BY region_code ORDER BY total_children_enumerated DESC;
SELECT district_code, SUM(enumerated_u5_count) AS total_children_enumerated FROM dm_registration_metrics_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code ORDER BY total_children_enumerated DESC;
SELECT healthfacility_code, SUM(enumerated_u5_count) AS total_children_enumerated FROM dm_registration_metrics_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code ORDER BY total_children_enumerated DESC;
SELECT settlement_code, SUM(enumerated_u5_count) AS total_children_enumerated FROM dm_registration_metrics_base WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id GROUP BY settlement_code ORDER BY total_children_enumerated DESC;

-- ---------------------------------------------------------------
-- KPI 2: Total Households Registered
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT region_code, SUM(total_households_registered) AS total_households_registered FROM dm_household_metrics_base WHERE campaign_id = :campaign_id GROUP BY region_code ORDER BY total_households_registered DESC;
SELECT district_code, SUM(total_households_registered) AS total_households_registered FROM dm_household_metrics_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code ORDER BY total_households_registered DESC;
SELECT healthfacility_code, SUM(total_households_registered) AS total_households_registered FROM dm_household_metrics_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code ORDER BY total_households_registered DESC;
SELECT settlement_code, SUM(total_households_registered) AS total_households_registered FROM dm_household_metrics_base WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id GROUP BY settlement_code ORDER BY total_households_registered DESC;

-- ---------------------------------------------------------------
-- KPI 3: Children by Age Band
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT region_code, age_band, SUM(enumerated_u5_count) AS children_count FROM dm_registration_metrics_base WHERE campaign_id = :campaign_id AND age_band != 'Other' GROUP BY region_code, age_band ORDER BY children_count DESC;
SELECT district_code, age_band, SUM(enumerated_u5_count) AS children_count FROM dm_registration_metrics_base WHERE region_code = :region_code AND campaign_id = :campaign_id AND age_band != 'Other' GROUP BY district_code, age_band ORDER BY children_count DESC;
SELECT healthfacility_code, age_band, SUM(enumerated_u5_count) AS children_count FROM dm_registration_metrics_base WHERE district_code = :district_code AND campaign_id = :campaign_id AND age_band != 'Other' GROUP BY healthfacility_code, age_band ORDER BY children_count DESC;
SELECT settlement_code, age_band, SUM(enumerated_u5_count) AS children_count FROM dm_registration_metrics_base WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id AND age_band != 'Other' GROUP BY settlement_code, age_band ORDER BY children_count DESC;
SELECT age_band, SUM(enumerated_u5_count) AS children_count FROM dm_registration_metrics_base WHERE settlement_code = :settlement_code AND campaign_id = :campaign_id AND age_band != 'Other' GROUP BY age_band ORDER BY children_count DESC;

-- ---------------------------------------------------------------
-- KPI 4: Gender Breakdown
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT region_code, gender, SUM(enumerated_u5_count) AS children_count FROM dm_registration_metrics_base WHERE campaign_id = :campaign_id GROUP BY region_code, gender ORDER BY children_count DESC;
SELECT district_code, gender, SUM(enumerated_u5_count) AS children_count FROM dm_registration_metrics_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code, gender ORDER BY children_count DESC;
SELECT healthfacility_code, gender, SUM(enumerated_u5_count) AS children_count FROM dm_registration_metrics_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code, gender ORDER BY children_count DESC;
SELECT settlement_code, gender, SUM(enumerated_u5_count) AS children_count FROM dm_registration_metrics_base WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id GROUP BY settlement_code, gender ORDER BY children_count DESC;
SELECT gender, SUM(enumerated_u5_count) AS children_count FROM dm_registration_metrics_base WHERE settlement_code = :settlement_code AND campaign_id = :campaign_id GROUP BY gender ORDER BY children_count DESC;

-- ---------------------------------------------------------------
-- KPI 5: Children Vaccinated vs Registered (Coverage)
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT region_code, SUM(vaccinated_u5_count) AS vaccinated_count, SUM(enumerated_u5_count) AS enumerated_count, ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS coverage_pct FROM dm_registration_metrics_base WHERE campaign_id = :campaign_id GROUP BY region_code ORDER BY coverage_pct DESC;
SELECT district_code, SUM(vaccinated_u5_count) AS vaccinated_count, SUM(enumerated_u5_count) AS enumerated_count, ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS coverage_pct FROM dm_registration_metrics_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code ORDER BY coverage_pct DESC;
SELECT healthfacility_code, SUM(vaccinated_u5_count) AS vaccinated_count, SUM(enumerated_u5_count) AS enumerated_count, ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS coverage_pct FROM dm_registration_metrics_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code ORDER BY coverage_pct DESC;
SELECT settlement_code, SUM(vaccinated_u5_count) AS vaccinated_count, SUM(enumerated_u5_count) AS enumerated_count, ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS coverage_pct FROM dm_registration_metrics_base WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id GROUP BY settlement_code ORDER BY coverage_pct DESC;

-- ---------------------------------------------------------------
-- KPI 6: Zero Dose Children
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT region_code, SUM(zero_dose_count) AS zero_dose_count FROM dm_registration_metrics_base WHERE campaign_id = :campaign_id GROUP BY region_code ORDER BY zero_dose_count DESC;
SELECT district_code, SUM(zero_dose_count) AS zero_dose_count FROM dm_registration_metrics_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code ORDER BY zero_dose_count DESC;
SELECT healthfacility_code, SUM(zero_dose_count) AS zero_dose_count FROM dm_registration_metrics_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code ORDER BY zero_dose_count DESC;
SELECT settlement_code, SUM(zero_dose_count) AS zero_dose_count FROM dm_registration_metrics_base WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id GROUP BY settlement_code ORDER BY zero_dose_count DESC;

-- ---------------------------------------------------------------
-- KPI 7: Enumerated but Not Yet Vaccinated (Health Facility Level)
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT healthfacility_code, SUM(enumerated_u5_count) AS enumeration_count, SUM(delivered_u5_count) AS delivered_count FROM dm_registration_metrics_base WHERE campaign_id = :campaign_id GROUP BY healthfacility_code HAVING SUM(delivered_u5_count) = 0 AND SUM(enumerated_u5_count) > 0 ORDER BY enumeration_count DESC;
SELECT healthfacility_code, SUM(enumerated_u5_count) AS enumeration_count, SUM(delivered_u5_count) AS delivered_count FROM dm_registration_metrics_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY healthfacility_code HAVING SUM(delivered_u5_count) = 0 AND SUM(enumerated_u5_count) > 0 ORDER BY enumeration_count DESC;
SELECT healthfacility_code, SUM(enumerated_u5_count) AS enumeration_count, SUM(delivered_u5_count) AS delivered_count FROM dm_registration_metrics_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code HAVING SUM(delivered_u5_count) = 0 AND SUM(enumerated_u5_count) > 0 ORDER BY enumeration_count DESC;

-- ---------------------------------------------------------------
-- KPI 8: Guest Members Registered
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT region_code, SUM(guest_member_count) AS guest_member_count FROM dm_registration_metrics_base WHERE campaign_id = :campaign_id GROUP BY region_code ORDER BY guest_member_count DESC;
SELECT district_code, SUM(guest_member_count) AS guest_member_count FROM dm_registration_metrics_base WHERE region_code = :region_code AND campaign_id = :campaign_id GROUP BY district_code ORDER BY guest_member_count DESC;
SELECT healthfacility_code, SUM(guest_member_count) AS guest_member_count FROM dm_registration_metrics_base WHERE district_code = :district_code AND campaign_id = :campaign_id GROUP BY healthfacility_code ORDER BY guest_member_count DESC;
SELECT settlement_code, SUM(guest_member_count) AS guest_member_count FROM dm_registration_metrics_base WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id GROUP BY settlement_code ORDER BY guest_member_count DESC;

-- ---------------------------------------------------------------
-- KPI 9: Hard-to-Reach / Nomads / Refugees Vaccination Rate
-- Time Complexity: O(log N + K) (N = total rows, K = matching rows)
-- ---------------------------------------------------------------
SELECT region_code, settlement_type, SUM(enumerated_u5_count) AS htr_enumerated_count, SUM(vaccinated_u5_count) AS htr_vaccinated_count, ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS htr_vaccination_rate FROM dm_registration_metrics_base WHERE campaign_id = :campaign_id AND settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees') GROUP BY region_code, settlement_type ORDER BY htr_vaccination_rate DESC;
SELECT district_code, settlement_type, SUM(enumerated_u5_count) AS htr_enumerated_count, SUM(vaccinated_u5_count) AS htr_vaccinated_count, ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS htr_vaccination_rate FROM dm_registration_metrics_base WHERE region_code = :region_code AND campaign_id = :campaign_id AND settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees') GROUP BY district_code, settlement_type ORDER BY htr_vaccination_rate DESC;
SELECT healthfacility_code, settlement_type, SUM(enumerated_u5_count) AS htr_enumerated_count, SUM(vaccinated_u5_count) AS htr_vaccinated_count, ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS htr_vaccination_rate FROM dm_registration_metrics_base WHERE district_code = :district_code AND campaign_id = :campaign_id AND settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees') GROUP BY healthfacility_code, settlement_type ORDER BY htr_vaccination_rate DESC;
SELECT settlement_code, settlement_type, SUM(enumerated_u5_count) AS htr_enumerated_count, SUM(vaccinated_u5_count) AS htr_vaccinated_count, ROUND(SUM(vaccinated_u5_count)::NUMERIC / NULLIF(SUM(enumerated_u5_count), 0) * 100, 2) AS htr_vaccination_rate FROM dm_registration_metrics_base WHERE healthfacility_code = :healthfacility_code AND campaign_id = :campaign_id AND settlement_type IN ('Hard to Reach', 'Nomads', 'Refugees') GROUP BY settlement_code, settlement_type ORDER BY htr_vaccination_rate DESC;

-- ================================================================
--  KPI RETRIEVAL QUERIES FOR Team Performance
-- ================================================================
-- KPI 1: League Table
SELECT team_id, vaccinated, target, performance_percentage, performance_rank 
FROM dm_team_performance_league 
WHERE campaign_id = :campaign_id 
ORDER BY performance_rank;

-- KPI 2: Daily Submission Velocity
SELECT task_date, submissions_per_day
FROM dm_team_daily_velocity
WHERE campaign_id = :campaign_id AND team_id = :team_id
ORDER BY task_date ASC;

-- KPI 3: Submission Rate per Hour (Outliers)
SELECT team_id FROM dm_team_submission_flags_village WHERE campaign_id = :campaign_id;

-- KPI 4 & 5: Consolidated Sync Metrics (Rate & Timing) Drill-downs

-- 1. Campaign Level (Overall)
SELECT 
    ROUND((SUM(synced_teams_count) * 100.0) / NULLIF(SUM(total_active_teams), 0), 2) AS campaign_sync_rate_percentage,
    SUM(under_1hr_count) AS under_1hr_count, SUM(one_to_6hr_count) AS one_to_6hr_count, SUM(six_to_24hr_count) AS six_to_24hr_count, SUM(over_24hr_count) AS over_24hr_count
FROM dm_team_sync_metrics_base
WHERE campaign_id = :campaign_id AND task_date = :task_date;

-- 2. Region Level
SELECT region_code,
    ROUND((SUM(synced_teams_count) * 100.0) / NULLIF(SUM(total_active_teams), 0), 2) AS sync_rate_percentage,
    SUM(under_1hr_count) AS under_1hr_count, SUM(one_to_6hr_count) AS one_to_6hr_count, SUM(six_to_24hr_count) AS six_to_24hr_count, SUM(over_24hr_count) AS over_24hr_count
FROM dm_team_sync_metrics_base
WHERE campaign_id = :campaign_id AND task_date = :task_date
GROUP BY region_code;

-- 3. District Level
SELECT district_code,
    ROUND((SUM(synced_teams_count) * 100.0) / NULLIF(SUM(total_active_teams), 0), 2) AS sync_rate_percentage,
    SUM(under_1hr_count) AS under_1hr_count, SUM(one_to_6hr_count) AS one_to_6hr_count, SUM(six_to_24hr_count) AS six_to_24hr_count, SUM(over_24hr_count) AS over_24hr_count
FROM dm_team_sync_metrics_base
WHERE campaign_id = :campaign_id AND region_code = :region_code AND task_date = :task_date
GROUP BY district_code;

-- 4. Health Facility Level
SELECT healthfacility_code,
    ROUND((SUM(synced_teams_count) * 100.0) / NULLIF(SUM(total_active_teams), 0), 2) AS sync_rate_percentage,
    SUM(under_1hr_count) AS under_1hr_count, SUM(one_to_6hr_count) AS one_to_6hr_count, SUM(six_to_24hr_count) AS six_to_24hr_count, SUM(over_24hr_count) AS over_24hr_count
FROM dm_team_sync_metrics_base
WHERE campaign_id = :campaign_id AND district_code = :district_code AND task_date = :task_date
GROUP BY healthfacility_code;

-- KPI 6: Overall Campaign Ranking by Sync Lag
SELECT team_id, avg_sync_lag, sync_lag_rank
FROM dm_team_sync_lag_campaign 
WHERE campaign_id = :campaign_id 
ORDER BY sync_lag_rank;
