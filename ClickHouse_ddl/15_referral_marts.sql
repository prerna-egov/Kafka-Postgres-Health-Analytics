-- ==========================================================================
-- SUPPLEMENTARY REFERRAL AND SUPERVISION MARTS
--
-- These are OPTIONAL performance/drill-down marts. They pre-aggregate data
-- from the authoritative source (dm_referral_summary) for specific query
-- patterns (by facility, by symptom, by checklist attribute).
--
-- They are NOT required for KPI queries — use them for speed if you need
-- facility-level or symptom-level drill-downs. Otherwise, query the primary
-- marts and GROUP BY the dimension you need.
--
-- All refresh at 1 HOUR to stay in sync with source data.
-- ==========================================================================

SET allow_experimental_refreshable_materialized_view = 1;


-- 1. mv_dm_referral_by_facility -> dm_referral_by_facility
--
-- Drill-down performance mart: pre-aggregated by health facility for fast
-- health-facility-level queries. Grain: (boundary, facility, cycle).
--
-- Use when: you need fast facility-level drill-down (r94).
-- Otherwise: query dm_referral_summary GROUP BY facility_id.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_referral_by_facility
REFRESH EVERY 1 HOUR
TO dm_referral_by_facility
AS
SELECT
    tenant_id,
    campaign_number,
    hierarchy_type,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code,
    facility_name,
    recipient_id AS facility_id,
    toString(JSONExtractString(additional_details, 'cycleIndex')) AS cycle_index,
    uniqExactState(toString(project_beneficiary_client_reference_id)) AS children_referred_uniq,
    toUInt64(uniqExact(toString(project_beneficiary_client_reference_id))) AS children_referred
FROM referral_entity FINAL
GROUP BY
    tenant_id, campaign_number, hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code,
    level_five_code, level_six_code, level_seven_code, level_eight_code,
    level_nine_code,
    facility_name, facility_id, cycle_index;


-- 2. mv_dm_referral_by_symptom -> dm_referral_by_symptom
--
-- Drill-down performance mart: pre-aggregated by symptom (FEVER, SICK, MALARIA,
-- DRUG_SE_PC) for fast symptom-level queries. Grain: (boundary, symptom, cycle).
-- Records count, not distinct children.
--
-- Use when: you need fast symptom-level drill-down (r52, r95: fever/malaria/ADRS).
-- Otherwise: query dm_referral_summary WHERE symptom = :symptom.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_referral_by_symptom
REFRESH EVERY 1 HOUR
TO dm_referral_by_symptom
AS
SELECT
    tenant_id,
    campaign_number,
    hierarchy_type,
    toString(JSONExtractString(additional_details, 'cycleIndex')) AS cycle_index,
    level_one_code,
    level_two_code,
    level_three_code,
    level_four_code,
    level_five_code,
    level_six_code,
    level_seven_code,
    level_eight_code,
    level_nine_code,
    symptom,
    toUInt64(count()) AS referrals
FROM hf_referral_entity FINAL
GROUP BY
    tenant_id, campaign_number, hierarchy_type, cycle_index,
    level_one_code, level_two_code, level_three_code, level_four_code,
    level_five_code, level_six_code, level_seven_code, level_eight_code,
    level_nine_code, symptom;


-- 3. mv_dm_hf_checklist_outcome -> dm_hf_checklist_outcome
--
-- Supervision KPI mart: health facility checklist results joined with
-- attribute values (e.g., fever screening, malaria test results).
-- Grain: (boundary, checklist, role, attribute_code, value, date, cycle).
--
-- Use when: you need checklist completion rates and attribute values
-- (r70, r71, r72, r809).
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_hf_checklist_outcome
REFRESH EVERY 1 HOUR
TO dm_hf_checklist_outcome
AS
SELECT
    st.tenant_id,
    st.campaign_number,
    st.hierarchy_type,
    toString(JSONExtractString(st.additional_details, 'cycleIndex')) AS cycle_index,
    st.level_one_code,
    st.level_two_code,
    st.level_three_code,
    st.level_four_code,
    st.level_five_code,
    st.level_six_code,
    st.level_seven_code,
    st.level_eight_code,
    st.level_nine_code,
    st.checklist_name,
    st.role,
    sta.attribute_code,
    sta.value,
    st.task_dates AS event_date,
    uniqExactState(toString(st.id)) AS checklists_uniq,
    toUInt64(uniqExact(st.id)) AS checklists
FROM service_task_entity AS st FINAL
INNER JOIN service_task_attribute_entity AS sta FINAL
    ON sta.reference_id = st.id
GROUP BY
    st.tenant_id, st.campaign_number, st.hierarchy_type, cycle_index,
    st.level_one_code, st.level_two_code, st.level_three_code, st.level_four_code,
    st.level_five_code, st.level_six_code, st.level_seven_code,
    st.level_eight_code, st.level_nine_code,
    st.checklist_name, st.role, sta.attribute_code, sta.value, event_date;