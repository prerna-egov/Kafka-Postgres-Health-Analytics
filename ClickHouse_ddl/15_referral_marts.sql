CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.rmv_mart_referral
REFRESH EVERY 1000 YEAR
TO analytics.mart_referral
EMPTY
AS
SELECT
    tenant_id,
    campaign_number,
    hierarchy_type,
    JSONExtractString(additional_details, 'cycleIndex') AS cycle_index,
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
    max(recipient_id) AS facility_id,
    uniqExact(project_beneficiary_client_reference_id) AS children_referred
FROM analytics.referral_entity FINAL
GROUP BY tenant_id, campaign_number, hierarchy_type, cycle_index, level_one_code, level_two_code, level_three_code, level_four_code, level_five_code, level_six_code, level_seven_code, level_eight_code, level_nine_code, facility_name;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.rmv_mart_hf_referral
REFRESH EVERY 1000 YEAR
TO analytics.mart_hf_referral
EMPTY
AS
SELECT
    tenant_id,
    campaign_number,
    hierarchy_type,
    JSONExtractString(additional_details, 'cycleIndex') AS cycle_index,
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
    max(project_facility_id) AS facility_id,
    count() AS referrals
FROM analytics.hf_referral_entity FINAL
GROUP BY tenant_id, campaign_number, hierarchy_type, cycle_index, level_one_code, level_two_code, level_three_code, level_four_code, level_five_code, level_six_code, level_seven_code, level_eight_code, level_nine_code, symptom;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.rmv_mart_hf_checklist_outcome
REFRESH EVERY 1000 YEAR
TO analytics.mart_hf_checklist_outcome
EMPTY
AS
SELECT
    st.tenant_id AS tenant_id,
    st.campaign_number AS campaign_number,
    st.hierarchy_type AS hierarchy_type,
    JSONExtractString(st.additional_details, 'cycleIndex') AS cycle_index,
    st.level_one_code AS level_one_code,
    st.level_two_code AS level_two_code,
    st.level_three_code AS level_three_code,
    st.level_four_code AS level_four_code,
    st.level_five_code AS level_five_code,
    st.level_six_code AS level_six_code,
    st.level_seven_code AS level_seven_code,
    st.level_eight_code AS level_eight_code,
    st.level_nine_code AS level_nine_code,
    st.checklist_name AS checklist_name,
    st.role AS role,
    sta.attribute_code AS attribute_code,
    sta.value AS value,
    uniqExact(st.id) AS checklists
FROM analytics.service_task_entity AS st FINAL
INNER JOIN analytics.service_task_attribute_entity AS sta FINAL
ON sta.reference_id = st.id
GROUP BY st.tenant_id, st.campaign_number, st.hierarchy_type, cycle_index, st.level_one_code, st.level_two_code, st.level_three_code, st.level_four_code, st.level_five_code, st.level_six_code, st.level_seven_code, st.level_eight_code, st.level_nine_code, st.checklist_name, st.role, sta.attribute_code, sta.value;