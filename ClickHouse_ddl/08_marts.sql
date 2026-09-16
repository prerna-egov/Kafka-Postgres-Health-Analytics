CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.rmv_mart_referral
REFRESH EVERY 1000 YEAR
TO analytics.mart_referral
EMPTY
AS
SELECT
    tenant_id,
    campaign_number,
    campaign_id,
    project_id,
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
    recipient_id AS facility_id,
    any(facility_name) AS facility_name,
    uniqExact(project_beneficiary_client_reference_id) AS children_referred
FROM analytics.referral_entity FINAL
GROUP BY tenant_id, campaign_number, campaign_id, project_id, hierarchy_type, level_one_code, level_two_code, level_three_code, level_four_code, level_five_code, level_six_code, level_seven_code, level_eight_code, level_nine_code, recipient_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.rmv_mart_hf_referral
REFRESH EVERY 1000 YEAR
TO analytics.mart_hf_referral
EMPTY
AS
SELECT
    tenant_id,
    campaign_number,
    campaign_id,
    project_id,
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
    project_facility_id AS facility_id,
    symptom,
    count() AS referrals
FROM analytics.hf_referral_entity FINAL
GROUP BY tenant_id, campaign_number, campaign_id, project_id, hierarchy_type, level_one_code, level_two_code, level_three_code, level_four_code, level_five_code, level_six_code, level_seven_code, level_eight_code, level_nine_code, project_facility_id, symptom;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.rmv_mart_hf_checklist_outcome
REFRESH EVERY 1000 YEAR
TO analytics.mart_hf_checklist_outcome
EMPTY
AS
SELECT
    st.tenant_id,
    st.campaign_number,
    st.campaign_id,
    st.project_id,
    st.hierarchy_type,
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
    uniqExact(st.id) AS checklists
FROM analytics.service_task_entity AS st FINAL
INNER JOIN analytics.service_task_attribute_entity AS sta FINAL
ON sta.reference_id = st.id
WHERE startsWith(upper(st.checklist_name), 'HF_RF')
GROUP BY st.tenant_id, st.campaign_number, st.campaign_id, st.project_id, st.hierarchy_type, st.level_one_code, st.level_two_code, st.level_three_code, st.level_four_code, st.level_five_code, st.level_six_code, st.level_seven_code, st.level_eight_code, st.level_nine_code, st.checklist_name, st.role, sta.attribute_code, sta.value;
