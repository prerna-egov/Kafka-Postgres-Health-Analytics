-- ============================================================================
-- REFERRAL KPIs
-- ============================================================================
-- Marts: mart_referral | mart_hf_referral | mart_hf_checklist_outcome
--
-- The checklist mart carries ALL checklists (no HF_RF filter), so every read
-- below pins checklist_name itself. That also means KPI 3 works the day
-- HF_RF_DRUG_SE data appears, with no mart change.
--
-- Substitute the health-facility level from boundary_hierarchy_dim (06); it is
-- not the same level_N_code for every hierarchy_type. level_three_code is used
-- below as the placeholder.
--
-- MEASURES COME FROM THREE DIFFERENT TABLES AND DO NOT RECONCILE:
--   children_referred        <- referral_entity      (CDD raised the referral)
--   children_present_at_hf   <- hf_referral_entity   (HF recorded an arrival)
--   tested_positive_malaria  <- service_task         (HF_RF_FEVER checklist)
-- Nothing links a specific referral to a specific HF arrival, so
-- present/referred can exceed 100%. This is inherited from the DSS charts.
--
-- CYCLE: cycle_index is now in every mart's grain. Omit it from a GROUP BY to
-- get campaign-to-date, or pin it for a single cycle. EXCEPT for
-- children_referred -- see the warning on KPI 1.
-- ============================================================================


-- ============================================================================
-- KPI 1  Summary of referred children, by LGA  (bar chart)
--        1. Total children referred
--        2. Total children actually present at the health facility
--        3. Total children referred due to fever
--        4. Children tested +ve for malaria
-- ============================================================================
-- UNION ALL of three sources with an outer GROUP BY, not a FULL OUTER JOIN:
-- with join_use_nulls = 0 the coalesce form silently loses boundary keys.
--
-- WARNING on children_referred: mart_referral stores a per-cycle distinct
-- count, so summing it across cycles double-counts a child referred in two
-- cycles. Pin a single cycle_index for an exact figure, or accept the sum as an
-- upper bound. The other three measures are plain counts and sum cleanly.
-- The inner columns carry a _part suffix deliberately: `sum(x) AS x` over a
-- subquery column named x is rejected as a nested aggregate
-- (ILLEGAL_AGGREGATION) once the alias is referenced again, as it is in the
-- pct expression below.
SELECT
    level_one_code,
    sum(children_referred_part)       AS children_referred,
    sum(children_present_at_hf_part)  AS children_present_at_hf,
    sum(referred_fever_part)          AS referred_due_to_fever,
    sum(tested_positive_malaria_part) AS tested_positive_malaria,
    if(sum(children_referred_part) = 0, NULL,
       round(100 * sum(children_present_at_hf_part) / sum(children_referred_part), 2)) AS pct_present_at_hf
FROM
(
    SELECT level_one_code,
           toUInt64(sum(children_referred)) AS children_referred_part,
           toUInt64(0) AS children_present_at_hf_part,
           toUInt64(0) AS referred_fever_part,
           toUInt64(0) AS tested_positive_malaria_part
    FROM analytics.mart_referral
    WHERE campaign_number = {campaign_number:String}
    GROUP BY level_one_code

    UNION ALL

    SELECT level_one_code,
           toUInt64(0),
           toUInt64(sum(referrals)),
           -- 'SICK,FEVER' is ONE literal and it IS fever-involved.
           toUInt64(sumIf(referrals, upper(symptom) IN ('FEVER', 'SICK,FEVER'))),
           toUInt64(0)
    FROM analytics.mart_hf_referral
    WHERE campaign_number = {campaign_number:String}
    GROUP BY level_one_code

    UNION ALL

    SELECT level_one_code,
           toUInt64(0),
           toUInt64(0),
           toUInt64(0),
           toUInt64(sum(checklists))
    FROM analytics.mart_hf_checklist_outcome
    WHERE campaign_number = {campaign_number:String}
      AND checklist_name  = 'HF_RF_FEVER'
      AND role IN ('HEALTH_FACILITY_WORKER', 'HEALTH_FACILITY_SUPERVISOR')
      AND upper(value)    = 'POSITIVE'
    GROUP BY level_one_code
)
GROUP BY level_one_code
ORDER BY children_referred DESC;


-- KPI 1 drill-down: swap level_one_code for level_two_code / level_three_code
-- in all four branches to descend to ward, then health facility.


-- ============================================================================
-- KPI 2  Summary of referred children by health facility  (table)
--        1. Health Facility  2. Children referred  3. Children actually present
--        4. Referred due to fever  5. Children tested +ve for malaria
--        6. Referred due to ADRS
-- ============================================================================
-- Health facility here is the BOUNDARY level, not project_facility_id --
-- the DSS drill chain labels the locality level "Health Facility"
-- (lga -> ward -> healthFacility -> community).
--
-- Referred due to ADRS comes from hf_referral_entity.symptom = 'DRUG_SE_*',
-- which is the referral FLAG. KPI 3 below breaks those down by reaction from a
-- different table; the two will not tie out.
SELECT
    level_three_code,
    sum(children_referred)       AS children_referred,
    sum(children_present_at_hf)  AS children_present_at_hf,
    sum(referred_fever)          AS referred_due_to_fever,
    sum(tested_positive_malaria) AS tested_positive_malaria,
    sum(referred_adrs)           AS referred_due_to_adrs
FROM
(
    SELECT level_three_code,
           toUInt64(sum(children_referred)) AS children_referred,
           toUInt64(0) AS children_present_at_hf,
           toUInt64(0) AS referred_fever,
           toUInt64(0) AS tested_positive_malaria,
           toUInt64(0) AS referred_adrs
    FROM analytics.mart_referral
    WHERE campaign_number = {campaign_number:String}
      AND hierarchy_type  = {hierarchy_type:String}
    GROUP BY level_three_code

    UNION ALL

    SELECT level_three_code,
           toUInt64(0),
           toUInt64(sum(referrals)),
           toUInt64(sumIf(referrals, upper(symptom) IN ('FEVER', 'SICK,FEVER'))),
           toUInt64(0),
           toUInt64(sumIf(referrals, startsWith(upper(symptom), 'DRUG_SE')))
    FROM analytics.mart_hf_referral
    WHERE campaign_number = {campaign_number:String}
      AND hierarchy_type  = {hierarchy_type:String}
    GROUP BY level_three_code

    UNION ALL

    SELECT level_three_code,
           toUInt64(0), toUInt64(0), toUInt64(0),
           toUInt64(sum(checklists)),
           toUInt64(0)
    FROM analytics.mart_hf_checklist_outcome
    WHERE campaign_number = {campaign_number:String}
      AND hierarchy_type  = {hierarchy_type:String}
      AND checklist_name  = 'HF_RF_FEVER'
      AND role IN ('HEALTH_FACILITY_WORKER', 'HEALTH_FACILITY_SUPERVISOR')
      AND upper(value)    = 'POSITIVE'
    GROUP BY level_three_code
)
GROUP BY level_three_code
ORDER BY children_present_at_hf DESC, level_three_code;


-- ============================================================================
-- KPI 3  Summary of referrals due to ADRS, by health facility  (table)
--        1. Health Facility  2. Vomiting  3. Abdominal pain
--        4. Skin reaction    5. Weakness  6. Other
-- ============================================================================
-- Source: the HF_RF_DRUG_SE checklist, attribute adverseReactions. The five
-- reaction literals are the complete set the Kibana dashboards filter on for
-- the SMC tenants (ABDOMINAL_PAIN / OTHERS / SKIN_REACTION / VOMITING /
-- WEAKNESS). AZM tenants use attribute AD3 with AD_-prefixed values instead.
--
-- `other` is a RESIDUAL, not a match on 'OTHERS': a reaction outside the four
-- named ones (DIARRHOEA, NAUSEA, STOMACH_PAIN all exist in the wider
-- vocabulary) then shows up in the table instead of vanishing from it.
--
-- attribute_code is NOT pinned. The code is form-defined and differs per
-- deployment ('adverseReactions' for SMC, 'AD3' for AZM), and pinning the
-- wrong one yields a silent zero. The value IN-list keeps it bounded.
SELECT
    level_three_code,
    sumIf(checklists, upper(value) = 'VOMITING')       AS vomiting,
    sumIf(checklists, upper(value) = 'ABDOMINAL_PAIN') AS abdominal_pain,
    sumIf(checklists, upper(value) = 'SKIN_REACTION')  AS skin_reaction,
    sumIf(checklists, upper(value) = 'WEAKNESS')       AS weakness,
    sum(checklists) - sumIf(checklists, upper(value) IN
        ('VOMITING', 'ABDOMINAL_PAIN', 'SKIN_REACTION', 'WEAKNESS')) AS other
FROM analytics.mart_hf_checklist_outcome
WHERE campaign_number = {campaign_number:String}
  AND hierarchy_type  = {hierarchy_type:String}
  AND checklist_name  = 'HF_RF_DRUG_SE'
GROUP BY level_three_code
ORDER BY (vomiting + abdominal_pain + skin_reaction + weakness + other) DESC;


-- ============================================================================
-- DIAGNOSTICS -- run these when a KPI reads zero
-- ============================================================================

-- Which checklists exist at all? HF_RF_FEVER / HF_RF_DRUG_SE absent => KPIs
-- 1.4, 2.5 and 3 are blocked by missing source data, not by the queries.
SELECT checklist_name, uniqExact(attribute_code) AS attribute_codes,
       sum(checklists) AS checklists
FROM analytics.mart_hf_checklist_outcome
GROUP BY checklist_name ORDER BY checklists DESC;

-- The symptom vocabulary actually present. The KPI literals are FEVER, SICK,
-- 'SICK,FEVER' and DRUG_SE_*; anything else is uncounted by KPI 2.
SELECT symptom, sum(referrals) AS referrals
FROM analytics.mart_hf_referral
GROUP BY symptom ORDER BY referrals DESC;

-- Cycle coverage per mart. A blank cycle_index is its own bucket, so pinning a
-- cycle silently excludes those rows.
SELECT 'referral' AS mart, cycle_index, sum(children_referred) AS measure
FROM analytics.mart_referral GROUP BY cycle_index
UNION ALL
SELECT 'hf_referral', cycle_index, sum(referrals)
FROM analytics.mart_hf_referral GROUP BY cycle_index
UNION ALL
SELECT 'checklist', cycle_index, sum(checklists)
FROM analytics.mart_hf_checklist_outcome GROUP BY cycle_index
ORDER BY mart, cycle_index;
