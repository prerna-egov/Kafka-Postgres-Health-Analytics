-- ==========================================================================
-- KPI QUERIES -- "Exsisting KPI" sheet
--
-- One query per KPI defined in KPI/SMC_Campaign_KPI_Framework_updated.xlsx,
-- sheet `Exsisting KPI` (the misspelling is the sheet's own). Each is headed by
-- its sheet row number and KPI Name so it traces back to the source.
--
-- READ-ONLY. Nothing here creates or alters anything; these are the queries a
-- dashboard or BI tool issues against the gold marts in 07-12.
--
-- A KPI that appears on both the National and State Overview pages is written
-- ONCE, with a note giving the boundary column to swap. The marts carry the
-- full level_one_code..level_nine_code path on every row, so the same query
-- serves every level by changing one column:
--
--     National : no boundary predicate
--     State    : WHERE level_two_code   = :state
--     LGA      : GROUP BY level_three_code
--     Ward     : GROUP BY level_four_code      (and so on down the path)
--
-- THREE RULES THAT APPLY TO EVERY QUERY BELOW
--
--   1. Distinct measures are read with uniqExactMerge(<x>_uniq). NEVER
--      sum(<x>_count) -- the _count columns are that row's own value and
--      over-count on any roll-up.
--   2. Targets are read from dm_targets_base with a LEVEL SELECTOR
--      (node_level = N, plus the parent level codes for a cascade). A bare SUM
--      multiplies the total by the depth of the tree.
--   3. Age bands are read-time predicates on age (in MONTHS): 3-11, 12-59 and
--      3-59, where 3-59 is the UNION of the first two, not a third band.
--
-- SPAQ1 / SPAQ2: the sheet names these as products, but product_name in silver
-- holds SP / AQ / Bednet variants, not a SPAQ1/SPAQ2 label. The AGE BAND is
-- used as the proxy throughout -- SPAQ1 = 3-11 months, SPAQ2 = 12-59 months.
-- Substitute a product predicate if a product mapping is introduced.
--
-- FLAGGED KPIs the current marts cannot answer are left as commented stubs in
-- place, each stating why and the smallest change that would unblock it. They
-- are not implemented speculatively.
-- ==========================================================================


-- ==========================================================================
-- PAGE: National   (also State Overview -- see the level note above)
-- ==========================================================================

-- r2 / r27  Total target children (3-59 months)
-- Campaign-wide target. The shallowest level the campaign has is its own
-- declared total; see mv_dm_campaign_coverage in 08 for why min(node_level)
-- rather than a fixed level.
SELECT sum(target_population) AS total_target_children
FROM (
    SELECT target_population, node_level,
           min(node_level) OVER (PARTITION BY tenant_id, campaign_number, target_type) AS root_level
    FROM dm_targets_base
    WHERE target_type = 'INDIVIDUAL'
      AND node_level > 0
      AND campaign_number = {campaign:String}
)
WHERE node_level = root_level;

-- FLAGGED  r6 / r10 / r31 / r35  Total target children (3-11) and (12-59)
--   dm_targets_base has no age dimension -- project_entity carries one
--   overall_target per target_type, not per age band. The 3-59 total above is
--   answerable; the per-band split is not.
--   UNBLOCK: an age-banded target in the source (microplan).

-- r3 / r28  Children administered SPAQ (3-59 months)
SELECT uniqExactMerge(administered_uniq) AS children_administered
FROM dm_smc_administered_base
WHERE administration_status = 'ADMINISTRATION_SUCCESS'
  AND delivered_to = 'INDIVIDUAL'
  AND age BETWEEN 3 AND 59
  AND campaign_number = {campaign:String};

-- r7 / r32  Children administered SPAQ1 (3-11 months)
SELECT uniqExactMerge(administered_uniq) AS children_administered_3_11
FROM dm_smc_administered_base
WHERE administration_status = 'ADMINISTRATION_SUCCESS'
  AND delivered_to = 'INDIVIDUAL'
  AND age BETWEEN 3 AND 11
  AND campaign_number = {campaign:String};

-- r11 / r36  Children administered SPAQ2 (12-59 months)
SELECT uniqExactMerge(administered_uniq) AS children_administered_12_59
FROM dm_smc_administered_base
WHERE administration_status = 'ADMINISTRATION_SUCCESS'
  AND delivered_to = 'INDIVIDUAL'
  AND age BETWEEN 12 AND 59
  AND campaign_number = {campaign:String};

-- r4 / r29  % Children administered SPAQ (3-59 months)
-- Administered / microplan target. NULL rather than 0 when there is no
-- denominator: no target means no rate, not 0%.
SELECT
    administered,
    target,
    round(administered / nullIf(target, 0) * 100, 2) AS pct_administered
FROM
(
    SELECT
        (SELECT uniqExactMerge(administered_uniq)
         FROM dm_smc_administered_base
         WHERE administration_status = 'ADMINISTRATION_SUCCESS'
           AND delivered_to = 'INDIVIDUAL'
           AND age BETWEEN 3 AND 59
           AND campaign_number = {campaign:String}) AS administered,
        (SELECT sum(target_population) FROM (
            SELECT target_population, node_level,
                   min(node_level) OVER (PARTITION BY tenant_id, campaign_number, target_type) AS root_level
            FROM dm_targets_base
            WHERE target_type = 'INDIVIDUAL' AND node_level > 0
              AND campaign_number = {campaign:String}
         ) WHERE node_level = root_level) AS target
);

-- FLAGGED  r8 / r12 / r33 / r37  % Children administered SPAQ1 / SPAQ2
--   Numerator is available (r7 / r11), denominator is not -- see the r6/r10
--   flag. Not expressible until targets carry an age band.

-- FLAGGED  r5 / r9 / r13 / r30 / r34 / r38  Total wasted SPAQ blisters
--   The sheet sources wastage from the CDD stock return form. stock_entity
--   .reason carries DAMAGED_IN_* and LOST_IN_* but no wastage category, and
--   there is no CDD-level return feed in silver.
--   UNBLOCK: a wastage reason on the stock source. DAMAGED/LOST are NOT a
--   substitute -- they describe storage and transit loss, not administration
--   wastage, and reporting one as the other would misstate the KPI.

-- FLAGGED  r14 / r15 / r39 / r40  Total redose (SPAQ1) and (SPAQ2)
--   project_task_entity.delivery_comments holds 'REDOSE', but
--   dm_smc_administered_base does not carry that column.
--   UNBLOCK: add delivery_comments (or an is_redose flag) to that mart's grain.

-- r16 / r41  Referred children
SELECT uniqExactMerge(referred_children_uniq) AS referred_children
FROM dm_referral_summary
WHERE campaign_number = {campaign:String};

-- r17 / r42  Total refusals  (consent not given)
SELECT uniqExactMerge(administered_uniq) AS total_refusals
FROM dm_smc_administered_base
WHERE administration_status = 'BENEFICIARY_REFUSED'
  AND campaign_number = {campaign:String};

-- r18 / r43  Ineligible children
SELECT uniqExactMerge(administered_uniq) AS ineligible_children
FROM dm_smc_administered_base
WHERE administration_status = 'INELIGIBLE'
  AND campaign_number = {campaign:String};

-- r19 / r44  Total users created
-- Drop the role predicate for all users; keep it for a single cadre.
SELECT uniqExactMerge(users_created_uniq) AS total_users_created
FROM dm_user_sync
WHERE campaign_number = {campaign:String};

-- r20 / r45  Total users synced  (synced at least once)
SELECT uniqExactMerge(users_synced_uniq) AS total_users_synced
FROM dm_user_sync
WHERE campaign_number = {campaign:String};

-- r21 / r46  % of synced users
SELECT
    uniqExactMerge(users_synced_uniq)  AS synced,
    uniqExactMerge(users_created_uniq) AS created,
    round(uniqExactMerge(users_synced_uniq) / nullIf(uniqExactMerge(users_created_uniq), 0) * 100, 2) AS pct_synced
FROM dm_user_sync
WHERE campaign_number = {campaign:String};

-- r22 / r53  Campaign summary  (by state, or by LGA on the State page)
-- One row per boundary. Swap level_two_code for level_three_code to get the
-- LGA cut. The "Total SPAQ wasted" column of this table is NOT included -- see
-- the r5 flag.
SELECT
    a.boundary                                                            AS boundary,
    t.target                                                              AS total_target,
    a.administered                                                        AS children_administered,
    round(a.administered / nullIf(t.target, 0) * 100, 2)                  AS pct_administered,
    a.refusals                                                            AS total_refusals,
    a.ineligible                                                          AS total_ineligible,
    r.referred                                                            AS total_referred
FROM
(
    SELECT
        level_two_code AS boundary,
        uniqExactMergeIf(administered_uniq, administration_status = 'ADMINISTRATION_SUCCESS' AND delivered_to = 'INDIVIDUAL') AS administered,
        uniqExactMergeIf(administered_uniq, administration_status = 'BENEFICIARY_REFUSED')  AS refusals,
        uniqExactMergeIf(administered_uniq, administration_status = 'INELIGIBLE')           AS ineligible
    FROM dm_smc_administered_base
    WHERE campaign_number = {campaign:String}
    GROUP BY boundary
) AS a
LEFT JOIN
(
    SELECT level_two_code AS boundary, sum(target_population) AS target
    FROM dm_targets_base
    WHERE target_type = 'INDIVIDUAL' AND node_level = 2
      AND campaign_number = {campaign:String}
    GROUP BY boundary
) AS t ON t.boundary = a.boundary
LEFT JOIN
(
    SELECT level_two_code AS boundary, uniqExactMerge(referred_children_uniq) AS referred
    FROM dm_referral_summary
    WHERE campaign_number = {campaign:String}
    GROUP BY boundary
) AS r ON r.boundary = a.boundary
ORDER BY children_administered DESC;


-- ==========================================================================
-- PAGE: Maps
-- ==========================================================================

-- FLAGGED  r24  SPAQ administered -- geo coordinates
-- FLAGGED  r25  SPAQ not administered -- coordinates by reason
--   project_task_entity carries latitude/longitude, but
--   dm_smc_administered_base does not: the mart is an aggregate and a point map
--   needs task-level rows.
--   UNBLOCK: either a point mart at task grain, or read silver directly -- the
--   same call already made for the complaints detail lists (see 09).


-- ==========================================================================
-- PAGE: State Overview   (rows 27-46 are the National set above)
-- ==========================================================================

-- r47 / r55  Coverage of SPAQ administered, by age group per LGA
-- Three series per boundary. 3-59 is the union of the two bands, not a third.
SELECT
    level_three_code AS lga,
    uniqExactMergeIf(administered_uniq, age BETWEEN  3 AND 11) AS age_3_11,
    uniqExactMergeIf(administered_uniq, age BETWEEN 12 AND 59) AS age_12_59,
    uniqExactMergeIf(administered_uniq, age BETWEEN  3 AND 59) AS age_3_59
FROM dm_smc_administered_base
WHERE administration_status = 'ADMINISTRATION_SUCCESS'
  AND delivered_to = 'INDIVIDUAL'
  AND campaign_number = {campaign:String}
GROUP BY lga
ORDER BY age_3_59 DESC;

-- FLAGGED  r56  Coverage of SPAQ by age  (% against microplan target by band)
--   Numerator available above; the per-band denominator is not. See r6/r10.

-- r48  Complaints by status
SELECT application_status, sum(complaint_count) AS complaints
FROM dm_complaints_base
WHERE application_status IN ('PENDING_ASSIGNMENT', 'RESOLVED', 'REJECTED')
  AND campaign_number = {campaign:String}
GROUP BY application_status
ORDER BY complaints DESC;

-- r49  Complaints by type
SELECT service_code AS complaint_type, sum(complaint_count) AS complaints
FROM dm_complaints_base
WHERE campaign_number = {campaign:String}
GROUP BY complaint_type
ORDER BY complaints DESC;

-- FLAGGED  r50  Checklists  (supervision checklists filled, per LGA)
--   service_task_entity is empty -- no checklist data in silver. Same reason
--   the Supervision tab and viz 716 were deferred.
--   UNBLOCK: populate the service-task feed.

-- r51  Stock Summary  (state facility level)
-- The sheet's balance is Received + Returned(Unused) - Issued. That exact form
-- is NOT available (see the Returned flag below); this returns the components
-- plus the dashboard's own RECEIVED - DISPATCHED balance.
SELECT
    sumIf(quantity_sum, event_type = 'RECEIVED'   AND reason = 'RECEIVED')  AS received,
    sumIf(quantity_sum, event_type = 'DISPATCHED' AND reason = '')          AS issued,
    sumIf(quantity_sum, event_type = 'RECEIVED'   AND reason = 'RETURNED')  AS returned,
    sumIf(quantity_sum, event_type = 'RECEIVED')
      - sumIf(quantity_sum, event_type = 'DISPATCHED')                      AS balance
FROM dm_stock_transactions
WHERE campaign_number = {campaign:String};

-- FLAGGED  r52  Summary of referred children  (fever, malaria-positive)
--   "Children referred due to fever" and "tested +ve for malaria" come from
--   checklist attributes (service_task_entity, empty) and referral reason
--   fields silver does not carry. Total referred and present-at-facility ARE
--   available -- see the Referral page.


-- ==========================================================================
-- PAGE: Administration
-- ==========================================================================

-- r55  SPAQ administered by age  -- see r47 above (same query, LGA grain).

-- FLAGGED  r57 / r58  Total redose (SPAQ1) / (SPAQ2), by reason for redose
--   Needs both the redose flag (see r14) AND a redose reason, which silver
--   does not carry at all.

-- FLAGGED  r59  SPAQ wasted  (SPAQ1, SPAQ2 and total, per LGA)
--   See the r5 flag -- no wastage category in the stock source.

-- r60  Total refusals  (households that did not provide consent)
-- NOTE: the sheet counts HOUSEHOLDS; dm_smc_administered_base is beneficiary
-- grain and carries no household id, so this counts refused BENEFICIARIES.
-- A household-level refusal count would need household_id on that mart.
SELECT
    level_three_code AS lga,
    uniqExactMerge(administered_uniq) AS refusals
FROM dm_smc_administered_base
WHERE administration_status = 'BENEFICIARY_REFUSED'
  AND campaign_number = {campaign:String}
GROUP BY lga
ORDER BY refusals DESC;

-- r61  Summary of SPAQ administered
-- Target group, total ineligible and % ineligible are available. The four
-- redose and wasted columns are NOT -- see the r14 and r5 flags.
SELECT
    level_three_code AS lga,
    uniqExactMergeIf(administered_uniq, administration_status = 'ADMINISTRATION_SUCCESS' AND delivered_to = 'INDIVIDUAL') AS target_group_administered,
    uniqExactMergeIf(administered_uniq, administration_status = 'INELIGIBLE') AS total_ineligible,
    round(
        uniqExactMergeIf(administered_uniq, administration_status = 'INELIGIBLE')
        / nullIf(uniqExactMerge(administered_uniq), 0) * 100, 2) AS pct_ineligible
FROM dm_smc_administered_base
WHERE campaign_number = {campaign:String}
GROUP BY lga
ORDER BY target_group_administered DESC;


-- ==========================================================================
-- PAGE: Inventory
-- ==========================================================================

-- FLAGGED (applies to r51 and r63-r68)  Returned (Unused) vs Returned (Partial)
--   stock_entity.reason carries a single 'RETURNED' with no unused/partial
--   split, so the sheet's balance formula Received + Returned(Unused) - Issued
--   cannot be computed as specified. The queries here return the components and
--   the RECEIVED - DISPATCHED balance the dashboard itself uses.
--   UNBLOCK: split the returned reason at source.

-- r63 / r64  Stock summary by LGA  (per product; SPAQ1/SPAQ2 via product_name)
SELECT
    level_three_code AS lga,
    product_name,
    sumIf(quantity_sum, event_type = 'RECEIVED'   AND reason = 'RECEIVED') AS received,
    sumIf(quantity_sum, event_type = 'DISPATCHED' AND reason = '')         AS issued,
    sumIf(quantity_sum, event_type = 'RECEIVED'   AND reason = 'RETURNED') AS returned,
    sumIf(quantity_sum, event_type = 'RECEIVED')
      - sumIf(quantity_sum, event_type = 'DISPATCHED')                     AS balance
FROM dm_stock_transactions
WHERE campaign_number = {campaign:String}
GROUP BY lga, product_name
ORDER BY lga, product_name;

-- r65 / r66  Stock summary by health facility
-- LGA / Ward / Health Facility, per product.
SELECT
    level_three_code AS lga,
    level_four_code  AS ward,
    facility_name    AS health_facility,
    product_name,
    sumIf(quantity_sum, event_type = 'RECEIVED'   AND reason = 'RECEIVED') AS received,
    sumIf(quantity_sum, event_type = 'DISPATCHED' AND reason = '')         AS issued,
    sumIf(quantity_sum, event_type = 'RECEIVED'   AND reason = 'RETURNED') AS returned,
    sumIf(quantity_sum, event_type = 'RECEIVED')
      - sumIf(quantity_sum, event_type = 'DISPATCHED')                     AS balance
FROM dm_stock_transactions
WHERE campaign_number = {campaign:String}
GROUP BY lga, ward, health_facility, product_name
ORDER BY lga, ward, health_facility, product_name;

-- FLAGGED  r67 / r68  Stock summary by CDD
--   dm_stock_transactions is keyed to facility, not user. stock_entity carries
--   user_name but the mart does not. "SPAQ consumed" (administered + redose)
--   additionally needs the redose flag -- see r14.
--   UNBLOCK: add user_name to dm_stock_transactions' grain.


-- ==========================================================================
-- PAGE: Supervision
-- ==========================================================================

-- FLAGGED  r70  Checklists submitted, per LGA
-- FLAGGED  r71  Checklist summary by health facility
-- FLAGGED  r72  Checklist summary by supervisor
--   All three read service_task_entity, which is empty. Note also that the
--   legacy ES configs for these compute a bare count with no denominator
--   despite being labelled a completion "rate" -- the intended denominator is
--   an open product question, not just a data gap.
--   UNBLOCK: populate the service-task feed, and define the denominator.


-- ==========================================================================
-- PAGE: Complaints
-- ==========================================================================

-- r74  Complaints by LGA
SELECT level_three_code AS lga, sum(complaint_count) AS complaints
FROM dm_complaints_base
WHERE application_status IN ('PENDING_ASSIGNMENT', 'RESOLVED', 'REJECTED')
  AND campaign_number = {campaign:String}
GROUP BY lga
ORDER BY complaints DESC;

-- r75  Complaints by status, per LGA  (stacked)
SELECT level_three_code AS lga, application_status, sum(complaint_count) AS complaints
FROM dm_complaints_base
WHERE application_status IN ('PENDING_ASSIGNMENT', 'RESOLVED', 'REJECTED')
  AND campaign_number = {campaign:String}
GROUP BY lga, application_status
ORDER BY lga, application_status;

-- r76  Complaints by type, per LGA  (stacked)
SELECT level_three_code AS lga, service_code AS complaint_type, sum(complaint_count) AS complaints
FROM dm_complaints_base
WHERE campaign_number = {campaign:String}
GROUP BY lga, complaint_type
ORDER BY lga, complaint_type;

-- r77  Average complaint resolution time (hours)
-- dm_complaints_resolution holds terminal complaints only, so no status filter
-- is needed or possible here -- that is the point of the separate mart.
SELECT
    level_three_code AS lga,
    sum(resolved_count) AS resolved_or_rejected,
    round(sum(resolved_duration_ms_sum) / nullIf(sum(resolved_count), 0) / 3600000, 2) AS avg_resolution_hours
FROM dm_complaints_resolution
WHERE campaign_number = {campaign:String}
GROUP BY lga
ORDER BY avg_resolution_hours DESC;

-- r78  Summary of complaint by status, per LGA
SELECT
    level_three_code AS lga,
    sumIf(complaint_count, application_status = 'PENDING_ASSIGNMENT') AS open,
    sumIf(complaint_count, application_status = 'RESOLVED')           AS resolved,
    sumIf(complaint_count, application_status = 'REJECTED')           AS rejected,
    open + resolved + rejected                                        AS registered
FROM dm_complaints_base
WHERE campaign_number = {campaign:String}
GROUP BY lga
ORDER BY registered DESC;

-- r79  Summary of open complaints, by time filed
SELECT
    level_three_code AS lga,
    age_bucket,
    sum(open_count) AS open_complaints
FROM dm_complaints_open_ageing
WHERE campaign_number = {campaign:String}
GROUP BY lga, age_bucket, age_bucket_order
ORDER BY lga, age_bucket_order;


-- ==========================================================================
-- PAGE: Sync
-- ==========================================================================

-- r81 / r82 / r83  CDDs created / synced / % synced
-- r84 / r85 / r86  Facility users, excluding CDDs
-- r87 / r88 / r89  Supervisors
-- One query, one row per role -- role is a grain column on dm_user_sync
-- precisely so these nine metric cards are one read rather than nine.
SELECT
    role,
    uniqExactMerge(users_created_uniq) AS created,
    uniqExactMerge(users_synced_uniq)  AS synced,
    round(uniqExactMerge(users_synced_uniq) / nullIf(uniqExactMerge(users_created_uniq), 0) * 100, 2) AS pct_synced
FROM dm_user_sync
WHERE campaign_number = {campaign:String}
GROUP BY role
ORDER BY created DESC;

-- FLAGGED  r90  CDD sync by hours  (histogram, last 24h)
-- FLAGGED  r91  CDD sync per health facility  (per-CDD record counts)
--   dm_user_sync stores distinct-user aggregate states, not per-user sync
--   events, so an hourly histogram and a per-user record count are not
--   derivable from it. r91 also wants CDDs with zero records, which a mart
--   built from record tables cannot produce by construction.
--   UNBLOCK: a per-user sync fact (user, timestamp, records) plus the staff
--   roster as the outer side of the join.


-- ==========================================================================
-- PAGE: Referral
-- ==========================================================================

-- r93  Summary of referred children, per LGA
-- Total referred and total present at the health facility are available. The
-- fever and malaria-positive columns are NOT -- see the r52 flag.
--
-- NOTE the two measures are different units by design: referred is DISTINCT
-- children, present-at-facility is RECORDS. Their ratio can exceed 100% when
-- one child attends twice, which is why they are not reconciled in the mart.
SELECT
    level_three_code AS lga,
    uniqExactMerge(referred_children_uniq) AS children_referred,
    sum(hf_referral_records)               AS present_at_health_facility
FROM dm_referral_summary
WHERE campaign_number = {campaign:String}
GROUP BY lga
ORDER BY children_referred DESC;

-- FLAGGED  r94  Summary of referred children by health facility
--   dm_referral_summary is boundary-grained; the health-facility dimension is
--   not carried. The fever / malaria / ADRS columns are separately blocked.
--   UNBLOCK: add the facility id to that mart's grain.

-- FLAGGED  r95  Summary of referrals due to ADRS
--   Needs the adverse-drug-reaction categories (vomiting, abdominal pain, skin
--   reaction, weakness, other) from the health-facility referral form. Silver
--   carries the referral records but not those reason fields.


-- ==========================================================================
-- PAGE: CDD team performance
-- ==========================================================================

-- FLAGGED  r97  Average successful administration per CDD, per LGA
-- FLAGGED  r98  Aggregated summary report  (assigned target vs administered)
-- FLAGGED  r99  CDD team performance  (per-CDD assigned target vs administered)
--   All three divide by an ASSIGNED TARGET PER CDD. Targets exist per boundary
--   node, not per worker, so the denominator does not exist at this grain.
--   The numerator side is available: successful administrations per boundary
--   from dm_smc_administered_base, and staff counts per role from
--   dm_user_sync -- an administrations-per-head figure is computable, but it is
--   NOT the KPI as specified and is deliberately not presented as one.
--   UNBLOCK: a per-CDD target assignment in the source.


-- ============================================================================
-- SYNC KPI QUERIES
-- ============================================================================
-- Counts are NOT pre-aggregated in the marts: the user is in the grain and the
-- distinct count happens here. uniqExact() is therefore exact at any level --
-- campaign, LGA, health facility -- with no double counting.
--
-- NEVER use sum() on a user count from dm_user_sync. Summing distinct counts
-- across boundary cells over-reports: measured 9 synced distributors against a
-- true 3 on unified-dev.
--
-- Filter on campaign_number, never campaign_id -- campaign_id is hardcoded ''
-- by the transformation DAGs and matches nothing.
-- ============================================================================


-- 0. WHICH level_N_code IS THE HEALTH FACILITY?
-- Run this first per hierarchy_type; the answer drives which level column the
-- facility-grain reads below should group on. The mapping is NOT global -- a
-- health facility sits at a different depth per hierarchy.
SELECT hierarchy_type, level, boundary_type, parent_boundary_type
FROM boundary_hierarchy_dim FINAL
WHERE tenant_id = {tenant_id:String}
ORDER BY hierarchy_type, level;


-- ============================================================================
-- KPIs 1-9  metric cards
-- ============================================================================

-- 1/2/3  Total CDDs created | synced | % synced
SELECT uniqExactIf(user_key, src = 'CREATED') AS users_created,
       uniqExactIf(user_key, src = 'SYNCED')  AS users_synced,
       round(100 * uniqExactIf(user_key, src = 'SYNCED')
                 / nullIf(uniqExactIf(user_key, src = 'CREATED'), 0), 2) AS pct_synced
FROM analytics.dm_user_sync
WHERE campaign_number = {campaign_number:String}
  AND role = 'DISTRIBUTOR';

-- 4/5/6  Facility users (excludes CDDs by definition -- separate role)
SELECT uniqExactIf(user_key, src = 'CREATED') AS users_created,
       uniqExactIf(user_key, src = 'SYNCED')  AS users_synced,
       round(100 * uniqExactIf(user_key, src = 'SYNCED')
                 / nullIf(uniqExactIf(user_key, src = 'CREATED'), 0), 2) AS pct_synced
FROM analytics.dm_user_sync
WHERE campaign_number = {campaign_number:String}
  AND role = 'WAREHOUSE_MANAGER';

-- 7/8/9  Supervisors -- four roles, aggregated
SELECT uniqExactIf(user_key, src = 'CREATED') AS users_created,
       uniqExactIf(user_key, src = 'SYNCED')  AS users_synced,
       round(100 * uniqExactIf(user_key, src = 'SYNCED')
                 / nullIf(uniqExactIf(user_key, src = 'CREATED'), 0), 2) AS pct_synced
FROM analytics.dm_user_sync
WHERE campaign_number = {campaign_number:String}
  AND role IN ('NATIONAL_SUPERVISOR','PROVINCIAL_SUPERVISOR',
               'DISTRICT_SUPERVISOR','TEAM_SUPERVISOR');

-- All nine cards in one read, one row per cadre
SELECT role,
       uniqExactIf(user_key, src = 'CREATED') AS users_created,
       uniqExactIf(user_key, src = 'SYNCED')  AS users_synced,
       round(100 * uniqExactIf(user_key, src = 'SYNCED')
                 / nullIf(uniqExactIf(user_key, src = 'CREATED'), 0), 2) AS pct_synced
FROM analytics.dm_user_sync
WHERE campaign_number = {campaign_number:String}
GROUP BY role
ORDER BY users_created DESC;

-- Same, broken down by boundary. Substitute the level from query 0.
SELECT level_three_code AS boundary_code, role,
       uniqExactIf(user_key, src = 'CREATED') AS users_created,
       uniqExactIf(user_key, src = 'SYNCED')  AS users_synced
FROM analytics.dm_user_sync
WHERE campaign_number = {campaign_number:String}
  AND hierarchy_type  = {hierarchy_type:String}
GROUP BY boundary_code, role
ORDER BY boundary_code, role;


-- ============================================================================
-- KPI 10  CDD sync by hours (histogram, last 24h)
-- ============================================================================
SELECT synced_hour,
       uniqExact(user_name) AS cdds_synced,
       sum(records)         AS records_synced
FROM analytics.dm_cdd_sync_hourly
WHERE campaign_number = {campaign_number:String}
  AND role = 'DISTRIBUTOR'
  AND synced_hour >= now() - INTERVAL 24 HOUR
GROUP BY synced_hour
ORDER BY synced_hour;

-- Same histogram scoped to one boundary. Substitute the level from query 0.
SELECT synced_hour, uniqExact(user_name) AS cdds_synced, sum(records) AS records_synced
FROM analytics.dm_cdd_sync_hourly
WHERE campaign_number  = {campaign_number:String}
  AND role             = 'DISTRIBUTOR'
  AND hierarchy_type   = {hierarchy_type:String}
  AND level_three_code = {boundary_code:String}
  AND synced_hour >= now() - INTERVAL 24 HOUR
GROUP BY synced_hour
ORDER BY synced_hour;


-- ============================================================================
-- KPI 11  CDD sync per health facility
-- ============================================================================
-- Includes CDDs with zero records -- the mart is roster-driven, so they are
-- present with total_records_synced = 0. Substitute the level from query 0.
SELECT level_three_code AS health_facility,
       cdd_name,
       total_records_synced
FROM analytics.dm_cdd_sync_facility
WHERE campaign_number = {campaign_number:String}
  AND hierarchy_type  = {hierarchy_type:String}
ORDER BY health_facility, total_records_synced DESC, cdd_name;

-- Facility roll-up. uniqExact on the user is exact here; sum(total_records_synced)
-- is NOT a safe grand total across the whole mart -- a CDD with two assignments
-- at different boundary paths carries their full record count on both rows.
SELECT level_three_code AS health_facility,
       uniqExact(user_id)                        AS cdds_assigned,
       uniqExactIf(user_id, total_records_synced > 0) AS cdds_synced,
       sum(total_records_synced)                 AS records_synced
FROM analytics.dm_cdd_sync_facility
WHERE campaign_number = {campaign_number:String}
  AND hierarchy_type  = {hierarchy_type:String}
GROUP BY health_facility
ORDER BY records_synced DESC;
