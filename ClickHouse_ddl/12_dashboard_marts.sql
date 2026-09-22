-- ==========================================================================
-- REMAINING-TAB MARTS -- LOGIC
--
-- Gold layer for the tabs of `provincial-health-dashboard-iccd` not covered by
-- 09 (complaints), 10 (overview) or 11 (inventory):
--   DSS_HEALTH_REGISTRATION_ADMINISTRATION  viz 747-755  (40 charts)
--   DSS_HEALTH_SUPERVISION                  viz 724-730
--   DSS_HEALTH_TEAM_PERFORMANCE             viz 780-783
--   DSS_HEALTH_DATA_QUALITY                 viz 788-794
--   DSS_HEALTH_REFERRAL_AND_SIDE_EFFECTS    viz 802-803
--   DSS_HEALTH_SPECIFIC_KPIS                viz 806-809
--   DSS_KIBANA_MAPS                         viz 760-762
--
-- LOGIC only. Targets dm_attendance, dm_household_registry,
-- dm_referral_summary and dm_suspected_fraud are defined in 07_mart_tables.sql
-- as items 19-22 and must be created BEFORE this file runs.
--
-- Every silver source is ReplacingMergeTree, so every read uses FINAL.
--
-- MOST OF THIS WORK IS REUSE, NOT NEW MARTS. The single largest tab,
-- REGISTRATION_ADMINISTRATION, needs no new table at all -- see the mapping
-- below. Four marts are added only where a genuinely new source is involved
-- (attendance logs, the household registry, referrals, and the fraud
-- heuristic).
--
-- TARGETS: read dm_targets_base (07 item 2) with a LEVEL SELECTOR --
-- node_level = <the level the chart buckets at> AND code = <the bucket key> --
-- never with is_target_type_root, which is the campaign-wide cut. Targets are
-- declared at the hierarchy's lowest level and summed upward, so each level's
-- row is already a complete subtree total and picking one level is what avoids
-- counting the same target once per level of depth. Full rules in the "HOW TO
-- READ A TARGET AT A LEVEL" block on item 6 in 07.
--
-- BOUNDARY LEVELS: same convention as every sibling file --
-- level_two=Province, level_three=District, level_four=AdministrativeProvince,
-- level_five=Locality, level_six=Village, documented not encoded. Marts bucket
-- on CODES, not display names.
--
-- HOW EACH TAB IS SERVED
--
-- REGISTRATION_ADMINISTRATION (747-755) -- NO NEW MART:
--   747 userSyncSummaryProvinceICCD        -> dm_user_sync (18), role='DISTRIBUTOR'
--   748 administrationCoverage*            -> uniqExactMerge(administered_uniq) FROM (11)
--                                             WHERE administration_status='ADMINISTRATION_SUCCESS'
--                                               AND delivered_to='INDIVIDUAL'
--                                             / dm_targets_base INDIVIDUAL,
--                                               node_level = the bucket level, code = bucket key
--   749 populationCoverageHeatMap*         -> same numerator/denominator, one row per boundary;
--                                             it is an intensity TABLE, not a geospatial heatmap
--   750 beneficiariesNotAdministered*      -> (11) BENEFICIARY_REFUSED + (13) side effects/referrals
--                                             + (11) INELIGIBLE; denominator = registered
--   751 populationNotAdministeredByReason* -> as panel 712, already documented in 10
--   752 administeredByGender*              -> (11) GROUP BY gender  [why gender is in that grain]
--   753 beneficiariesNotAdministeredByReasonBarChart* -> same four sources as 750
--   754 actualVsPlanned*                   -> actual  = (11) by event_date, cumulative
--                                             planned = dm_targets_base.target_per_day
--                                                       at node_level=2 (province tile),
--                                                       replayed cumulatively, capped at
--                                                       target_population
--   755 summaryByDistrict / ByDay          -> (11) + (20) households + (13) adverse
--                                             + (6) targets at node_level=3 AND code=level_three_code
--
-- SUPERVISION (724-730):
--   724/725 supervisor + team-supervisor sync -> dm_user_sync (18) by role
--   726-730 the six checklist charts          -> NOT BUILT (see 07 banner)
--
-- TEAM_PERFORMANCE (780-783):
--   780 userSyncSummaryProvinceICCD -> dm_user_sync (18), role='DISTRIBUTOR'
--   781 averageNumberOfPopulationAdministered
--                                   -> sum(task_rows) FROM (11) filtered INDIVIDUAL+SUCCESS
--                                      / uniqExactMerge(users_created_uniq) FROM (18) DISTRIBUTOR
--   782 attendanceFrontLineWorkers*  -> uniqExact(individual_id) FROM dm_attendance (19)
--                                      / staff count FROM (18)
--   783 summaryTeamPerformance*      -> (19) attendance + (18) staff + (11) administered
--
-- DATA_QUALITY (788-794):
--   788 userSyncSummaryProvinceICCD        -> dm_user_sync (18)
--   789 housesWithMoreThanTwentyMembers*   -> countIf(member_count > 20) FROM (20)
--   791 entriesDoneByWMs*                  -> sum(transaction_count) FROM dm_stock_transactions (15)
--                                             -- no new mart: stock already carries role? NO, it does
--                                             -- not; see KNOWN GAPS below.
--   792 incompleteRecords*                 -> registered FROM (20)/household_member
--                                             - visited FROM (11) over the four terminal statuses
--   793 suspectedFrauds*                   -> uniqExact(created_by) FROM dm_suspected_fraud (22)
--   794 dataQualityRecordsSummary*         -> (20) + (11) + (22) combined
--
-- REFERRAL_AND_SIDE_EFFECTS (802-803):
--   802/803 -> dm_referral_summary (21). Note NEITHER viz queries a side-effect
--              index despite the tab name.
--
-- SPECIFIC_KPIS (806-809):
--   806 successfullAdministrationAcrossCycles -> (11) GROUP BY cycle_index, using
--       uniqExactMerge per cycle and per cycle-set. This is why cycle_index is a
--       grain column on (11). NOTE the ES Venn is WRONG and is not reproduced:
--       its triple and quadruple regions use the same `intersection_count > 1`
--       predicate as the pairs, so a beneficiary present in only some of the
--       cycles is counted into the all-cycles region; its inner terms agg also
--       has no `size`, silently capping every region.
--   807 administeredByAgeGroup*  -> (11) WHERE age BETWEEN 3 AND 11 / 12 AND 59 / 3 AND 59.
--                                   The third band is the UNION of the first two, not a
--                                   disjoint band -- the percent variant divides each of the
--                                   first two by it.
--   808 administeredByHeightGroup -> NOT BUILT, no height in silver (see 07 banner)
--   809 testedPositiveForMalaria* -> NOT BUILT, service_task_entity is empty (see 07 banner)
--
-- KNOWN GAPS
--   * Several sources carry no resolved district, so the panels built on them
--     are correct but return a single blank bucket until boundary enrichment
--     improves.
--   * viz 791 filters stock records by role, which dm_stock_transactions (15)
--     does not carry -- the Inventory tab did not need it. Serving it exactly
--     needs role added to that grain or a direct read of stock_entity. Left
--     unbuilt rather than silently widening an existing mart for one chart.
--   * Target-dependent numbers are empty until dm_targets_base is populated.
--   * Every "users created" denominator is empty until project_staff_entity.role
--     is populated. The numerators work.
-- ==========================================================================

SET allow_experimental_refreshable_materialized_view = 1;


-- 1. mv_dm_attendance -> dm_attendance
--
-- EXIT + ACTIVE is the presence signal, per the ES charts. Grouping by
-- individual and day makes "distinct people present" exact; log_count exposes
-- duplicate clock-outs rather than hiding them behind a distinct count.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_attendance
REFRESH EVERY 1 HOUR
TO dm_attendance
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
    -- attendance_log_entity has no task_dates column; `time` (epoch-ms) is the
    -- log's own clock and is exactly what the ES dateRefField
    -- "attendanceLog.time" reads.
    toDate32(toDateTime(intDiv(time, 1000))) AS event_date,
    individual_id,
    role,
    toUInt64(count()) AS log_count
FROM attendance_log_entity FINAL
WHERE upper(type) = 'EXIT'
  AND upper(status) = 'ACTIVE'
  AND individual_id != ''
  AND time > 0
GROUP BY
    tenant_id, campaign_number, hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code,
    level_five_code, level_six_code, level_seven_code, level_eight_code,
    level_nine_code,
    event_date, individual_id, role;


-- 2. mv_dm_household_registry -> dm_household_registry
--
-- One row per household, member_count carried raw so the ">20" threshold stays
-- a read-time predicate. No filter here: the registry is also the denominator
-- source for the incomplete-records metric, which needs ALL households, not
-- just the oversized ones.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_household_registry
REFRESH EVERY 1 HOUR
TO dm_household_registry
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
    id                AS household_id,
    max(member_count) AS member_count,
    max(task_dates)   AS event_date
FROM household_entity FINAL
GROUP BY
    tenant_id, campaign_number, hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code,
    level_five_code, level_six_code, level_seven_code, level_eight_code,
    level_nine_code,
    household_id;


-- 3. mv_dm_referral_summary -> dm_referral_summary
--
-- Two legs, two counting rules, deliberately not reconciled -- see the table
-- comment in 07. The referral leg contributes a distinct-beneficiary STATE and
-- zero records; the HF leg contributes records and an empty state. Summing the
-- two legs per cell then yields both measures side by side.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_referral_summary
REFRESH EVERY 1 HOUR
TO dm_referral_summary
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
    event_date,
    uniqExactStateIf(beneficiary_key, src = 'REFERRAL')  AS referred_children_uniq,
    -- Same measure for this row only, so the table reads without a Merge.
    toUInt64(uniqExactIf(beneficiary_key, src = 'REFERRAL')) AS referred_children_count,
    toUInt64(countIf(src = 'HF_REFERRAL'))               AS hf_referral_records
FROM
(
    SELECT
        tenant_id, campaign_number, hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code,
        level_five_code, level_six_code, level_seven_code, level_eight_code,
        level_nine_code,
        task_dates AS event_date,
        'REFERRAL' AS src,
        toString(project_beneficiary_client_reference_id) AS beneficiary_key
    FROM referral_entity FINAL

    UNION ALL

    SELECT
        tenant_id, campaign_number, hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code,
        level_five_code, level_six_code, level_seven_code, level_eight_code,
        level_nine_code,
        task_dates    AS event_date,
        'HF_REFERRAL' AS src,
        ''            AS beneficiary_key
    FROM hf_referral_entity FINAL
)
GROUP BY
    tenant_id, campaign_number, hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code,
    level_five_code, level_six_code, level_seven_code, level_eight_code,
    level_nine_code,
    event_date;


-- 4. mv_dm_suspected_fraud -> dm_suspected_fraud
--
-- The throughput heuristic, materialized as the offending (user, minute)
-- buckets only. HAVING count() >= 4 is the whole rule; everything downstream is
-- a matter of whether you count the users or the buckets.
--
-- created_time (the device clock) is used for the minute bucket rather than an
-- ingest timestamp -- see the table comment in 07 for why that matters on a
-- backfilled load.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dm_suspected_fraud
REFRESH EVERY 1 HOUR
TO dm_suspected_fraud
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
    created_by,
    any(user_name)    AS user_name,
    minute_bucket,
    toUInt64(count()) AS task_count
FROM
(
    SELECT
        tenant_id, campaign_number, hierarchy_type,
        level_one_code, level_two_code, level_three_code, level_four_code,
        level_five_code, level_six_code, level_seven_code, level_eight_code,
        level_nine_code,
        created_by,
        user_name,
        toStartOfMinute(toDateTime(intDiv(created_time, 1000))) AS minute_bucket
    FROM project_task_entity FINAL
    WHERE delivered_to = 'INDIVIDUAL'
      AND administration_status = 'ADMINISTRATION_SUCCESS'
      AND created_by != ''
      AND created_time > 0
)
GROUP BY
    tenant_id, campaign_number, hierarchy_type,
    level_one_code, level_two_code, level_three_code, level_four_code,
    level_five_code, level_six_code, level_seven_code, level_eight_code,
    level_nine_code,
    created_by, minute_bucket
HAVING count() >= 4;
