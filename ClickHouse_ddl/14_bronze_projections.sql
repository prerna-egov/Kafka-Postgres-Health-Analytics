-- ============================================================================
-- BRONZE PROJECTIONS -- every projection the bronze -> silver reads rely on
-- ============================================================================
-- 40 projections on 31 stg_* tables, used by:
--   - the SQL-side transforms in airflow_dags/dags/sql_based_dags/ (all 22 entities);
--   - project_transformation.py (prj_ingested_at on stg_project, prj_by_project
--     on stg_project_address / stg_project_target).
--
-- Purpose: give the bronze reads a physical ordering they can prune on,
-- WITHOUT changing the bronze tables' own sort key, which ReplacingMergeTree
-- dedup depends on.
--
-- Why: stg_* are ORDER BY (tenant_id, id) with no partition key, so
--   - `WHERE _ingested_at >= start AND _ingested_at < end` scans the whole table
--     on every page of the window read (measured: 1.59M rows read for a 1-second
--     window; ~2B rows per full `project` run). A minmax skip index does not
--     help either: rows are physically ordered by id, so every 8192-row granule
--     spans the part's entire ingestion-time range;
--   - a join on any column other than the table's own id (`ON x.project_id =
--     p.id`, `ON b.client_reference_id = t.project_beneficiary_client_reference_id`
--     ...) can never prune, so the right side of the join is read whole on
--     every chunk (measured on project_task: one 6-table FINAL join reads
--     3.36 M rows and peaks at 2.43 GiB per chunk).
-- See airflow_dags/BRONZE_READ_SCALABILITY.md, Option A'.
--
-- Two kinds of projection (both deliberately NARROW):
--   prj_ingested_at (_ingested_at, tenant_id, id)  -- PART 2: the window read of
--       every driving table. It answers "which (tenant_id, id) changed in this
--       window"; the DAG then fetches those ids from the base table WITH FINAL,
--       where the (tenant_id, id) primary key prunes.
--   prj_by_<key>    (tenant_id, <key>, id, ...)    -- PART 3: a join side re-keyed
--       by a non-primary key. Carries only the columns the DAGs read, plus the
--       table's own RMT key (id) and version column, so the DAGs'
--       `argMax(col, version) ... GROUP BY key, id` subqueries reproduce FINAL
--       from the projection alone.
--
-- Tables joined on their OWN primary key need nothing here and stay FINAL in
-- the DAGs: stg_address, stg_project (by id), stg_product_variant, stg_facility,
-- stg_individual (by id), stg_attendance_register, stg_service_definition,
-- stg_expense_bill (by id), stg_individual_address (its sort key starts with
-- individual_id).
--
-- Rules / gotchas:
--   - ClickHouse refuses projections on ReplacingMergeTree (Code 344) unless
--     deduplicate_merge_projection_mode = 'rebuild'; PART 1 sets it, and must
--     run BEFORE any ADD PROJECTION. 'rebuild' means every dedup merge on these
--     tables also rebuilds their projections -- narrow projections keep that
--     cheap; watch system.part_log MergeParts durations.
--   - A projection is IGNORED by any query that uses FINAL (measured). Only the
--     non-FINAL window reads and the non-FINAL argMax join subqueries benefit;
--     every other reader of these tables is unaffected.
--   - A projection carries exactly the columns the DAGs' subqueries touch.
--     Reading one more column through it silently makes it ineligible and the
--     full scan is back: extend the projection here FIRST. Changing a
--     projection's columns = DROP PROJECTION + ADD + MATERIALIZE.
--   - MATERIALIZE PROJECTION is an async mutation (system.mutations) with no
--     IF NOT EXISTS guard: run it ONCE per projection. Re-running schedules a
--     full rebuild. On an install where a projection already exists, skip its
--     MATERIALIZE line. Reads stay correct while it runs (parts without the
--     projection fall back to the base table).
--   - Disk: expect roughly 30% of each base table per prj_by_* projection
--     (uuid id columns compress poorly; String columns such as
--     additional_details grow it further). Check system.disks free_space first.
--   - lightweight_mutation_projection_mode defaults to 'throw': a
--     `DELETE FROM <table> ...` fails once a projection exists; use
--     `ALTER TABLE ... DELETE WHERE` (heavy mutation) instead.
--   - Applied with ALTER (not embedded in 04's CREATE TABLE IF NOT EXISTS)
--     because the tables already exist on the cluster; ALTER is the one path
--     that applies identically to fresh and existing installs. Run after 04.
--   - Plain (non-Replicated) MergeTree tables: no ON CLUSTER.
--
-- Upgrading an install that ran an earlier version of this file: an older
-- stg_household / stg_individual prj_by_client_ref may lack address_id /
-- additional_details. ADD ... IF NOT EXISTS will not widen it: DROP PROJECTION
-- it first, then run its ADD + MATERIALIZE below.
-- ============================================================================


-- ============================================================================
-- PART 1: allow projections on these ReplacingMergeTree tables
-- ============================================================================
ALTER TABLE analytics.stg_attendance_attendee     MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_attendance_log          MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_attendance_register     MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_attendance_staff        MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_attendance_summary      MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_device_tokens           MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_expense_bill            MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_expense_billdetail      MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_expense_lineitem        MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_expense_party           MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_hf_referral             MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_household               MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_household_member        MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_individual              MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_muster_roll             MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_pgr_address             MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_pgr_service             MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_project                 MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_project_address         MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_project_beneficiary     MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_project_staff           MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_project_target          MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_project_task            MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_referral                MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_service                 MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_service_attribute_value MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_side_effect             MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_stock                   MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_stock_reconciliation    MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_task_resource           MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE analytics.stg_user_action             MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';


-- ============================================================================
-- PART 2: window reads -- prj_ingested_at on every driving table (22)
-- ============================================================================
-- A time-ordered copy of the sort key, for the window read (plan_slices /
-- SliceFilter in sql_flatten_common.py; _count_bronze_records /
-- _iter_bronze_chunks in project_transformation.py). Pruning uses the first key
-- only; tenant_id, id make the order total.
--
-- stg_attendance_staff, stg_attendance_attendee, stg_expense_billdetail,
-- stg_project_beneficiary, stg_project_task, stg_side_effect, stg_household,
-- stg_project_staff and stg_project also have a join-side projection in PART 3.
-- ----------------------------------------------------------------------------

-- stg_attendance_attendee (attendee)
ALTER TABLE analytics.stg_attendance_attendee
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_attendance_attendee MATERIALIZE PROJECTION prj_ingested_at;

-- stg_attendance_log (attendance_log)
ALTER TABLE analytics.stg_attendance_log
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_attendance_log MATERIALIZE PROJECTION prj_ingested_at;

-- stg_attendance_register (attendance_register)
ALTER TABLE analytics.stg_attendance_register
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_attendance_register MATERIALIZE PROJECTION prj_ingested_at;

-- stg_attendance_staff (attendance_staff)
ALTER TABLE analytics.stg_attendance_staff
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_attendance_staff MATERIALIZE PROJECTION prj_ingested_at;

-- stg_device_tokens (device_token)
ALTER TABLE analytics.stg_device_tokens
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_device_tokens MATERIALIZE PROJECTION prj_ingested_at;

-- stg_expense_bill (bill)
ALTER TABLE analytics.stg_expense_bill
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_expense_bill MATERIALIZE PROJECTION prj_ingested_at;

-- stg_expense_billdetail (bill_detail)
ALTER TABLE analytics.stg_expense_billdetail
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_expense_billdetail MATERIALIZE PROJECTION prj_ingested_at;

-- stg_hf_referral (hf_referral)
ALTER TABLE analytics.stg_hf_referral
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_hf_referral MATERIALIZE PROJECTION prj_ingested_at;

-- stg_household (household)
ALTER TABLE analytics.stg_household
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_household MATERIALIZE PROJECTION prj_ingested_at;

-- stg_household_member (household_member)
ALTER TABLE analytics.stg_household_member
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_household_member MATERIALIZE PROJECTION prj_ingested_at;

-- stg_muster_roll (muster_roll)
ALTER TABLE analytics.stg_muster_roll
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_muster_roll MATERIALIZE PROJECTION prj_ingested_at;

-- stg_pgr_service (pgr)
ALTER TABLE analytics.stg_pgr_service
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_pgr_service MATERIALIZE PROJECTION prj_ingested_at;

-- stg_project (project; also project_transformation.py)
ALTER TABLE analytics.stg_project
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_project MATERIALIZE PROJECTION prj_ingested_at;

-- stg_project_beneficiary (project_beneficiary)
ALTER TABLE analytics.stg_project_beneficiary
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_project_beneficiary MATERIALIZE PROJECTION prj_ingested_at;

-- stg_project_staff (project_staff)
ALTER TABLE analytics.stg_project_staff
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_project_staff MATERIALIZE PROJECTION prj_ingested_at;

-- stg_project_task (project_task)
ALTER TABLE analytics.stg_project_task
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_project_task MATERIALIZE PROJECTION prj_ingested_at;

-- stg_referral (referral)
ALTER TABLE analytics.stg_referral
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_referral MATERIALIZE PROJECTION prj_ingested_at;

-- stg_service (service_task)
ALTER TABLE analytics.stg_service
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_service MATERIALIZE PROJECTION prj_ingested_at;

-- stg_side_effect (side_effect)
ALTER TABLE analytics.stg_side_effect
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_side_effect MATERIALIZE PROJECTION prj_ingested_at;

-- stg_stock (stock)
ALTER TABLE analytics.stg_stock
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_stock MATERIALIZE PROJECTION prj_ingested_at;

-- stg_stock_reconciliation (stock_reconciliation)
ALTER TABLE analytics.stg_stock_reconciliation
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_stock_reconciliation MATERIALIZE PROJECTION prj_ingested_at;

-- stg_user_action (user_action)
ALTER TABLE analytics.stg_user_action
    ADD PROJECTION IF NOT EXISTS prj_ingested_at
    (
        SELECT _ingested_at, tenant_id, id
        ORDER BY (_ingested_at, tenant_id, id)
    );

ALTER TABLE analytics.stg_user_action MATERIALIZE PROJECTION prj_ingested_at;


-- ============================================================================
-- PART 3: join sides -- prj_by_<key> (18)
-- ============================================================================
-- Each block names the entities whose DAGs read it. is_deleted, where carried,
-- is applied AFTER argMax (a row whose newest version is deleted must drop out,
-- as `FINAL ... ON is_deleted = false` did).
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- stg_project_address by project_id: one boundary per project. RMT key
-- (tenant_id, id), version _ingested_at. Read by project (also
-- project_transformation.py), project_staff, project_task, project_beneficiary,
-- attendance_register, attendance_staff, attendee, muster_roll, device_token,
-- stock, stock_reconciliation, hf_referral, referral, side_effect.
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_project_address
    ADD PROJECTION IF NOT EXISTS prj_by_project
    (
        SELECT tenant_id, project_id, id, boundary, _ingested_at
        ORDER BY (tenant_id, project_id, id)
    );

ALTER TABLE analytics.stg_project_address MATERIALIZE PROJECTION prj_by_project;

-- ----------------------------------------------------------------------------
-- stg_project_target by project_id (project). This table has NO tenant_id
-- column; RMT key (id), version last_modified_time.
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_project_target
    ADD PROJECTION IF NOT EXISTS prj_by_project
    (
        SELECT project_id, id, beneficiary_type, target_no, is_deleted, last_modified_time
        ORDER BY (project_id, id)
    );

ALTER TABLE analytics.stg_project_target MATERIALIZE PROJECTION prj_by_project;

-- ----------------------------------------------------------------------------
-- stg_project: projects by NAME (service_task's definition-code fallback,
-- lowest project id per name).
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_project
    ADD PROJECTION IF NOT EXISTS prj_by_name
    (
        SELECT tenant_id, name, id, project_type, project_type_id, reference_id, additional_details, is_deleted, last_modified_time
        ORDER BY (tenant_id, name, id)
    );

ALTER TABLE analytics.stg_project MATERIALIZE PROJECTION prj_by_name;

-- ----------------------------------------------------------------------------
-- stg_task_resource by task_id (project_task): one silver row per resource.
-- additional_details is NOT carried: the Python DAG selects it but never reads it.
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_task_resource
    ADD PROJECTION IF NOT EXISTS prj_by_task
    (
        SELECT tenant_id, task_id, id, product_variant_id, quantity, is_delivered,
               reason_if_not_delivered, client_reference_id, is_deleted, last_modified_time
        ORDER BY (tenant_id, task_id, id)
    );

ALTER TABLE analytics.stg_task_resource MATERIALIZE PROJECTION prj_by_task;

-- ----------------------------------------------------------------------------
-- stg_project_task by client_reference_id, with the beneficiary ref, address
-- and project (side_effect, referral; lowest task id per reference).
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_project_task
    ADD PROJECTION IF NOT EXISTS prj_by_client_ref
    (
        SELECT tenant_id, client_reference_id, id, project_beneficiary_client_reference_id, additional_details, address_id, project_id, is_deleted, last_modified_time
        ORDER BY (tenant_id, client_reference_id, id)
    );

ALTER TABLE analytics.stg_project_task MATERIALIZE PROJECTION prj_by_client_ref;

-- ----------------------------------------------------------------------------
-- stg_project_beneficiary by client_reference_id: the bridge from a task (or
-- side effect / referral) to its household or individual (project_task,
-- side_effect, referral).
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_project_beneficiary
    ADD PROJECTION IF NOT EXISTS prj_by_client_ref
    (
        SELECT tenant_id, client_reference_id, id, beneficiary_client_reference_id,
               is_deleted, last_modified_time
        ORDER BY (tenant_id, client_reference_id, id)
    );

ALTER TABLE analytics.stg_project_beneficiary MATERIALIZE PROJECTION prj_by_client_ref;

-- ----------------------------------------------------------------------------
-- stg_household by client_reference_id (= the bridge's
-- beneficiary_client_reference_id / household_member's
-- household_client_reference_id). member_count for project_task; address_id
-- (address/boundary lookup) and additional_details (merged into the member's)
-- for household_member; also read by project_beneficiary.
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_household
    ADD PROJECTION IF NOT EXISTS prj_by_client_ref
    (
        SELECT tenant_id, client_reference_id, id, member_count, address_id, additional_details,
               is_deleted, last_modified_time
        ORDER BY (tenant_id, client_reference_id, id)
    );

ALTER TABLE analytics.stg_household MATERIALIZE PROJECTION prj_by_client_ref;

-- ----------------------------------------------------------------------------
-- stg_individual by client_reference_id: date_of_birth and gender (project_task,
-- side_effect, referral, project_beneficiary); additional_details for the
-- height / disabilityType fields (household_member, side_effect, referral).
-- Largest table of the set; additional_details is a String column, so expect
-- the projection to be noticeably large -- check system.disks first.
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_individual
    ADD PROJECTION IF NOT EXISTS prj_by_client_ref
    (
        SELECT tenant_id, client_reference_id, id, date_of_birth, gender, additional_details,
               is_deleted, last_modified_time
        ORDER BY (tenant_id, client_reference_id, id)
    );

ALTER TABLE analytics.stg_individual MATERIALIZE PROJECTION prj_by_client_ref;

-- ----------------------------------------------------------------------------
-- stg_side_effect by client_reference_id (referral).
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_side_effect
    ADD PROJECTION IF NOT EXISTS prj_by_client_ref
    (
        SELECT tenant_id, client_reference_id, id, task_id, symptoms, additional_details, is_deleted, last_modified_time
        ORDER BY (tenant_id, client_reference_id, id)
    );

ALTER TABLE analytics.stg_side_effect MATERIALIZE PROJECTION prj_by_client_ref;

-- ----------------------------------------------------------------------------
-- stg_project_staff by staff_id: the user -> project bridge. A user's
-- lowest-id non-deleted staff assignment gives the project
-- (= ProjectStaffRepository's ORDER BY id ASC, first row). Read by household,
-- household_member, attendance_staff, attendee, muster_roll, device_token,
-- stock, bill, bill_detail, side_effect.
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_project_staff
    ADD PROJECTION IF NOT EXISTS prj_by_staff
    (
        SELECT tenant_id, staff_id, id, project_id, is_deleted, last_modified_time
        ORDER BY (tenant_id, staff_id, id)
    );

ALTER TABLE analytics.stg_project_staff MATERIALIZE PROJECTION prj_by_staff;

-- ----------------------------------------------------------------------------
-- stg_attendance_staff / stg_attendance_attendee by register_id
-- (attendance_register; grouped per register).
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_attendance_staff
    ADD PROJECTION IF NOT EXISTS prj_by_register
    (
        SELECT tenant_id, register_id, id, last_modified_time
        ORDER BY (tenant_id, register_id, id)
    );

ALTER TABLE analytics.stg_attendance_staff MATERIALIZE PROJECTION prj_by_register;

ALTER TABLE analytics.stg_attendance_attendee
    ADD PROJECTION IF NOT EXISTS prj_by_register
    (
        SELECT tenant_id, register_id, id, individual_id, last_modified_time
        ORDER BY (tenant_id, register_id, id)
    );

ALTER TABLE analytics.stg_attendance_attendee MATERIALIZE PROJECTION prj_by_register;

-- ----------------------------------------------------------------------------
-- stg_attendance_summary by muster_roll_id (muster_roll). No tenant_id column.
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_attendance_summary
    ADD PROJECTION IF NOT EXISTS prj_by_muster_roll
    (
        SELECT muster_roll_id, id, individual_id, actual_total_attendance, last_modified_time
        ORDER BY (muster_roll_id, id)
    );

ALTER TABLE analytics.stg_attendance_summary MATERIALIZE PROJECTION prj_by_muster_roll;

-- ----------------------------------------------------------------------------
-- stg_pgr_address by parent_id (pgr; one address per service).
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_pgr_address
    ADD PROJECTION IF NOT EXISTS prj_by_parent
    (
        SELECT tenant_id, parent_id, id, locality, latitude, longitude, additional_details, last_modified_time
        ORDER BY (tenant_id, parent_id, id)
    );

ALTER TABLE analytics.stg_pgr_address MATERIALIZE PROJECTION prj_by_parent;

-- ----------------------------------------------------------------------------
-- stg_expense_billdetail by bill_id (bill, bill_detail; grouped per bill).
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_expense_billdetail
    ADD PROJECTION IF NOT EXISTS prj_by_bill
    (
        SELECT tenant_id, bill_id, id, last_modified_time
        ORDER BY (tenant_id, bill_id, id)
    );

ALTER TABLE analytics.stg_expense_billdetail MATERIALIZE PROJECTION prj_by_bill;

-- ----------------------------------------------------------------------------
-- stg_expense_party by parent_id = bill id or bill detail id: payer / payee
-- rows (bill, bill_detail).
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_expense_party
    ADD PROJECTION IF NOT EXISTS prj_by_parent
    (
        SELECT tenant_id, parent_id, id, type, status, identifier, payment_provider, payee_name, payee_phone_number, bank_account, bank_code, beneficiary_code, last_modified_time
        ORDER BY (tenant_id, parent_id, id)
    );

ALTER TABLE analytics.stg_expense_party MATERIALIZE PROJECTION prj_by_parent;

-- ----------------------------------------------------------------------------
-- stg_expense_lineitem by bill_detail_id (bill_detail; grouped per detail).
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_expense_lineitem
    ADD PROJECTION IF NOT EXISTS prj_by_billdetail
    (
        SELECT tenant_id, bill_detail_id, id, head_code, amount, paid_amount, type, status, payment_status, is_line_item_payable, last_modified_time
        ORDER BY (tenant_id, bill_detail_id, id)
    );

ALTER TABLE analytics.stg_expense_lineitem MATERIALIZE PROJECTION prj_by_billdetail;

-- ----------------------------------------------------------------------------
-- stg_service_attribute_value by reference_id = the service id (service_task).
-- No tenant_id column.
-- ----------------------------------------------------------------------------
ALTER TABLE analytics.stg_service_attribute_value
    ADD PROJECTION IF NOT EXISTS prj_by_reference
    (
        SELECT reference_id, id, attribute_code, value, created_by, last_modified_by, created_time, last_modified_time, additional_details, client_reference_id, service_client_reference_id
        ORDER BY (reference_id, id)
    );

ALTER TABLE analytics.stg_service_attribute_value MATERIALIZE PROJECTION prj_by_reference;


-- ============================================================================
-- VERIFICATION (read-only)
-- ============================================================================
-- Materialization done?  is_done = 1 and latest_fail_reason = '' for all.
--   SELECT table, mutation_id, command, is_done, parts_to_do, latest_fail_reason
--   FROM system.mutations
--   WHERE database = 'analytics' AND table LIKE 'stg_%' AND command LIKE '%PROJECTION%'
--   ORDER BY create_time DESC;
--
-- Every active part carries every projection?  Expect 40 rows, pct_rows = 100.
-- (Use system.parts.projections; on 25.7 system.projection_parts.part_name is
-- the projection name, not the parent part, so joining on it misreports.)
--   WITH defined AS (
--       SELECT table, arrayJoin(groupUniqArrayArray(projections)) AS projection
--       FROM system.parts
--       WHERE database = 'analytics' AND active AND table LIKE 'stg_%'
--       GROUP BY table
--   )
--   SELECT d.table, d.projection, count() AS parts,
--          round(100 * sumIf(p.rows, has(p.projections, d.projection)) / sum(p.rows), 1) AS pct_rows
--   FROM defined AS d
--   INNER JOIN system.parts AS p ON p.table = d.table
--   WHERE p.database = 'analytics' AND p.active
--   GROUP BY d.table, d.projection ORDER BY d.table, d.projection;
--
-- Storage cost:
--   SELECT table, name, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS size
--   FROM system.projection_parts
--   WHERE database = 'analytics' AND active
--   GROUP BY table, name ORDER BY table, name;
--
-- Did a query use it?  Run the DAG's query with a known query_id, then:
--   SELECT read_rows, read_bytes, query_duration_ms, projections
--   FROM system.query_log
--   WHERE query_id = '<id>' AND type = 'QueryFinish';
--   -- `projections` should list e.g. analytics.stg_project.prj_ingested_at.
--   -- Or: EXPLAIN indexes = 1 <query>  -> ReadFromMergeTree names the
--   -- projection and shows Granules: k/N with k small.
--
-- Rebuild merge cost:
--   SELECT table, event_type, count(),
--          quantile(0.5)(duration_ms) AS p50_ms, quantile(0.95)(duration_ms) AS p95_ms
--   FROM system.part_log
--   WHERE database = 'analytics' AND table LIKE 'stg_%'
--     AND event_type IN ('MergeParts', 'MutatePart')
--     AND event_time >= now() - INTERVAL 7 DAY
--   GROUP BY table, event_type ORDER BY table, event_type;


-- ============================================================================
-- ROLLBACK -- drops every projection, then restores the RMT guard
-- ============================================================================
-- ALTER TABLE analytics.stg_attendance_attendee     DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_attendance_attendee     DROP PROJECTION IF EXISTS prj_by_register;
-- ALTER TABLE analytics.stg_attendance_log          DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_attendance_register     DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_attendance_staff        DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_attendance_staff        DROP PROJECTION IF EXISTS prj_by_register;
-- ALTER TABLE analytics.stg_attendance_summary      DROP PROJECTION IF EXISTS prj_by_muster_roll;
-- ALTER TABLE analytics.stg_device_tokens           DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_expense_bill            DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_expense_billdetail      DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_expense_billdetail      DROP PROJECTION IF EXISTS prj_by_bill;
-- ALTER TABLE analytics.stg_expense_lineitem        DROP PROJECTION IF EXISTS prj_by_billdetail;
-- ALTER TABLE analytics.stg_expense_party           DROP PROJECTION IF EXISTS prj_by_parent;
-- ALTER TABLE analytics.stg_hf_referral             DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_household               DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_household               DROP PROJECTION IF EXISTS prj_by_client_ref;
-- ALTER TABLE analytics.stg_household_member        DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_individual              DROP PROJECTION IF EXISTS prj_by_client_ref;
-- ALTER TABLE analytics.stg_muster_roll             DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_pgr_address             DROP PROJECTION IF EXISTS prj_by_parent;
-- ALTER TABLE analytics.stg_pgr_service             DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_project                 DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_project                 DROP PROJECTION IF EXISTS prj_by_name;
-- ALTER TABLE analytics.stg_project_address         DROP PROJECTION IF EXISTS prj_by_project;
-- ALTER TABLE analytics.stg_project_beneficiary     DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_project_beneficiary     DROP PROJECTION IF EXISTS prj_by_client_ref;
-- ALTER TABLE analytics.stg_project_staff           DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_project_staff           DROP PROJECTION IF EXISTS prj_by_staff;
-- ALTER TABLE analytics.stg_project_target          DROP PROJECTION IF EXISTS prj_by_project;
-- ALTER TABLE analytics.stg_project_task            DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_project_task            DROP PROJECTION IF EXISTS prj_by_client_ref;
-- ALTER TABLE analytics.stg_referral                DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_service                 DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_service_attribute_value DROP PROJECTION IF EXISTS prj_by_reference;
-- ALTER TABLE analytics.stg_side_effect             DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_side_effect             DROP PROJECTION IF EXISTS prj_by_client_ref;
-- ALTER TABLE analytics.stg_stock                   DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_stock_reconciliation    DROP PROJECTION IF EXISTS prj_ingested_at;
-- ALTER TABLE analytics.stg_task_resource           DROP PROJECTION IF EXISTS prj_by_task;
-- ALTER TABLE analytics.stg_user_action             DROP PROJECTION IF EXISTS prj_ingested_at;
-- then, on each of the 31 tables in PART 1:
--   ALTER TABLE analytics.<table> MODIFY SETTING deduplicate_merge_projection_mode = 'throw';
