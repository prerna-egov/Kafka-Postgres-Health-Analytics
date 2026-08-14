CREATE TABLE IF NOT EXISTS boundary_hierarchy_dim (
    id                      String,             -- source boundary_hierarchy.id (repeated across all rows unfurled from one definition)
    tenant_id               LowCardinality(String),
    hierarchy_type          LowCardinality(String),
    boundary_type           LowCardinality(String),
    parent_boundary_type    LowCardinality(String), -- '' for the root row
    level                   UInt8,              -- 1-indexed, root = 1 (matches level_one_code/level_two_code/... convention)
    is_active               Bool,
    created_by              String,
    last_modified_by        String,
    created_time            Int64,
    last_modified_time      Int64
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, hierarchy_type, boundary_type)
SETTINGS index_granularity = 8192;
