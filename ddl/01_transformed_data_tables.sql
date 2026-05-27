CREATE TABLE IF NOT EXISTS project_task_enriched (
                                                                                                                                                         
      -- Task core fields                                                   
      id                                          VARCHAR(64),                                                                                           
      tenant_id                                   VARCHAR(64)         NOT NULL,                                                                          
      client_reference_id                         VARCHAR(64),                                                                                           
      project_id                                  VARCHAR(64),                                                                                           
      project_beneficiary_id                      VARCHAR(64),                                                                                           
      project_beneficiary_client_reference_id     VARCHAR(64),
      planned_start_date                          BIGINT,
      planned_end_date                            BIGINT,                                                                                                
      actual_start_date                           BIGINT,
      actual_end_date                             BIGINT,                                                                                                
      status                                      TEXT,
      is_deleted                                  BOOLEAN,
      row_version                                 INTEGER,                                                                                               
  
      -- Task.address (flattened)                                                                                                                        
      address_id                                  VARCHAR(64),
      address_latitude                            DOUBLE PRECISION,
      address_longitude                           DOUBLE PRECISION,
      address_location_accuracy                   DOUBLE PRECISION,
      address_type                                TEXT,                                                                                                  
      address_locality                            JSONB,
                                                                                                                                                         
      -- Task.additionalFields
      task_additional_fields                      JSONB,

      -- Task.auditDetails
      created_by                                  TEXT,
      last_modified_by                            TEXT,                                                                                                  
      created_time                                BIGINT,
      last_modified_time                          BIGINT,                                                                                                
                  
      -- Task.clientAuditDetails
      client_created_by                           TEXT,
      client_last_modified_by                     TEXT,                                                                                                  
      client_created_time                         BIGINT,
      client_last_modified_time                   BIGINT,                                                                                                
                  
      -- TaskResource fields (one row per resource per task)
      task_resource_id                            VARCHAR(64),
      task_resource_client_reference_id           VARCHAR(64),                                                                                           
      product_variant_id                          VARCHAR(64),
      quantity                                    BIGINT,                                                                                                
      is_delivered                                BOOLEAN,
      delivery_comment                            TEXT,
                                                                                                                                                         
      -- ProjectTaskIndexV1 top-level enriched fields
      task_type                                   TEXT,                                                                                                  
      administration_status                       TEXT,
      project_type                                TEXT,
      project_type_id                             VARCHAR(64),
      locality_code                               TEXT,
      user_name                                   TEXT,                                                                                                  
      name_of_user                                TEXT,
      role                                        TEXT,                                                                                                  
      user_address                                TEXT,
      product_name                                TEXT,
      delivered_to                                TEXT,
      household_id                                TEXT,
      member_count                                INTEGER,                                                                                               
      individual_id                               TEXT,
      date_of_birth                               BIGINT,                                                                                                
      age                                         INTEGER,
      gender                                      TEXT,
      task_dates                                  DATE,
      synced_date                                 TEXT,                                                                                                  
      synced_time_stamp                           TIMESTAMPTZ,
      synced_time                                 BIGINT,                                                                                                
      boundary_hierarchy                          JSONB,
      boundary_hierarchy_code                     JSONB,
      geo_point                                   JSONB,
      additional_details                          JSONB                                                                                                  
  );
