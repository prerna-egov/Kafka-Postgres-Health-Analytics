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
      status                                      VARCHAR(64),
      is_deleted                                  BOOLEAN,
      row_version                                 INTEGER,                                                                                               
  
      -- Task.address (flattened)                                                                                                                        
      address_id                                  VARCHAR(64),
      address_latitude                            DOUBLE PRECISION,
      address_longitude                           DOUBLE PRECISION,
      address_location_accuracy                   DOUBLE PRECISION,
      address_type                                VARCHAR(64),                                                                                                  
      address_locality                            JSONB,
                                                                                                                                                         
      -- Task.additionalFields
      task_additional_fields                      JSONB,

      -- Task.auditDetails
      created_by                                  VARCHAR(64),
      last_modified_by                            VARCHAR(64),                                                                                                  
      created_time                                BIGINT,
      last_modified_time                          BIGINT,                                                                                                
                  
      -- Task.clientAuditDetails
      client_created_by                           VARCHAR(64),
      client_last_modified_by                     VARCHAR(64),                                                                                                  
      client_created_time                         BIGINT,
      client_last_modified_time                   BIGINT,                                                                                                
                  
      -- TaskResource fields (one row per resource per task)
      task_resource_id                            VARCHAR(64),
      task_resource_client_reference_id           VARCHAR(64),                                                                                           
      product_variant_id                          VARCHAR(64),
      quantity                                    BIGINT,                                                                                                
      is_delivered                                BOOLEAN,
      delivery_comment                            VARCHAR(64),
                                                                                                                                                         
      -- ProjectTaskIndexV1 top-level enriched fields
      task_type                                   VARCHAR(64),                                                                                                  
      administration_status                       VARCHAR(64),
      project_type                                VARCHAR(64),
      project_type_id                             VARCHAR(64),
      locality_code                               VARCHAR(64),
      user_name                                   VARCHAR(64),                                                                                                  
      name_of_user                                VARCHAR(64),
      role                                        VARCHAR(64),                                                                                                  
      user_address                                VARCHAR(64),
      product_name                                VARCHAR(64),
      delivered_to                                VARCHAR(64),
      household_id                                VARCHAR(64),
      member_count                                INTEGER,                                                                                               
      individual_id                               VARCHAR(64),
      date_of_birth                               BIGINT,                                                                                                
      age                                         INTEGER,
      gender                                      VARCHAR(64),
      task_dates                                  DATE,
      synced_date                                 VARCHAR(64),                                                                                                  
      synced_time_stamp                           TIMESTAMPTZ,
      synced_time                                 BIGINT,                                                                                                
      boundary_hierarchy                          JSONB,
      boundary_hierarchy_code                     JSONB,
      geo_point                                   JSONB,
      additional_details                          JSONB                                                                                                  
  );
