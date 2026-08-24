locals {
  raw_schema     = "RAW"
  core_schema    = "CORE"
  mart_schema    = "MART"
  ops_schema     = "OPS"
  copilot_schema = "COST_COPILOT"
  stage_name     = "LAB_INT_STAGE"
  file_format    = "LAB_JSON_FF"
}

resource "snowflake_database" "lab" {
  name = var.database_name
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.lab.name
  name     = local.raw_schema
}

resource "snowflake_schema" "core" {
  database = snowflake_database.lab.name
  name     = local.core_schema
}

resource "snowflake_schema" "mart" {
  database = snowflake_database.lab.name
  name     = local.mart_schema
}

resource "snowflake_schema" "ops" {
  database = snowflake_database.lab.name
  name     = local.ops_schema
}

resource "snowflake_schema" "copilot" {
  database = snowflake_database.lab.name
  name     = local.copilot_schema
}

resource "snowflake_warehouse" "lab" {
  name                = var.warehouse_name
  warehouse_size      = var.warehouse_size
  auto_suspend        = 120
  auto_resume         = true
  initially_suspended = true
}

resource "snowflake_file_format" "lab_json" {
  database    = snowflake_database.lab.name
  schema      = snowflake_schema.raw.name
  name        = local.file_format
  format_type = "JSON"
}

resource "snowflake_stage" "lab_internal" {
  database = snowflake_database.lab.name
  schema   = snowflake_schema.raw.name
  name     = local.stage_name
}

resource "snowflake_table" "events_landing" {
  database = snowflake_database.lab.name
  schema   = snowflake_schema.raw.name
  name     = "EVENTS_LANDING"

  column {
    name = "EVENT_ID"
    type = "NUMBER"
  }
  column {
    name = "EVENT_TS"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "CUSTOMER_ID"
    type = "NUMBER"
  }
  column {
    name = "PRODUCT_ID"
    type = "NUMBER"
  }
  column {
    name = "QTY"
    type = "NUMBER"
  }
  column {
    name = "UNIT_PRICE"
    type = "NUMBER(12,2)"
  }
  column {
    name = "CHANNEL"
    type = "VARCHAR"
  }
  column {
    name = "EVENT_TYPE"
    type = "VARCHAR"
  }
  column {
    name = "PAYLOAD"
    type = "VARIANT"
  }
}

resource "snowflake_table" "pipe_events" {
  database = snowflake_database.lab.name
  schema   = snowflake_schema.raw.name
  name     = "PIPE_EVENTS"

  column {
    name = "EVENT_ID"
    type = "NUMBER"
  }
  column {
    name = "EVENT_TS"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "CUSTOMER_ID"
    type = "NUMBER"
  }
  column {
    name = "PRODUCT_ID"
    type = "NUMBER"
  }
  column {
    name = "QTY"
    type = "NUMBER"
  }
  column {
    name = "UNIT_PRICE"
    type = "NUMBER(12,2)"
  }
  column {
    name = "EVENT_TYPE"
    type = "VARCHAR"
  }
  column {
    name = "RAW_RECORD"
    type = "VARIANT"
  }
}

resource "snowflake_table" "dim_customer" {
  database = snowflake_database.lab.name
  schema   = snowflake_schema.core.name
  name     = "DIM_CUSTOMER"

  column {
    name = "CUSTOMER_ID"
    type = "NUMBER"
  }
  column {
    name = "CUSTOMER_SEGMENT"
    type = "VARCHAR"
  }
  column {
    name = "REGION"
    type = "VARCHAR"
  }
  column {
    name = "CREATED_AT"
    type = "TIMESTAMP_NTZ"
  }
}

resource "snowflake_table" "dim_product" {
  database = snowflake_database.lab.name
  schema   = snowflake_schema.core.name
  name     = "DIM_PRODUCT"

  column {
    name = "PRODUCT_ID"
    type = "NUMBER"
  }
  column {
    name = "CATEGORY"
    type = "VARCHAR"
  }
  column {
    name = "BRAND"
    type = "VARCHAR"
  }
  column {
    name = "LIST_PRICE"
    type = "NUMBER(12,2)"
  }
}

resource "snowflake_table" "fact_events" {
  database = snowflake_database.lab.name
  schema   = snowflake_schema.core.name
  name     = "FACT_EVENTS"

  column {
    name = "EVENT_ID"
    type = "NUMBER"
  }
  column {
    name = "EVENT_TS"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "CUSTOMER_ID"
    type = "NUMBER"
  }
  column {
    name = "PRODUCT_ID"
    type = "NUMBER"
  }
  column {
    name = "QTY"
    type = "NUMBER"
  }
  column {
    name = "UNIT_PRICE"
    type = "NUMBER(12,2)"
  }
  column {
    name = "EXTENDED_PRICE"
    type = "NUMBER(16,2)"
  }
  column {
    name = "CHANNEL"
    type = "VARCHAR"
  }
  column {
    name = "EVENT_TYPE"
    type = "VARCHAR"
  }
  column {
    name = "LOAD_TS"
    type = "TIMESTAMP_LTZ"
  }
}

resource "snowflake_table" "fact_daily_revenue" {
  database = snowflake_database.lab.name
  schema   = snowflake_schema.mart.name
  name     = "FACT_DAILY_REVENUE"

  column {
    name = "EVENT_DATE"
    type = "DATE"
  }
  column {
    name = "CHANNEL"
    type = "VARCHAR"
  }
  column {
    name = "CATEGORY"
    type = "VARCHAR"
  }
  column {
    name = "REVENUE"
    type = "NUMBER(18,2)"
  }
  column {
    name = "EVENT_COUNT"
    type = "NUMBER"
  }
  column {
    name = "REFRESH_TS"
    type = "TIMESTAMP_LTZ"
  }
}

resource "snowflake_pipe" "pipe_events" {
  database       = snowflake_database.lab.name
  schema         = snowflake_schema.raw.name
  name           = "PIPE_EVENTS_JSON"
  auto_ingest    = false
  copy_statement = <<-SQL
    COPY INTO ${snowflake_database.lab.name}.${snowflake_schema.raw.name}.PIPE_EVENTS
      (EVENT_ID, EVENT_TS, CUSTOMER_ID, PRODUCT_ID, QTY, UNIT_PRICE, EVENT_TYPE, RAW_RECORD)
    FROM (
      SELECT
        $1:event_id::NUMBER,
        $1:event_ts::TIMESTAMP_NTZ,
        $1:customer_id::NUMBER,
        $1:product_id::NUMBER,
        $1:qty::NUMBER,
        $1:unit_price::NUMBER(12,2),
        $1:event_type::VARCHAR,
        $1
      FROM @${snowflake_database.lab.name}.${snowflake_schema.raw.name}.${snowflake_stage.lab_internal.name}/pipe_events/
    )
    FILE_FORMAT = (FORMAT_NAME = ${snowflake_database.lab.name}.${snowflake_schema.raw.name}.${snowflake_file_format.lab_json.name})
  SQL
}
