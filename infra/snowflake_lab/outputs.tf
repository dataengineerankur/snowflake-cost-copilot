output "database_name" {
  value = snowflake_database.lab.name
}

output "warehouse_name" {
  value = snowflake_warehouse.lab.name
}

output "schemas" {
  value = [
    snowflake_schema.raw.name,
    snowflake_schema.core.name,
    snowflake_schema.mart.name,
    snowflake_schema.ops.name,
    snowflake_schema.copilot.name,
  ]
}

output "stage_name" {
  value = snowflake_stage.lab_internal.name
}

output "pipe_name" {
  value = snowflake_pipe.pipe_events.name
}
