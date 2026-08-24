variable "snowflake_account" {
  type        = string
  description = "Snowflake account identifier."
}

variable "snowflake_user" {
  type        = string
  description = "Snowflake user."
}

variable "snowflake_password" {
  type        = string
  description = "Snowflake password."
  default     = ""
  sensitive   = true
}

variable "snowflake_authenticator" {
  type        = string
  description = "Snowflake authenticator (snowflake|externalbrowser)."
  default     = "snowflake"
}

variable "snowflake_role" {
  type        = string
  description = "Snowflake role for Terraform."
  default     = "ACCOUNTADMIN"
}

variable "snowflake_bootstrap_warehouse" {
  type        = string
  description = "Existing warehouse used to authenticate Terraform before lab warehouse is created."
  default     = "COMPUTE_WH"
}

variable "database_name" {
  type        = string
  description = "Database for synthetic lab."
  default     = "COST_COPILOT_DB"
}

variable "warehouse_name" {
  type        = string
  description = "Warehouse for synthetic lab workloads."
  default     = "COST_COPILOT_LAB_WH"
}

variable "warehouse_size" {
  type        = string
  description = "Warehouse size."
  default     = "MEDIUM"
}
