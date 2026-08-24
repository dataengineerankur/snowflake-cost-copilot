terraform {
  required_version = ">= 1.5.0"
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "0.73.0"
    }
  }
}

provider "snowflake" {
  account       = var.snowflake_account
  username      = var.snowflake_user
  warehouse     = var.snowflake_bootstrap_warehouse
  password      = lower(var.snowflake_authenticator) == "snowflake" ? var.snowflake_password : null
  authenticator = var.snowflake_authenticator
  role          = var.snowflake_role
}
