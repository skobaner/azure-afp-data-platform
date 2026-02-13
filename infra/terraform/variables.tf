variable "project_name" {
  description = "Short project name used for resource naming."
  type        = string
  default     = "csvplatform"
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "eastus"
}

variable "environment" {
  description = "Environment name, e.g. dev, test, prod."
  type        = string
  default     = "dev"
}

variable "sql_admin_username" {
  description = "Admin username for Azure SQL server."
  type        = string
}

variable "sql_admin_password" {
  description = "Admin password for Azure SQL server."
  type        = string
  sensitive   = true
}

variable "allowed_ip_address" {
  description = "Optional public IP allowed to connect to Azure SQL for local admin/debugging."
  type        = string
  default     = ""
}
