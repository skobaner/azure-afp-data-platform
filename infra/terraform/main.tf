resource "random_string" "suffix" {
  length  = 6
  upper   = false
  lower   = true
  numeric = true
  special = false
}

locals {
  base_name            = "${var.project_name}-${var.environment}-${random_string.suffix.result}"
  resource_group_name  = "rg-${local.base_name}"
  storage_account_name = substr(replace("st${var.project_name}${var.environment}${random_string.suffix.result}", "-", ""), 0, 24)
  sql_server_name      = "sql-${local.base_name}"
  sql_database_name    = "sqldb-${var.environment}"
  app_service_plan     = "asp-${local.base_name}"
  api_app_name         = "api-${local.base_name}"
  function_app_name    = "func-${local.base_name}"
  tags = {
    project     = var.project_name
    environment = var.environment
    managedBy   = "terraform"
  }
}

resource "azurerm_resource_group" "main" {
  name     = local.resource_group_name
  location = var.location
  tags     = local.tags
}

resource "azurerm_storage_account" "main" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

resource "azurerm_storage_container" "input" {
  name                  = "input-data"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "processed" {
  name                  = "processed-data"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_mssql_server" "main" {
  name                         = local.sql_server_name
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = var.sql_admin_password
  minimum_tls_version          = "1.2"
  tags                         = local.tags
}

resource "azurerm_mssql_firewall_rule" "azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_mssql_firewall_rule" "local_debug" {
  count            = trimspace(var.allowed_ip_address) != "" ? 1 : 0
  name             = "AllowLocalClient"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = trimspace(var.allowed_ip_address)
  end_ip_address   = trimspace(var.allowed_ip_address)
}

resource "azurerm_mssql_database" "main" {
  name           = local.sql_database_name
  server_id      = azurerm_mssql_server.main.id
  sku_name       = "Basic"
  max_size_gb    = 2
  zone_redundant = false
  tags           = local.tags
}

resource "azurerm_service_plan" "main" {
  name                = local.app_service_plan
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  os_type             = "Linux"
  sku_name            = "B1"
  tags                = local.tags
}

resource "azurerm_linux_web_app" "api" {
  name                = local.api_app_name
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  service_plan_id     = azurerm_service_plan.main.id

  site_config {
    always_on        = true
    app_command_line = "bash startup.sh"

    application_stack {
      python_version = "3.11"
    }
  }

  app_settings = {
    WEBSITES_ENABLE_APP_SERVICE_STORAGE = "false"
    SCM_DO_BUILD_DURING_DEPLOYMENT      = "true"

    BLOB_CONNECTION_STRING = azurerm_storage_account.main.primary_connection_string
    BLOB_CONTAINER_NAME    = azurerm_storage_container.input.name

    SQL_HOST     = azurerm_mssql_server.main.fully_qualified_domain_name
    SQL_DATABASE = azurerm_mssql_database.main.name
    SQL_USER     = var.sql_admin_username
    SQL_PASSWORD = var.sql_admin_password
  }

  tags = local.tags
}

resource "azurerm_linux_function_app" "pipeline" {
  name                = local.function_app_name
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  storage_account_name       = azurerm_storage_account.main.name
  storage_account_access_key = azurerm_storage_account.main.primary_access_key
  service_plan_id            = azurerm_service_plan.main.id

  site_config {
    always_on = true

    application_stack {
      python_version = "3.11"
    }
  }

  app_settings = {
    FUNCTIONS_WORKER_RUNTIME       = "python"
    FUNCTIONS_EXTENSION_VERSION    = "~4"
    SCM_DO_BUILD_DURING_DEPLOYMENT = "true"
    ENABLE_ORYX_BUILD              = "true"

    BLOB_STORAGE_CONNECTION_STRING = azurerm_storage_account.main.primary_connection_string
    BLOB_INPUT_CONTAINER           = azurerm_storage_container.input.name

    SQL_HOST     = azurerm_mssql_server.main.fully_qualified_domain_name
    SQL_DATABASE = azurerm_mssql_database.main.name
    SQL_USER     = var.sql_admin_username
    SQL_PASSWORD = var.sql_admin_password
  }

  tags = local.tags
}
