output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = azurerm_storage_account.main.name
}

output "input_container_name" {
  value = azurerm_storage_container.input.name
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.main.fully_qualified_domain_name
}

output "sql_database_name" {
  value = azurerm_mssql_database.main.name
}

output "api_app_name" {
  value = azurerm_linux_web_app.api.name
}

output "api_app_url" {
  value = "https://${azurerm_linux_web_app.api.default_hostname}"
}

output "function_app_name" {
  value = azurerm_linux_function_app.pipeline.name
}
