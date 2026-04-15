output "access_connector_id" {
  description = "ID of the Azure Databricks Access Connector."
  value       = azurerm_databricks_access_connector.this.id
}

output "access_connector_name" {
  description = "Name of the Azure Databricks Access Connector."
  value       = azurerm_databricks_access_connector.this.name
}

output "access_connector_principal_id" {
  description = "Principal ID of the system-assigned managed identity."
  value       = azurerm_databricks_access_connector.this.identity[0].principal_id
}

output "access_connector_tenant_id" {
  description = "Tenant ID of the system-assigned managed identity."
  value       = azurerm_databricks_access_connector.this.identity[0].tenant_id
}