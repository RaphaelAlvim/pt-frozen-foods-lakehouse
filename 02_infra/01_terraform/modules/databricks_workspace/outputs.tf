output "databricks_workspace_name" {
  description = "Name of the created Azure Databricks workspace."
  value       = azurerm_databricks_workspace.this.name
}

output "databricks_workspace_id" {
  description = "ID of the created Azure Databricks workspace."
  value       = azurerm_databricks_workspace.this.id
}

output "workspace_url" {
  description = "Workspace URL of the created Azure Databricks workspace."
  value       = azurerm_databricks_workspace.this.workspace_url
}

output "managed_resource_group_id" {
  description = "Managed Resource Group ID created for the Databricks workspace."
  value       = azurerm_databricks_workspace.this.managed_resource_group_id
}