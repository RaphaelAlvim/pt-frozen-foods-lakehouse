output "log_analytics_workspace_name" {
  description = "Name of the created Log Analytics workspace."
  value       = azurerm_log_analytics_workspace.this.name
}

output "log_analytics_workspace_id" {
  description = "ID of the created Log Analytics workspace."
  value       = azurerm_log_analytics_workspace.this.id
}

output "log_analytics_workspace_guid" {
  description = "Workspace ID of the created Log Analytics workspace."
  value       = azurerm_log_analytics_workspace.this.workspace_id
}