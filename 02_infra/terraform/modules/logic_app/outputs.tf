output "logic_app_id" {
  description = "ID of the Logic App"
  value       = azurerm_logic_app_workflow.this.id
}

output "logic_app_name" {
  description = "Name of the Logic App"
  value       = azurerm_logic_app_workflow.this.name
}

output "logic_app_principal_id" {
  description = "System-assigned managed identity principal ID"
  value       = azurerm_logic_app_workflow.this.identity[0].principal_id
}

output "logic_app_tenant_id" {
  description = "System-assigned managed identity tenant ID"
  value       = azurerm_logic_app_workflow.this.identity[0].tenant_id
}