output "resource_group_name" {
  description = "Name of the created Resource Group."
  value       = azurerm_resource_group.this.name
}

output "resource_group_id" {
  description = "ID of the created Resource Group."
  value       = azurerm_resource_group.this.id
}

output "location" {
  description = "Azure region of the created Resource Group."
  value       = azurerm_resource_group.this.location
}