output "data_factory_name" {
  description = "Name of the created Data Factory."
  value       = azurerm_data_factory.this.name
}

output "data_factory_id" {
  description = "ID of the created Data Factory."
  value       = azurerm_data_factory.this.id
}