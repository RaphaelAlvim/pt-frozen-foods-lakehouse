output "storage_account_name" {
  description = "Name of the created storage account."
  value       = azurerm_storage_account.this.name
}

output "storage_account_id" {
  description = "ID of the created storage account."
  value       = azurerm_storage_account.this.id
}

output "primary_dfs_endpoint" {
  description = "Primary DFS endpoint of the ADLS Gen2 storage account."
  value       = azurerm_storage_account.this.primary_dfs_endpoint
}

output "filesystem_names" {
  description = "Names of the created ADLS Gen2 filesystems."
  value       = [for filesystem in azurerm_storage_data_lake_gen2_filesystem.this : filesystem.name]
}