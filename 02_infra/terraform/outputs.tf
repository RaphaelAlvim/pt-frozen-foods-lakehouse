output "resource_group_name" {
  description = "Name of the created Resource Group."
  value       = module.resource_group.resource_group_name
}

output "resource_group_id" {
  description = "ID of the created Resource Group."
  value       = module.resource_group.resource_group_id
}

output "storage_account_name" {
  description = "Name of the created ADLS Gen2 storage account."
  value       = module.storage_account.storage_account_name
}

output "storage_account_id" {
  description = "ID of the created ADLS Gen2 storage account."
  value       = module.storage_account.storage_account_id
}

output "primary_dfs_endpoint" {
  description = "Primary DFS endpoint of the created ADLS Gen2 storage account."
  value       = module.storage_account.primary_dfs_endpoint
}

output "filesystem_names" {
  description = "Names of the created ADLS Gen2 filesystems."
  value       = module.storage_account.filesystem_names
}