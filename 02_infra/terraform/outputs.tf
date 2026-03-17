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

output "key_vault_name" {
  description = "Name of the created Key Vault."
  value       = module.key_vault.key_vault_name
}

output "key_vault_id" {
  description = "ID of the created Key Vault."
  value       = module.key_vault.key_vault_id
}

output "key_vault_uri" {
  description = "URI of the created Key Vault."
  value       = module.key_vault.vault_uri
}

output "log_analytics_workspace_name" {
  description = "Name of the created Log Analytics workspace."
  value       = module.monitoring.log_analytics_workspace_name
}

output "log_analytics_workspace_id" {
  description = "ID of the created Log Analytics workspace."
  value       = module.monitoring.log_analytics_workspace_id
}

output "log_analytics_workspace_guid" {
  description = "Workspace ID of the created Log Analytics workspace."
  value       = module.monitoring.log_analytics_workspace_guid
}