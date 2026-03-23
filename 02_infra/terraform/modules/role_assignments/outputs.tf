output "storage_blob_data_contributor_role_assignment_id" {
  description = "ID of the Storage Blob Data Contributor role assignment."
  value       = azurerm_role_assignment.storage_blob_data_contributor.id
}