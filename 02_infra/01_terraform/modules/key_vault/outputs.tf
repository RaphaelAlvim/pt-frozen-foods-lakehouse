output "key_vault_name" {
  description = "Name of the created Key Vault."
  value       = azurerm_key_vault.this.name
}

output "key_vault_id" {
  description = "ID of the created Key Vault."
  value       = azurerm_key_vault.this.id
}

output "vault_uri" {
  description = "Vault URI of the created Key Vault."
  value       = azurerm_key_vault.this.vault_uri
}