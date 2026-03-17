data "azurerm_client_config" "current" {}

module "resource_group" {
  source = "./modules/resource_group"

  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = var.tags
}

module "storage_account" {
  source = "./modules/storage_account"

  storage_account_name = var.storage_account_name
  resource_group_name  = module.resource_group.resource_group_name
  location             = module.resource_group.location
  containers           = var.storage_containers
  tags                 = var.tags
} 

module "key_vault" {
  source = "./modules/key_vault"

  key_vault_name      = var.key_vault_name
  resource_group_name = module.resource_group.resource_group_name
  location            = module.resource_group.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  tags                = var.tags
}

module "monitoring" {
  source = "./modules/monitoring"

  log_analytics_workspace_name = var.log_analytics_workspace_name
  resource_group_name          = module.resource_group.resource_group_name
  location                     = module.resource_group.location
  tags                         = var.tags
}

module "data_factory" {
  source = "./modules/data_factory"

  data_factory_name   = var.data_factory_name
  resource_group_name = module.resource_group.resource_group_name
  location            = module.resource_group.location
  tags                = var.tags
}

module "databricks_workspace" {
  source = "./modules/databricks_workspace"

  databricks_workspace_name   = var.databricks_workspace_name
  resource_group_name         = module.resource_group.resource_group_name
  location                    = module.resource_group.location
  sku                         = var.databricks_sku
  managed_resource_group_name = var.databricks_managed_resource_group_name
  tags                        = var.tags
}