variable "project_name" {
  description = "Short project name used in resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment, for example: dev, qa, prod."
  type        = string
}

variable "location" {
  description = "Azure region where resources will be deployed."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the main Resource Group."
  type        = string
}

variable "storage_account_name" {
  description = "Name of the lake Storage Account. Must be globally unique, lowercase, and without hyphens."
  type        = string
}

variable "data_factory_name" {
  description = "Name of the Azure Data Factory instance."
  type        = string
}

variable "databricks_workspace_name" {
  description = "Name of the Azure Databricks workspace."
  type        = string
}

variable "databricks_sku" {
  description = "SKU for the Azure Databricks workspace."
  type        = string
  default     = "standard"
}

variable "databricks_managed_resource_group_name" {
  description = "Managed Resource Group name for Azure Databricks."
  type        = string
}

variable "key_vault_name" {
  description = "Name of the Azure Key Vault."
  type        = string
}

variable "log_analytics_workspace_name" {
  description = "Name of the Log Analytics workspace."
  type        = string
}

variable "storage_containers" {
  description = "List of lake containers/filesystems to be created in ADLS Gen2."
  type        = list(string)
  default     = ["raw", "bronze", "silver", "gold"]
}

variable "logic_app_name" {
  description = "Name of the Logic App workflow"
  type        = string
}

variable "access_connector_name" {
  description = "Name of the Azure Databricks Access Connector."
  type        = string
}

variable "tags" {
  description = "Standard tags applied to all resources."
  type        = map(string)
  default     = {}
}