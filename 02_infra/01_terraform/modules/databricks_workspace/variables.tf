variable "databricks_workspace_name" {
  description = "Name of the Azure Databricks workspace."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Resource Group where the Databricks workspace will be created."
  type        = string
}

variable "location" {
  description = "Azure region where the Databricks workspace will be created."
  type        = string
}

variable "sku" {
  description = "SKU of the Azure Databricks workspace. Allowed values are standard, premium, or trial."
  type        = string
  default     = "standard"
}

variable "managed_resource_group_name" {
  description = "Name of the managed Resource Group created by Azure Databricks."
  type        = string
}

variable "tags" {
  description = "Tags applied to the Databricks workspace."
  type        = map(string)
  default     = {}
}