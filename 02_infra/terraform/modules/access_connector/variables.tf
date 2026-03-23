variable "access_connector_name" {
  description = "Name of the Azure Databricks Access Connector."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Resource Group where the Access Connector will be created."
  type        = string
}

variable "location" {
  description = "Azure region where the Access Connector will be created."
  type        = string
}

variable "tags" {
  description = "Standard tags applied to the Access Connector."
  type        = map(string)
  default     = {}
}