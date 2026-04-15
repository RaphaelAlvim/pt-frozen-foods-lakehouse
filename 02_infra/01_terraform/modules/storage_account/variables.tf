variable "storage_account_name" {
  description = "Name of the ADLS Gen2 storage account."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Resource Group where the storage account will be created."
  type        = string
}

variable "location" {
  description = "Azure region where the storage account will be created."
  type        = string
}

variable "containers" {
  description = "List of ADLS Gen2 filesystem names to create."
  type        = list(string)
}

variable "tags" {
  description = "Tags applied to the storage account."
  type        = map(string)
  default     = {}
}