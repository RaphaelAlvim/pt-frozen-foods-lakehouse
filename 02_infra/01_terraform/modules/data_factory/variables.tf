variable "data_factory_name" {
  description = "Name of the Azure Data Factory instance."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Resource Group where the Data Factory will be created."
  type        = string
}

variable "location" {
  description = "Azure region where the Data Factory will be created."
  type        = string
}

variable "tags" {
  description = "Tags applied to the Data Factory."
  type        = map(string)
  default     = {}
}