variable "log_analytics_workspace_name" {
  description = "Name of the Log Analytics workspace."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Resource Group where the Log Analytics workspace will be created."
  type        = string
}

variable "location" {
  description = "Azure region where the Log Analytics workspace will be created."
  type        = string
}

variable "tags" {
  description = "Tags applied to the Log Analytics workspace."
  type        = map(string)
  default     = {}
}