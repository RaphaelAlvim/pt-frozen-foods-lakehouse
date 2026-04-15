variable "logic_app_name" {
  description = "Name of the Logic App workflow"
  type        = string
}

variable "location" {
  description = "Azure region for the Logic App"
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name where the Logic App will be deployed"
  type        = string
}

variable "tags" {
  description = "Tags to apply to the Logic App"
  type        = map(string)
  default     = {}
}