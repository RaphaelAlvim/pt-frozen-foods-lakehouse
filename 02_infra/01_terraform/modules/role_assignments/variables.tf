variable "storage_account_id" {
  description = "ID of the Storage Account where RBAC will be assigned."
  type        = string
}

variable "principal_id" {
  description = "Principal ID of the identity receiving the RBAC assignment."
  type        = string
}