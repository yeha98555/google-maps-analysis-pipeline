variable "project_id" {
  description = "The ID of the GCP project"
  type        = string
}

variable "common_tags" {
  description = "Common tags for resources"
  type        = map(string)
}

variable "airflow_sa_email" {
  description = "Email of the Airflow service account"
  type        = string
}

variable "scheduler_cpu" {
  description = "The number of CPUs used by the scheduler"
  type = number
  default = 1
}

variable "scheduler_memory_gb" {
  description = "The amount of memory used by the scheduler"
  type = number
  default = 4
}

variable "scheduler_storage_gb" {
  description = "The amount of storage used by the scheduler"
  type = number
  default = 10
}

variable "web_server_cpu" {
  description = "The number of CPUs used by the web server"
  type = number
  default = 1
}

variable "web_server_memory_gb" {
  description = "The amount of memory used by the web server"
  type = number
  default = 4
}

variable "web_server_storage_gb" {
  description = "The amount of storage used by the web server"
  type = number
  default = 10
}

variable "worker_cpu" {
  description = "The number of CPUs used by the worker"
  type = number
  default = 1
}

variable "worker_memory_gb" {
  description = "The amount of memory used by the worker"
  type = number
  default = 4
}

variable "worker_storage_gb" {
  description = "The amount of storage used by the worker"
  type = number
  default = 10
}

variable "worker_min_count" {
  description = "The minimum number of workers"
  type = number
  default = 1  
}

variable "worker_max_count" {
  description = "The maximum number of workers"
  type = number
  default = 3
}

variable "environment_size" {
  description = "The size of the environment"
  type = string
  default = "ENVIRONMENT_SIZE_SMALL"
}
