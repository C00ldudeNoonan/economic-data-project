variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "us-central1-a"
}

variable "authorized_email" {
  description = "Email address authorized to access the Dagster UI"
  type        = string
}

variable "machine_type" {
  description = "GCE machine type"
  type        = string
  default     = "e2-standard-4" # 4 vCPU, 16GB RAM
}

variable "disk_size_gb" {
  description = "Boot disk size in GB"
  type        = number
  default     = 50
}

variable "dagster_pg_password" {
  description = "PostgreSQL password for Dagster"
  type        = string
  sensitive   = true
}

# Environment variables for the application
variable "motherduck_token" {
  description = "MotherDuck API token"
  type        = string
  sensitive   = true
}

variable "motherduck_database" {
  description = "MotherDuck database name"
  type        = string
  default     = "economic_data"
}

variable "motherduck_prod_schema" {
  description = "MotherDuck production schema"
  type        = string
  default     = "main"
}

variable "fred_api_key" {
  description = "FRED API key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "openai_api_key" {
  description = "OpenAI API key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "anthropic_api_key" {
  description = "Anthropic API key"
  type        = string
  sensitive   = true
  default     = ""
}
