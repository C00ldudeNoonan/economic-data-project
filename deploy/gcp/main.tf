terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "compute" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iap" {
  service            = "iap.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "secretmanager" {
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "biglake" {
  service            = "biglake.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigqueryconnection" {
  service            = "bigqueryconnection.googleapis.com"
  disable_on_destroy = false
}

# Store secrets in Secret Manager
resource "google_secret_manager_secret" "dagster_env" {
  secret_id = "dagster-env"

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "dagster_env" {
  secret = google_secret_manager_secret.dagster_env.id

  secret_data = <<-EOF
    DAGSTER_PG_PASSWORD=${var.dagster_pg_password}
    BIGQUERY_PROJECT=${var.bigquery_project}
    BIGQUERY_LOCATION=${var.bigquery_location}
    DBT_TARGET=prod
    FRED_API_KEY=${var.fred_api_key}
    OPENAI_API_KEY=${var.openai_api_key}
    ANTHROPIC_API_KEY=${var.anthropic_api_key}
  EOF
}

# Service account for the VM
resource "google_service_account" "dagster_vm" {
  account_id   = "dagster-vm"
  display_name = "Dagster VM Service Account"
}

# Grant secret access to VM service account
resource "google_secret_manager_secret_iam_member" "dagster_env_access" {
  secret_id = google_secret_manager_secret.dagster_env.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dagster_vm.email}"
}

# Static external IP for the VM
resource "google_compute_address" "dagster" {
  name   = "dagster-ip"
  region = var.region

  depends_on = [google_project_service.compute]
}

# Firewall rule - only allow IAP traffic
resource "google_compute_firewall" "allow_iap" {
  name    = "allow-iap-to-dagster"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22", "3000"]
  }

  # IAP's IP range
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["dagster-server"]

  depends_on = [google_project_service.compute]
}

# Firewall rule - deny direct access to port 3000
resource "google_compute_firewall" "deny_direct" {
  name     = "deny-direct-dagster"
  network  = "default"
  priority = 1000

  deny {
    protocol = "tcp"
    ports    = ["3000"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["dagster-server"]

  depends_on = [google_project_service.compute]
}

# VM instance
resource "google_compute_instance" "dagster" {
  name         = "dagster-server"
  machine_type = var.machine_type
  zone         = var.zone

  tags = ["dagster-server"]

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable"
      size  = var.disk_size_gb
      type  = "pd-ssd"
    }
  }

  network_interface {
    network = "default"

    access_config {
      nat_ip = google_compute_address.dagster.address
    }
  }

  service_account {
    email = google_service_account.dagster_vm.email
    # Narrow the OAuth scope envelope to what the VM actually needs. The
    # service account's IAM roles still gate API access, but constraining
    # scopes here means a compromised process on the VM can't request
    # tokens for unrelated APIs (Compute Engine admin, Cloud SQL, etc.).
    scopes = [
      "https://www.googleapis.com/auth/devstorage.read_write",
      "https://www.googleapis.com/auth/bigquery",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring.write",
      "https://www.googleapis.com/auth/cloud-platform.read-only",
    ]
  }

  metadata = {
    google-logging-enabled = "true"
    # Startup script to install Docker and clone repo
    startup-script = file("${path.module}/startup.sh")
  }

  # Allow stopping for updates
  allow_stopping_for_update = true

  depends_on = [
    google_project_service.compute,
    google_secret_manager_secret_version.dagster_env
  ]
}

# IAP OAuth brand (consent screen) - may need manual setup
resource "google_iap_brand" "dagster" {
  support_email     = var.authorized_email
  application_title = "Dagster Economic Data"
  project           = var.project_id

  depends_on = [google_project_service.iap]
}

# IAP OAuth client
resource "google_iap_client" "dagster" {
  display_name = "Dagster Web UI"
  brand        = google_iap_brand.dagster.name
}

# IAP tunnel access for authorized user
resource "google_iap_tunnel_iam_member" "dagster_access" {
  project = var.project_id
  role    = "roles/iap.tunnelResourceAccessor"
  member  = "user:${var.authorized_email}"

  depends_on = [google_project_service.iap]
}

# Grant SSH access through IAP
resource "google_compute_instance_iam_member" "ssh_access" {
  project       = var.project_id
  zone          = var.zone
  instance_name = google_compute_instance.dagster.name
  role          = "roles/compute.osLogin"
  member        = "user:${var.authorized_email}"
}

# GCS bucket for Iceberg table storage
resource "google_storage_bucket" "iceberg_data" {
  name                        = var.iceberg_bucket_name
  location                    = var.bigquery_location
  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }
}

# BigQuery datasets
locals {
  bigquery_datasets = [
    # prod datasets (canonical)
    "economics_raw",
    "economics_staging",
    "economics_marts",
    "economics_signals",
    "economics_analysis",
    "economics_backtesting",
    # staging environment datasets
    "economics_raw_staging",
    "economics_staging_staging",
    "economics_marts_staging",
    "economics_signals_staging",
    "economics_analysis_staging",
    "economics_backtesting_staging",
    # dev environment datasets
    "economics_raw_dev",
    "economics_staging_dev",
    "economics_marts_dev",
    "economics_signals_dev",
    "economics_analysis_dev",
    "economics_backtesting_dev",
  ]
}

resource "google_bigquery_dataset" "economics" {
  for_each   = toset(local.bigquery_datasets)
  dataset_id = each.key
  location   = var.bigquery_location
  project    = var.bigquery_project

  depends_on = [google_project_service.bigquery]
}

# BigLake connection for Iceberg
resource "google_bigquery_connection" "biglake" {
  connection_id = "biglake-iceberg"
  location      = var.bigquery_location
  project       = var.bigquery_project

  cloud_resource {}

  depends_on = [google_project_service.bigqueryconnection]
}

# Grant BigLake connection service account objectAdmin on Iceberg bucket
resource "google_storage_bucket_iam_member" "biglake_object_admin" {
  bucket = google_storage_bucket.iceberg_data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_bigquery_connection.biglake.cloud_resource[0].service_account_id}"
}

# Grant Dagster VM service account BigQuery data editor
resource "google_project_iam_member" "dagster_bq_data_editor" {
  project = var.bigquery_project
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dagster_vm.email}"
}

# Grant Dagster VM service account BigQuery job user
resource "google_project_iam_member" "dagster_bq_job_user" {
  project = var.bigquery_project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dagster_vm.email}"
}
