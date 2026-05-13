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
    MOTHERDUCK_TOKEN=${var.motherduck_token}
    MOTHERDUCK_DATABASE=${var.motherduck_database}
    MOTHERDUCK_PROD_SCHEMA=${var.motherduck_prod_schema}
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
    email  = google_service_account.dagster_vm.email
    scopes = ["cloud-platform"]
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
