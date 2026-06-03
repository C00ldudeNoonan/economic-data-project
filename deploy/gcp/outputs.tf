output "instance_name" {
  description = "Name of the Dagster VM instance"
  value       = google_compute_instance.dagster.name
}

output "instance_zone" {
  description = "Zone of the Dagster VM instance"
  value       = google_compute_instance.dagster.zone
}

output "external_ip" {
  description = "External IP address of the Dagster VM"
  value       = google_compute_address.dagster.address
}

output "iap_tunnel_command" {
  description = "Command to create IAP tunnel to Dagster UI"
  value       = "gcloud compute start-iap-tunnel ${google_compute_instance.dagster.name} 3000 --local-host-port=localhost:3000 --zone=${var.zone}"
}

output "ssh_command" {
  description = "Command to SSH into the VM via IAP"
  value       = "gcloud compute ssh ${google_compute_instance.dagster.name} --zone=${var.zone} --tunnel-through-iap"
}

output "service_account_email" {
  description = "Service account email for the VM"
  value       = google_service_account.dagster_vm.email
}

output "iceberg_bucket_name" {
  description = "GCS bucket for Iceberg table storage"
  value       = google_storage_bucket.iceberg_data.name
}

output "biglake_connection_id" {
  description = "BigLake connection ID for Iceberg"
  value       = google_bigquery_connection.biglake.connection_id
}

output "biglake_service_account" {
  description = "Service account email used by BigLake connection"
  value       = google_bigquery_connection.biglake.cloud_resource[0].service_account_id
}

output "bigquery_datasets" {
  description = "BigQuery dataset IDs created for the warehouse"
  value       = [for ds in google_bigquery_dataset.economics : ds.dataset_id]
}
