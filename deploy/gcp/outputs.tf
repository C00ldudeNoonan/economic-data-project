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
