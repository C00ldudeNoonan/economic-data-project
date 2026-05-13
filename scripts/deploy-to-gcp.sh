#!/bin/bash
# Deploy Dagster code to existing GCP VM
# This script copies the latest code and restarts Dagster services

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
VM_NAME="${DAGSTER_VM_NAME:-dagster-oss-vm}"
ZONE="${GCP_ZONE:-us-central1-a}"

print_status() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_status "Starting deployment to $VM_NAME..."

# Step 1: Copy files to VM
print_status "Copying configuration files..."
gcloud compute scp docker-compose.yml "$VM_NAME":~/dagster/ --zone="$ZONE"
gcloud compute scp dagster.yaml "$VM_NAME":~/dagster/ --zone="$ZONE"
gcloud compute scp workspace.yaml "$VM_NAME":~/dagster/ --zone="$ZONE"

print_status "Copying application code..."
gcloud compute scp --recurse macro_agents "$VM_NAME":~/dagster/ --zone="$ZONE"
gcloud compute scp --recurse dbt_project "$VM_NAME":~/dagster/ --zone="$ZONE"

# Step 2: Deploy on VM
print_status "Deploying services on VM..."
gcloud compute ssh "$VM_NAME" --zone="$ZONE" --command='
    cd ~/dagster

    # Pull latest base images
    docker compose pull dagster_webserver dagster_daemon

    # Stop existing containers
    docker compose down

    # Build new user code image
    docker compose build dagster_user_code

    # Start all services
    docker compose up -d

    # Wait for services
    sleep 15

    # Check status
    docker compose ps
'

# Step 3: Verify deployment
print_status "Verifying deployment..."
EXTERNAL_IP=$(gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

sleep 10

if curl -f -s -o /dev/null "http://$EXTERNAL_IP:3000"; then
    echo -e "${GREEN}✅ Deployment successful!${NC}"
    echo "Dagster UI: http://$EXTERNAL_IP:3000"
else
    echo -e "${YELLOW}⚠️ Deployment completed but health check failed${NC}"
    echo "Check logs with: gcloud compute ssh $VM_NAME --zone=$ZONE --command='cd ~/dagster && docker compose logs'"
fi
