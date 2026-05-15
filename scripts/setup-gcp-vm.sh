#!/bin/bash
# Setup script for Dagster OSS on GCP Compute Engine
# This script provisions and configures a GCP VM for running Dagster OSS with Docker

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration variables (customize these)
PROJECT_ID="${GCP_PROJECT_ID:-your-gcp-project-id}"
VM_NAME="${DAGSTER_VM_NAME:-dagster-oss-vm}"
ZONE="${GCP_ZONE:-us-central1-a}"
REGION="${GCP_REGION:-us-central1}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_SIZE="${BOOT_DISK_SIZE:-100GB}"
POSTGRES_DISK_SIZE="${POSTGRES_DISK_SIZE:-50GB}"

echo -e "${GREEN}=== Dagster OSS GCP Setup ===${NC}"
echo "Project ID: $PROJECT_ID"
echo "VM Name: $VM_NAME"
echo "Zone: $ZONE"
echo "Machine Type: $MACHINE_TYPE"
echo ""

# Function to print status messages
print_status() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    print_error "gcloud CLI not found. Please install Google Cloud SDK first."
    exit 1
fi

# Set project
print_status "Setting GCP project to $PROJECT_ID..."
gcloud config set project "$PROJECT_ID"

# Step 1: Create VM instance
print_status "Creating VM instance: $VM_NAME..."
if gcloud compute instances describe "$VM_NAME" --zone="$ZONE" &> /dev/null; then
    print_warning "VM $VM_NAME already exists. Skipping creation."
else
    gcloud compute instances create "$VM_NAME" \
        --project="$PROJECT_ID" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --boot-disk-size="$BOOT_DISK_SIZE" \
        --boot-disk-type=pd-ssd \
        --image-family=ubuntu-2204-lts \
        --image-project=ubuntu-os-cloud \
        --scopes=https://www.googleapis.com/auth/devstorage.read_write,https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/cloud-platform.read-only \
        --tags=dagster,http-server \
        --metadata=enable-oslogin=TRUE

    print_status "VM created successfully!"
fi

# Step 2: Create and attach persistent disk for PostgreSQL
print_status "Creating persistent disk for PostgreSQL..."
if gcloud compute disks describe "dagster-postgres-data" --zone="$ZONE" &> /dev/null; then
    print_warning "Disk dagster-postgres-data already exists. Skipping creation."
else
    gcloud compute disks create dagster-postgres-data \
        --project="$PROJECT_ID" \
        --type=pd-ssd \
        --size="$POSTGRES_DISK_SIZE" \
        --zone="$ZONE"

    print_status "Attaching disk to VM..."
    gcloud compute instances attach-disk "$VM_NAME" \
        --disk=dagster-postgres-data \
        --zone="$ZONE"

    print_status "Disk created and attached!"
fi

# Step 3: Configure firewall rules
print_status "Configuring firewall rules..."
if gcloud compute firewall-rules describe allow-dagster-ui &> /dev/null; then
    print_warning "Firewall rule allow-dagster-ui already exists. Skipping creation."
else
    gcloud compute firewall-rules create allow-dagster-ui \
        --project="$PROJECT_ID" \
        --direction=INGRESS \
        --priority=1000 \
        --network=default \
        --action=ALLOW \
        --rules=tcp:3000 \
        --source-ranges=0.0.0.0/0 \
        --target-tags=dagster

    print_status "Firewall rules created!"
fi

# Step 4: Wait for VM to be ready
print_status "Waiting for VM to be ready..."
sleep 30

# Step 5: Install Docker and dependencies on VM
print_status "Installing Docker and dependencies on VM..."
gcloud compute ssh "$VM_NAME" --zone="$ZONE" --command="
    # Update system
    sudo apt-get update

    # Install prerequisites
    sudo apt-get install -y \
        ca-certificates \
        curl \
        gnupg \
        lsb-release

    # Add Docker GPG key
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
        sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

    # Add Docker repository
    echo \
      \"deb [arch=\$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
      https://download.docker.com/linux/ubuntu \
      \$(lsb_release -cs) stable\" | \
      sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    # Install Docker
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    # Add user to docker group
    sudo usermod -aG docker \$USER

    echo 'Docker installation completed!'
"

print_status "Docker installed successfully!"

# Step 6: Format and mount PostgreSQL disk
print_status "Setting up PostgreSQL data disk..."
gcloud compute ssh "$VM_NAME" --zone="$ZONE" --command="
    # Check if disk is already formatted
    if ! sudo blkid /dev/sdb; then
        echo 'Formatting disk /dev/sdb...'
        sudo mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb
    else
        echo 'Disk /dev/sdb already formatted. Skipping format.'
    fi

    # Create mount point
    sudo mkdir -p /mnt/dagster-postgres

    # Mount disk
    if ! mountpoint -q /mnt/dagster-postgres; then
        sudo mount -o discard,defaults /dev/sdb /mnt/dagster-postgres
        echo 'Disk mounted successfully.'
    else
        echo 'Disk already mounted.'
    fi

    # Add to fstab for automatic mounting
    if ! grep -q '/mnt/dagster-postgres' /etc/fstab; then
        echo '/dev/sdb /mnt/dagster-postgres ext4 discard,defaults,nofail 0 2' | sudo tee -a /etc/fstab
    fi

    # Create PostgreSQL data directory
    sudo mkdir -p /mnt/dagster-postgres/data
    sudo chown -R 999:999 /mnt/dagster-postgres/data

    echo 'PostgreSQL disk setup completed!'
"

print_status "PostgreSQL disk configured successfully!"

# Step 7: Create dagster directory on VM
print_status "Creating dagster directory on VM..."
gcloud compute ssh "$VM_NAME" --zone="$ZONE" --command="mkdir -p ~/dagster"

# Get VM external IP
EXTERNAL_IP=$(gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

# Summary
echo ""
echo -e "${GREEN}=== Setup Complete! ===${NC}"
echo ""
echo "VM Name: $VM_NAME"
echo "External IP: $EXTERNAL_IP"
echo "SSH Command: gcloud compute ssh $VM_NAME --zone=$ZONE"
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "1. Copy your .env file to the VM:"
echo "   gcloud compute scp .env $VM_NAME:~/dagster/ --zone=$ZONE"
echo ""
echo "2. Copy your GCP service account JSON to the VM:"
echo "   gcloud compute scp gcp-service-account.json $VM_NAME:~/dagster/ --zone=$ZONE"
echo ""
echo "3. Copy project files to the VM:"
echo "   gcloud compute scp docker-compose.yml dagster.yaml workspace.yaml $VM_NAME:~/dagster/ --zone=$ZONE"
echo "   gcloud compute scp --recurse macro_agents $VM_NAME:~/dagster/ --zone=$ZONE"
echo "   gcloud compute scp --recurse dbt_project $VM_NAME:~/dagster/ --zone=$ZONE"
echo ""
echo "4. SSH into the VM and start Dagster:"
echo "   gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "   cd ~/dagster"
echo "   docker compose up -d"
echo ""
echo "5. Access Dagster UI at: http://$EXTERNAL_IP:3000"
echo ""
echo -e "${GREEN}Setup completed successfully!${NC}"
