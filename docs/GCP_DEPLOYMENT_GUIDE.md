# GCP Deployment Guide

Deploy Dagster on a Compute Engine VM with your frontend on Cloud Run.

## Architecture Overview

```
                    Internet
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────────┐         ┌───────────────────┐
│    Cloud Run      │         │  Compute Engine   │
│   (Frontend App)  │         │  (Dagster Stack)  │
│   Port 443        │         │  Port 3000        │
└────────┬──────────┘         └────────┬──────────┘
         │                             │
         │      reads                  │ writes
         ▼                             ▼
┌─────────────────────────────────────────────────┐
│         MotherDuck / BigQuery                   │
│              (Data Warehouse)                   │
└─────────────────────────────────────────────────┘
```

## Prerequisites

- GCP account with billing enabled
- `gcloud` CLI installed locally
- Docker and Docker Compose (for local testing)
- Your `.env` file with all secrets

## Cost Estimate

| Component | Spec | Monthly Cost |
|-----------|------|--------------|
| Compute Engine | e2-small (2 vCPU, 2GB) | ~$15 |
| Cloud Run | Frontend (scales to zero) | ~$0-5 |
| Cloud Storage | Artifacts, backups | ~$1-2 |
| Static IP | For VM | ~$3 |
| **Total** | | **~$20-25/mo** |

---

## Part 1: Initial GCP Setup

### 1.1 Create Project & Enable APIs

```bash
# Set your project ID
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export ZONE="us-central1-a"

# Create project (or use existing)
gcloud projects create $PROJECT_ID --name="Economic Data Project"

# Set as default
gcloud config set project $PROJECT_ID

# Enable required APIs
gcloud services enable \
    compute.googleapis.com \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    secretmanager.googleapis.com \
    artifactregistry.googleapis.com
```

### 1.2 Create Artifact Registry (for Docker images)

```bash
# Create repository for Docker images
gcloud artifacts repositories create dagster-repo \
    --repository-format=docker \
    --location=$REGION \
    --description="Dagster Docker images"

# Configure Docker to use Artifact Registry
gcloud auth configure-docker $REGION-docker.pkg.dev
```

### 1.3 Create Service Account

```bash
# Create service account for the VM
gcloud iam service-accounts create dagster-vm \
    --display-name="Dagster VM Service Account"

# Grant necessary permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:dagster-vm@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:dagster-vm@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:dagster-vm@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
```

---

## Part 2: Store Secrets in Secret Manager

### 2.1 Create Secrets

```bash
# Create secrets from your .env file
# Run these for each secret you need

echo -n "your-motherduck-token" | \
    gcloud secrets create MOTHERDUCK_TOKEN --data-file=-

echo -n "your-fred-api-key" | \
    gcloud secrets create FRED_API_KEY --data-file=-

echo -n "your-openai-key" | \
    gcloud secrets create OPENAI_API_KEY --data-file=-

# For the GCP service account JSON (multiline)
gcloud secrets create GCP_SERVICE_ACCOUNT \
    --data-file=./gcp-service-account.json
```

### 2.2 Create .env Template for VM

Create `docker/.env.gcp` with secret references:

```bash
# Dagster PostgreSQL (local to VM)
DAGSTER_PG_PASSWORD=secure-local-password

# MotherDuck
MOTHERDUCK_TOKEN=${MOTHERDUCK_TOKEN}
MOTHERDUCK_DATABASE=econ_agent
MOTHERDUCK_PROD_SCHEMA=main

# Environment
ENVIRONMENT=prod
DBT_TARGET=prod
DBT_PROJECT_DIR=/opt/dagster/dbt_project

# GCP
GCS_BUCKET_NAME=your-bucket-name
BIGQUERY_PROJECT_ID=your-project-id
BIGQUERY_DATASET=economic_data

# API Keys (populated from Secret Manager)
FRED_API_KEY=${FRED_API_KEY}
OPENAI_API_KEY=${OPENAI_API_KEY}
ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
```

---

## Part 3: Create Compute Engine VM

### 3.1 Create VM Instance

```bash
# Reserve static IP
gcloud compute addresses create dagster-ip \
    --region=$REGION

# Get the IP
export DAGSTER_IP=$(gcloud compute addresses describe dagster-ip \
    --region=$REGION --format='get(address)')
echo "Dagster IP: $DAGSTER_IP"

# Create VM
gcloud compute instances create dagster-vm \
    --zone=$ZONE \
    --machine-type=e2-small \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=30GB \
    --boot-disk-type=pd-ssd \
    --address=$DAGSTER_IP \
    --service-account=dagster-vm@$PROJECT_ID.iam.gserviceaccount.com \
    --scopes=cloud-platform \
    --tags=dagster-server \
    --metadata=startup-script='#!/bin/bash
apt-get update
apt-get install -y docker.io docker-compose-v2
systemctl enable docker
systemctl start docker
usermod -aG docker ubuntu'
```

### 3.2 Configure Firewall

```bash
# Allow Dagster UI access (restrict to your IP for security)
gcloud compute firewall-rules create allow-dagster-ui \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:3000 \
    --source-ranges=YOUR_IP/32 \
    --target-tags=dagster-server

# Allow SSH
gcloud compute firewall-rules create allow-ssh \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:22 \
    --source-ranges=YOUR_IP/32 \
    --target-tags=dagster-server
```

---

## Part 4: Deploy Dagster to VM

### 4.1 SSH into VM

```bash
gcloud compute ssh dagster-vm --zone=$ZONE
```

### 4.2 Clone Repository & Setup

```bash
# On the VM
cd /home/ubuntu

# Clone your repo
git clone https://github.com/C00ldudeNoonan/economic-data-project.git
cd economic-data-project

# Create .env file with your secrets
cat > .env << 'EOF'
DAGSTER_PG_PASSWORD=your-secure-password
MOTHERDUCK_TOKEN=your-token
MOTHERDUCK_DATABASE=econ_agent
MOTHERDUCK_PROD_SCHEMA=main
ENVIRONMENT=prod
DBT_TARGET=prod
DBT_PROJECT_DIR=/opt/dagster/dbt_project
FRED_API_KEY=your-key
# ... add all your env vars
EOF

# Pull secrets from Secret Manager (optional automation)
export MOTHERDUCK_TOKEN=$(gcloud secrets versions access latest --secret=MOTHERDUCK_TOKEN)
```

### 4.3 Build and Start Dagster

```bash
# Build images
docker compose build

# Start services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f dagster_user_code
```

### 4.4 Verify Deployment

```bash
# Check all containers are healthy
docker ps

# Test Dagster UI (from your local machine)
curl http://$DAGSTER_IP:3000
```

Access Dagster UI at: `http://<DAGSTER_IP>:3000`

---

## Part 5: Setup Automatic Deployment (CI/CD)

### 5.1 Create Cloud Build Trigger

Create `cloudbuild.yaml` in your repo:

```yaml
steps:
  # Build Docker image
  - name: 'gcr.io/cloud-builders/docker'
    args: ['compose', 'build', 'dagster_user_code']

  # Push to Artifact Registry
  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', '${_REGION}-docker.pkg.dev/${PROJECT_ID}/dagster-repo/dagster-macro-agents:latest']

  # Deploy to VM via SSH
  - name: 'gcr.io/cloud-builders/gcloud'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        gcloud compute ssh dagster-vm --zone=${_ZONE} --command="
          cd /home/ubuntu/economic-data-project &&
          git pull &&
          docker compose pull &&
          docker compose up -d --force-recreate dagster_user_code &&
          docker compose restart dagster_webserver dagster_daemon
        "

substitutions:
  _REGION: us-central1
  _ZONE: us-central1-a

options:
  logging: CLOUD_LOGGING_ONLY
```

### 5.2 Create Trigger

```bash
gcloud builds triggers create github \
    --repo-name=economic-data-project \
    --repo-owner=C00ldudeNoonan \
    --branch-pattern=^main$ \
    --build-config=cloudbuild.yaml \
    --name=deploy-dagster
```

---

## Part 6: Deploy Frontend to Cloud Run

### 6.1 Build and Push Frontend Image

```bash
# From your local machine
cd frontend

# Build image
docker build -t $REGION-docker.pkg.dev/$PROJECT_ID/dagster-repo/frontend:latest .

# Push to Artifact Registry
docker push $REGION-docker.pkg.dev/$PROJECT_ID/dagster-repo/frontend:latest
```

### 6.2 Deploy to Cloud Run

```bash
gcloud run deploy economic-data-frontend \
    --image=$REGION-docker.pkg.dev/$PROJECT_ID/dagster-repo/frontend:latest \
    --region=$REGION \
    --platform=managed \
    --allow-unauthenticated \
    --memory=512Mi \
    --cpu=1 \
    --min-instances=0 \
    --max-instances=3 \
    --set-env-vars="MOTHERDUCK_TOKEN=your-token,DATABASE=econ_agent"
```

---

## Part 7: Security Hardening

### 7.1 Enable Identity-Aware Proxy (IAP) for Dagster UI

```bash
# Enable IAP API
gcloud services enable iap.googleapis.com

# Configure OAuth consent screen in Cloud Console
# Then enable IAP for the VM
```

### 7.2 Use HTTPS with Managed Certificates

For production, put Dagster behind a load balancer with HTTPS:

```bash
# Create health check
gcloud compute health-checks create http dagster-health \
    --port=3000 \
    --request-path=/health

# Create backend service
gcloud compute backend-services create dagster-backend \
    --protocol=HTTP \
    --health-checks=dagster-health \
    --global

# Add instance group and configure load balancer...
```

### 7.3 Restrict Firewall Rules

```bash
# Update firewall to only allow your IP
gcloud compute firewall-rules update allow-dagster-ui \
    --source-ranges=YOUR_STATIC_IP/32
```

---

## Part 8: Monitoring & Maintenance

### 8.1 Setup Cloud Monitoring

```bash
# Install monitoring agent on VM
# SSH into VM first
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install
```

### 8.2 Create Uptime Check

```bash
gcloud monitoring uptime-check-configs create dagster-uptime \
    --display-name="Dagster UI Uptime" \
    --resource-type=uptime-url \
    --hostname=$DAGSTER_IP \
    --port=3000 \
    --path=/health \
    --check-interval=300
```

### 8.3 Useful Maintenance Commands

```bash
# SSH to VM
gcloud compute ssh dagster-vm --zone=$ZONE

# View logs
docker compose logs -f

# Restart all services
docker compose restart

# Update and redeploy
git pull && docker compose build && docker compose up -d

# Backup PostgreSQL
docker exec dagster_postgresql pg_dump -U dagster_user dagster > backup.sql

# Check disk space
df -h
```

---

## Quick Reference

| Task | Command |
|------|---------|
| SSH to VM | `gcloud compute ssh dagster-vm --zone=us-central1-a` |
| View logs | `docker compose logs -f` |
| Restart Dagster | `docker compose restart` |
| Rebuild & deploy | `docker compose build && docker compose up -d` |
| Check status | `docker compose ps` |
| Dagster UI | `http://<VM_IP>:3000` |

---

## Troubleshooting

### Container won't start
```bash
docker compose logs dagster_user_code
docker compose down && docker compose up -d
```

### Out of disk space
```bash
docker system prune -a
```

### Can't connect to Dagster UI
```bash
# Check firewall
gcloud compute firewall-rules list --filter="name=allow-dagster-ui"

# Check container is running
docker ps | grep webserver
```

### Database connection issues
```bash
# Check PostgreSQL is healthy
docker exec dagster_postgresql pg_isready -U dagster_user
```
