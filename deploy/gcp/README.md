# GCP Deployment for Dagster

Deploy Dagster on Google Cloud Platform with Identity-Aware Proxy (IAP) for secure single-user access.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Google Cloud                          │
│                                                              │
│  ┌──────────────┐    ┌─────────────────────────────────┐   │
│  │     IAP      │───▶│      Compute Engine VM          │   │
│  │ (Auth Layer) │    │  ┌─────────────────────────┐    │   │
│  └──────────────┘    │  │    Docker Compose       │    │   │
│         ▲            │  │  ┌─────────┐ ┌───────┐  │    │   │
│         │            │  │  │Webserver│ │Daemon │  │    │   │
│  ┌──────┴──────┐     │  │  └────┬────┘ └───┬───┘  │    │   │
│  │  Your Email │     │  │       │          │      │    │   │
│  │  (Allowed)  │     │  │  ┌────┴──────────┴───┐  │    │   │
│  └─────────────┘     │  │  │    PostgreSQL     │  │    │   │
│                      │  │  └───────────────────┘  │    │   │
│                      │  └─────────────────────────┘    │   │
│                      └─────────────────────────────────┘   │
│                                                              │
│  ┌──────────────┐    ┌─────────────────────────────────┐   │
│  │Secret Manager│    │         MotherDuck              │   │
│  │  (API Keys)  │    │     (Data Warehouse)            │   │
│  └──────────────┘    └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

1. **GCP Account** with billing enabled
2. **gcloud CLI** installed and authenticated
3. **Terraform** installed (v1.0+)

```bash
# Authenticate with GCP
gcloud auth login
gcloud auth application-default login

# Set your project
gcloud config set project YOUR_PROJECT_ID
```

## Deployment

### 1. Configure Variables

```bash
cd deploy/gcp
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your values:
- `project_id`: Your GCP project ID
- `authorized_email`: Your email (only this can access Dagster)
- `dagster_pg_password`: A secure password for PostgreSQL
- `motherduck_token`: Your MotherDuck API token
- Other API keys as needed

### 2. Initialize and Deploy

```bash
# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Deploy (takes ~5 minutes)
terraform apply
```

### 3. Access Dagster UI

After deployment, access the Dagster UI through IAP tunnel:

```bash
# Start IAP tunnel (shown in terraform output)
gcloud compute start-iap-tunnel dagster-server 3000 \
  --local-host-port=localhost:3000 \
  --zone=us-central1-a

# Open browser to http://localhost:3000
```

Or SSH into the VM:

```bash
gcloud compute ssh dagster-server --zone=us-central1-a --tunnel-through-iap
```

## Costs

Estimated monthly costs:
- **e2-standard-4 VM**: ~$100/month
- **50GB SSD disk**: ~$8/month
- **Static IP**: ~$3/month
- **Total**: ~$110-120/month

To reduce costs:
- Use `e2-standard-2` (~$50/month) for lighter workloads
- Use preemptible/spot instances for dev (~70% cheaper)

## Updating the Application

SSH into the VM and update:

```bash
gcloud compute ssh dagster-server --zone=us-central1-a --tunnel-through-iap

# On the VM:
cd /opt/dagster/app
git pull origin main
docker-compose build
docker-compose up -d
```

## Monitoring

View logs:

```bash
# SSH into VM, then:
docker-compose logs -f dagster_webserver
docker-compose logs -f dagster_daemon
```

View startup logs:

```bash
cat /var/log/dagster-startup.log
```

## Teardown

To destroy all resources:

```bash
terraform destroy
```

## Troubleshooting

### IAP Tunnel Not Working

1. Ensure IAP API is enabled:
   ```bash
   gcloud services enable iap.googleapis.com
   ```

2. Check IAM permissions:
   ```bash
   gcloud iap tunnel-instances describe dagster-server --zone=us-central1-a
   ```

### VM Not Starting Services

SSH in and check:

```bash
# Check Docker status
docker ps -a

# Check startup log
cat /var/log/dagster-startup.log

# Manually start services
cd /opt/dagster/app
docker-compose up -d
```

### OAuth Consent Screen Issues

If IAP brand creation fails, manually configure:
1. Go to APIs & Services > OAuth consent screen
2. Configure consent screen (Internal or External)
3. Re-run `terraform apply`
