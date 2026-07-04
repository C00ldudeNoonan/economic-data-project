# Utility Scripts

This project keeps utility scripts in domain-specific script directories, including
`macro_agents/scripts/` for Python helpers and deployment script locations noted
below.

## Available Scripts

### `bigquery_access_audit.py`

Audits BigQuery query job history so we can see which tables were accessed,
who accessed them, and how much data each access pattern processed or billed.
This is useful for tuning agent/dbt semantic-layer models and spotting tables
that are expensive, unused, or missing from expected analytical workflows.

**Usage:**
```bash
uv run --project macro_agents python macro_agents/scripts/bigquery_access_audit.py --days 14
```

Filter to the dbt Platform service account:
```bash
uv run --project macro_agents python macro_agents/scripts/bigquery_access_audit.py \
  --days 30 \
  --user-email dbt-service-account@example.iam.gserviceaccount.com
```

Export recent query-level access to CSV:
```bash
uv run --project macro_agents python macro_agents/scripts/bigquery_access_audit.py \
  --mode jobs \
  --format csv \
  --output bigquery_access_jobs.csv
```

**Prerequisites:**
- `BIGQUERY_PROJECT` or `BIGQUERY_PROJECT_ID` set, unless using the repo default.
- `BIGQUERY_LOCATION` set when the job history is outside the `US` multi-region.
- Google Application Default Credentials, or `GOOGLE_APPLICATION_CREDENTIALS`
  set to either a credentials file path or inline service-account JSON.
- IAM permissions that allow reading project job history, including
  `bigquery.jobs.listAll`.

### `setup-gcp-vm.sh`

Sets up a Google Cloud Platform VM instance for running the Economic Data Platform.

**Purpose:**
- Creates a GCP Compute Engine instance
- Installs Docker and Docker Compose
- Configures firewall rules
- Sets up the project repository

**Usage:**
```bash
./scripts/setup-gcp-vm.sh
```

**Prerequisites:**
- Google Cloud SDK (`gcloud`) installed and authenticated
- GCP project with Compute Engine API enabled
- Appropriate IAM permissions

**What it does:**
1. Creates a VM instance with specified machine type
2. Installs Docker and Docker Compose
3. Clones the project repository
4. Configures environment variables
5. Opens required firewall ports

### `deploy-to-gcp.sh`

Deploys the application stack to a GCP VM instance.

**Purpose:**
- Pulls latest code changes
- Rebuilds Docker images
- Restarts services with zero downtime

**Usage:**
```bash
./scripts/deploy-to-gcp.sh
```

**What it does:**
1. SSH into the GCP VM
2. Pull latest code from Git
3. Rebuild Docker images
4. Restart Docker Compose services
5. Verify services are running

## Environment Setup

Before running scripts, ensure environment variables are set:

```bash
export GCP_PROJECT_ID="your-project-id"
export GCP_ZONE="us-central1-a"
export GCP_INSTANCE_NAME="economic-data-platform"
```

## Script Permissions

Ensure scripts are executable:

```bash
chmod +x scripts/*.sh
```

## Common Operations

### Initial Deployment

```bash
# 1. Set up VM
./scripts/setup-gcp-vm.sh

# 2. SSH into VM
gcloud compute ssh $GCP_INSTANCE_NAME --zone=$GCP_ZONE

# 3. Configure environment
cd economic-data-project-full
cp .env.example .env
# Edit .env with your API keys

# 4. Start services
docker-compose up -d
```

### Updating Production

```bash
./scripts/deploy-to-gcp.sh
```

### Checking Logs

```bash
gcloud compute ssh $GCP_INSTANCE_NAME --zone=$GCP_ZONE \
  --command="cd economic-data-project-full && docker-compose logs -f"
```

### Restarting Services

```bash
gcloud compute ssh $GCP_INSTANCE_NAME --zone=$GCP_ZONE \
  --command="cd economic-data-project-full && docker-compose restart"
```

## Troubleshooting

### VM Connection Issues

```bash
# Check VM status
gcloud compute instances describe $GCP_INSTANCE_NAME --zone=$GCP_ZONE

# Check firewall rules
gcloud compute firewall-rules list
```

### Docker Issues

```bash
# SSH into VM
gcloud compute ssh $GCP_INSTANCE_NAME --zone=$GCP_ZONE

# Check Docker status
sudo systemctl status docker

# View container status
docker ps -a

# View container logs
docker logs <container_id>
```

### Database Issues

```bash
# Check PostgreSQL container
docker exec -it dagster_postgresql psql -U dagster -d dagster

# Check MotherDuck connection
docker exec -it dagster_user_code python -c "from macro_agents.defs.resources.motherduck import motherduck_resource; print(motherduck_resource.get_connection())"
```
