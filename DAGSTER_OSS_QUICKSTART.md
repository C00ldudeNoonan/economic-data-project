# Dagster OSS Migration - Quick Start Guide

This guide provides step-by-step instructions for migrating from Dagster+ to Dagster OSS on GCP.

## Prerequisites

- [ ] GCP account with billing enabled
- [ ] `gcloud` CLI installed and configured
- [ ] GCP project created
- [ ] Service account with necessary permissions
- [ ] All environment variables and secrets ready

## Quick Setup (Automated)

### 1. Set Environment Variables

```bash
export GCP_PROJECT_ID="your-gcp-project-id"
export DAGSTER_VM_NAME="dagster-oss-vm"
export GCP_ZONE="us-central1-a"
export MACHINE_TYPE="e2-standard-4"
```

### 2. Run Setup Script

```bash
# Make script executable (if not already)
chmod +x scripts/setup-gcp-vm.sh

# Run the setup script
./scripts/setup-gcp-vm.sh
```

This script will:
- ✅ Create GCP VM instance
- ✅ Create and attach persistent disk for PostgreSQL
- ✅ Configure firewall rules
- ✅ Install Docker and Docker Compose
- ✅ Format and mount PostgreSQL data disk

### 3. Prepare Environment File

```bash
# Copy the example env file
cp .env.dagster.example .env

# Edit .env and fill in all your secrets
nano .env
# OR
vi .env
```

**Important**: Fill in all required values:
- PostgreSQL password
- MotherDuck credentials
- GCP configuration
- API keys (FRED, MarketStack, OpenAI, Anthropic, etc.)

### 4. Copy Files to VM

```bash
# Copy environment file
gcloud compute scp .env $DAGSTER_VM_NAME:~/dagster/ --zone=$GCP_ZONE

# Copy GCP service account JSON
gcloud compute scp gcp-service-account.json $DAGSTER_VM_NAME:~/dagster/ --zone=$GCP_ZONE

# Copy configuration files
gcloud compute scp docker-compose.yml dagster.yaml workspace.yaml $DAGSTER_VM_NAME:~/dagster/ --zone=$GCP_ZONE

# Copy application code
gcloud compute scp --recurse macro_agents $DAGSTER_VM_NAME:~/dagster/ --zone=$GCP_ZONE

# Copy dbt project
gcloud compute scp --recurse dbt_project $DAGSTER_VM_NAME:~/dagster/ --zone=$GCP_ZONE
```

### 5. Start Dagster

```bash
# SSH into VM
gcloud compute ssh $DAGSTER_VM_NAME --zone=$GCP_ZONE

# Navigate to dagster directory
cd ~/dagster

# Update docker-compose.yml to use mounted disk (if using persistent disk)
# Edit the postgres_data volume path from:
#   postgres_data:/var/lib/postgresql/data
# To:
#   /mnt/dagster-postgres/data:/var/lib/postgresql/data

# Start services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f
```

### 6. Access Dagster UI

```bash
# Get VM external IP
gcloud compute instances describe $DAGSTER_VM_NAME --zone=$GCP_ZONE --format='get(networkInterfaces[0].accessConfigs[0].natIP)'

# Open browser to: http://<EXTERNAL_IP>:3000
```

## Manual Setup (Step by Step)

### Step 1: Create GCP VM

```bash
gcloud compute instances create dagster-oss-vm \
  --project=your-gcp-project-id \
  --zone=us-central1-a \
  --machine-type=e2-standard-4 \
  --boot-disk-size=100GB \
  --boot-disk-type=pd-ssd \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --scopes=cloud-platform \
  --tags=dagster,http-server
```

### Step 2: Create Persistent Disk

```bash
# Create disk
gcloud compute disks create dagster-postgres-data \
  --type=pd-ssd \
  --size=50GB \
  --zone=us-central1-a

# Attach to VM
gcloud compute instances attach-disk dagster-oss-vm \
  --disk=dagster-postgres-data \
  --zone=us-central1-a
```

### Step 3: Configure Firewall

```bash
gcloud compute firewall-rules create allow-dagster-ui \
  --direction=INGRESS \
  --priority=1000 \
  --network=default \
  --action=ALLOW \
  --rules=tcp:3000 \
  --source-ranges=0.0.0.0/0 \
  --target-tags=dagster
```

### Step 4: Install Docker

```bash
# SSH into VM
gcloud compute ssh dagster-oss-vm --zone=us-central1-a

# Install Docker
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker
```

### Step 5: Format and Mount Disk

```bash
# Format disk (ONLY FIRST TIME!)
sudo mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb

# Create mount point
sudo mkdir -p /mnt/dagster-postgres

# Mount disk
sudo mount -o discard,defaults /dev/sdb /mnt/dagster-postgres

# Add to fstab
echo '/dev/sdb /mnt/dagster-postgres ext4 discard,defaults,nofail 0 2' | sudo tee -a /etc/fstab

# Create PostgreSQL directory
sudo mkdir -p /mnt/dagster-postgres/data
sudo chown -R 999:999 /mnt/dagster-postgres/data
```

### Step 6: Deploy Dagster

Follow steps 3-5 from the Quick Setup section above.

## Verification Checklist

After deployment, verify the following:

### Container Health
```bash
# All containers should be running
docker compose ps

# Should show:
# - dagster_postgresql (healthy)
# - dagster_user_code (running)
# - dagster_webserver (running)
# - dagster_daemon (running)
```

### Database Connection
```bash
# Test PostgreSQL connection
docker compose exec dagster_postgresql psql -U dagster_user -d dagster -c "\dt"

# Should list Dagster tables (runs, event_logs, etc.)
```

### Dagster UI
- Open http://<VM_IP>:3000
- Should see Dagster UI
- Go to "Assets" tab - should see all your assets
- Go to "Runs" tab - should be empty (fresh start)

### Test Asset Materialization
1. In Dagster UI, go to "Assets"
2. Select a simple asset (e.g., FRED data asset)
3. Click "Materialize"
4. Should execute successfully
5. Check MotherDuck to verify data was written

### Schedules and Sensors
```bash
# Check daemon logs
docker compose logs dagster_daemon

# Should see messages about evaluating schedules and sensors
```

## Common Issues and Solutions

### Issue: Containers won't start

```bash
# Check logs
docker compose logs

# Common causes:
# 1. Missing environment variables - check .env file
# 2. Port already in use - stop conflicting services
# 3. Disk space - check with df -h
```

### Issue: PostgreSQL fails to start

```bash
# Check permissions on data directory
ls -la /mnt/dagster-postgres/

# Should be owned by 999:999
# If not, fix with:
sudo chown -R 999:999 /mnt/dagster-postgres/data

# Restart
docker compose restart dagster_postgresql
```

### Issue: Can't access Dagster UI

```bash
# 1. Check firewall rules
gcloud compute firewall-rules list | grep dagster

# 2. Check if webserver is running
docker compose ps dagster_webserver

# 3. Check webserver logs
docker compose logs dagster_webserver

# 4. Verify VM external IP
gcloud compute instances describe dagster-oss-vm --zone=us-central1-a \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

### Issue: Assets fail to materialize

```bash
# 1. Check user code container logs
docker compose logs dagster_user_code

# 2. Check environment variables
docker compose exec dagster_user_code env | grep -E 'MOTHERDUCK|BIGQUERY|FRED'

# 3. Test database connection manually
docker compose exec dagster_user_code python -c "
import duckdb
import os
token = os.getenv('MOTHERDUCK_TOKEN')
db = os.getenv('MOTHERDUCK_DATABASE')
conn = duckdb.connect(f'md:{db}?motherduck_token={token}')
print('Connection successful!')
"
```

## Useful Commands

### Container Management

```bash
# Start services
docker compose up -d

# Stop services
docker compose down

# Restart specific service
docker compose restart dagster_user_code

# View logs
docker compose logs -f

# View logs for specific service
docker compose logs -f dagster_webserver

# Check resource usage
docker stats
```

### Database Operations

```bash
# Access PostgreSQL
docker compose exec dagster_postgresql psql -U dagster_user -d dagster

# Backup database
docker compose exec dagster_postgresql pg_dump -U dagster_user dagster > backup_$(date +%Y%m%d).sql

# Restore database
docker compose exec -T dagster_postgresql psql -U dagster_user dagster < backup.sql

# Check database size
docker compose exec dagster_postgresql psql -U dagster_user -d dagster -c "
  SELECT pg_size_pretty(pg_database_size('dagster'));
"
```

### Deployment Updates

```bash
# Quick deploy (from local machine)
./scripts/deploy-to-gcp.sh

# Manual deploy
gcloud compute scp --recurse macro_agents dagster-oss-vm:~/dagster/ --zone=us-central1-a
gcloud compute ssh dagster-oss-vm --zone=us-central1-a --command='
  cd ~/dagster && \
  docker compose down && \
  docker compose build dagster_user_code && \
  docker compose up -d
'
```

## Next Steps After Migration

### Week 1: Monitoring
- [ ] Set up monitoring (Prometheus/Grafana - optional)
- [ ] Monitor resource usage (CPU, memory, disk)
- [ ] Set up alerts for container failures
- [ ] Verify all schedules are running
- [ ] Test all sensors

### Week 2-4: Optimization
- [ ] Review and adjust VM size if needed
- [ ] Optimize PostgreSQL configuration
- [ ] Set up automated backups
- [ ] Document runbooks
- [ ] Security hardening (restrict firewall to specific IPs)

### Month 2: Decommission Dagster+
- [ ] Export any data from Dagster Cloud
- [ ] Cancel Dagster+ subscription
- [ ] Update documentation
- [ ] Remove old GitHub Actions workflows

## Getting Help

- **Dagster Documentation**: https://docs.dagster.io/
- **Dagster Slack**: https://dagster.io/slack
- **GitHub Issues**: https://github.com/dagster-io/dagster/issues
- **Full Migration Plan**: See `DAGSTER_OSS_MIGRATION_PLAN.md`

## Security Recommendations

1. **Restrict Firewall Rules**
   ```bash
   # Instead of 0.0.0.0/0, use your office IP
   gcloud compute firewall-rules update allow-dagster-ui \
     --source-ranges=YOUR_OFFICE_IP/32
   ```

2. **Use Secret Manager**
   - Store secrets in GCP Secret Manager instead of .env file
   - Grant VM service account access to secrets

3. **Enable HTTPS**
   - Set up nginx reverse proxy with Let's Encrypt
   - Redirect HTTP to HTTPS

4. **Regular Updates**
   ```bash
   # Update Docker images regularly
   docker compose pull
   docker compose up -d

   # Update system packages
   sudo apt-get update && sudo apt-get upgrade -y
   ```

5. **Enable OS Login**
   ```bash
   # Use OS Login instead of SSH keys
   gcloud compute instances add-metadata dagster-oss-vm \
     --metadata enable-oslogin=TRUE
   ```

## Cost Monitoring

```bash
# Set up budget alert
gcloud billing budgets create \
  --billing-account=YOUR_BILLING_ACCOUNT_ID \
  --display-name="Dagster OSS Budget" \
  --budget-amount=200 \
  --threshold-rule=percent=80 \
  --threshold-rule=percent=100
```

---

**Ready to migrate?** Start with the Quick Setup section above!
