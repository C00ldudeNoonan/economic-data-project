#!/bin/bash
set -e

# Log startup
echo "=== Dagster VM Startup Script ==="
exec > >(tee /var/log/dagster-startup.log) 2>&1

# Wait for Docker to be available (Container-Optimized OS)
echo "Waiting for Docker..."
while ! docker info > /dev/null 2>&1; do
  sleep 2
done
echo "Docker is ready"

# Install docker-compose (not included in COS by default)
echo "Installing docker-compose..."
DOCKER_COMPOSE_VERSION="v2.24.0"
mkdir -p /opt/bin
curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64" -o /opt/bin/docker-compose
chmod +x /opt/bin/docker-compose
export PATH="/opt/bin:$PATH"

# Create app directory
APP_DIR="/opt/dagster"
mkdir -p $APP_DIR
cd $APP_DIR

# Fetch secrets from Secret Manager
echo "Fetching secrets..."
gcloud secrets versions access latest --secret="dagster-env" > /opt/dagster/.env

# Clone the repository (or pull if exists)
REPO_URL="https://github.com/C00ldudeNoonan/economic-data-project-full.git"
if [ -d "$APP_DIR/app" ]; then
  echo "Updating existing repo..."
  cd $APP_DIR/app
  git pull origin main
else
  echo "Cloning repository..."
  git clone $REPO_URL $APP_DIR/app
  cd $APP_DIR/app
fi

# Copy .env file
cp /opt/dagster/.env $APP_DIR/app/.env

# Build and start services
echo "Building Docker images..."
/opt/bin/docker-compose build

echo "Starting Dagster services..."
/opt/bin/docker-compose up -d

# Wait for services to be healthy
echo "Waiting for services to start..."
sleep 30

# Check status
echo "=== Service Status ==="
/opt/bin/docker-compose ps

echo "=== Startup Complete ==="
echo "Dagster UI should be available on port 3000 (via IAP tunnel)"
