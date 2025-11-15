#!/bin/bash
set -e

echo "=== Starting GitHub Actions Runner Setup ==="

# Install dependencies
echo "Installing dependencies..."
apt-get update
apt-get install -y curl jq

# Install Docker
echo "Installing Docker..."
curl -fsSL https://get.docker.com/ -o get-docker.sh
sh get-docker.sh
systemctl start docker
systemctl enable docker
docker --version

# Create a user for the runner
echo "Creating runner user..."
useradd -m -s /bin/bash runner
usermod -aG docker runner

# Download and install GitHub Actions runner
echo "Downloading GitHub Actions runner..."
cd /home/runner
RUNNER_VERSION="2.329.0"
curl -o actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz -L https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz
tar xzf ./actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz
chown -R runner:runner /home/runner

# Get runner token from metadata
echo "Fetching configuration from GCP metadata..."
RUNNER_TOKEN=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/runner-token)
REPO_URL=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/repo-url)
RUNNER_NAME=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/runner-name)

echo "Repository URL: ${REPO_URL}"
echo "Runner Name: ${RUNNER_NAME}"
echo "Token length: ${#RUNNER_TOKEN} characters"

# Validate we got the token
if [ -z "$RUNNER_TOKEN" ] || [ "$RUNNER_TOKEN" = "null" ]; then
  echo "ERROR: Failed to get runner token from metadata"
  exit 1
fi

# Configure and start the runner
echo "Configuring GitHub Actions runner..."
su - runner -c "cd /home/runner && ./config.sh --url ${REPO_URL} --token ${RUNNER_TOKEN} --name ${RUNNER_NAME} --labels self-hosted,gcp,spot --unattended"

echo "Starting runner..."
su - runner -c "cd /home/runner && ./run.sh"
