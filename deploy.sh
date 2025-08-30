#!/bin/bash

set -euo pipefail

# Cleanup on exit (success or error)
cleanup() {
  echo "Cleaning up .env.yaml..."
  rm -f .env.yaml
}
trap cleanup EXIT

# Check for .env file
if [ ! -f .env ]; then
  echo ".env file not found"
  exit 1
fi

# Check required env vars
if ! grep -q "^service_name=" .env; then
  echo "service_name is not set in the .env file"
  exit 1
fi

if ! grep -q "^mixpanel_token=" .env; then
  echo "mixpanel_token is not set in the .env file"
  exit 1
fi

# Load service_name into environment
export $(grep "^service_name=" .env | xargs)

# Convert .env to flat YAML format
echo "Generating .env.yaml from .env file..."
grep -v '^#' .env | grep -v '^\s*$' | while IFS='=' read -r key value; do
  # Remove quotes if present and escape any remaining quotes
  value=$(echo "$value" | sed 's/^["'\'']//' | sed 's/["'\'']$//' | sed 's/"/\\"/g')
  echo "$key: \"$value\""
done > .env.yaml

echo "Generated .env.yaml:"
cat .env.yaml

# Deploy using Cloud Build to Cloud Run
echo "Deploying $service_name to Cloud Run using Cloud Build..."
gcloud builds submit \
  --config cloudbuild.yaml \
  --substitutions _SERVICE_NAME="$service_name" \
  --region us-central1

echo "✅ Cloud Run deployment complete!"
echo "🌐 Your service should be available at:"
echo "https://$service_name-$(gcloud config get-value project).a.run.app"