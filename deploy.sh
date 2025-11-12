#!/bin/bash

set -euo pipefail

# Check for .env file
if [ ! -f .env ]; then
  echo "❌ .env file not found"
  exit 1
fi

# Function to read env var from .env file
get_env_var() {
  local key=$1
  local value=$(grep "^${key}=" .env | cut -d '=' -f2- | sed 's/^["'\'']//' | sed 's/["'\'']$//')
  echo "$value"
}

# Read all required env vars from .env
echo "📖 Reading environment variables from .env..."

MIXPANEL_TOKEN=$(get_env_var "mixpanel_token")
MIXPANEL_SECRET=$(get_env_var "mixpanel_secret")
SLACK_BOT_TOKEN=$(get_env_var "slack_bot_token")
SLACK_USER_TOKEN=$(get_env_var "slack_user_token")
SERVICE_NAME=$(get_env_var "service_name")
SLACK_PREFIX=$(get_env_var "slack_prefix")
COMPANY_DOMAIN=$(get_env_var "company_domain")
CHANNEL_GROUP_KEY=$(get_env_var "channel_group_key")
GCS_PATH=$(get_env_var "gcs_path")

# Validate required vars
if [ -z "$SERVICE_NAME" ]; then
  echo "❌ service_name is not set in .env"
  exit 1
fi

if [ -z "$MIXPANEL_TOKEN" ]; then
  echo "❌ mixpanel_token is not set in .env"
  exit 1
fi

# Build substitutions string
SUBSTITUTIONS="_SERVICE_NAME=${SERVICE_NAME}"
SUBSTITUTIONS="${SUBSTITUTIONS},_MIXPANEL_TOKEN=${MIXPANEL_TOKEN}"
SUBSTITUTIONS="${SUBSTITUTIONS},_MIXPANEL_SECRET=${MIXPANEL_SECRET}"
SUBSTITUTIONS="${SUBSTITUTIONS},_SLACK_BOT_TOKEN=${SLACK_BOT_TOKEN}"
SUBSTITUTIONS="${SUBSTITUTIONS},_SLACK_USER_TOKEN=${SLACK_USER_TOKEN}"
SUBSTITUTIONS="${SUBSTITUTIONS},_SLACK_PREFIX=${SLACK_PREFIX}"
SUBSTITUTIONS="${SUBSTITUTIONS},_COMPANY_DOMAIN=${COMPANY_DOMAIN}"
SUBSTITUTIONS="${SUBSTITUTIONS},_CHANNEL_GROUP_KEY=${CHANNEL_GROUP_KEY}"
SUBSTITUTIONS="${SUBSTITUTIONS},_GCS_PATH=${GCS_PATH}"

echo "🚀 Deploying $SERVICE_NAME to Cloud Run via Cloud Build..."
echo "📦 Region: us-central1"

# Deploy using Cloud Build
gcloud builds submit \
  --config cloudbuild.yaml \
  --substitutions "$SUBSTITUTIONS" \
  --region us-central1

echo ""
echo "✅ Cloud Run deployment complete!"
echo "🌐 Service URL: https://$SERVICE_NAME-$(gcloud config get-value project 2>/dev/null).a.run.app"