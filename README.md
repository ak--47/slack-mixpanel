# slack-mixpanel-analytics

Extract Slack analytics data and stream it to Mixpanel. Transforms Slack member activity, channel usage, and workspace insights into Mixpanel events, user profiles, and group analytics.

Production-ready, cloud-native data pipeline designed for Cloud Run deployment.

## 🚀 Quick Start

```bash
# Setup
git clone <your-repo>
cd slack-mixpanel-analytics
cp .env.example .env

# Configure environment (see Environment Variables below)
# Add your tokens to .env

# Install and run locally
npm install
npm run dev        # Development mode (writes to tmp/ files)
npm start          # Production mode (uploads to Mixpanel)
```

## 🌍 Deployment

### Cloud Run (Recommended)
```bash
npm run deploy
```

### Manual Cloud Build
```bash
gcloud builds submit --config cloudbuild.yaml
```

## 🔧 Environment Variables

**Required:**
```bash
slack_bot_token=xoxb-...        # Slack bot token
slack_user_token=xoxp-...       # Slack user token (for analytics API)
mixpanel_token=...              # Mixpanel project token
mixpanel_secret=...             # Mixpanel project secret
service_name=slack-mixpanel-analytics
```

**Optional:**
```bash
NODE_ENV=production             # Environment mode
PORT=8080                       # Server port
CONCURRENCY=2                   # Slack API concurrency (rate limiting)
slack_prefix=https://yourworkspace.slack.com/archives
company_domain=yourcompany.com  # Email domain filter
channel_group_key=channel_id    # Mixpanel group key for channels
channel_datagroup_id=...        # Mixpanel group ID for channels
```

## 📊 API Usage

### HTTP Endpoints

**Main pipeline:**
```bash
# Query parameters (case-insensitive)
POST /slack-analytics?days=7
POST /slack-analytics?start_date=2024-01-01&end_date=2024-01-31

# JSON body
POST /slack-analytics
Content-Type: application/json
{"days": 7}

# Health check
GET /
```

**Parameters:**
- `days` - Number of days to process (mutually exclusive with date range)
- `start_date` / `end_date` - Custom date range in YYYY-MM-DD format

### Direct Execution
```bash
# Run pipeline directly
node src/jobs/slack-mixpanel-analytics.js

# Test individual components
node src/services/slack.js
node src/models/slack-members.js
```

## 🎯 Data Pipeline

**Imported to Mixpanel:**
- **Member Events** - Daily user activity summaries
- **Member Profiles** - User information and metadata  
- **Channel Events** - Daily channel activity summaries
- **Channel Profiles** - Channel metadata and settings

**Processing Modes:**
- `dev` - Write to local files (`tmp/` directory)
- `production` - Upload directly to Mixpanel
- `backfill` - Process historical data
- `test` - Write mode for testing

## 🧪 Development

### VSCode Launch Configurations
- **"go"** - Run any file with debugger support
- **"server"** - Auto-restarting development server
- **"tests"** - Execute test suite
- **"backfill"** - Historical data processing

### Commands
```bash
npm run dev          # Development mode (local files)
npm start            # Production mode (Mixpanel upload)
npm test             # Run pipeline tests
npm run deploy       # Deploy to Cloud Run
npm run prune        # Clean temp files
```

## 🔒 Authentication

### Slack Tokens
1. **Bot Token** (`slack_bot_token`): Standard bot token for workspace access
2. **User Token** (`slack_user_token`): Required for Analytics API access

### Mixpanel
1. **Project Token** (`mixpanel_token`): For data import
2. **Project Secret** (`mixpanel_secret`): For authenticated imports

## ⚙️ Configuration

### Rate Limiting
Slack API rate limits are respected with:
- Configurable concurrency (`CONCURRENCY` env var)
- 2-second delays between requests
- Automatic retry on rate limit hits

### Performance
- Direct array uploads to `mixpanel-import`
- Parallel processing where possible
- Memory-efficient streaming architecture

## 🚀 Deployment Details

**Cloud Run Configuration:**
- 4GB memory, 2 CPU
- Auto-scaling (0-10 instances)
- 1 hour timeout for large backfills
- Source-based deployment (no Dockerfile needed)

**Environment Variables:**
Deployed via `.env.yaml` file generated from your `.env`

## 📋 Troubleshooting

**Common Issues:**
- **Rate limits**: Reduce `CONCURRENCY` value
- **Authentication**: Verify token permissions
- **No data**: Slack analytics has 1-2 day delay
- **Memory errors**: Use smaller date ranges for backfill

**Debug Mode:**
Set `NODE_ENV=dev` for detailed logging and debugger breakpoints.