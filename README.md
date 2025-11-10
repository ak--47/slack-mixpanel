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
CONCURRENCY=1                   # Slack API concurrency (default: 1 for conservative rate limiting)
slack_prefix=https://yourworkspace.slack.com/archives
company_domain=yourcompany.com  # Email domain filter
channel_group_key=channel_id    # Mixpanel group key for channels
channel_datagroup_id=...        # Mixpanel group ID for channels

# Cloud Storage (for NODE_ENV=cloud)
gcs_project=your-gcs-project    # Google Cloud project ID
gcs_path=gs://bucket/path/      # GCS bucket and path
```

## 📊 Usage

### Command-Line Scripts (Recommended)

Run pipelines directly without HTTP server. Output streams to both console and log files in `./logs/`:

```bash
# Process last 7 days
node scripts/run-all-7days.js

# Process custom date range
node scripts/run-all-daterange.js 2024-01-01 2024-01-31

# Process full backfill (13 months)
node scripts/run-all-backfill.js
```

**Features:**
- No HTTP server required
- Live console output with progress tracking
- Simultaneous logging to `./logs/` directory
- Conservative rate limiting (1 req/sec with jitter) to avoid Slack API 429s
- Graceful error handling and cleanup

### HTTP API Endpoints

For cloud deployments and webhook integrations:

**Health Check:**
```bash
GET /         # Service status and endpoint list
GET /health   # Health check
```

**Pipeline Endpoints:**
```bash
# Process Slack members only
POST /mixpanel-members?days=7
POST /mixpanel-members?start_date=2024-01-01&end_date=2024-01-31
POST /mixpanel-members?backfill=true

# Process Slack channels only
POST /mixpanel-channels?days=7
POST /mixpanel-channels?start_date=2024-01-01&end_date=2024-01-31
POST /mixpanel-channels?backfill=true

# Process both members and channels
POST /mixpanel-all?days=7
POST /mixpanel-all?start_date=2024-01-01&end_date=2024-01-31
POST /mixpanel-all?backfill=true
```

**JSON Body Examples:**
```bash
# Using JSON body instead of query params
POST /mixpanel-members
Content-Type: application/json
{"days": 7}

POST /mixpanel-channels
Content-Type: application/json
{"start_date": "2024-01-01", "end_date": "2024-01-31"}

POST /mixpanel-all
Content-Type: application/json
{"backfill": true}
```

**Parameters:**
- `days` - Number of days to process (mutually exclusive with date range and backfill)
- `start_date` / `end_date` - Custom date range in YYYY-MM-DD format (mutually exclusive with days and backfill)
- `backfill=true` - Process 13 months of historical data (mutually exclusive with days and date range)

**Parameter Rules:**
- Query parameters take precedence over JSON body
- Parameters are case-insensitive in query strings
- Only one parameter type allowed: `days` OR `start_date/end_date` OR `backfill`
- `backfill=true` automatically sets processing mode to backfill environment (13+ months of data)

### Direct Execution
```bash
# Run pipeline function directly
node src/jobs/run-pipeline.js

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
- `cloud` - Upload to Google Cloud Storage
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
npm run deploy       # Deploy to Cloud Run
npm run prune        # Clean temp files
```

### Testing
```bash
npm test             # Run all tests
npm run test:unit    # Run unit tests only
npm run test:integration  # Run integration tests only
npm run test:watch   # Run tests in watch mode
npm run test:coverage     # Run tests with coverage report
npm run test:ui      # Open Vitest UI
npm run test:legacy  # Run legacy test.js script
```

**Test Structure:**
- **Unit Tests** (`test/unit/`) - Test helper functions, parameter validation, and data processing logic
- **Integration Tests** (`test/integration/`) - Test API endpoints and full pipeline execution with real Slack credentials
- **Coverage Reports** - Generated in `coverage/` directory

**Integration Test Requirements:**
- Requires valid Slack credentials in `.env` file
- Tests are automatically skipped if credentials are missing
- Uses small date ranges (1-2 days) to avoid rate limits
- Tests against real Slack API in test mode (writes to files, not Mixpanel)

## 🔒 Authentication

### Slack Tokens
1. **Bot Token** (`slack_bot_token`): Standard bot token for workspace access
2. **User Token** (`slack_user_token`): Required for Analytics API access

### Mixpanel
1. **Project Token** (`mixpanel_token`): For data import
2. **Project Secret** (`mixpanel_secret`): For authenticated imports

## ⚙️ Configuration

### Rate Limiting
Slack Analytics API rate limits are respected with ultra-conservative settings:
- **Concurrency**: 1 (sequential requests only, configurable via `CONCURRENCY` env var)
- **Base delay**: 1500ms between requests
- **Randomized jitter**: +0-1500ms (total delay: 1500-3000ms per request)
- **Automatic retry**: 60-second wait on 429 rate limit errors
- **Burst protection**: Prevents predictable request patterns

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