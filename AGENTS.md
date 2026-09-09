# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Node.js data pipeline that extracts data from Slack Analytics API and transforms it into Mixpanel events, users, and groups. The project uses a **file-based ETL architecture** designed for reliability, resumability, and cloud deployment.

## Architecture

### File-Based Extract-Load Pattern

The pipeline follows a two-stage file-based architecture to solve memory issues and enable job resumption:

1. **Extract Stage**: Fetches data from Slack Analytics API and writes to JSONL.gz files
   - One file per date: `members/YYYY-MM-DD-members.jsonl.gz`
   - Skips files that already exist (resumable)
   - Filters members by `company_domain` during extraction
   - Stores in Google Cloud Storage (GCS) or local `./tmp` directory

2. **Load Stage**: Reads files and uploads to Mixpanel with transformations
   - Uses `mixpanel-import` library with GCS URL support
   - Applies transformations via `transformFunc` and `heavyObjects` parameters
   - Sequential uploads: events first, then profiles
   - 3-retry logic with exponential backoff (2s, 4s, 6s)
   - Deletes files after successful upload

### Key Components

- **Storage Service** ([src/services/storage.js](src/services/storage.js)): Abstracts local vs GCS file operations
  - Writes/reads JSONL.gz files
  - Checks file existence for resumability
  - Automatic GCS detection via `gcs_path` environment variable

- **Extract Jobs** ([src/jobs/extract.js](src/jobs/extract.js)): Fetch and store raw Slack data
  - `extractMemberAnalytics()`: Member analytics by date
  - `extractChannelAnalytics()`: Channel analytics by date
  - Filters members by email domain during extraction

- **Load Jobs** ([src/jobs/load.js](src/jobs/load.js)): Upload files to Mixpanel
  - `loadMemberAnalytics()`: Upload member events + profiles
  - `loadChannelAnalytics()`: Upload channel events + group profiles
  - Retry logic and file cleanup

- **Transform Functions** ([src/transforms/](src/transforms/)): Data transformations
  - [src/transforms/members.js](src/transforms/members.js): Member event/profile transforms
  - [src/transforms/channels.js](src/transforms/channels.js): Channel event/profile transforms
  - Receives cached workspace data via `heavyObjects` parameter

- **Pipeline Runner** ([src/jobs/run-pipeline.js](src/jobs/run-pipeline.js)): Orchestrates extract + load stages
  - Caches Slack workspace data (members/channels) once at start
  - Passes cached data to transforms
  - Supports `extractOnly` and `loadOnly` modes
  - Returns detailed results for each stage

- **Slack Service** ([src/services/slack.js](src/services/slack.js)): Handles Slack API interactions
  - Ultra-conservative rate limiting (concurrency=1, 1500-3000ms delays with jitter)
  - Analytics data fetching with retry logic
  - User and channel caching

### Data Flow

```
Slack API → Extract (JSONL.gz files) → Load (with transforms) → Mixpanel
            ↓                            ↓
        GCS/Local Storage         mixpanel-import library
```

### File Structure

```
tmp/ (or GCS bucket)
├── members/
│   ├── 2024-01-01-members.jsonl.gz
│   ├── 2024-01-02-members.jsonl.gz
│   └── ...
└── channels/
    ├── 2024-01-01-channels.jsonl.gz
    ├── 2024-01-02-channels.jsonl.gz
    └── ...
```

## Environment Modes

- `dev`: Local development, writes to `./tmp`
- `production`: Direct Mixpanel upload, uses GCS if configured
- `backfill`: Historical data (13 months), writes to GCS
- `test`: Testing mode (5 days default)
- `cloud`: Cloud storage processing (14 days)

## Key Dependencies

- `@slack/web-api`: Slack API client
- `mixpanel-import`: Mixpanel bulk import with GCS support
- `dayjs`: Date manipulation with UTC plugin
- `ak-tools`: Custom utilities (logging, timers, progress)
- `p-limit`: Concurrency control
- `@google-cloud/storage`: Cloud storage operations
- `pako`: Gzip compression for JSONL files

## Rate Limiting

**Ultra-conservative settings** to avoid Slack Analytics API 429 errors:
- Concurrency: 1 (sequential requests only)
- Delay: 1500-3000ms with random jitter between requests
- Configurable via `CONCURRENCY` environment variable

## Data Transformation Patterns

### Events
- Include `event`, `distinct_id`, `time`, `properties` fields
- Members: `distinct_id` = email (lowercase)
- Channels: `distinct_id` = channel_id
- Time: UTC date + 4h20m offset (Unix timestamp)

### User Profiles
- Use email as `$distinct_id`
- Include `$email`, `$set` fields
- Profile data: name, avatar, Slack ID, etc.

### Group Profiles (Channels)
- Use `channel_id` as `$group_id`
- Group key: configurable via `channel_group_key` env var
- Include channel metadata: name, purpose, topic, member count

### Deduplication
- Files serve as natural deduplication (one file per date)
- Resumable: skip existing files

## Pipeline Modes

### Full Pipeline (Extract + Load)
```javascript
await runPipeline({
  days: 7,
  pipelines: ['members', 'channels']
});
```

### Extract Only (for backups/debugging)
```javascript
await runPipeline({
  days: 30,
  pipelines: ['members'],
  extractOnly: true  // Only fetch and store, don't upload
});
```

### Load Only (from existing files)
```javascript
await runPipeline({
  loadOnly: true  // TODO: Needs file discovery implementation
});
```

## Deployment

- **Cloud Run Compatible**: Containerized Express.js server
- **Dockerized**: Uses Node.js 18 slim image
- **Auto-scaling**: 0-10 instances with 1 request concurrency
- **Resources**: 4GB memory, 2 CPU, 3600s timeout
- **CI/CD**: Cloud Build integration with `cloudbuild.yaml`
- **Secrets Management**: Uses Google Secret Manager for tokens

## Common Development Commands

```bash
npm start              # Production mode (HTTP server)
npm run dev            # Development mode (HTTP server, local files)
node test.js           # Run pipeline tests
npm test               # Run all tests (vitest)

# Node.js Scripts (no HTTP server)
node scripts/run-all-7days.js             # 7-day pipeline
node scripts/run-all-daterange.js START END  # Custom date range
node scripts/run-all-backfill.js          # 13-month backfill
```

## API Endpoints

All endpoints accept parameters via JSON body or query string (query takes precedence).

### Mixpanel Pipelines

- `POST /mixpanel-members` - Process Slack members pipeline
  - Parameters: `days`, `start_date`, `end_date`, or `backfill`
  - Returns: extract stats, load stats, timing

- `POST /mixpanel-channels` - Process Slack channels pipeline
  - Parameters: same as members
  - Returns: extract stats, load stats, timing

- `POST /mixpanel-all` - Process both members and channels
  - Parameters: same as members
  - Returns: combined extract/load stats

### Health Checks

- `GET /` - Service status and endpoint list
- `GET /health` - Health check

### Parameter Options

- `days=N` - Process last N days (mutually exclusive with date range)
- `start_date=YYYY-MM-DD` - Start date (requires end_date)
- `end_date=YYYY-MM-DD` - End date (requires start_date)
- `backfill=true` - Run 13-month backfill (mutually exclusive with other date params)
- `extractOnly=true` - Only extract, don't load to Mixpanel
- `loadOnly=true` - Only load from existing files (TODO: needs implementation)

## Authentication

Requires environment variables:

- `slack_bot_token`: Slack bot token for API access
- `slack_user_token`: Slack user token for analytics API
- `mixpanel_token`: Mixpanel project token
- `mixpanel_secret`: Mixpanel API secret
- `company_domain`: Email domain to filter members (e.g., "mixpanel.com")

Optional:

- `slack_prefix`: Slack workspace URL prefix
- `channel_group_key`: Mixpanel group key for channels (default: "channel_id")
- `gcs_project`: Google Cloud project ID (for GCS storage)
- `gcs_path`: GCS bucket path (e.g., "gs://bucket-name/path")

## Testing

```bash
npm test                    # Run all tests with vitest
node test.js                # Run basic pipeline validation

# Test files
test/integration/pipeline.test.js  # Pipeline and transform tests
test/integration/api.test.js       # HTTP endpoint tests
test/unit/helpers.test.js          # Unit tests
```

## Error Handling

- **Extract failures**: Logs error, continues to next date
- **Load failures**: 3 retries with exponential backoff
- **API rate limits**: Ultra-conservative delays prevent 429 errors
- **File conflicts**: Skips existing files (resumable behavior)
- **Parameter validation**: Returns 400 status for invalid parameters
- **Slack API errors**: Graceful handling with detailed error messages

## Resumability

The file-based architecture enables job resumption:

1. Files are checked before extraction
2. Existing files are skipped (not re-downloaded)
3. Failed jobs can be restarted without re-processing completed dates
4. Files serve as a checkpoint system

This is critical for long-running backfill jobs that may fail partway through.

## Memory Efficiency

Previous Highland stream-based architecture caused OOM errors. New file-based approach:

- Processes one file at a time
- No in-memory stream buffering
- Explicit file cleanup after upload
- Suitable for multi-month backfills

## Cache Strategy

Workspace data (members/channels) is cached once at pipeline start:

```javascript
// Cached at pipeline start
const slackMembers = await slack.getUsers();
const slackChannels = await slack.getChannels();

// Passed to transforms via heavyObjects
const context = { slackMembers, slackChannels, slack_prefix };
```

This prevents repeated API calls during transform operations.

## Troubleshooting

### Stuck at 280/305 days
- Old Highland stream architecture issue (fixed with file-based approach)
- Check logs for specific date failures
- Resume from failed date using existing files

### 429 Rate Limit Errors
- Increase delays in `src/services/slack.js`
- Reduce concurrency (already at 1)
- Add longer jitter range

### Out of Memory
- Should not occur with file-based architecture
- Check for file cleanup after upload
- Verify sequential processing (not parallel)

### Missing Data
- Check `company_domain` filter setting
- Verify Slack Analytics API access/permissions
- Check date range (Analytics API has delays)
