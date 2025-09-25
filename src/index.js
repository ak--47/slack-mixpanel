import express from 'express';
import dotenv from 'dotenv';
import slackMemberPipeline from './models/slack-members.js';
import slackChannelPipeline from './models/slack-channels.js';
import { devTask, writeTask, uploadTask, cloudTask } from './jobs/tasks.js';
import { runPipeline as runUserAnalyticsPipeline, getAnalyticsSummary } from './jobs/slack-bigquery-user-analytics.js';
import * as akTools from 'ak-tools';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dotenv.config();
dayjs.extend(utc);

const { sLog, timer } = akTools;
const { 
	NODE_ENV = "production", 
	PORT = 8080, 
	CONCURRENCY = 10,
	mixpanel_token, 
	mixpanel_secret, 
	channel_group_key = 'channel_id',
	gcs_project,
	gcs_path
} = process.env;

const app = express();

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Health check endpoint
app.get('/', (req, res) => {
	res.json({
		status: 'healthy',
		service: 'slack-mixpanel-pipeline',
		version: '1.0.0',
		environment: NODE_ENV,
		endpoints: {
			mixpanel: [
				'POST /mixpanel-members',
				'POST /mixpanel-channels',
				'POST /mixpanel-all'
			],
			bigquery: [
				'POST /bigquery-user-analytics',
				'GET /bigquery-user-analytics/:userId',
				'POST /bigquery-user-analytics/:userId/complete'
			]
		},
		timestamp: new Date().toISOString()
	});
});

// Helper function to parse and validate parameters
function parseParameters(req) {
	const { body = {}, query = {} } = req;
	
	// Merge query params into body, with query taking precedence
	const normalizedQuery = {};
	Object.keys(query).forEach(key => {
		normalizedQuery[key.toLowerCase()] = query[key];
	});
	
	const params = { ...body, ...normalizedQuery };
	
	// Handle backfill parameter (mutually exclusive with date params)
	if (params.backfill === 'true' || params.backfill === true) {
		if (params.days !== undefined || params.start_date !== undefined || params.end_date !== undefined) {
			throw new Error('Parameter "backfill" is mutually exclusive with "days", "start_date", and "end_date"');
		}
		// Set environment to backfill mode
		return { ...params, env: 'backfill' };
	}
	
	// Validate mutually exclusive parameters
	const hasDays = params.days !== undefined;
	const hasDateRange = params.start_date !== undefined || params.end_date !== undefined;
	
	if (hasDays && hasDateRange) {
		throw new Error('Parameters "days" and "start_date/end_date" are mutually exclusive. Use one or the other.');
	}
	
	// Convert days to number if provided
	if (params.days !== undefined) {
		params.days = parseInt(params.days);
		if (isNaN(params.days) || params.days < 1) {
			throw new Error('Parameter "days" must be a positive integer');
		}
	}
	
	// Validate date format if provided
	const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
	if (params.start_date && !dateRegex.test(params.start_date)) {
		throw new Error('Parameter "start_date" must be in YYYY-MM-DD format');
	}
	
	if (params.end_date && !dateRegex.test(params.end_date)) {
		throw new Error('Parameter "end_date" must be in YYYY-MM-DD format');
	}
	
	return params;
}

// Helper function to determine date range
function getDateRange(params) {
	const NOW = dayjs.utc();
	const sfdcDateTimeFmt = "YYYY-MM-DDTHH:mm:ss.SSS[Z]";
	
	// Handle backfill mode (13 months)
	if (params.env === 'backfill') {
		const BACKFILL_DAYS = 365 + 30; // ~13 months
		const start = NOW.subtract(BACKFILL_DAYS, "d").format(sfdcDateTimeFmt);
		const end = NOW.add(2, "d").format(sfdcDateTimeFmt);
		return {
			start,
			end,
			simpleStart: dayjs.utc(start).format("YYYY-MM-DD"),
			simpleEnd: dayjs.utc(end).format("YYYY-MM-DD"),
			days: BACKFILL_DAYS
		};
	}
	
	// Determine default days based on environment
	let DAYS;
	if (NODE_ENV === "dev") DAYS = 1;
	if (NODE_ENV === "production") DAYS = 2;
	if (NODE_ENV === "backfill") DAYS = 365;
	if (NODE_ENV === "test") DAYS = 5;
	if (NODE_ENV === "cloud") DAYS = 14;
	if (params.days) DAYS = params.days;
	
	let start = NOW.subtract(DAYS, "d").format(sfdcDateTimeFmt);
	let end = NOW.add(2, "d").format(sfdcDateTimeFmt);
	
	if (params.start_date) start = dayjs.utc(params.start_date).format(sfdcDateTimeFmt);
	if (params.end_date) end = dayjs.utc(params.end_date).format(sfdcDateTimeFmt);
	
	return {
		start,
		end,
		simpleStart: dayjs.utc(start).format("YYYY-MM-DD"),
		simpleEnd: dayjs.utc(end).format("YYYY-MM-DD"),
		days: DAYS
	};
}

// Helper function to get work function based on environment
function getWorkFunction(envMode) {
	const env = { mixpanel_token, mixpanel_secret, channel_group_key, gcs_project, gcs_path, NODE_ENV: envMode || NODE_ENV };
	
	const actualEnv = envMode || NODE_ENV;
	switch (actualEnv) {
		case "backfill":
			return (jobs, stats) => cloudTask(jobs, stats, env);
		case "cloud":
			return (jobs, stats) => cloudTask(jobs, stats, env);
		case "test":
			return (jobs, stats) => writeTask(jobs, stats, env);
		case "dev":
			return (jobs, stats) => devTask(jobs, stats, env);
		case "production":
			return (jobs, stats) => uploadTask(jobs, stats, env);
		default:
			return (jobs, stats) => uploadTask(jobs, stats, env);
	}
}

// Helper function to create stats object
function createStats() {
	return {
		events: { processed: 0, uploaded: 0 },
		users: { processed: 0, uploaded: 0 },
		groups: { processed: 0, uploaded: 0 },
		
		reset() {
			this.events = { processed: 0, uploaded: 0 };
			this.users = { processed: 0, uploaded: 0 };
			this.groups = { processed: 0, uploaded: 0 };
		},
		
		report() {
			return {
				events: { ...this.events },
				users: { ...this.users },
				groups: { ...this.groups }
			};
		}
	};
}

// Error handling for development
if (NODE_ENV === 'dev') {
	process.on('uncaughtException', (e, p) => {
		console.error('Uncaught Exception:', e);
	});

	process.on('unhandledRejection', (e, p) => {
		console.error('Unhandled Rejection:', e);
	});
}

// Health check endpoints
app.get('/', (req, res) => {
	res.json({ 
		status: "ok", 
		message: "Slack-Mixpanel pipeline service is alive",
		env: NODE_ENV,
		timestamp: new Date().toISOString(),
		endpoints: {
			health: "GET /health - Health check",
			members: "POST /members - Process Slack members pipeline",
			channels: "POST /channels - Process Slack channels pipeline", 
			all: "POST /all - Process both members and channels pipelines"
		}
	});
});

app.get('/health', (req, res) => {
	res.json({ 
		status: "ok", 
		message: "Slack-Mixpanel pipeline service is healthy",
		env: NODE_ENV,
		timestamp: new Date().toISOString()
	});
});

// Members pipeline endpoint
app.post('/mixpanel-members', async (req, res) => {
	const t = timer('slack-members');
	t.start();
	
	try {
		sLog('START JOB: slack-members');
		
		const params = parseParameters(req);
		const dateRange = getDateRange(params);
		const stats = createStats();
		const work = getWorkFunction(params.env);
		
		console.log(`SLACK-MEMBERS ${params.env || NODE_ENV}: ${dateRange.simpleStart} TO ${dateRange.simpleEnd} (${dateRange.days} DAYS)`);
		
		// Get data streams from members pipeline
		const { slackMemberEvents, slackMemberProfiles } = await slackMemberPipeline(dateRange.simpleStart, dateRange.simpleEnd);
		
		// Execute pipeline
		const jobs = [
			{ stream: slackMemberEvents, type: "event", label: "slack-members" },
			{ stream: slackMemberProfiles, type: "user", label: "slack-member-profiles" }
		];
		
		const results = await work(jobs, stats);
		
		let success = [];
		let failed = [];
		
		results.forEach(result => {
			if (result.status === "fulfilled") {
				success.push(result.value);
			} else {
				failed.push(result.reason);
			}
		});
		
		sLog(`FINISH JOB: slack-members ... ${t.end()}`, { success: success.length, failed: failed.length });
		
		res.status(200).json({
			status: 'success',
			pipeline: 'members',
			timing: t.report(false),
			params: { start_date: dateRange.start, end_date: dateRange.end },
			results: { success, failed },
			stats: stats.report()
		});
		
	} catch (error) {
		console.error('ERROR JOB: slack-members', error);
		
		// Handle parameter validation errors with 400 status
		const isValidationError = error.message.includes('Parameter') || error.message.includes('mutually exclusive');
		
		res.status(isValidationError ? 400 : 500).json({
			status: 'error',
			pipeline: 'members',
			error: error.message,
			timestamp: new Date().toISOString()
		});
	}
});

// Channels pipeline endpoint
app.post('/mixpanel-channels', async (req, res) => {
	const t = timer('slack-channels');
	t.start();
	
	try {
		sLog('START JOB: slack-channels');
		
		const params = parseParameters(req);
		const dateRange = getDateRange(params);
		const stats = createStats();
		const work = getWorkFunction(params.env);
		
		console.log(`SLACK-CHANNELS ${params.env || NODE_ENV}: ${dateRange.simpleStart} TO ${dateRange.simpleEnd} (${dateRange.days} DAYS)`);
		
		// Get data streams from channels pipeline
		const { slackChannelEvents, slackChannelProfiles } = await slackChannelPipeline(dateRange.simpleStart, dateRange.simpleEnd);
		
		// Execute pipeline
		const jobs = [
			{ stream: slackChannelEvents, type: "event", label: "slack-channels" },
			{ stream: slackChannelProfiles, type: "group", groupKey: channel_group_key, label: "slack-channel-profiles" }
		];
		
		const results = await work(jobs, stats);
		
		let success = [];
		let failed = [];
		
		results.forEach(result => {
			if (result.status === "fulfilled") {
				success.push(result.value);
			} else {
				failed.push(result.reason);
			}
		});
		
		sLog(`FINISH JOB: slack-channels ... ${t.end()}`, { success: success.length, failed: failed.length });
		
		res.status(200).json({
			status: 'success',
			pipeline: 'channels',
			timing: t.report(false),
			params: { start_date: dateRange.start, end_date: dateRange.end },
			results: { success, failed },
			stats: stats.report()
		});
		
	} catch (error) {
		console.error('ERROR JOB: slack-channels', error);
		
		// Handle parameter validation errors with 400 status
		const isValidationError = error.message.includes('Parameter') || error.message.includes('mutually exclusive');
		
		res.status(isValidationError ? 400 : 500).json({
			status: 'error',
			pipeline: 'channels',
			error: error.message,
			timestamp: new Date().toISOString()
		});
	}
});

// Combined pipeline endpoint (both members and channels)
app.post('/mixpanel-all', async (req, res) => {
	const t = timer('slack-all');
	t.start();
	
	try {
		sLog('START JOB: slack-all');
		
		const params = parseParameters(req);
		const dateRange = getDateRange(params);
		const stats = createStats();
		const work = getWorkFunction(params.env);
		
		console.log(`SLACK-ALL ${params.env || NODE_ENV}: ${dateRange.simpleStart} TO ${dateRange.simpleEnd} (${dateRange.days} DAYS)`);
		
		// Get data streams from both pipelines
		const { slackMemberEvents, slackMemberProfiles } = await slackMemberPipeline(dateRange.simpleStart, dateRange.simpleEnd);
		const { slackChannelEvents, slackChannelProfiles } = await slackChannelPipeline(dateRange.simpleStart, dateRange.simpleEnd);
		
		// Execute combined pipeline
		const memberJobs = [
			{ stream: slackMemberEvents, type: "event", label: "slack-members" },
			{ stream: slackMemberProfiles, type: "user", label: "slack-member-profiles" }
		];
		
		const channelJobs = [
			{ stream: slackChannelEvents, type: "event", label: "slack-channels" },
			{ stream: slackChannelProfiles, type: "group", groupKey: channel_group_key, label: "slack-channel-profiles" }
		];
		
		const pipeline = [
			work(memberJobs, stats),
			work(channelJobs, stats)
		];
		
		const results = await Promise.allSettled(pipeline);
		
		let success = [];
		let failed = [];
		
		results.forEach((result) => {
			if (result.status === "fulfilled") {
				result.value.forEach(innerResult => {
					if (innerResult.status === "fulfilled") {
						success.push(innerResult.value);
					} else {
						failed.push(innerResult.reason);
					}
				});
			} else {
				failed.push(result.reason);
			}
		});
		
		sLog(`FINISH JOB: slack-all ... ${t.end()}`, { success: success.length, failed: failed.length });
		
		res.status(200).json({
			status: 'success',
			pipeline: 'all',
			timing: t.report(false),
			params: { start_date: dateRange.start, end_date: dateRange.end },
			results: { success, failed },
			stats: stats.report()
		});
		
	} catch (error) {
		console.error('ERROR JOB: slack-all', error);
		
		// Handle parameter validation errors with 400 status
		const isValidationError = error.message.includes('Parameter') || error.message.includes('mutually exclusive');
		
		res.status(isValidationError ? 400 : 500).json({
			status: 'error',
			pipeline: 'all',
			error: error.message,
			timestamp: new Date().toISOString()
		});
	}
});

// BigQuery User Analytics pipeline endpoint
app.post('/bigquery-user-analytics', async (req, res) => {
    const t = timer('bigquery-user-analytics');
    t.start();

    try {
        sLog('START JOB: bigquery-user-analytics');

        const params = parseParameters(req);
        const { userId, days = 7, dataset, table } = params;

        // Validate required parameters
        if (!userId) {
            throw new Error('Parameter "userId" is required');
        }

        console.log(`BIGQUERY-USER-ANALYTICS: Processing user ${userId} for last ${days} days`);

        // Run the pipeline
        const result = await runUserAnalyticsPipeline({
            userId,
            days: parseInt(days),
            ...(dataset && { dataset }),
            ...(table && { table })
        });

        sLog(`FINISH JOB: bigquery-user-analytics ... ${t.end()}`, {
            success: result.success,
            messagesProcessed: result.messagesProcessed || 0,
            duplicatesSkipped: result.duplicatesSkipped || 0
        });

        res.status(200).json({
            status: result.success ? 'success' : 'error',
            pipeline: 'bigquery-user-analytics',
            timing: t.report(false),
            params: { userId, days, dataset, table },
            results: {
                messagesRetrieved: result.messagesRetrieved || 0,
                messagesProcessed: result.messagesProcessed || 0,
                duplicatesSkipped: result.duplicatesSkipped || 0,
                ...(result.error && { error: result.error })
            },
            duration: result.duration
        });

    } catch (error) {
        console.error('ERROR JOB: bigquery-user-analytics', error);

        // Handle parameter validation errors with 400 status
        const isValidationError = error.message.includes('Parameter') || error.message.includes('required');

        res.status(isValidationError ? 400 : 500).json({
            status: 'error',
            pipeline: 'bigquery-user-analytics',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// BigQuery User Analytics summary endpoint
app.get('/bigquery-user-analytics/:userId', async (req, res) => {
    const t = timer('bigquery-user-analytics-summary');
    t.start();

    try {
        sLog('START JOB: bigquery-user-analytics-summary');

        const { userId } = req.params;
        const { days = 7, dataset, table } = req.query;

        console.log(`BIGQUERY-USER-ANALYTICS-SUMMARY: Getting analytics for user ${userId} for last ${days} days`);

        // Get analytics summary
        const analytics = await getAnalyticsSummary({
            userId,
            days: parseInt(days),
            ...(dataset && { dataset }),
            ...(table && { table })
        });

        sLog(`FINISH JOB: bigquery-user-analytics-summary ... ${t.end()}`, {
            totalMessages: analytics.total_messages || 0
        });

        res.status(200).json({
            status: 'success',
            pipeline: 'bigquery-user-analytics-summary',
            timing: t.report(false),
            params: { userId, days, dataset, table },
            analytics
        });

    } catch (error) {
        console.error('ERROR JOB: bigquery-user-analytics-summary', error);

        res.status(500).json({
            status: 'error',
            pipeline: 'bigquery-user-analytics-summary',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// Combined endpoint to run pipeline and get analytics in one call
app.post('/bigquery-user-analytics/:userId/complete', async (req, res) => {
    const t = timer('bigquery-user-analytics-complete');
    t.start();

    try {
        sLog('START JOB: bigquery-user-analytics-complete');

        const { userId } = req.params;
        const params = parseParameters(req);
        const { days = 7, dataset, table } = params;

        console.log(`BIGQUERY-USER-ANALYTICS-COMPLETE: Processing and analyzing user ${userId} for last ${days} days`);

        // Run the pipeline first
        const pipelineResult = await runUserAnalyticsPipeline({
            userId,
            days: parseInt(days),
            ...(dataset && { dataset }),
            ...(table && { table })
        });

        let analytics = null;

        // If pipeline was successful and processed messages, get analytics
        if (pipelineResult.success && pipelineResult.messagesProcessed > 0) {
            analytics = await getAnalyticsSummary({
                userId,
                days: parseInt(days),
                ...(dataset && { dataset }),
                ...(table && { table })
            });
        }

        sLog(`FINISH JOB: bigquery-user-analytics-complete ... ${t.end()}`, {
            success: pipelineResult.success,
            messagesProcessed: pipelineResult.messagesProcessed || 0,
            hasAnalytics: !!analytics
        });

        res.status(200).json({
            status: pipelineResult.success ? 'success' : 'error',
            pipeline: 'bigquery-user-analytics-complete',
            timing: t.report(false),
            params: { userId, days, dataset, table },
            pipelineResults: {
                messagesRetrieved: pipelineResult.messagesRetrieved || 0,
                messagesProcessed: pipelineResult.messagesProcessed || 0,
                duplicatesSkipped: pipelineResult.duplicatesSkipped || 0,
                ...(pipelineResult.error && { error: pipelineResult.error })
            },
            analytics,
            duration: pipelineResult.duration
        });

    } catch (error) {
        console.error('ERROR JOB: bigquery-user-analytics-complete', error);

        // Handle parameter validation errors with 400 status
        const isValidationError = error.message.includes('Parameter') || error.message.includes('required');

        res.status(isValidationError ? 400 : 500).json({
            status: 'error',
            pipeline: 'bigquery-user-analytics-complete',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});


// Start server
app.listen(PORT, () => {
	sLog(`Slack-Mixpanel pipeline server running on port ${PORT} in ${NODE_ENV} mode`);
});