import express from 'express';
import dotenv from 'dotenv';
import { runPipeline } from './jobs/run-pipeline.js';
import * as akTools from 'ak-tools';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dotenv.config();
dayjs.extend(utc);

const { sLog, timer } = akTools;
const {
	NODE_ENV = "unknown",
	PORT = 8080
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
		endpoints: [
			'POST /mixpanel-members',
			'POST /mixpanel-channels',
			'POST /mixpanel-all'
		],
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

	// Parse boolean parameters (from query strings or JSON)
	if (params.cleanup !== undefined) {
		params.cleanup = params.cleanup === 'true' || params.cleanup === true;
	}

	if (params.extractOnly !== undefined) {
		params.extractOnly = params.extractOnly === 'true' || params.extractOnly === true;
	}

	if (params.loadOnly !== undefined) {
		params.loadOnly = params.loadOnly === 'true' || params.loadOnly === true;
	}

	return params;
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
			members: "POST /mixpanel-members - Process Slack members pipeline",
			channels: "POST /mixpanel-channels - Process Slack channels pipeline",
			all: "POST /mixpanel-all - Process both members and channels pipelines"
		},
		parameters: {
			days: "number - Number of days to process (default: 5)",
			start_date: "string - Start date in YYYY-MM-DD format",
			end_date: "string - End date in YYYY-MM-DD format",
			backfill: "boolean - Run in backfill mode (~13 months)",
			cleanup: "boolean - Delete files after successful upload (default: false)",
			extractOnly: "boolean - Only extract, don't load (default: false)",
			loadOnly: "boolean - Only load existing files (default: false)"
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

		// Run file-based pipeline
		const result = await runPipeline({
			...params,
			pipelines: ['members']
		});

		sLog(`FINISH JOB: slack-members ... ${t.end()}`);

		res.status(200).json({
			status: 'success',
			pipeline: 'members',
			timing: t.report(false),
			...result
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

		// Run file-based pipeline
		const result = await runPipeline({
			...params,
			pipelines: ['channels']
		});

		sLog(`FINISH JOB: slack-channels ... ${t.end()}`);

		res.status(200).json({
			status: 'success',
			pipeline: 'channels',
			timing: t.report(false),
			...result
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

		// Run file-based pipeline
		const result = await runPipeline({
			...params,
			pipelines: ['members', 'channels']
		});

		sLog(`FINISH JOB: slack-all ... ${t.end()}`);

		res.status(200).json({
			status: 'success',
			pipeline: 'all',
			timing: t.report(false),
			...result
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


// Start server
app.listen(PORT, () => {
	sLog(`Slack-Mixpanel pipeline server running on port ${PORT} in ${NODE_ENV} mode`);
});