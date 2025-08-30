import express from 'express';
import dotenv from 'dotenv';
import slackPipeline from './jobs/slack-mixpanel-analytics.js';
import * as akTools from 'ak-tools';

dotenv.config();

const { sLog, timer } = akTools;
const { NODE_ENV = "production", PORT = 8080, CONCURRENCY = 10 } = process.env;

const app = express();

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Error handling for development
if (NODE_ENV === 'dev') {
	process.on('uncaughtException', (e, p) => {
		console.error('Uncaught Exception:', e);
	});

	process.on('unhandledRejection', (e, p) => {
		console.error('Unhandled Rejection:', e);
	});
}

// Health check endpoint
app.get('/', (req, res) => {
	res.json({ 
		status: "ok", 
		message: "Slack-Mixpanel pipeline service is alive",
		env: NODE_ENV,
		timestamp: new Date().toISOString()
	});
});

// Main pipeline endpoint
app.post('/slack-analytics', async (req, res) => {
	const t = timer('slack-analytics');
	t.start();
	
	try {
		sLog('START JOB: slack-analytics');
		
		// Parse parameters from both body and query string
		const { body = {}, query = {} } = req;
		
		// Merge query params into body, with query taking precedence
		// Convert query keys to lowercase for case-insensitive matching
		const normalizedQuery = {};
		Object.keys(query).forEach(key => {
			normalizedQuery[key.toLowerCase()] = query[key];
		});
		
		const params = { ...body, ...normalizedQuery };
		
		// Validate mutually exclusive parameters
		const hasDays = params.days !== undefined;
		const hasDateRange = params.start_date !== undefined || params.end_date !== undefined;
		
		if (hasDays && hasDateRange) {
			return res.status(400).json({
				status: 'error',
				error: 'Parameters "days" and "start_date/end_date" are mutually exclusive. Use one or the other.',
				timestamp: new Date().toISOString()
			});
		}
		
		// Convert days to number if provided
		if (params.days !== undefined) {
			params.days = parseInt(params.days);
			if (isNaN(params.days) || params.days < 1) {
				return res.status(400).json({
					status: 'error',
					error: 'Parameter "days" must be a positive integer',
					timestamp: new Date().toISOString()
				});
			}
		}
		
		// Validate date format if provided
		if (params.start_date) {
			const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
			if (!dateRegex.test(params.start_date)) {
				return res.status(400).json({
					status: 'error',
					error: 'Parameter "start_date" must be in YYYY-MM-DD format',
					timestamp: new Date().toISOString()
				});
			}
		}
		
		if (params.end_date) {
			const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
			if (!dateRegex.test(params.end_date)) {
				return res.status(400).json({
					status: 'error',
					error: 'Parameter "end_date" must be in YYYY-MM-DD format',
					timestamp: new Date().toISOString()
				});
			}
		}
		
		const result = await slackPipeline(params);
		
		sLog(`FINISH JOB: slack-analytics ... ${t.end()}`, result);
		
		res.status(200).json({
			status: 'success',
			...result
		});
		
	} catch (error) {
		console.error('ERROR JOB: slack-analytics', error);
		
		res.status(500).json({
			status: 'error',
			error: error.message,
			timestamp: new Date().toISOString()
		});
	}
});

// Legacy compatibility endpoint
app.get('/slack-metrics', async (req, res) => {
	// Redirect GET to POST for compatibility
	req.method = 'POST';
	req.url = '/slack-analytics';
	app.handle(req, res);
});

// Start server
app.listen(PORT, () => {
	sLog(`Slack-Mixpanel pipeline server running on port ${PORT} in ${NODE_ENV} mode`);
});