#!/usr/bin/env node
/**
 * Run Slack-Mixpanel pipeline for last 7 days
 * Usage: node scripts/run-all-7days.js
 */

import { runPipeline } from '../src/jobs/run-pipeline.js';
import { createLogger } from '../src/utils/logger.js';
import dayjs from 'dayjs';

const logFile = `logs/all-7days-${dayjs().format('YYYYMMDD-HHmmss')}.log`;
const logger = createLogger(logFile);

console.log(`Logging to: ${logFile}\n`);

try {
	const result = await runPipeline({ days: 7 });

	console.log(`\n✅ Pipeline completed successfully`);
	console.log(`📝 Full logs: ${logFile}\n`);

	logger.restore();
	process.exit(0);

} catch (error) {
	console.error('\n❌ Pipeline failed:', error.message);
	console.error(error.stack);
	console.log(`\n📝 Full logs: ${logFile}\n`);

	logger.restore();
	process.exit(1);
}
