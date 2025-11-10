#!/usr/bin/env node
/**
 * Run Slack-Mixpanel pipeline for full backfill (13 months)
 * Usage: node scripts/run-all-backfill.js
 */

import { runPipeline } from '../src/jobs/run-pipeline.js';
import { createLogger } from '../src/utils/logger.js';
import dayjs from 'dayjs';

const logFile = `logs/all-backfill-${dayjs().format('YYYYMMDD-HHmmss')}.log`;
const logger = createLogger(logFile);

console.log(`⚠️  WARNING: This will process 13 months of historical data and may take a while\n`);
console.log(`Logging to: ${logFile}\n`);

try {
	const result = await runPipeline({ backfill: true });

	if (result.stats) {
		console.log('\n📊 Final Statistics:');
		console.log(`   Events: ${result.stats.events.processed} processed, ${result.stats.events.uploaded} uploaded`);
		console.log(`   Users: ${result.stats.users.processed} processed, ${result.stats.users.uploaded} uploaded`);
		console.log(`   Groups: ${result.stats.groups.processed} processed, ${result.stats.groups.uploaded} uploaded`);
	}

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
