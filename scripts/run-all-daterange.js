#!/usr/bin/env node
/**
 * Run Slack-Mixpanel pipeline for custom date range
 * Usage: node scripts/run-all-daterange.js START_DATE END_DATE
 * Example: node scripts/run-all-daterange.js 2024-01-01 2024-01-31
 */

import { runPipeline } from '../src/jobs/run-pipeline.js';

// Parse command line arguments
const [startDate, endDate] = process.argv.slice(2);

if (!startDate || !endDate) {
	console.error('Usage: node scripts/run-all-daterange.js START_DATE END_DATE');
	console.error('Example: node scripts/run-all-daterange.js 2024-01-01 2024-01-31');
	process.exit(1);
}

try {
	const result = await runPipeline({
		start_date: startDate,
		end_date: endDate
	});

	console.log(`\n✅ Pipeline completed successfully`);
	process.exit(0);

} catch (error) {
	console.error('\n❌ Pipeline failed:', error.message);
	console.error(error.stack);
	process.exit(1);
}
