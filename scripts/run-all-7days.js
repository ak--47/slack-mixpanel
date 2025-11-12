#!/usr/bin/env node
/**
 * Run Slack-Mixpanel pipeline for last 7 days
 * Usage: node scripts/run-all-7days.js
 */

import { runPipeline } from '../src/jobs/run-pipeline.js';

try {
	const result = await runPipeline({ days: 7 });

	console.log(`\n✅ Pipeline completed successfully`);
	process.exit(0);

} catch (error) {
	console.error('\n❌ Pipeline failed:', error.message);
	console.error(error.stack);
	process.exit(1);
}
