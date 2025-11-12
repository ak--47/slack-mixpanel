#!/usr/bin/env node
/**
 * Run Slack-Mixpanel pipeline for full backfill (13 months)
 * Usage: node scripts/run-all-backfill.js
 */

import { runPipeline } from '../src/jobs/run-pipeline.js';

console.log(`⚠️  WARNING: This will process 13 months of historical data and may take a while\n`);

try {
	const result = await runPipeline({ backfill: true });

	console.log(`\n✅ Pipeline completed successfully`);
	process.exit(0);

} catch (error) {
	console.error('\n❌ Pipeline failed:', error.message);
	console.error(error.stack);
	process.exit(1);
}
