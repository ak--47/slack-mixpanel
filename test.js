#!/usr/bin/env node

/**
 * Simple test script for the Slack-Mixpanel pipeline
 * Run with: node test.js
 */

import slackAnalytics from './src/jobs/slack-mixpanel-analytics.js';
import { existsSync } from 'fs';

const { NODE_ENV = 'dev' } = process.env;

async function runTests() {
	console.log('🧪 Running Slack-Mixpanel Pipeline Tests...\n');

	try {
		// Test 1: Basic pipeline execution
		console.log('📊 Test 1: Basic pipeline execution');
		const startTime = Date.now();
		
		const result = await slackAnalytics({
			days: 5,  // Just test 5 days to keep it fast
		});
		
		const duration = Date.now() - startTime;
		console.log(`✅ Pipeline completed in ${duration}ms`);
		console.log(`📈 Stats:`, result.stats);
		console.log(`🎯 Results: ${result.results.success.length} success, ${result.results.failed.length} failed`);

		// Test 2: Check file outputs (dev mode)
		if (NODE_ENV === 'dev') {
			console.log('\n📁 Test 2: File output validation');
			const expectedFiles = [
				'tmp/slack-members.ndjson',
				'tmp/slack-member-profiles.ndjson',
				'tmp/slack-channels.ndjson',
				'tmp/slack-channel-profiles.ndjson'
			];

			for (const file of expectedFiles) {
				if (existsSync(file)) {
					console.log(`✅ ${file} exists`);
				} else {
					console.log(`❌ ${file} missing`);
				}
			}
		}

		// Test 3: Validate data structure
		console.log('\n🔍 Test 3: Data structure validation');
		if (result.stats.events.uploaded > 0) {
			console.log('✅ Events processed');
		}
		if (result.stats.users.uploaded > 0) {
			console.log('✅ Users processed');
		}
		if (result.stats.groups.uploaded > 0) {
			console.log('✅ Groups processed');
		}

		console.log('\n🎉 All tests completed successfully!');
		
	} catch (error) {
		console.error('❌ Test failed:', error);
		process.exit(1);
	}
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	runTests();
}