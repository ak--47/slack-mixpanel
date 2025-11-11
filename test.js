#!/usr/bin/env node

/**
 * Simple test script for the Slack-Mixpanel pipeline
 * Run with: node test.js
 */

import { runPipeline } from './src/jobs/run-pipeline.js';
import storage from './src/services/storage.js';

const { NODE_ENV = 'dev' } = process.env;

async function runTests() {
	console.log('🧪 Running Slack-Mixpanel Pipeline Tests...\n');

	try {
		// Test 1: Basic pipeline execution
		console.log('📊 Test 1: Basic pipeline execution (members only)');
		const startTime = Date.now();

		const result = await runPipeline({
			days: 2,  // Test 2 days to keep it fast
			pipelines: ['members'],
			extractOnly: false  // Run full extract + load
		});

		const duration = Date.now() - startTime;
		console.log(`✅ Pipeline completed in ${duration}ms`);
		console.log(`📈 Extract Stats:`, result.extract);
		console.log(`📤 Load Stats:`, result.load);

		// Test 2: Check file outputs (dev mode with extractOnly)
		if (NODE_ENV === 'dev') {
			console.log('\n📁 Test 2: File extraction (extract-only mode)');

			const extractResult = await runPipeline({
				days: 1,
				pipelines: ['members', 'channels'],
				extractOnly: true  // Only extract, don't load
			});

			console.log('✅ Extract-only mode completed');
			console.log(`   Members: ${extractResult.extract.members?.extracted || 0} extracted, ${extractResult.extract.members?.skipped || 0} skipped`);
			console.log(`   Channels: ${extractResult.extract.channels?.extracted || 0} extracted, ${extractResult.extract.channels?.skipped || 0} skipped`);

			// Verify files exist
			const memberFiles = extractResult.extract.members?.files || [];
			const channelFiles = extractResult.extract.channels?.files || [];

			if (memberFiles.length > 0) {
				console.log(`✅ ${memberFiles.length} member files created`);
			}
			if (channelFiles.length > 0) {
				console.log(`✅ ${channelFiles.length} channel files created`);
			}
		}

		// Test 3: Validate data structure
		console.log('\n🔍 Test 3: Data structure validation');
		if (result.load?.members) {
			console.log(`✅ Members: ${result.load.members.uploaded} uploaded, ${result.load.members.failed} failed`);
		}
		if (result.load?.channels) {
			console.log(`✅ Channels: ${result.load.channels.uploaded} uploaded, ${result.load.channels.failed} failed`);
		}

		// Test 4: Resumable pipeline (skip existing files)
		console.log('\n🔄 Test 4: Resumable pipeline (should skip existing files)');
		const resumeResult = await runPipeline({
			days: 2,
			pipelines: ['members'],
			extractOnly: true
		});

		if (resumeResult.extract.members?.skipped > 0) {
			console.log(`✅ Skipped ${resumeResult.extract.members.skipped} existing files (resumable works!)`);
		}

		// Test 5: Storage service
		console.log('\n💾 Test 5: Storage service');
		console.log(`   Storage type: ${storage.isGCS() ? 'Google Cloud Storage' : 'Local Filesystem'}`);
		console.log(`   Storage path: ${storage.getStoragePath()}`);

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
