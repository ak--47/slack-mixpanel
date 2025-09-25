/**
 * @fileoverview Task functions for Slack-Mixpanel pipeline processing
 * @module PipelineTasks
 */

import { createWriteStream } from "fs";
import path from "path";
import _ from "highland";
import mixpanel from "../services/mixpanel.js";
import { Storage } from '@google-cloud/storage';

/**
 * @typedef {Object} StreamJob
 * @property {Stream} stream - Highland stream to process
 * @property {('event'|'user'|'group')} type - Type of data
 * @property {string} [groupKey] - Group key for group data
 * @property {string} label - Job label for logging
 */

/**
 * @typedef {Object} TaskStats
 * @property {Object} events - Event statistics
 * @property {number} events.processed - Events processed
 * @property {number} events.uploaded - Events uploaded
 * @property {Object} users - User statistics  
 * @property {number} users.processed - Users processed
 * @property {number} users.uploaded - Users uploaded
 * @property {Object} groups - Group statistics
 * @property {number} groups.processed - Groups processed
 * @property {number} groups.uploaded - Groups uploaded
 */

/**
 * Dev task - dry run with mixpanel-import (no actual upload)
 * @param {StreamJob[]} jobs - Jobs to process
 * @param {TaskStats} stats - Statistics object to update
 * @param {Object} env - Environment variables
 * @returns {Promise<Array>} Results of all jobs
 */
export async function devTask(jobs = [], stats, env = {}) {
	if (!Array.isArray(jobs)) jobs = [jobs];
	
	const { 
		mixpanel_token = 'dev-token', 
		mixpanel_secret, 
		channel_group_key = 'channel_id' 
	} = env;

	const DEV_PIPELINE = jobs.map(job => {
		const { stream = _([]), type = "event", groupKey = "", label = "" } = job;

		return new Promise((resolve, _reject) => {
			const nodeStream = stream
				.tap(() => {
					if (type === "user") stats.users.processed++;
					else if (type === "group") stats.groups.processed++;
					else if (type === "event") stats.events.processed++;
				})
				.toNodeStream({ objectMode: true });

			// Use mixpanel-import in dry-run mode
			mixpanel.upload(nodeStream, { 
				type, 
				groupKey: groupKey || (type === 'group' ? channel_group_key : ''), 
				token: mixpanel_token, 
				secret: mixpanel_secret,
				dryRun: true, // This prevents actual upload
				showProgress: true // Always show progress in dev mode
			}, null, null)
				.then(result => {
					const { meta = {} } = result;
					console.log(`DEV ${label}: ${meta.rows_total || 0} rows processed (dry run)`);
					
					// Update upload stats for dev
					if (type === "user") stats.users.uploaded += meta.rows_total || 0;
					else if (type === "group") stats.groups.uploaded += meta.rows_total || 0;
					else if (type === "event") stats.events.uploaded += meta.rows_total || 0;
					
					resolve({ label, meta, mode: 'dry-run' });
				})
				.catch(err => {
					console.error(`DEV Error in ${label}:`, err);
					resolve({ label, error: err.message, mode: 'dry-run' });
				});
		});
	});

	return Promise.allSettled(DEV_PIPELINE);
}

/**
 * Upload task - streaming upload to Mixpanel (end-to-end streaming)
 * @param {StreamJob[]} jobs - Jobs to process
 * @param {TaskStats} stats - Statistics object to update
 * @param {Object} env - Environment variables
 * @returns {Promise<Array>} Results of all jobs
 */
export async function uploadTask(jobs = [], stats, env = {}) {
	if (!Array.isArray(jobs)) jobs = [jobs];
	
	const { 
		mixpanel_token, 
		mixpanel_secret, 
		channel_group_key = 'channel_id' 
	} = env;

	console.log('🌊 STREAMING MODE: Direct Highland → Node stream → Mixpanel upload');

	const UPLOAD_PIPELINE = jobs.map(job => {
		const { stream = _([]), type = "event", groupKey = "", label = "" } = job;

		return new Promise((resolve, _reject) => {
			// Convert Highland stream to Node.js object mode stream for mixpanel-import
			const nodeStream = stream
				.tap(() => {
					if (type === "user") stats.users.processed++;
					else if (type === "group") stats.groups.processed++;
					else if (type === "event") stats.events.processed++;
				})
				.toNodeStream({ objectMode: true });

			// Use the stream upload method for true end-to-end streaming
			mixpanel.upload(nodeStream, { 
				type, 
				groupKey: groupKey || (type === 'group' ? channel_group_key : ''), 
				token: mixpanel_token, 
				secret: mixpanel_secret,
				// Streaming optimizations
				workers: 100,
				recordsPerBatch: 5000,
				compress: true,
				strict: false
			}, null, (response) => {
				// Track upload success in real-time
				if (response?.num_records_imported || response?.success) {
					const imported = response.num_records_imported || response.success || 0;
					if (type === "user") stats.users.uploaded += imported;
					else if (type === "group") stats.groups.uploaded += imported;
					else if (type === "event") stats.events.uploaded += imported;
				}
			})
				.then(result => {
					const { meta = {} } = result;
					console.log(`🌊 STREAM UPLOAD ${label}: ${meta.rows_imported || 0} rows uploaded`);
					resolve({ label, meta });
				})
				.catch(err => {
					console.error(`💥 STREAM Error in ${label}:`, err);
					resolve({ label, error: err.message });
				});
		});
	});

	return Promise.allSettled(UPLOAD_PIPELINE);
}

/**
 * Simple Array Upload task - converts streams to arrays and uploads directly
 * No stream overhead, just direct array uploads to mixpanel-import
 * @param {StreamJob[]} jobs - Jobs to process
 * @param {TaskStats} stats - Statistics object to update
 * @param {Object} env - Environment variables
 * @returns {Promise<Array>} Results of all jobs
 */
export async function arrayUploadTask(jobs = [], stats, env = {}) {
	if (!Array.isArray(jobs)) jobs = [jobs];
	
	const { 
		mixpanel_token, 
		mixpanel_secret, 
		channel_group_key = 'channel_id' 
	} = env;

	console.log('🚀 ARRAY MODE: Converting streams to arrays...');

	// Convert all streams to arrays in parallel
	const streamToArrayPromises = jobs.map((job) => {
		const { stream = _([]), type = "event", groupKey = "", label = "" } = job;
		
		return new Promise((resolve, _reject) => {
			const data = [];
			
			stream
				.tap(() => {
					// Count processed items
					if (type === "user") stats.users.processed++;
					else if (type === "group") stats.groups.processed++;
					else if (type === "event") stats.events.processed++;
				})
				.each((record) => {
					data.push(record);
				})
				.done(() => {
					console.log(`📦 ${label}: ${data.length} items ready for upload`);
					resolve({ data, type, groupKey, label });
				});
		});
	});

	// Wait for all streams to be converted
	const arrayData = await Promise.all(streamToArrayPromises);
	
	// Upload all arrays in parallel - NO STREAM CONVERSION!
	const uploadPromises = arrayData.map(async ({ data, type, groupKey, label }) => {
		if (data.length === 0) {
			console.log(`UPLOAD ${label}: 0 rows (empty dataset)`);
			return { label, meta: { rows_total: 0, rows_imported: 0 } };
		}

		try {
			// Direct array upload - mixpanel-import handles the rest!
			const result = await mixpanel.uploadArray(data, { 
				type, 
				groupKey: groupKey || (type === 'group' ? channel_group_key : ''), 
				token: mixpanel_token, 
				secret: mixpanel_secret,
				// Optimized settings for direct array upload
				workers: 150,
				recordsPerBatch: 10000,
				compress: true,
				strict: false
				// showProgress is automatically handled by mixpanel service based on NODE_ENV
			});

			const { meta = {} } = result;
			
			// Update upload stats
			if (type === "user") stats.users.uploaded += meta.rows_imported || 0;
			else if (type === "group") stats.groups.uploaded += meta.rows_imported || 0;
			else if (type === "event") stats.events.uploaded += meta.rows_imported || 0;
			
			console.log(`🚀 UPLOAD ${label}: ${meta.rows_imported || 0} rows uploaded (DIRECT ARRAY)`);
			return { label, meta };
			
		} catch (err) {
			console.error(`💥 UPLOAD Error in ${label}:`, err);
			return { label, error: err.message };
		}
	});

	return Promise.allSettled(uploadPromises);
}

/**
 * Write task - backfill mode, write to files
 * @param {StreamJob[]} jobs - Jobs to process
 * @param {TaskStats} stats - Statistics object to update
 * @param {Object} env - Environment variables (unused for write task)
 * @returns {Promise<Array>} Results of all jobs
 */
export async function writeTask(jobs = [], stats, env = {}) {
	if (!Array.isArray(jobs)) jobs = [jobs];

	const writeOperations = jobs.map(job => {
		const { stream = _([]), type = "event", label = "" } = job;
		if (!label) throw new Error("label required for write task");
		
		let finalPath = label;
		if (!finalPath.startsWith("./tmp")) finalPath = `./tmp/${finalPath}`;
		if (!finalPath.endsWith(".ndjson")) finalPath += ".ndjson";
		finalPath = path.resolve(finalPath);

		const outStream = createWriteStream(finalPath, { highWaterMark: 1024 * 1024 * 25 });

		return new Promise((resolve, _reject) => {
			let count = 0;
			let progressInterval = null;
			
			// Show progress for non-production environments
			const { NODE_ENV = "unknown" } = process.env;
			if (NODE_ENV !== "production") {
				console.log(`📝 Starting write for ${label}...`);
				progressInterval = setInterval(() => {
					if (count > 0) {
						console.log(`📝 ${label}: ${count} records written so far...`);
					}
				}, 2000); // Update every 2 seconds
			}
			
			stream
				.map(data => {
					count++;
					if (type === "user") stats.users.processed++;
					else if (type === "group") stats.groups.processed++;
					else if (type === "event") stats.events.processed++;
					return JSON.stringify(data) + "\n";
				})
				.errors(err => {
					console.error(`WRITE Error in ${label}:`, err);
					throw err;
				})
				.pipe(outStream)
				.on('finish', () => {
					// Clear progress interval
					if (progressInterval) clearInterval(progressInterval);
					
					// For write task, processed = uploaded since we wrote them all
					if (type === "user") stats.users.uploaded += count;
					else if (type === "group") stats.groups.uploaded += count;
					else if (type === "event") stats.events.uploaded += count;
					
					console.log(`✅ WRITE ${label}: ${count} rows written to ${finalPath}`);
					resolve({ label, filePath: finalPath, type, rowCount: count });
				})
				.on('error', (err) => {
					// Clear progress interval on error
					if (progressInterval) clearInterval(progressInterval);
					console.error(`❌ WRITE Error in ${label}:`, err);
					resolve({ label, error: err.message });
				});
		});
	});

	return Promise.allSettled(writeOperations);
}

/**
 * Cloud task - upload data to Google Cloud Storage
 * @param {StreamJob[]} jobs - Jobs to process
 * @param {TaskStats} stats - Statistics object to update
 * @param {Object} env - Environment variables with gcs_project and gcs_path
 * @returns {Promise<Array>} Results of all jobs
 */
export async function cloudTask(jobs = [], stats, env = {}) {
	if (!Array.isArray(jobs)) jobs = [jobs];
	
	const { 
		gcs_project, 
		gcs_path, // Format: gs://bucket/path/
		NODE_ENV = "unknown"
	} = env;

	if (!gcs_project || !gcs_path) {
		throw new Error('gcs_project and gcs_path environment variables required for cloud task');
	}

	// Parse GCS path: gs://bucket/path/
	const gcsMatch = gcs_path.match(/^gs:\/\/([^\/]+)\/(.*)$/);
	if (!gcsMatch) {
		throw new Error('gcs_path must be in format gs://bucket/path/');
	}
	
	const [, bucketName, basePath] = gcsMatch;
	
	console.log(`☁️  CLOUD MODE: Uploading to ${bucketName}/${basePath}`);

	// Initialize Google Cloud Storage
	const storage = new Storage({ projectId: gcs_project });
	const bucket = storage.bucket(bucketName);

	const uploadOperations = jobs.map(job => {
		const { stream = _([]), type = "event", label = "" } = job;
		if (!label) throw new Error("label required for cloud task");
		
		// Generate filename with timestamp
		const timestamp = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
		const filename = `${basePath}${label}-${timestamp}.ndjson`;
		
		return new Promise((resolve, _reject) => {
			let count = 0;
			let progressInterval = null;
			
			// Show progress for non-production environments
			if (NODE_ENV !== "production") {
				console.log(`☁️  Starting cloud upload for ${label}...`);
				progressInterval = setInterval(() => {
					if (count > 0) {
						console.log(`☁️  ${label}: ${count} records uploaded so far...`);
					}
				}, 5000); // Update every 5 seconds
			}

			// Create GCS write stream
			const file = bucket.file(filename);
			const gcsStream = file.createWriteStream({
				metadata: {
					contentType: 'application/json',
					cacheControl: 'public, max-age=31536000', // 1 year
				},
				resumable: false, // For smaller files, non-resumable is faster
			});

			// Convert Highland stream to array first to ensure proper termination
			stream
				.map(data => {
					count++;
					if (type === "user") stats.users.processed++;
					else if (type === "group") stats.groups.processed++;
					else if (type === "event") stats.events.processed++;
					return data;
				})
				.errors(err => {
					console.error(`☁️  Cloud Error in ${label}:`, err);
					if (progressInterval) clearInterval(progressInterval);
					resolve({ label, error: err.message });
				})
				.toArray(data => {
					// Clear progress interval
					if (progressInterval) clearInterval(progressInterval);
					
					console.log(`🔍 CLOUD FINISH ${label}: type=${type}, count=${count}`);
					
					// Write all data to GCS
					const ndjsonData = data.map(item => JSON.stringify(item)).join('\n') + '\n';
					
					gcsStream.write(ndjsonData);
					gcsStream.end();
					
					gcsStream.on('finish', () => {
						// For cloud task, processed = uploaded since we wrote them all
						if (type === "user") stats.users.uploaded += count;
						else if (type === "group") stats.groups.uploaded += count;
						else if (type === "event") stats.events.uploaded += count;
						
						console.log(`✅ CLOUD ${label}: ${count} rows uploaded to gs://${bucketName}/${filename}`);
						resolve({ label, gcsPath: `gs://${bucketName}/${filename}`, type, rowCount: count });
					});
					
					gcsStream.on('error', (err) => {
						console.error(`❌ CLOUD GCS Error in ${label}:`, err);
						resolve({ label, error: err.message });
					});
				});
		});
	});

	return Promise.allSettled(uploadOperations);
}

// Direct execution capability for testing tasks
if (import.meta.url === `file://${process.argv[1]}`) {
	console.log('🔧 Testing pipeline tasks directly...');
	
	// Create a simple test stream
	const testStream = _([
		{ test: 'data1', timestamp: new Date().toISOString() },
		{ test: 'data2', timestamp: new Date().toISOString() }
	]);
	
	// Test stats object
	const testStats = {
		events: { processed: 0, uploaded: 0 },
		users: { processed: 0, uploaded: 0 },
		groups: { processed: 0, uploaded: 0 }
	};
	
	// Test environment
	const testEnv = {
		mixpanel_token: 'test-token',
		mixpanel_secret: 'test-secret',
		channel_group_key: 'test_channel_id'
	};
	
	// Test jobs
	const testJobs = [
		{ stream: testStream.fork(), type: 'event', label: 'test-events' },
		{ stream: testStream.fork(), type: 'user', label: 'test-users' }
	];
	
	try {
		console.log('Testing devTask...');
		const devResults = await devTask(testJobs, testStats, testEnv);
		console.log('✅ devTask results:', devResults.map(r => r.value || r.reason));
		console.log('📊 Stats after devTask:', testStats);
		
		// Reset stats for next test
		Object.keys(testStats).forEach(key => {
			testStats[key] = { processed: 0, uploaded: 0 };
		});
		
		console.log('\nTesting writeTask...');
		const writeResults = await writeTask([
			{ stream: testStream.fork(), type: 'event', label: 'test-write-events' }
		], testStats, testEnv);
		console.log('✅ writeTask results:', writeResults.map(r => r.value || r.reason));
		console.log('📊 Stats after writeTask:', testStats);
		
		console.log('\n🎉 All task tests completed!');
		
	} catch (error) {
		console.error('❌ Task test failed:', error);
		process.exit(1);
	}
}