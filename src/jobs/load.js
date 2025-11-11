/**
 * @fileoverview Load stage - upload files to Mixpanel with retry logic and transforms
 * @module Load
 */

import mixpanelImport from 'mixpanel-import';
import storage from '../services/storage.js';
import { transformMemberEvent, transformMemberProfile } from '../transforms/members.js';
import { transformChannelEvent, transformChannelProfile } from '../transforms/channels.js';
import path from 'path';
import 'dotenv/config';

const { mixpanel_token, mixpanel_secret, slack_prefix, channel_group_key = 'channel_id' } = process.env;

/**
 * Extract relative path from full file path
 * @param {string} fullPath - Full path (GCS or local)
 * @returns {string} Relative path
 */
function getRelativePath(fullPath) {
	if (storage.isGCS()) {
		const gcsPath = storage.getStoragePath();
		const match = fullPath.match(/^gs:\/\/[^\/]+\/(.+)$/);
		if (match) {
			const fullGcsPath = match[1];
			const prefixMatch = gcsPath.match(/^gs:\/\/[^\/]+\/(.+)$/);
			if (prefixMatch) {
				const prefix = prefixMatch[1];
				return fullGcsPath.replace(prefix + '/', '');
			}
			return fullGcsPath;
		}
		return fullPath;
	} else {
		const basePath = path.resolve(storage.getStoragePath());
		const resolved = path.resolve(fullPath);
		return resolved.replace(basePath + path.sep, '');
	}
}

/**
 * Upload file to Mixpanel with retry logic
 * @param {string} filePath - Full path to file (GCS or local)
 * @param {Object} options - Upload options
 * @returns {Promise<Object>} Upload result
 */
async function uploadFileWithRetry(filePath, options) {
	const { type, groupKey, transformFunc, heavyObjects, maxRetries = 3 } = options;
	let lastError = null;

	for (let attempt = 1; attempt <= maxRetries; attempt++) {
		try {
			console.log(`📤 Uploading ${filePath} (attempt ${attempt}/${maxRetries})...`);

			const importOptions = {
				token: mixpanel_token,
				secret: mixpanel_secret,
				logs: false,
				compress: true,
				...(transformFunc && { transformFunc }),
				...(heavyObjects && { heavyObjects })
			};

			let result;

			if (type === 'event') {
				result = await mixpanelImport.events(filePath, importOptions);
			} else if (type === 'user') {
				result = await mixpanelImport.people(filePath, importOptions);
			} else if (type === 'group') {
				if (!groupKey) {
					throw new Error('Group key required for group imports');
				}
				result = await mixpanelImport.groups(filePath, groupKey, importOptions);
			} else {
				throw new Error(`Unknown import type: ${type}`);
			}

			console.log(`✅ Upload successful: ${filePath}`);
			return { success: true, filePath, result, attempts: attempt };

		} catch (error) {
			lastError = error;
			console.error(`❌ Upload attempt ${attempt} failed: ${error.message}`);

			if (attempt < maxRetries) {
				const delay = attempt * 2000; // Exponential backoff: 2s, 4s, 6s
				console.log(`⏳ Waiting ${delay}ms before retry...`);
				await new Promise(resolve => setTimeout(resolve, delay));
			}
		}
	}

	return {
		success: false,
		filePath,
		error: lastError?.message || 'Unknown error',
		attempts: maxRetries
	};
}

/**
 * Load member analytics files to Mixpanel (events + profiles)
 * @param {Array<string>} files - Array of file paths
 * @param {Object} context - Context with slackMembers cache
 * @returns {Promise<{uploaded: number, failed: number, results: Array}>}
 */
export async function loadMemberAnalytics(files, context) {
	const { slackMembers } = context;

	console.log(`\n📤 LOAD: Uploading ${files.length} member analytics files to Mixpanel`);

	const heavyObjects = {
		slackMembers,
		slack_prefix
	};

	const results = {
		events: { uploaded: 0, failed: 0, results: [] },
		profiles: { uploaded: 0, failed: 0, results: [] }
	};

	// Upload events first
	console.log(`\n  → Uploading member events...`);
	for (const file of files) {
		const result = await uploadFileWithRetry(file, {
			type: 'event',
			transformFunc: transformMemberEvent,
			heavyObjects
		});

		if (result.success) {
			results.events.uploaded++;
		} else {
			results.events.failed++;
		}
		results.events.results.push(result);
	}

	// Then upload profiles
	console.log(`\n  → Uploading member profiles...`);
	for (const file of files) {
		const result = await uploadFileWithRetry(file, {
			type: 'user',
			transformFunc: transformMemberProfile,
			heavyObjects
		});

		if (result.success) {
			results.profiles.uploaded++;
			// Delete file after successful profile upload (last step)
			try {
				const relativePath = getRelativePath(file);
				await storage.deleteFile(relativePath);
			} catch (deleteError) {
				console.warn(`⚠️  Failed to delete file ${file}:`, deleteError.message);
			}
		} else {
			results.profiles.failed++;
		}
		results.profiles.results.push(result);
	}

	const totalUploaded = results.events.uploaded + results.profiles.uploaded;
	const totalFailed = results.events.failed + results.profiles.failed;

	console.log(`\n📊 MEMBER LOAD COMPLETE:`);
	console.log(`   Events: ${results.events.uploaded} uploaded, ${results.events.failed} failed`);
	console.log(`   Profiles: ${results.profiles.uploaded} uploaded, ${results.profiles.failed} failed`);
	console.log(`   Total: ${totalUploaded} uploaded, ${totalFailed} failed`);

	return {
		uploaded: totalUploaded,
		failed: totalFailed,
		results
	};
}

/**
 * Load channel analytics files to Mixpanel (events + profiles)
 * @param {Array<string>} files - Array of file paths
 * @param {Object} context - Context with slackChannels cache
 * @returns {Promise<{uploaded: number, failed: number, results: Array}>}
 */
export async function loadChannelAnalytics(files, context) {
	const { slackChannels } = context;

	console.log(`\n📤 LOAD: Uploading ${files.length} channel analytics files to Mixpanel`);

	const heavyObjects = {
		slackChannels,
		slack_prefix,
		channel_group_key
	};

	const results = {
		events: { uploaded: 0, failed: 0, results: [] },
		profiles: { uploaded: 0, failed: 0, results: [] }
	};

	// Upload events first
	console.log(`\n  → Uploading channel events...`);
	for (const file of files) {
		const result = await uploadFileWithRetry(file, {
			type: 'event',
			transformFunc: transformChannelEvent,
			heavyObjects
		});

		if (result.success) {
			results.events.uploaded++;
		} else {
			results.events.failed++;
		}
		results.events.results.push(result);
	}

	// Then upload profiles
	console.log(`\n  → Uploading channel group profiles...`);
	for (const file of files) {
		const result = await uploadFileWithRetry(file, {
			type: 'group',
			groupKey: channel_group_key,
			transformFunc: transformChannelProfile,
			heavyObjects
		});

		if (result.success) {
			results.profiles.uploaded++;
			// Delete file after successful profile upload (last step)
			try {
				const relativePath = getRelativePath(file);
				await storage.deleteFile(relativePath);
			} catch (deleteError) {
				console.warn(`⚠️  Failed to delete file ${file}:`, deleteError.message);
			}
		} else {
			results.profiles.failed++;
		}
		results.profiles.results.push(result);
	}

	const totalUploaded = results.events.uploaded + results.profiles.uploaded;
	const totalFailed = results.events.failed + results.profiles.failed;

	console.log(`\n📊 CHANNEL LOAD COMPLETE:`);
	console.log(`   Events: ${results.events.uploaded} uploaded, ${results.events.failed} failed`);
	console.log(`   Profiles: ${results.profiles.uploaded} uploaded, ${results.profiles.failed} failed`);
	console.log(`   Total: ${totalUploaded} uploaded, ${totalFailed} failed`);

	return {
		uploaded: totalUploaded,
		failed: totalFailed,
		results
	};
}

export default {
	loadMemberAnalytics,
	loadChannelAnalytics
};
