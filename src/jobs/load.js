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
	const { type, groupKey, transformFunc, heavyObjects, maxRetries = 3, progress = '' } = options;
	let lastError = null;

	// Extract filename for cleaner logs
	const filename = path.basename(filePath);

	for (let attempt = 1; attempt <= maxRetries; attempt++) {
		try {
			if (attempt === 1) {
				console.log(`[MIXPANEL] ${progress} Uploading ${filename}...`);
			} else {
				console.log(`[MIXPANEL] ${progress} Retry ${attempt}/${maxRetries}: ${filename}`);
			}

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

			console.log(`[MIXPANEL] ${progress} ✅ ${filename}`);
			return { success: true, filePath, result, attempts: attempt };

		} catch (error) {
			lastError = error;
			console.error(`[MIXPANEL] ${progress} ❌ ${filename}: ${error.message}`);

			if (attempt < maxRetries) {
				const delay = attempt * 2000; // Exponential backoff: 2s, 4s, 6s
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
	const totalFiles = files.length;

	console.log(`\n[LOAD] Member analytics: ${totalFiles} files to Mixpanel`);

	const heavyObjects = {
		slackMembers,
		slack_prefix
	};

	const results = {
		events: { uploaded: 0, failed: 0, results: [] },
		profiles: { uploaded: 0, failed: 0, results: [] }
	};

	// Upload events first
	console.log(`[LOAD] → Events (${totalFiles} files)`);
	let currentFile = 0;
	for (const file of files) {
		currentFile++;
		const progress = `[${currentFile}/${totalFiles}]`;

		const result = await uploadFileWithRetry(file, {
			type: 'event',
			transformFunc: transformMemberEvent,
			heavyObjects,
			progress
		});

		if (result.success) {
			results.events.uploaded++;
		} else {
			results.events.failed++;
		}
		results.events.results.push(result);
	}

	// Then upload profiles
	console.log(`[LOAD] → Profiles (${totalFiles} files)`);
	currentFile = 0;
	for (const file of files) {
		currentFile++;
		const progress = `[${currentFile}/${totalFiles}]`;

		const result = await uploadFileWithRetry(file, {
			type: 'user',
			transformFunc: transformMemberProfile,
			heavyObjects,
			progress
		});

		if (result.success) {
			results.profiles.uploaded++;
			// Delete file after successful profile upload (last step)
			try {
				const relativePath = getRelativePath(file);
				await storage.deleteFile(relativePath);
			} catch (deleteError) {
				console.warn(`[LOAD] ⚠️  Failed to delete ${file}: ${deleteError.message}`);
			}
		} else {
			results.profiles.failed++;
		}
		results.profiles.results.push(result);
	}

	const totalUploaded = results.events.uploaded + results.profiles.uploaded;
	const totalFailed = results.events.failed + results.profiles.failed;

	console.log(`[LOAD] ✅ Members complete: ${totalUploaded} uploaded, ${totalFailed} failed`);

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
	const totalFiles = files.length;

	console.log(`\n[LOAD] Channel analytics: ${totalFiles} files to Mixpanel`);

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
	console.log(`[LOAD] → Events (${totalFiles} files)`);
	let currentFile = 0;
	for (const file of files) {
		currentFile++;
		const progress = `[${currentFile}/${totalFiles}]`;

		const result = await uploadFileWithRetry(file, {
			type: 'event',
			transformFunc: transformChannelEvent,
			heavyObjects,
			progress
		});

		if (result.success) {
			results.events.uploaded++;
		} else {
			results.events.failed++;
		}
		results.events.results.push(result);
	}

	// Then upload profiles
	console.log(`[LOAD] → Group Profiles (${totalFiles} files)`);
	currentFile = 0;
	for (const file of files) {
		currentFile++;
		const progress = `[${currentFile}/${totalFiles}]`;

		const result = await uploadFileWithRetry(file, {
			type: 'group',
			groupKey: channel_group_key,
			transformFunc: transformChannelProfile,
			heavyObjects,
			progress
		});

		if (result.success) {
			results.profiles.uploaded++;
			// Delete file after successful profile upload (last step)
			try {
				const relativePath = getRelativePath(file);
				await storage.deleteFile(relativePath);
			} catch (deleteError) {
				console.warn(`[LOAD] ⚠️  Failed to delete ${file}: ${deleteError.message}`);
			}
		} else {
			results.profiles.failed++;
		}
		results.profiles.results.push(result);
	}

	const totalUploaded = results.events.uploaded + results.profiles.uploaded;
	const totalFailed = results.events.failed + results.profiles.failed;

	console.log(`[LOAD] ✅ Channels complete: ${totalUploaded} uploaded, ${totalFailed} failed`);

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
