/**
 * @fileoverview Load stage - upload files to Mixpanel with retry logic and transforms
 * @module Load
 */

import mixpanelImport from 'mixpanel-import';
import storage from '../services/storage.js';
import logger from '../utils/logger.js';
import { transformMemberEvent, transformMemberProfile } from '../transforms/members.js';
import { transformChannelEvent, transformChannelProfile } from '../transforms/channels.js';
import path from 'path';
import 'dotenv/config';

const { mixpanel_token, mixpanel_secret, slack_prefix, channel_group_key = 'channel_id', NODE_ENV = "unknown" } = process.env;

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
 * Upload batch of files to Mixpanel with retry logic
 * @param {Array<string>} files - Array of file paths (GCS or local)
 * @param {Object} options - Upload options
 * @returns {Promise<Object>} Upload result
 */
async function uploadBatch(files, options) {
	const { type, groupKey, transformFunc, heavyObjects, maxRetries = 3 } = options;
	let lastError = null;

	const fileCount = files.length;
	const typeLabel = type === 'event' ? 'Events' : type === 'user' ? 'User Profiles' : 'Group Profiles';

	for (let attempt = 1; attempt <= maxRetries; attempt++) {
		try {
			if (attempt === 1) {
				logger.verbose(`[MIXPANEL] Uploading ${typeLabel}: ${fileCount} files...`);
			} else {
				logger.verbose(`[MIXPANEL] Retry ${attempt}/${maxRetries}: ${typeLabel}`);
			}

			// Credentials for mixpanel-import
			const creds = {
				token: mixpanel_token,
				secret: mixpanel_secret
			};

			// Import options with recordType
			/** @type {import('mixpanel-import').Options} */
			const importOptions = {
				recordType: type, // 'event', 'user', or 'group'
				logs: false,
				compress: true,
				fixData: true,
				removeNulls: true,
				abridged: false, // Always get full details for structured logging
				fixTime: true,
				keepBadRecords: true, // Always keep bad records for debugging
				...(transformFunc && { transformFunc }),
				...(heavyObjects && { heavyObjects }),
				...(groupKey && { groupKey })
			};

			if (NODE_ENV !== "production") importOptions.showProgress = true;

			// Validate group imports
			if (type === 'group' && !groupKey) {
				throw new Error('Group key required for group imports');
			}

			// Pass array of file paths to mixpanel-import
			const result = await mixpanelImport(creds, files, importOptions);

			logger.verbose(`[MIXPANEL] ✅ ${typeLabel}: ${fileCount} files uploaded`);
			return { success: true, files, result, attempts: attempt };

		} catch (error) {
			lastError = error;
			logger.error(`[MIXPANEL] ❌ ${typeLabel}: ${error.message}`);

			if (attempt < maxRetries) {
				const delay = attempt * 2000; // Exponential backoff: 2s, 4s, 6s
				await new Promise(resolve => setTimeout(resolve, delay));
			}
		}
	}

	return {
		success: false,
		files,
		error: lastError?.message || 'Unknown error',
		attempts: maxRetries
	};
}

/**
 * Load member analytics files to Mixpanel (events + profiles)
 * @param {Array<string>} files - Array of file paths (GCS or local)
 * @param {Object} context - Context with slackMembers cache and options
 * @param {Object} options - Load options
 * @param {boolean} [options.cleanup=false] - Delete files after successful upload
 * @returns {Promise<{uploaded: number, failed: number, results: Object}>}
 */
export async function loadMemberAnalytics(files, context, options = {}) {
	const { slackMembers } = context;
	const { cleanup = false } = options;
	const totalFiles = files.length;

	logger.verbose(`\n[LOAD] Member analytics: ${totalFiles} files to Mixpanel`);

	const heavyObjects = {
		slackMembers,
		slack_prefix
	};

	const results = {
		events: { success: false, error: null },
		profiles: { success: false, error: null }
	};

	// Upload events first (batch upload)
	logger.verbose(`[LOAD] → Events (${totalFiles} files)`);
	const eventsResult = await uploadBatch(files, {
		type: 'event',
		transformFunc: transformMemberEvent,
		heavyObjects
	});

	results.events.success = eventsResult.success;
	results.events.error = eventsResult.error || null;
	results.events.count = totalFiles;
	results.events.result = eventsResult.result; // Full mixpanel-import response

	if (!eventsResult.success) {
		logger.error(`[LOAD] ⚠️  Events upload failed, skipping profiles`);
		return {
			uploaded: 0,
			failed: totalFiles * 2, // events + profiles
			results
		};
	}

	// Then upload profiles (batch upload)
	logger.verbose(`[LOAD] → User Profiles (${totalFiles} files)`);
	const profilesResult = await uploadBatch(files, {
		type: 'user',
		transformFunc: transformMemberProfile,
		heavyObjects
	});

	results.profiles.success = profilesResult.success;
	results.profiles.error = profilesResult.error || null;
	results.profiles.count = totalFiles;
	results.profiles.result = profilesResult.result; // Full mixpanel-import response

	// Debug inspection point in dev mode
	if (NODE_ENV === 'dev') debugger;

	// Cleanup files after successful upload if requested
	if (cleanup && eventsResult.success && profilesResult.success) {
		logger.verbose(`[LOAD] → Cleanup: Deleting ${totalFiles} files...`);
		let deleted = 0;
		let deleteFailed = 0;

		for (const file of files) {
			try {
				const relativePath = getRelativePath(file);
				await storage.deleteFile(relativePath);
				deleted++;
			} catch (deleteError) {
				deleteFailed++;
				logger.warn(`[LOAD] ⚠️  Failed to delete ${path.basename(file)}: ${deleteError.message}`);
			}
		}

		logger.verbose(`[LOAD] → Cleanup complete: ${deleted} deleted, ${deleteFailed} failed`);
	}

	const uploaded = (eventsResult.success ? totalFiles : 0) + (profilesResult.success ? totalFiles : 0);
	const failed = (eventsResult.success ? 0 : totalFiles) + (profilesResult.success ? 0 : totalFiles);

	const result = { uploaded, failed, files: totalFiles, results };
	logger.summary('[LOAD] Members Complete', result);
	logger.verbose(`[LOAD] ✅ Members complete: ${uploaded} uploaded, ${failed} failed`);

	return {
		uploaded,
		failed,
		results
	};
}

/**
 * Load channel analytics files to Mixpanel (events + profiles)
 * @param {Array<string>} files - Array of file paths (GCS or local)
 * @param {Object} context - Context with slackChannels cache
 * @param {Object} options - Load options
 * @param {boolean} [options.cleanup=false] - Delete files after successful upload
 * @returns {Promise<{uploaded: number, failed: number, results: Object}>}
 */
export async function loadChannelAnalytics(files, context, options = {}) {
	const { slackChannels } = context;
	const { cleanup = false } = options;
	const totalFiles = files.length;

	logger.verbose(`\n[LOAD] Channel analytics: ${totalFiles} files to Mixpanel`);

	const heavyObjects = {
		slackChannels,
		slack_prefix,
		channel_group_key
	};

	const results = {
		events: { success: false, error: null },
		profiles: { success: false, error: null }
	};

	// Upload events first (batch upload)
	logger.verbose(`[LOAD] → Events (${totalFiles} files)`);
	const eventsResult = await uploadBatch(files, {
		type: 'event',
		transformFunc: transformChannelEvent,
		heavyObjects
	});

	results.events.success = eventsResult.success;
	results.events.error = eventsResult.error || null;
	results.events.count = totalFiles;
	results.events.result = eventsResult.result; // Full mixpanel-import response

	if (!eventsResult.success) {
		logger.error(`[LOAD] ⚠️  Events upload failed, skipping group profiles`);
		return {
			uploaded: 0,
			failed: totalFiles * 2, // events + profiles
			results
		};
	}

	// Then upload group profiles (batch upload)
	logger.verbose(`[LOAD] → Group Profiles (${totalFiles} files)`);
	const profilesResult = await uploadBatch(files, {
		type: 'group',
		groupKey: channel_group_key,
		transformFunc: transformChannelProfile,
		heavyObjects
	});

	results.profiles.success = profilesResult.success;
	results.profiles.error = profilesResult.error || null;
	results.profiles.count = totalFiles;
	results.profiles.result = profilesResult.result; // Full mixpanel-import response

	// Debug inspection point in dev mode
	if (NODE_ENV === 'dev') debugger;

	// Cleanup files after successful upload if requested
	if (cleanup && eventsResult.success && profilesResult.success) {
		logger.verbose(`[LOAD] → Cleanup: Deleting ${totalFiles} files...`);
		let deleted = 0;
		let deleteFailed = 0;

		for (const file of files) {
			try {
				const relativePath = getRelativePath(file);
				await storage.deleteFile(relativePath);
				deleted++;
			} catch (deleteError) {
				deleteFailed++;
				logger.warn(`[LOAD] ⚠️  Failed to delete ${path.basename(file)}: ${deleteError.message}`);
			}
		}

		logger.verbose(`[LOAD] → Cleanup complete: ${deleted} deleted, ${deleteFailed} failed`);
	}

	const uploaded = (eventsResult.success ? totalFiles : 0) + (profilesResult.success ? totalFiles : 0);
	const failed = (eventsResult.success ? 0 : totalFiles) + (profilesResult.success ? 0 : totalFiles);

	const result = { uploaded, failed, files: totalFiles, results };
	logger.summary('[LOAD] Channels Complete', result);
	logger.verbose(`[LOAD] ✅ Channels complete: ${uploaded} uploaded, ${failed} failed`);

	return {
		uploaded,
		failed,
		results
	};
}

export default {
	loadMemberAnalytics,
	loadChannelAnalytics
};
