/**
 * @fileoverview Load stage - upload files to Mixpanel with retry logic
 * @module Load
 */

import mixpanelImport from 'mixpanel-import';
import storage from '../services/storage.js';
import path from 'path';
import 'dotenv/config';

const { mixpanel_token, mixpanel_secret } = process.env;

/**
 * Extract relative path from full file path
 * @param {string} fullPath - Full path (GCS or local)
 * @returns {string} Relative path
 */
function getRelativePath(fullPath) {
	if (storage.isGCS()) {
		// For GCS: gs://bucket/prefix/members/2024-01-01-members.jsonl.gz
		// Extract: members/2024-01-01-members.jsonl.gz
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
		// For local: /path/to/tmp/members/2024-01-01-members.jsonl.gz
		// Extract: members/2024-01-01-members.jsonl.gz
		const basePath = path.resolve(storage.getStoragePath());
		const resolved = path.resolve(fullPath);
		return resolved.replace(basePath + path.sep, '');
	}
}

/**
 * Upload file to Mixpanel with retry logic
 * @param {string} filePath - Full path to file (GCS or local)
 * @param {Object} options - Upload options
 * @param {string} options.type - Import type ('event', 'user', 'group')
 * @param {string} [options.groupKey] - Group key for group imports
 * @param {number} [options.maxRetries=3] - Maximum retry attempts
 * @returns {Promise<Object>} Upload result
 */
async function uploadFileWithRetry(filePath, options) {
	const { type, groupKey, maxRetries = 3 } = options;
	let lastError = null;

	for (let attempt = 1; attempt <= maxRetries; attempt++) {
		try {
			console.log(`📤 Uploading ${filePath} (attempt ${attempt}/${maxRetries})...`);

			const importOptions = {
				token: mixpanel_token,
				secret: mixpanel_secret,
				logs: false,
				compress: true
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
 * Generic function to load analytics files to Mixpanel
 * @param {Array<string>} files - Array of file paths
 * @param {string} label - Label for logging
 * @param {Object} uploadOptions - Options for upload
 * @returns {Promise<{uploaded: number, failed: number, results: Array}>}
 */
async function loadFiles(files, label, uploadOptions) {
	console.log(`\n📤 LOAD: Uploading ${files.length} ${label} files to Mixpanel`);

	const results = [];
	let uploaded = 0;
	let failed = 0;

	for (const file of files) {
		const result = await uploadFileWithRetry(file, uploadOptions);

		if (result.success) {
			uploaded++;
			// Delete file after successful upload
			try {
				const relativePath = getRelativePath(file);
				await storage.deleteFile(relativePath);
			} catch (deleteError) {
				console.warn(`⚠️  Failed to delete file ${file}:`, deleteError.message);
			}
		} else {
			failed++;
		}

		results.push(result);
	}

	console.log(`\n📊 LOAD COMPLETE: ${uploaded} uploaded, ${failed} failed`);

	return { uploaded, failed, results };
}

/**
 * Load member analytics files to Mixpanel
 * @param {Array<string>} files - Array of file paths
 * @returns {Promise<{uploaded: number, failed: number, results: Array}>}
 */
export async function loadMemberAnalytics(files) {
	return loadFiles(files, 'member analytics', { type: 'event' });
}

/**
 * Load channel analytics files to Mixpanel
 * @param {Array<string>} files - Array of file paths
 * @returns {Promise<{uploaded: number, failed: number, results: Array}>}
 */
export async function loadChannelAnalytics(files) {
	return loadFiles(files, 'channel analytics', { type: 'event' });
}

export default {
	loadMemberAnalytics,
	loadChannelAnalytics
};
