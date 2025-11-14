/**
 * @fileoverview Extract stage - fetch Slack data and write to files
 * @module Extract
 */

import slackService from '../services/slack.js';
import storage from '../services/storage.js';
import logger from '../utils/logger.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import pLimit from 'p-limit';
import 'dotenv/config';

dayjs.extend(utc);

const { company_domain } = process.env;

/**
 * Enrich user analytics records with detailed user information
 * Adds an ENRICHED key to each record containing:
 * - user: Full user object from users.info (name, email, timezone, etc.)
 * - profile: Extended profile from users.profile.get (custom fields, status, etc.)
 * - ok: Boolean indicating if the fetch was successful
 * - error: Error message if the fetch failed
 *
 * @param {Array} records - Analytics records with user_id fields
 * @returns {Promise<Array>} Records with ENRICHED key containing user details
 * @example
 * // Enriched record structure:
 * {
 *   user_id: 'U123',
 *   email_address: 'user@example.com',
 *   messages_posted: 10,
 *   ENRICHED: {
 *     user: { id: 'U123', real_name: 'John Doe', tz: 'America/Los_Angeles', ... },
 *     profile: { email: 'user@example.com', phone: '555-1234', title: 'Engineer', ... },
 *     ok: true
 *   }
 * }
 */
async function enrichUserRecords(records) {
	if (!records || records.length === 0) return records;

	// Get unique user IDs from the records
	const uniqueUserIds = [...new Set(records.map(r => r.user_id).filter(Boolean))];

	if (uniqueUserIds.length === 0) return records;

	logger.verbose(`[ENRICH] Fetching details for ${uniqueUserIds.length} unique users`);

	// Fetch user details with concurrency control (rate limiting)
	const limit = pLimit(2); // Conservative: 2 concurrent requests
	const userDetailsMap = new Map();

	const fetchPromises = uniqueUserIds.map(userId =>
		limit(async () => {
			try {
				const details = await slackService.getUserDetails(userId);
				userDetailsMap.set(userId, details);
				logger.verbose(`[ENRICH] ✅ User ${userId}`);
			} catch (error) {
				logger.verbose(`[ENRICH] ⚠️  Failed to fetch user ${userId}: ${error.message}`);
				// Store error state so we don't retry
				userDetailsMap.set(userId, { error: error.message });
			}
		})
	);

	await Promise.all(fetchPromises);

	// Add ENRICHED key to each record
	const enrichedRecords = records.map(record => ({
		...record,
		ENRICHED: userDetailsMap.get(record.user_id) || null
	}));

	logger.verbose(`[ENRICH] ✅ Enriched ${enrichedRecords.length} member records`);

	return enrichedRecords;
}

/**
 * Enrich channel analytics records with detailed channel information
 * Adds an ENRICHED key to each record containing:
 * - channel: Full channel object from conversations.info (name, topic, purpose, members, etc.)
 * - ok: Boolean indicating if the fetch was successful
 * - error: Error message if the fetch failed
 *
 * @param {Array} records - Analytics records with channel_id fields
 * @returns {Promise<Array>} Records with ENRICHED key containing channel details
 * @example
 * // Enriched record structure:
 * {
 *   channel_id: 'C123',
 *   messages_posted: 50,
 *   members_who_posted: 10,
 *   ENRICHED: {
 *     channel: {
 *       id: 'C123',
 *       name: 'general',
 *       topic: { value: 'Company-wide announcements' },
 *       purpose: { value: 'General discussion' },
 *       num_members: 100,
 *       is_private: false,
 *       ...
 *     },
 *     ok: true
 *   }
 * }
 */
async function enrichChannelRecords(records) {
	if (!records || records.length === 0) return records;

	// Get unique channel IDs from the records
	const uniqueChannelIds = [...new Set(records.map(r => r.channel_id).filter(Boolean))];

	if (uniqueChannelIds.length === 0) return records;

	logger.verbose(`[ENRICH] Fetching details for ${uniqueChannelIds.length} unique channels`);

	// Fetch channel details with concurrency control (rate limiting)
	const limit = pLimit(2); // Conservative: 2 concurrent requests
	const channelDetailsMap = new Map();

	const fetchPromises = uniqueChannelIds.map(channelId =>
		limit(async () => {
			try {
				const details = await slackService.getChannelDetails(channelId);
				channelDetailsMap.set(channelId, details);
				logger.verbose(`[ENRICH] ✅ Channel ${channelId}`);
			} catch (error) {
				logger.verbose(`[ENRICH] ⚠️  Failed to fetch channel ${channelId}: ${error.message}`);
				// Store error state so we don't retry
				channelDetailsMap.set(channelId, { error: error.message });
			}
		})
	);

	await Promise.all(fetchPromises);

	// Add ENRICHED key to each record
	const enrichedRecords = records.map(record => ({
		...record,
		ENRICHED: channelDetailsMap.get(record.channel_id) || null
	}));

	logger.verbose(`[ENRICH] ✅ Enriched ${enrichedRecords.length} channel records`);

	return enrichedRecords;
}

/**
 * Extract member analytics data for date range
 * @param {string} startDate - Start date (YYYY-MM-DD)
 * @param {string} endDate - End date (YYYY-MM-DD)
 * @returns {Promise<{extracted: number, skipped: number, files: string[]}>}
 */
export async function extractMemberAnalytics(startDate, endDate) {
	const start = dayjs.utc(startDate);
	const end = dayjs.utc(endDate);
	const delta = end.diff(start, 'd');
	const daysToFetch = Array.from({ length: delta + 1 }, (_, i) => start.add(i, 'd').format('YYYY-MM-DD'));
	const totalDays = daysToFetch.length;

	logger.verbose(`\n[EXTRACT] Member analytics: ${totalDays} days (${startDate} to ${endDate})`);

	let extracted = 0;
	let skipped = 0;
	const files = [];
	let currentDay = 0;

	for (const date of daysToFetch) {
		currentDay++;
		const progress = `[${currentDay}/${totalDays}]`;
		const filePath = `members/${date}-members.jsonl.gz`;

		// Skip if already exists
		if (await storage.fileExists(filePath)) {
			logger.verbose(`[EXTRACT] ${progress} ⏭️  ${date} (cached)`);
			skipped++;
			files.push(storage.getFullPath(filePath));
			continue;
		}

		try {
			// Fetch analytics for this date
			const data = await slackService.analytics(date, date, 'member', false);

			if (data && data.length > 0) {
				// Filter to only company domain users
				const filteredData = company_domain
					? data.filter(record => record.email_address && record.email_address.endsWith(`@${company_domain}`))
					: data;

				if (filteredData.length > 0) {
					// Enrich with detailed user information
					const enrichedData = await enrichUserRecords(filteredData);

					// Write enriched data to file
					const writtenPath = await storage.writeJSONLGz(filePath, enrichedData);
					logger.verbose(`[EXTRACT] ${progress} ✅ ${date}: ${filteredData.length}/${data.length} records (@${company_domain})`);
					extracted++;
					files.push(writtenPath);
				} else {
					logger.verbose(`[EXTRACT] ${progress} ⚠️  ${date}: No @${company_domain} users`);
				}
			} else {
				logger.verbose(`[EXTRACT] ${progress} ⚠️  ${date}: No data`);
			}

		} catch (error) {
			logger.error(`[EXTRACT] ${progress} ❌ ${date}: ${error.message}`);
		}
	}

	const result = { extracted, skipped, files: files.length, dateRange: `${startDate} to ${endDate}` };
	logger.summary('[EXTRACT] Members Complete', result);
	logger.verbose(`[EXTRACT] ✅ Complete: ${extracted} extracted, ${skipped} cached`);

	return { extracted, skipped, files };
}

/**
 * Extract channel analytics data for date range
 * @param {string} startDate - Start date (YYYY-MM-DD)
 * @param {string} endDate - End date (YYYY-MM-DD)
 * @returns {Promise<{extracted: number, skipped: number, files: string[]}>}
 */
export async function extractChannelAnalytics(startDate, endDate) {
	const start = dayjs.utc(startDate);
	const end = dayjs.utc(endDate);
	const delta = end.diff(start, 'd');
	const daysToFetch = Array.from({ length: delta + 1 }, (_, i) => start.add(i, 'd').format('YYYY-MM-DD'));
	const totalDays = daysToFetch.length;

	logger.verbose(`\n[EXTRACT] Channel analytics: ${totalDays} days (${startDate} to ${endDate})`);

	let extracted = 0;
	let skipped = 0;
	const files = [];
	let currentDay = 0;

	for (const date of daysToFetch) {
		currentDay++;
		const progress = `[${currentDay}/${totalDays}]`;
		const filePath = `channels/${date}-channels.jsonl.gz`;

		// Skip if already exists
		if (await storage.fileExists(filePath)) {
			logger.verbose(`[EXTRACT] ${progress} ⏭️  ${date} (cached)`);
			skipped++;
			files.push(storage.getFullPath(filePath));
			continue;
		}

		try {
			// Fetch analytics for this date
			const data = await slackService.analytics(date, date, 'public_channel', false);

			if (data && data.length > 0) {
				// Enrich with detailed channel information
				const enrichedData = await enrichChannelRecords(data);

				// Write enriched data to file
				const writtenPath = await storage.writeJSONLGz(filePath, enrichedData);
				logger.verbose(`[EXTRACT] ${progress} ✅ ${date}: ${data.length} records`);
				extracted++;
				files.push(writtenPath);
			} else {
				logger.verbose(`[EXTRACT] ${progress} ⚠️  ${date}: No data`);
			}

		} catch (error) {
			logger.error(`[EXTRACT] ${progress} ❌ ${date}: ${error.message}`);
		}
	}

	const result = { extracted, skipped, files: files.length, dateRange: `${startDate} to ${endDate}` };
	logger.summary('[EXTRACT] Channels Complete', result);
	logger.verbose(`[EXTRACT] ✅ Complete: ${extracted} extracted, ${skipped} cached`);

	return { extracted, skipped, files };
}

export default {
	extractMemberAnalytics,
	extractChannelAnalytics
};
