/**
 * @fileoverview Extract stage - fetch Slack data and write to files
 * @module Extract
 */

import slackService from '../services/slack.js';
import storage from '../services/storage.js';
import logger from '../utils/logger.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import 'dotenv/config';

dayjs.extend(utc);

const { company_domain } = process.env;

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
					// Write to file
					const writtenPath = await storage.writeJSONLGz(filePath, filteredData);
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
				// Write to file
				const writtenPath = await storage.writeJSONLGz(filePath, data);
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
