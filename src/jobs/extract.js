/**
 * @fileoverview Extract stage - fetch Slack data and write to files
 * @module Extract
 */

import slackService from '../services/slack.js';
import storage from '../services/storage.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dayjs.extend(utc);

/**
 * Extract member analytics data for date range
 * @param {string} startDate - Start date (YYYY-MM-DD)
 * @param {string} endDate - End date (YYYY-MM-DD)
 * @returns {Promise<{extracted: number, skipped: number, files: string[]}>}
 */
export async function extractMemberAnalytics(startDate, endDate) {
	console.log(`\n📥 EXTRACT: Member analytics from ${startDate} to ${endDate}`);

	const start = dayjs.utc(startDate);
	const end = dayjs.utc(endDate);
	const delta = end.diff(start, 'd');
	const daysToFetch = Array.from({ length: delta + 1 }, (_, i) => start.add(i, 'd').format('YYYY-MM-DD'));

	let extracted = 0;
	let skipped = 0;
	const files = [];

	for (const date of daysToFetch) {
		const filePath = `members/${date}-members.jsonl.gz`;

		// Skip if already exists
		if (await storage.fileExists(filePath)) {
			console.log(`⏭️  Skip ${date}: file already exists`);
			skipped++;
			files.push(storage.getFullPath(filePath));
			continue;
		}

		try {
			// Fetch analytics for this date
			const data = await slackService.analytics(date, date, 'member', false);

			if (data && data.length > 0) {
				// Write to file
				const writtenPath = await storage.writeJSONLGz(filePath, data);
				console.log(`✅ Extracted ${date}: ${data.length} records → ${filePath}`);
				extracted++;
				files.push(writtenPath);
			} else {
				console.log(`⚠️  No data for ${date}`);
			}

		} catch (error) {
			console.error(`❌ Error extracting ${date}:`, error.message);
		}
	}

	console.log(`\n📊 EXTRACT COMPLETE: ${extracted} extracted, ${skipped} skipped`);

	return { extracted, skipped, files };
}

/**
 * Extract channel analytics data for date range
 * @param {string} startDate - Start date (YYYY-MM-DD)
 * @param {string} endDate - End date (YYYY-MM-DD)
 * @returns {Promise<{extracted: number, skipped: number, files: string[]}>}
 */
export async function extractChannelAnalytics(startDate, endDate) {
	console.log(`\n📥 EXTRACT: Channel analytics from ${startDate} to ${endDate}`);

	const start = dayjs.utc(startDate);
	const end = dayjs.utc(endDate);
	const delta = end.diff(start, 'd');
	const daysToFetch = Array.from({ length: delta + 1 }, (_, i) => start.add(i, 'd').format('YYYY-MM-DD'));

	let extracted = 0;
	let skipped = 0;
	const files = [];

	for (const date of daysToFetch) {
		const filePath = `channels/${date}-channels.jsonl.gz`;

		// Skip if already exists
		if (await storage.fileExists(filePath)) {
			console.log(`⏭️  Skip ${date}: file already exists`);
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
				console.log(`✅ Extracted ${date}: ${data.length} records → ${filePath}`);
				extracted++;
				files.push(writtenPath);
			} else {
				console.log(`⚠️  No data for ${date}`);
			}

		} catch (error) {
			console.error(`❌ Error extracting ${date}:`, error.message);
		}
	}

	console.log(`\n📊 EXTRACT COMPLETE: ${extracted} extracted, ${skipped} skipped`);

	return { extracted, skipped, files };
}

export default {
	extractMemberAnalytics,
	extractChannelAnalytics
};
