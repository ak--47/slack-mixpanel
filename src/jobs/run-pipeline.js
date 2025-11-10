#!/usr/bin/env node
/**
 * @fileoverview Direct pipeline runner without HTTP server - file-based architecture
 * @module RunPipeline
 */

import dotenv from 'dotenv';
import { extractMemberAnalytics, extractChannelAnalytics } from './extract.js';
import { loadMemberAnalytics, loadChannelAnalytics } from './load.js';
import * as akTools from 'ak-tools';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dotenv.config();
dayjs.extend(utc);

const { sLog, timer } = akTools;

const {
	NODE_ENV = "production"
} = process.env;

/**
 * Parse and validate pipeline parameters
 * @param {Object} params - Pipeline parameters
 * @returns {Object} Validated parameters
 */
function parseParameters(params) {
	// Handle backfill parameter (mutually exclusive with date params)
	if (params.backfill === 'true' || params.backfill === true) {
		if (params.days !== undefined || params.start_date !== undefined || params.end_date !== undefined) {
			throw new Error('Parameter "backfill" is mutually exclusive with "days", "start_date", and "end_date"');
		}
		return { ...params, env: 'backfill' };
	}

	// Validate mutually exclusive parameters
	const hasDays = params.days !== undefined;
	const hasDateRange = params.start_date !== undefined || params.end_date !== undefined;

	if (hasDays && hasDateRange) {
		throw new Error('Parameters "days" and "start_date/end_date" are mutually exclusive. Use one or the other.');
	}

	// Convert days to number if provided
	if (params.days !== undefined) {
		params.days = parseInt(params.days);
		if (isNaN(params.days) || params.days < 1) {
			throw new Error('Parameter "days" must be a positive integer');
		}
	}

	// Validate date format if provided
	const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
	if (params.start_date && !dateRegex.test(params.start_date)) {
		throw new Error('Parameter "start_date" must be in YYYY-MM-DD format');
	}

	if (params.end_date && !dateRegex.test(params.end_date)) {
		throw new Error('Parameter "end_date" must be in YYYY-MM-DD format');
	}

	return params;
}

/**
 * Determine date range for pipeline
 * @param {Object} params - Validated parameters
 * @returns {Object} Date range configuration
 */
function getDateRange(params) {
	const NOW = dayjs.utc();
	const sfdcDateTimeFmt = "YYYY-MM-DDTHH:mm:ss.SSS[Z]";

	// Handle backfill mode (13 months)
	if (params.env === 'backfill') {
		const BACKFILL_DAYS = 365 + 30; // ~13 months
		const start = NOW.subtract(BACKFILL_DAYS, "d").format(sfdcDateTimeFmt);
		const end = NOW.add(2, "d").format(sfdcDateTimeFmt);
		return {
			start,
			end,
			simpleStart: dayjs.utc(start).format("YYYY-MM-DD"),
			simpleEnd: dayjs.utc(end).format("YYYY-MM-DD"),
			days: BACKFILL_DAYS
		};
	}

	// Determine default days based on environment
	let DAYS;
	if (NODE_ENV === "dev") DAYS = 1;
	if (NODE_ENV === "production") DAYS = 2;
	if (NODE_ENV === "backfill") DAYS = 365;
	if (NODE_ENV === "test") DAYS = 5;
	if (NODE_ENV === "cloud") DAYS = 14;
	if (params.days) DAYS = params.days;

	let start = NOW.subtract(DAYS, "d").format(sfdcDateTimeFmt);
	let end = NOW.add(2, "d").format(sfdcDateTimeFmt);

	if (params.start_date) start = dayjs.utc(params.start_date).format(sfdcDateTimeFmt);
	if (params.end_date) end = dayjs.utc(params.end_date).format(sfdcDateTimeFmt);

	return {
		start,
		end,
		simpleStart: dayjs.utc(start).format("YYYY-MM-DD"),
		simpleEnd: dayjs.utc(end).format("YYYY-MM-DD"),
		days: DAYS
	};
}


/**
 * Run the complete pipeline (members and channels) with file-based extract/load
 * @param {Object} options - Pipeline options
 * @param {number} [options.days] - Number of days to process
 * @param {string} [options.start_date] - Start date (YYYY-MM-DD)
 * @param {string} [options.end_date] - End date (YYYY-MM-DD)
 * @param {boolean} [options.backfill] - Run in backfill mode
 * @param {Array<string>} [options.pipelines] - Which pipelines to run (default: ['members', 'channels'])
 * @param {boolean} [options.extractOnly] - Only extract, don't load
 * @param {boolean} [options.loadOnly] - Only load existing files, don't extract
 * @returns {Promise<Object>} Pipeline results
 */
export async function runPipeline(options = {}) {
	const t = timer('pipeline');
	t.start();

	try {
		const params = parseParameters(options);
		const dateRange = getDateRange(params);
		const pipelines = options.pipelines || ['members', 'channels'];
		const extractOnly = options.extractOnly || false;
		const loadOnly = options.loadOnly || false;

		console.log(`\n${'='.repeat(80)}`);
		console.log(`PIPELINE START: ${params.env || NODE_ENV}`);
		console.log(`Date Range: ${dateRange.simpleStart} to ${dateRange.simpleEnd} (${dateRange.days} days)`);
		console.log(`Pipelines: ${pipelines.join(', ')}`);
		console.log(`Mode: ${extractOnly ? 'Extract Only' : loadOnly ? 'Load Only' : 'Extract + Load'}`);
		console.log(`${'='.repeat(80)}\n`);

		const extractResults = {};
		const loadResults = {};

		// EXTRACT STAGE
		if (!loadOnly) {
			console.log(`\n${'='.repeat(80)}`);
			console.log(`STAGE 1: EXTRACT`);
			console.log(`${'='.repeat(80)}`);

			if (pipelines.includes('members')) {
				extractResults.members = await extractMemberAnalytics(
					dateRange.simpleStart,
					dateRange.simpleEnd
				);
			}

			if (pipelines.includes('channels')) {
				extractResults.channels = await extractChannelAnalytics(
					dateRange.simpleStart,
					dateRange.simpleEnd
				);
			}
		}

		// LOAD STAGE
		if (!extractOnly) {
			console.log(`\n${'='.repeat(80)}`);
			console.log(`STAGE 2: LOAD`);
			console.log(`${'='.repeat(80)}`);

			if (pipelines.includes('members')) {
				const files = loadOnly
					? [] // TODO: discover existing files
					: extractResults.members?.files || [];

				if (files.length > 0) {
					loadResults.members = await loadMemberAnalytics(files);
				} else {
					console.log(`⚠️  No member files to load`);
				}
			}

			if (pipelines.includes('channels')) {
				const files = loadOnly
					? [] // TODO: discover existing files
					: extractResults.channels?.files || [];

				if (files.length > 0) {
					loadResults.channels = await loadChannelAnalytics(files);
				} else {
					console.log(`⚠️  No channel files to load`);
				}
			}
		}

		const timing = t.end();

		console.log(`\n${'='.repeat(80)}`);
		console.log(`PIPELINE COMPLETE: ${timing}`);
		console.log(`${'='.repeat(80)}`);

		if (extractResults.members || extractResults.channels) {
			console.log(`\nEXTRACT RESULTS:`);
			if (extractResults.members) {
				console.log(`  Members: ${extractResults.members.extracted} extracted, ${extractResults.members.skipped} skipped`);
			}
			if (extractResults.channels) {
				console.log(`  Channels: ${extractResults.channels.extracted} extracted, ${extractResults.channels.skipped} skipped`);
			}
		}

		if (loadResults.members || loadResults.channels) {
			console.log(`\nLOAD RESULTS:`);
			if (loadResults.members) {
				console.log(`  Members: ${loadResults.members.uploaded} uploaded, ${loadResults.members.failed} failed`);
			}
			if (loadResults.channels) {
				console.log(`  Channels: ${loadResults.channels.uploaded} uploaded, ${loadResults.channels.failed} failed`);
			}
		}

		console.log();

		return {
			status: 'success',
			timing: t.report(false),
			params: {
				start_date: dateRange.start,
				end_date: dateRange.end,
				days: dateRange.days
			},
			extract: extractResults,
			load: loadResults
		};

	} catch (error) {
		console.error('PIPELINE ERROR:', error);
		throw error;
	}
}

export default runPipeline;
