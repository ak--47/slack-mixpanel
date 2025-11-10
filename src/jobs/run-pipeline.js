#!/usr/bin/env node
/**
 * @fileoverview Direct pipeline runner without HTTP server
 * @module RunPipeline
 */

import dotenv from 'dotenv';
import slackMemberPipeline from '../models/slack-members.js';
import slackChannelPipeline from '../models/slack-channels.js';
import { devTask, writeTask, uploadTask, cloudTask } from './tasks.js';
import * as akTools from 'ak-tools';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dotenv.config();
dayjs.extend(utc);

const { sLog, timer } = akTools;

const {
	NODE_ENV = "production",
	CONCURRENCY = 1,
	mixpanel_token,
	mixpanel_secret,
	channel_group_key = 'channel_id',
	gcs_project,
	gcs_path
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
 * Get work function based on environment
 * @param {string} envMode - Environment mode override
 * @returns {Function} Work function
 */
function getWorkFunction(envMode) {
	const env = { mixpanel_token, mixpanel_secret, channel_group_key, gcs_project, gcs_path, NODE_ENV: envMode || NODE_ENV };

	const actualEnv = envMode || NODE_ENV;
	switch (actualEnv) {
		case "backfill":
			return (jobs, stats) => cloudTask(jobs, stats, env);
		case "cloud":
			return (jobs, stats) => cloudTask(jobs, stats, env);
		case "test":
			return (jobs, stats) => writeTask(jobs, stats, env);
		case "dev":
			return (jobs, stats) => devTask(jobs, stats, env);
		case "production":
			return (jobs, stats) => uploadTask(jobs, stats, env);
		default:
			return (jobs, stats) => uploadTask(jobs, stats, env);
	}
}

/**
 * Create stats tracking object
 * @returns {Object} Stats object
 */
function createStats() {
	return {
		events: { processed: 0, uploaded: 0 },
		users: { processed: 0, uploaded: 0 },
		groups: { processed: 0, uploaded: 0 },

		reset() {
			this.events = { processed: 0, uploaded: 0 };
			this.users = { processed: 0, uploaded: 0 };
			this.groups = { processed: 0, uploaded: 0 };
		},

		report() {
			return {
				events: { ...this.events },
				users: { ...this.users },
				groups: { ...this.groups }
			};
		}
	};
}

/**
 * Run the complete pipeline (members and channels)
 * @param {Object} options - Pipeline options
 * @param {number} [options.days] - Number of days to process
 * @param {string} [options.start_date] - Start date (YYYY-MM-DD)
 * @param {string} [options.end_date] - End date (YYYY-MM-DD)
 * @param {boolean} [options.backfill] - Run in backfill mode
 * @param {Array<string>} [options.pipelines] - Which pipelines to run (default: ['members', 'channels'])
 * @returns {Promise<Object>} Pipeline results
 */
export async function runPipeline(options = {}) {
	const t = timer('pipeline');
	t.start();

	try {
		const params = parseParameters(options);
		const dateRange = getDateRange(params);
		const stats = createStats();
		const work = getWorkFunction(params.env);
		const pipelines = options.pipelines || ['members', 'channels'];

		console.log(`\n${'='.repeat(80)}`);
		console.log(`PIPELINE START: ${params.env || NODE_ENV}`);
		console.log(`Date Range: ${dateRange.simpleStart} to ${dateRange.simpleEnd} (${dateRange.days} days)`);
		console.log(`Pipelines: ${pipelines.join(', ')}`);
		console.log(`Concurrency: ${CONCURRENCY}`);
		console.log(`${'='.repeat(80)}\n`);

		const pipelineJobs = [];

		// Run members pipeline
		if (pipelines.includes('members')) {
			sLog('PIPELINE: Fetching member data...');
			const { slackMemberEvents, slackMemberProfiles } = await slackMemberPipeline(dateRange.simpleStart, dateRange.simpleEnd);

			const memberJobs = [
				{ stream: slackMemberEvents, type: "event", label: "slack-members" },
				{ stream: slackMemberProfiles, type: "user", label: "slack-member-profiles" }
			];

			pipelineJobs.push(work(memberJobs, stats));
		}

		// Run channels pipeline
		if (pipelines.includes('channels')) {
			sLog('PIPELINE: Fetching channel data...');
			const { slackChannelEvents, slackChannelProfiles } = await slackChannelPipeline(dateRange.simpleStart, dateRange.simpleEnd);

			const channelJobs = [
				{ stream: slackChannelEvents, type: "event", label: "slack-channels" },
				{ stream: slackChannelProfiles, type: "group", groupKey: channel_group_key, label: "slack-channel-profiles" }
			];

			pipelineJobs.push(work(channelJobs, stats));
		}

		// Execute all pipelines
		const results = await Promise.allSettled(pipelineJobs);

		let success = [];
		let failed = [];

		results.forEach((result) => {
			if (result.status === "fulfilled") {
				result.value.forEach(innerResult => {
					if (innerResult.status === "fulfilled") {
						success.push(innerResult.value);
					} else {
						failed.push(innerResult.reason);
					}
				});
			} else {
				failed.push(result.reason);
			}
		});

		const timing = t.end();

		console.log(`\n${'='.repeat(80)}`);
		console.log(`PIPELINE COMPLETE: ${timing}`);
		console.log(`Success: ${success.length} | Failed: ${failed.length}`);
		console.log(`Stats:`, JSON.stringify(stats.report(), null, 2));
		console.log(`${'='.repeat(80)}\n`);

		return {
			status: 'success',
			timing: t.report(false),
			params: {
				start_date: dateRange.start,
				end_date: dateRange.end,
				days: dateRange.days
			},
			results: { success, failed },
			stats: stats.report()
		};

	} catch (error) {
		console.error('PIPELINE ERROR:', error);
		throw error;
	}
}

export default runPipeline;
