/**
 * @fileoverview Slack to Mixpanel analytics pipeline
 * @module SlackMixpanelAnalytics
 */

import { execSync } from "child_process";
import dayjs from "dayjs";
import utc from 'dayjs/plugin/utc.js';
import * as akTools from "ak-tools";
const { NODE_ENV = "unknown" } = process.env;
// Tasks
import { devTask, writeTask, uploadTask } from "./tasks.js";

// Models
import slackMemberPipeline from "../models/slack-members.js";
import slackChannelPipeline from "../models/slack-channels.js";

dayjs.extend(utc);

const { 
	mixpanel_token, 
	mixpanel_secret, 
	channel_group_key = 'channel_id'
} = process.env;

const jobTimer = akTools.timer("SLACK_MIXPANEL_ANALYTICS");
const NOW = dayjs.utc();
const sfdcDateTimeFmt = "YYYY-MM-DDTHH:mm:ss.SSS[Z]";

/**
 * @typedef {Object} JobParams
 * @property {number} [days] - Number of days to process
 * @property {string} [start_date] - Start date in YYYY-MM-DD format
 * @property {string} [end_date] - End date in YYYY-MM-DD format
 */

/**
 * @typedef {Object} JobSummary
 * @property {Object} timing - Job timing information
 * @property {JobParams} params - Job parameters used
 * @property {Object} results - Job results with success/failed arrays
 * @property {Object} stats - Processing statistics
 */

/**
 * @typedef {Object} StreamJob
 * @property {Stream} stream - Highland stream to process
 * @property {('event'|'user'|'group')} type - Type of data
 * @property {string} [groupKey] - Group key for group data
 * @property {string} label - Job label for logging
 */

/**
 * Simple statistics tracking
 */
const stats = {
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

/**
 * Main Slack to Mixpanel analytics pipeline
 * @param {JobParams} [params={}] - Job parameters
 * @returns {Promise<JobSummary>} Job execution summary
 */
async function main(params = {}) {
	jobTimer.start();
	stats.reset();
	
	// Determine date range
	let DAYS;
	if (NODE_ENV === "dev") DAYS = 1;
	if (NODE_ENV === "production") DAYS = 2;
	if (NODE_ENV === "backfill") DAYS = 365;
	if (NODE_ENV === "test") DAYS = 5;
	if (params.days) DAYS = params.days;

	let start = NOW.subtract(DAYS, "d").format(sfdcDateTimeFmt);
	let end = NOW.add(2, "d").format(sfdcDateTimeFmt);
	
	if (params.start_date) start = dayjs.utc(params.start_date).format(sfdcDateTimeFmt);
	if (params.end_date) end = dayjs.utc(params.end_date).format(sfdcDateTimeFmt);
	
	let simpleStart = dayjs.utc(start).format("YYYY-MM-DD");
	let simpleEnd = dayjs.utc(end).format("YYYY-MM-DD");
	
	console.log(`SLACK-MIXPANEL ${NODE_ENV}: ${simpleStart} TO ${simpleEnd} (${akTools.comma(DAYS)} DAYS)`);

	// Clean up temp files in dev mode
	if (NODE_ENV === "dev") {
		try {
			execSync(`npm run prune`);
		} catch (e) {
			// Ignore prune errors
		}
	}

	// Get data streams from pipelines
	const { slackMemberEvents, slackMemberProfiles } = await slackMemberPipeline(simpleStart, simpleEnd);
	const { slackChannelEvents, slackChannelProfiles } = await slackChannelPipeline(simpleStart, simpleEnd);

	// Determine work function based on environment
	let work;
	const env = { mixpanel_token, mixpanel_secret, channel_group_key };
	
	switch (NODE_ENV) {
		case "backfill":
			work = (jobs) => uploadTask(jobs, stats, env);  // Streaming upload for backfill
			// work = (jobs) => writeTask(jobs, stats, env);      // Uncomment to write files instead
			break;
		case "test":
			work = (jobs) => writeTask(jobs, stats, env);
			break;
		case "dev":
			work = (jobs) => devTask(jobs, stats, env);
			break;
		case "production":
			work = (jobs) => uploadTask(jobs, stats, env);  // Streaming upload for production
			break;
		default:
			work = (jobs) => uploadTask(jobs, stats, env);  // Streaming upload by default
	}

	// Define pipeline jobs
	const PIPELINE = [
		work([
			{ stream: slackMemberEvents, type: "event", label: "slack-members" },
			{ stream: slackMemberProfiles, type: "user", label: "slack-member-profiles" }
		]),
		work([
			{ stream: slackChannelEvents, type: "event", label: "slack-channels" },
			{ stream: slackChannelProfiles, type: "group", groupKey: channel_group_key, label: "slack-channel-profiles" }
		])
	];

	// Execute pipeline
	let success = [];
	let failed = [];
	
	try {
		const results = await Promise.allSettled(PIPELINE);
		
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

	} catch (error) {
		console.error(`SLACK-MIXPANEL ${NODE_ENV}: PIPELINE ERROR`, error);
		if (NODE_ENV === "dev") throw error;
	}

	jobTimer.stop();
	const { delta, end: endTime, human, start: startTime } = jobTimer.report(false);
	console.log(`SLACK-MIXPANEL ${NODE_ENV}: COMPLETE ... ${human}`);
	
	const summary = {
		timing: { delta, startTime, endTime, human },
		params: { start_date: start, end_date: end },
		results: { success, failed },
		stats: stats.report()
	};
	
	return summary;
}


// Direct execution capability for debugging
if (import.meta.url === `file://${process.argv[1]}`) {
	console.log(`🔧 Running Slack-Mixpanel Analytics job directly... in ${NODE_ENV} mode`);
	
	const opts = {};
	if (NODE_ENV === "dev") opts.days = 2
	
	try {
		const result = await main(opts);
		
		console.log('✅ Job completed successfully!');
		console.log('📊 Final Stats:', result.stats);
		console.log('⏱️  Timing:', result.timing.human);
		console.log('🎯 Results:', `${result.results.success.length} success, ${result.results.failed.length} failed`);
		
		if (result.results.success.length > 0) {
			console.log('📋 Success Details:', result.results.success.map(s => s.label || 'unknown'));
		}
		
		if (result.results.failed.length > 0) {
			console.log('❌ Failed Details:', result.results.failed);
		}
		
		// Debugger for dev inspection
		if (NODE_ENV === 'dev') debugger;
		
	} catch (error) {
		console.error('❌ Slack-Mixpanel Analytics job test failed:', error);
		if (NODE_ENV === 'dev') debugger;
		process.exit(1);
	}
}

export default main;