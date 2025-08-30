/**
 * @fileoverview Slack API service for analytics data extraction and workspace management
 * @module SlackService
 */

import { WebClient } from '@slack/web-api';
import 'dotenv/config';
import _ from 'highland';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import * as akTools from 'ak-tools';
import pLimit from 'p-limit';

dayjs.extend(utc);
const { progress, sleep } = akTools;

/**
 * @typedef {Object} SlackAuthResponse
 * @property {boolean} ok - Whether the auth test was successful
 * @property {string} user - User ID
 * @property {string} team - Team ID
 * @property {string} url - Team URL
 */

/**
 * @typedef {Object} SlackAnalyticsRecord
 * @property {string} date - Date in YYYY-MM-DD format
 * @property {string} user_id - Slack user ID
 * @property {string} email_address - User email address
 * @property {string} team_id - Slack team ID
 * @property {number} messages_posted - Number of messages posted
 * @property {number} files_uploaded - Number of files uploaded
 */

/**
 * @typedef {Object} SlackChannel
 * @property {string} id - Channel ID
 * @property {string} name - Channel name
 * @property {boolean} is_private - Whether channel is private
 * @property {boolean} is_ext_shared - Whether channel is externally shared
 * @property {boolean} is_shared - Whether channel is shared
 * @property {number} created - Channel creation timestamp
 * @property {number} num_members - Number of channel members
 * @property {Object} purpose - Channel purpose object
 * @property {string} purpose.value - Channel purpose text
 * @property {Object} topic - Channel topic object
 * @property {string} topic.value - Channel topic text
 */

/**
 * @typedef {Object} SlackUser
 * @property {string} id - User ID
 * @property {string} real_name - User's real name
 * @property {boolean} deleted - Whether user is deleted
 * @property {Object} profile - User profile object
 * @property {string} profile.image_512 - Profile image URL
 * @property {string} profile.title - User title
 * @property {string} profile.display_name - Display name
 */

const { slack_bot_token, slack_user_token, NODE_ENV = "unknown", CONCURRENCY = 2 } = process.env;

if (!slack_bot_token) throw new Error('No slack_bot_token in environment variables');
if (!slack_user_token) throw new Error('No slack_user_token in environment variables');

const limit = pLimit(parseInt(CONCURRENCY));
const initStartTime = Date.now();

/** @type {WebClient} */
const slackBotClient = new WebClient(slack_bot_token);
/** @type {WebClient} */
const slackUserClient = new WebClient(slack_user_token);

/** @type {Object.<string, any>} */
const cache = {};

/**
 * Initialize Slack service and test authentication for both bot and user tokens
 * @returns {Promise<{ready: boolean, userAuth: SlackAuthResponse, botAuth: SlackAuthResponse}>} 
 * @throws {Error} When authentication fails for either token
 */
async function initializeSlack() {
	const userAuth = await testAuth('user');
	const botAuth = await testAuth('bot');
	
	if (userAuth.ok && botAuth.ok) {
		const initTime = Date.now() - initStartTime;
		console.log(`SLACK: service initialized in ${initTime}ms`);
		return { ready: true, userAuth, botAuth };
	}
	
	throw new Error('Slack authentication failed');
}

/**
 * Test Slack authentication for a specific client type
 * @param {('bot'|'user')} [clientType='bot'] - The type of client to test
 * @returns {Promise<SlackAuthResponse>} Authentication response from Slack
 * @throws {Error} When authentication fails
 */
async function testAuth(clientType = 'bot') {
	try {
		const client = clientType === 'user' ? slackUserClient : slackBotClient;
		const response = await client.auth.test();
		console.log(`SLACK: ${clientType} token validated`);
		return response;
	} catch (error) {
		console.error(`SLACK: Error testing ${clientType} authentication:`, error);
		throw error;
	}
}

/**
 * Fetch Slack analytics data for a date range with support for streaming or batch results
 * @param {string} [startDate] - Start date in YYYY-MM-DD format (defaults to 3 days ago)
 * @param {string} [endDate] - End date in YYYY-MM-DD format (defaults to 2 days ago)
 * @param {('member'|'public_channel'|'private_channel')} [type='member'] - Analytics type to fetch
 * @param {boolean} [streamResult=true] - Whether to return Highland stream (true) or consolidated array (false)
 * @returns {Promise<Stream|SlackAnalyticsRecord[]>} Highland stream of analytics records or array of all records
 * @throws {Error} When API calls fail (unless known errors like data_not_available)
 * @example
 * // Stream analytics data
 * const stream = await analytics('2024-01-01', '2024-01-07', 'member', true);
 * stream.each(record => console.log(record));
 * 
 * // Get all analytics as array
 * const data = await analytics('2024-01-01', '2024-01-07', 'member', false);
 */
async function analytics(startDate, endDate, type = 'member', streamResult = true) {
	if (!startDate) startDate = dayjs.utc().subtract(3, 'd').format('YYYY-MM-DD');
	if (!endDate) endDate = dayjs.utc().subtract(2, 'd').format('YYYY-MM-DD');

	let start = dayjs.utc(startDate);
	let end = dayjs.utc(endDate);
	let delta = end.diff(start, 'd');
	let daysToFetch = Array.from({ length: delta + 1 }, (_, i) => start.add(i, 'd').format('YYYY-MM-DD'));
	
	console.log(`📊 SLACK ANALYTICS: Fetching ${daysToFetch.length} days (${startDate} to ${endDate}) with ${CONCURRENCY} concurrent requests`);
	
	const internalStream = streamResult ? _() : null;
	const results = [];
	let completed = 0;

	/**
	 * Fetch analytics data for a specific date
	 * @param {string} date - Date in YYYY-MM-DD format
	 * @returns {Promise<void>}
	 */
	const fetchData = async (date) => {
		try {
			/** @type {import('@slack/web-api').AdminAnalyticsGetFileArguments} */
			const options = { date, type };
			const response = await slackUserClient.admin.analytics.getFile(options);
			
			if (!response || !response.file_data) {
				throw new Error('Failed to get file data from Slack API');
			}
			
			const fileData = response.file_data;
			
			if (internalStream) {
				fileData.forEach(record => internalStream.write(record));
			} else {
				results.push(...fileData);
			}
			
			// Progress tracking
			completed++;
			if (NODE_ENV !== "production" && completed % 10 === 0) {
				console.log(`📊 SLACK PROGRESS: ${completed}/${daysToFetch.length} days completed (${Math.round(completed/daysToFetch.length*100)}%)`);
			}
			
			// Rate limiting delay - respect Slack API limits
			await sleep(2000); // Wait 2 seconds between requests to be safe
			return Promise.resolve();
			
		} catch (error) {
			const knownErrors = ['data_not_available', 'file_not_yet_available', 'file_not_found'];
			const isRateLimit = error?.data?.error === 'ratelimited';
			
			if (isRateLimit) {
				console.log(`⏱️  Rate limited on ${date}, waiting 60 seconds before continuing...`);
				await sleep(60000); // Wait 60 seconds for rate limit
				// Don't increment completed counter for rate limited requests
				return Promise.resolve();
			}
			
			if (!knownErrors.includes(error?.data?.error)) {
				console.error(`Error fetching analytics data for date ${date}:`, error);
				if (internalStream) {
					internalStream.emit('error', error);
				} else {
					throw error;
				}
			}
			return Promise.resolve();
		}
	};

	const fetchPromises = daysToFetch.map(date => limit(() => fetchData(date)));

	if (streamResult) {
		Promise.all(fetchPromises)
			.then(() => internalStream.end())
			.catch(error => internalStream.emit('error', error));
		return internalStream;
	} else {
		await Promise.all(fetchPromises);
		return results;
	}
}

/**
 * Fetch all Slack channels with caching and pagination support
 * @returns {Promise<SlackChannel[]>} Array of channel objects (excludes archived channels)
 * @throws {Error} When API calls fail
 * @example
 * const channels = await getChannels();
 * console.log(`Found ${channels.length} active channels`);
 */
async function getChannels() {
	if (cache.channels) return cache.channels;
	
	const channels = [];
	const options = { exclude_archived: true, limit: 1000 };
	const firstResponse = await slackUserClient.conversations.list(options);
	channels.push(...firstResponse.channels);
	
	let { next_cursor = "" } = firstResponse.response_metadata;
	while (next_cursor) {
		const response = await slackUserClient.conversations.list({ ...options, cursor: next_cursor });
		channels.push(...response.channels);
		next_cursor = response.response_metadata.next_cursor;
	}
	
	cache.channels = channels;
	return channels;
}

/**
 * Fetch all Slack users with caching and pagination support
 * @returns {Promise<SlackUser[]>} Array of user objects including locale information
 * @throws {Error} When API calls fail
 * @example
 * const users = await getUsers();
 * const activeUsers = users.filter(user => !user.deleted);
 * console.log(`Found ${activeUsers.length} active users`);
 */
async function getUsers() {
	if (cache.users) return cache.users;
	
	const users = [];
	const options = { limit: 1000, include_locale: true };
	const firstResponse = await slackUserClient.users.list(options);
	users.push(...firstResponse.members);
	
	let { next_cursor = "" } = firstResponse.response_metadata;
	while (next_cursor) {
		const response = await slackUserClient.users.list({ ...options, cursor: next_cursor });
		users.push(...response.members);
		next_cursor = response.response_metadata.next_cursor;
	}
	
	cache.users = users;
	return users;
}

// Initialize on module load
const { ready, userAuth, botAuth } = await initializeSlack();

/**
 * @typedef {Object} SlackService
 * @property {string} name - Service name
 * @property {string} description - Service description
 * @property {WebClient} slackBotClient - Bot client instance
 * @property {WebClient} slackUserClient - User client instance
 * @property {SlackAuthResponse} userAuth - User authentication response
 * @property {SlackAuthResponse} botAuth - Bot authentication response
 * @property {boolean} ready - Whether service is ready
 * @property {Function} analytics - Analytics data fetcher
 * @property {Function} getChannels - Channels fetcher
 * @property {Function} getUsers - Users fetcher
 * @property {Function} testAuth - Auth tester
 */
const slackService = {
	name: 'slack-service',
	description: 'Service for interacting with Slack API',
	slackBotClient,
	slackUserClient,
	userAuth,
	botAuth,
	ready,
	analytics,
	getChannels,
	getUsers,
	testAuth
};

// Direct execution capability for debugging
if (import.meta.url === `file://${process.argv[1]}`) {
	console.log('🔧 Running Slack service directly...');
	
	const { NODE_ENV = "unknown" } = process.env;
	
	try {
		// Test authentication
		console.log('✅ Authentication successful:', { userAuth, botAuth });
		
		// Test channel fetching
		const channels = await getChannels();
		console.log(`📺 Found ${channels.length} channels`);
		console.log('Sample channels:', channels.slice(0, 3).map(c => ({ id: c.id, name: c.name })));
		
		// Test user fetching
		const users = await getUsers();
		console.log(`👥 Found ${users.length} users`);
		console.log('Sample users:', users.slice(0, 3).map(u => ({ id: u.id, name: u.real_name })));
		
		// Test analytics (just 1 day to keep it fast)
		const startDate = dayjs.utc().subtract(1, 'd').format('YYYY-MM-DD');
		const endDate = dayjs.utc().format('YYYY-MM-DD');
		console.log(`📊 Testing analytics for ${startDate} to ${endDate}`);
		
		const analyticsStream = await analytics(startDate, endDate, 'member', true);
		
		// Convert analytics stream to array for inspection
		const analyticsData = await new Promise((resolve, _reject) => {
			analyticsStream
				.take(10) // Take first 10 records for testing
				.toArray((results) => {
					resolve(results);
				});
		});
		
		console.log(`📈 Found ${analyticsData.length} analytics records`);
		
		if (analyticsData.length > 0) {
			console.log('Sample analytics record:', {
				date: analyticsData[0].date,
				user_id: analyticsData[0].user_id,
				email: analyticsData[0].email_address?.substring(0, 10) + '...' // Truncate for privacy
			});
		}
		
		console.log('✅ Slack service test completed successfully!');
		
		// Debugger for dev inspection
		if (NODE_ENV === 'dev') debugger;
			
	} catch (error) {
		console.error('❌ Slack service test failed:', error);
		if (NODE_ENV === 'dev') debugger;
		process.exit(1);
	}
}

export default slackService;