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
 * @property {string} [user] - User ID (optional)
 * @property {string} [team] - Team ID
 * @property {string} [url] - Team URL
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
 * @property {string} [id] - Channel ID
 * @property {string} [name] - Channel name
 * @property {boolean} [is_private] - Whether channel is private
 * @property {boolean} [is_archived] - Whether channel is archived
 * @property {boolean} [is_ext_shared] - Whether channel is externally shared
 * @property {boolean} [is_shared] - Whether channel is shared
 * @property {number} [created] - Channel creation timestamp
 * @property {number} [num_members] - Number of channel members
 * @property {Object} [purpose] - Channel purpose object
 * @property {string} [purpose.value] - Channel purpose text
 * @property {Object} [topic] - Channel topic object
 * @property {string} [topic.value] - Channel topic text
 */

/**
 * @typedef {Object} SlackUser
 * @property {string} [id] - User ID
 * @property {string} [real_name] - User's real name
 * @property {boolean} [deleted] - Whether user is deleted
 * @property {boolean} [is_bot] - Whether user is a bot
 * @property {Object} [profile] - User profile object
 * @property {string} [profile.image_512] - Profile image URL
 * @property {string} [profile.title] - User title
 * @property {string} [profile.display_name] - Display name
 */

const { slack_bot_token, slack_user_token, NODE_ENV = "unknown", CONCURRENCY = "1" } = process.env;

if (!slack_bot_token) throw new Error('No slack_bot_token in environment variables');
if (!slack_user_token) throw new Error('No slack_user_token in environment variables');

const limit = pLimit(parseInt(CONCURRENCY, 10));
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
 * @returns {Promise<any|SlackAnalyticsRecord[]>} Highland stream of analytics records or array of all records
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
	// Ensure Slack is initialized before making API calls
	await ensureSlackInitialized();

	if (!startDate) startDate = dayjs.utc().subtract(3, 'd').format('YYYY-MM-DD');
	if (!endDate) endDate = dayjs.utc().subtract(2, 'd').format('YYYY-MM-DD');

	let start = dayjs.utc(startDate);
	let end = dayjs.utc(endDate);
	let delta = end.diff(start, 'd');
	let daysToFetch = Array.from({ length: delta + 1 }, (_, i) => start.add(i, 'd').format('YYYY-MM-DD'));
	
	// Removed verbose per-call logging - progress is shown in extract stage
	
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

			// Conservative rate limiting with randomized jitter to avoid 429s
			const baseDelay = 1500;
			const jitter = Math.floor(Math.random() * 1500); // 0-1500ms random jitter
			await sleep(baseDelay + jitter); // 1500-3000ms between requests
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
	// Ensure Slack is initialized before making API calls
	await ensureSlackInitialized();

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
	// Ensure Slack is initialized before making API calls
	await ensureSlackInitialized();

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

/**
 * @typedef {Object} UserMessage
 * @property {string} type - Message type
 * @property {string} text - Message text content
 * @property {string} ts - Message timestamp
 * @property {string} user - User ID who sent the message
 * @property {Object} channel - Channel information
 * @property {string} [channel.id] - Channel ID
 * @property {string} [channel.name] - Channel name
 * @property {Array} [reactions] - Array of reactions to the message
 * @property {number} [reply_count] - Number of replies to the message
 * @property {boolean} [is_starred] - Whether message is starred
 * @property {number} [reaction_count] - Total reaction count
 * @property {Object} permalink - Permanent link to the message
 * @property {number} score - Search relevance score
 */

/**
 * Get all messages for a specific user with optional filtering
 * @param {string} userId - The Slack user ID to get messages for
 * @param {Object} [options] - Optional filtering parameters
 * @param {string} [options.startDate] - Start date in YYYY-MM-DD format (defaults to 3 days ago)
 * @param {string} [options.endDate] - End date in YYYY-MM-DD format (defaults to today)
 * @param {string} [options.oldest] - Oldest timestamp to filter from
 * @param {string} [options.latest] - Latest timestamp to filter to
 * @param {number} [options.limit] - Maximum number of messages to return (default: no limit)
 * @param {boolean} [options.includeReactions=true] - Whether to include reaction data
 * @param {boolean} [options.includeReplies=true] - Whether to include reply counts
 * @returns {Promise<UserMessage[]>} Array of all messages from the user
 * @throws {Error} When API calls fail
 * @example
 * // Get all messages from user in last 30 days
 * const messages = await getUserMessages('U1234567890', {
 *   startDate: '2024-01-01',
 *   endDate: '2024-01-31'
 * });
 *
 * // Get recent messages with analytics
 * const recentMessages = await getUserMessages('U1234567890', {
 *   limit: 100,
 *   includeReactions: true,
 *   includeReplies: true
 * });
 */
async function getUserMessages(userId, options = {}) {
	const {
		startDate = dayjs.utc().subtract(3, 'days').format('YYYY-MM-DD'),
		endDate = dayjs.utc().format('YYYY-MM-DD'),
		oldest,
		latest,
		limit: maxMessages,
		includeReactions = true,
		includeReplies = true
	} = options;

	// Build search query for user messages
	let query = `from:<@${userId}>`;

	// Add date filters if provided
	if (startDate) {
		query += ` after:${startDate}`;
	}
	if (endDate) {
		query += ` before:${endDate}`;
	}

	console.log(`🔍 SLACK: Searching messages for user ${userId} with query: ${query}`);

	const allMessages = [];
	let page = 1;
	const searchLimit = 100; // Max per page for search API

	try {
		while (true) {
			const searchOptions = {
				query,
				count: searchLimit,
				page,
				sort: /** @type {'timestamp' | 'score'} */ ('timestamp'),
				sort_dir: /** @type {'desc' | 'asc'} */ ('desc'),
				...(oldest && { oldest }),
				...(latest && { latest })
			};

			const response = await limit(() => slackUserClient.search.messages(searchOptions));

			if (!response.messages || !response.messages.matches) {
				break;
			}

			const messages = response.messages.matches;

			// Process each message to include analytics data
			const processedMessages = await Promise.all(messages.map(async (message) => {
				const processed = {
					type: message.type,
					text: message.text,
					ts: message.ts,
					user: message.user,
					channel: message.channel,
					permalink: message.permalink,
					score: message.score
				};

				// Add reaction and reply data if requested
				if (includeReactions || includeReplies) {
					try {
						// Get detailed message info to include reactions and thread info
						const detailResponse = await slackUserClient.conversations.history({
							channel: message.channel.id,
							latest: message.ts,
							oldest: message.ts,
							inclusive: true,
							limit: 1
						});

						if (detailResponse.messages && detailResponse.messages.length > 0) {
							const detailedMessage = detailResponse.messages[0];

							if (includeReactions) {
								processed.reactions = detailedMessage.reactions || [];
								processed.reaction_count = processed.reactions.reduce((sum, r) => sum + r.count, 0);
							}

							if (includeReplies) {
								processed.reply_count = detailedMessage.reply_count || 0;
								processed.reply_users_count = detailedMessage.reply_users_count || 0;
								processed.latest_reply = detailedMessage.latest_reply;
							}

							// Additional analytics fields
							// @ts-ignore - Properties may not be in type definition
							processed.is_starred = detailedMessage.is_starred || false;
							// @ts-ignore - Properties may not be in type definition
							processed.pinned_to = detailedMessage.pinned_to || [];
							// @ts-ignore - Properties may not be in type definition
							processed.pinned_info = detailedMessage.pinned_info;
						}

					} catch (detailError) {
						console.warn(`Could not get detailed info for message ${message.ts}:`, detailError.message);
					}
				}

				return processed;
			}));

			allMessages.push(...processedMessages);

			// Check if we've hit our limit
			if (maxMessages && allMessages.length >= maxMessages) {
				console.log(`📊 SLACK: Reached limit of ${maxMessages} messages`);
				break;
			}

			// Check if there are more pages
			if (!response.messages.pagination ||
				page >= response.messages.pagination.page_count ||
				messages.length < searchLimit) {
				break;
			}

			page++;

			// Rate limiting - search API has strict limits
			await sleep(1000);
		}

	} catch (error) {
		console.error('Error fetching user messages:', error);
		throw error;
	}

	// Apply limit if specified and sort by timestamp
	const finalMessages = allMessages
		.sort((a, b) => parseFloat(b.ts) - parseFloat(a.ts)) // Sort by timestamp, newest first
		.slice(0, maxMessages || allMessages.length);

	console.log(`📊 SLACK: Found ${finalMessages.length} messages for user ${userId}`);
	return finalMessages;
}

/**
 * Get message analytics summary for a specific user
 * @param {string} userId - The Slack user ID to analyze
 * @param {Object} [options] - Optional filtering parameters (same as getUserMessages)
 * @returns {Promise<Object>} Analytics summary object
 * @example
 * const analytics = await getUserMessageAnalytics('U1234567890', {
 *   startDate: '2024-01-01',
 *   endDate: '2024-01-31'
 * });
 * console.log(analytics.totalMessages, analytics.avgReactions);
 */
async function getUserMessageAnalytics(userId, options = {}) {
	const messages = await getUserMessages(userId, {
		...options,
		includeReactions: true,
		includeReplies: true
	});

	const analytics = {
		totalMessages: messages.length,
		totalReactions: 0,
		totalReplies: 0,
		avgReactions: 0,
		avgReplies: 0,
		mostReactedMessage: null,
		mostRepliedMessage: null,
		channelDistribution: {},
		timeDistribution: {},
		topReactionTypes: {}
	};

	if (messages.length === 0) return analytics;

	// Calculate aggregated metrics
	messages.forEach(message => {
		// Reaction metrics
		// @ts-ignore - Properties may not be in type definition
		const reactionCount = message.reaction_count || 0;
		analytics.totalReactions += reactionCount;

		// @ts-ignore - Properties may not be in type definition
		if (!analytics.mostReactedMessage || reactionCount > (analytics.mostReactedMessage.reaction_count || 0)) {
			analytics.mostReactedMessage = message;
		}

		// Reply metrics
		const replyCount = message.reply_count || 0;
		analytics.totalReplies += replyCount;

		if (!analytics.mostRepliedMessage || replyCount > (analytics.mostRepliedMessage.reply_count || 0)) {
			analytics.mostRepliedMessage = message;
		}

		// Channel distribution
		const channelName = message.channel?.name || 'unknown';
		analytics.channelDistribution[channelName] = (analytics.channelDistribution[channelName] || 0) + 1;

		// Time distribution (by hour of day)
		const hour = new Date(parseFloat(message.ts) * 1000).getHours();
		analytics.timeDistribution[hour] = (analytics.timeDistribution[hour] || 0) + 1;

		// Top reaction types
		if (message.reactions) {
			message.reactions.forEach(reaction => {
				analytics.topReactionTypes[reaction.name] = (analytics.topReactionTypes[reaction.name] || 0) + reaction.count;
			});
		}
	});

	// Calculate averages
	analytics.avgReactions = Number((analytics.totalReactions / messages.length).toFixed(2));
	analytics.avgReplies = Number((analytics.totalReplies / messages.length).toFixed(2));

	return analytics;
}

/**
 * @typedef {Object} ChannelMessage
 * @property {string} type - Message type
 * @property {string} text - Message text content
 * @property {string} ts - Message timestamp
 * @property {string} user - User ID who sent the message
 * @property {string} channel_id - Channel ID
 * @property {string} channel_name - Channel name
 * @property {Array} [reactions] - Array of reactions to the message
 * @property {number} [reply_count] - Number of replies to the message
 * @property {number} [reaction_count] - Total reaction count
 * @property {boolean} [is_starred] - Whether message is starred
 * @property {string} permalink - Permanent link to the message
 */

/**
 * Get all messages for a specific channel with optional filtering
 * @param {string} channelId - The Slack channel ID to get messages for
 * @param {Object} [options] - Optional filtering parameters
 * @param {string} [options.startDate] - Start date in YYYY-MM-DD format (defaults to 3 days ago)
 * @param {string} [options.endDate] - End date in YYYY-MM-DD format (defaults to today)
 * @param {string} [options.oldest] - Oldest timestamp to filter from
 * @param {string} [options.latest] - Latest timestamp to filter to
 * @param {number} [options.limit] - Maximum number of messages to return (default: no limit)
 * @param {boolean} [options.includeReactions=true] - Whether to include reaction data
 * @param {boolean} [options.includeReplies=true] - Whether to include reply counts
 * @returns {Promise<ChannelMessage[]>} Array of all messages from the channel
 * @throws {Error} When API calls fail
 * @example
 * // Get all messages from channel in last 3 days (default)
 * const messages = await getChannelMessages('C1234567890');
 *
 * // Get messages with custom date range
 * const recentMessages = await getChannelMessages('C1234567890', {
 *   startDate: '2024-01-01',
 *   endDate: '2024-01-31',
 *   limit: 100
 * });
 */
async function getChannelMessages(channelId, options = {}) {
	const {
		startDate = dayjs.utc().subtract(3, 'days').format('YYYY-MM-DD'),
		endDate = dayjs.utc().format('YYYY-MM-DD'),
		oldest,
		latest,
		limit: maxMessages,
		includeReactions = true,
		includeReplies = true
	} = options;

	console.log(`🔍 SLACK: Fetching messages for channel ${channelId} from ${startDate} to ${endDate}`);

	const allMessages = [];
	let cursor = null;
	const apiLimit = 200; // Max per page for conversations.history

	try {
		// Convert dates to timestamps if provided
		const oldestTs = oldest || dayjs.utc(startDate).unix();
		const latestTs = latest || dayjs.utc(endDate).add(1, 'day').unix(); // Include full end date

		while (true) {
			const historyOptions = {
				channel: channelId,
				limit: apiLimit,
				oldest: oldestTs.toString(),
				latest: latestTs.toString(),
				inclusive: true,
				...(cursor && { cursor })
			};

			const response = await limit(() => slackUserClient.conversations.history(historyOptions));

			if (!response.messages || response.messages.length === 0) {
				break;
			}

			const messages = response.messages;

			// Process each message to include analytics data
			const processedMessages = await Promise.all(messages.map(async (message) => {
				const processed = {
					type: message.type,
					text: message.text || '',
					ts: message.ts,
					user: message.user,
					channel_id: channelId,
					channel_name: '', // Will be filled if we have channel info cached
					// @ts-ignore - team.domain may not be in type definition
					permalink: `https://${slackUserClient.team?.domain || 'workspace'}.slack.com/archives/${channelId}/p${message.ts.replace('.', '')}`
				};

				// Add reaction and reply data if requested
				if (includeReactions) {
					processed.reactions = message.reactions || [];
					processed.reaction_count = processed.reactions.reduce((sum, r) => sum + r.count, 0);
				}

				if (includeReplies) {
					processed.reply_count = message.reply_count || 0;
					processed.reply_users_count = message.reply_users_count || 0;
					processed.latest_reply = message.latest_reply;
				}

				// Additional analytics fields
				processed.is_starred = message.is_starred || false;
				processed.pinned_to = message.pinned_to || [];
				processed.pinned_info = message.pinned_info;

				return processed;
			}));

			allMessages.push(...processedMessages);

			// Check if we've hit our limit
			if (maxMessages && allMessages.length >= maxMessages) {
				console.log(`📊 SLACK: Reached limit of ${maxMessages} messages`);
				break;
			}

			// Check if there are more pages
			if (!response.has_more || !response.response_metadata?.next_cursor) {
				break;
			}

			cursor = response.response_metadata.next_cursor;

			// Rate limiting between requests
			await sleep(1000);
		}

	} catch (error) {
		console.error('Error fetching channel messages:', error);
		throw error;
	}

	// Apply limit if specified and sort by timestamp
	const finalMessages = allMessages
		.sort((a, b) => parseFloat(b.ts) - parseFloat(a.ts)) // Sort by timestamp, newest first
		.slice(0, maxMessages || allMessages.length);

	console.log(`📊 SLACK: Found ${finalMessages.length} messages for channel ${channelId}`);
	return finalMessages;
}

/**
 * Get detailed information about a specific user
 * @param {string} userId - The Slack user ID
 * @returns {Promise<Object>} User details with both user info and profile
 * @throws {Error} When API calls fail
 * @example
 * const details = await getUserDetails('U1234567890');
 * console.log(details.user.real_name, details.profile.title);
 */
async function getUserDetails(userId) {
	await ensureSlackInitialized();

	try {
		// Fetch both user info and profile in parallel
		const [userInfo, profileInfo] = await Promise.all([
			slackBotClient.users.info({ user: userId }),
			slackBotClient.users.profile.get({ user: userId })
		]);

		return {
			user: userInfo.user,
			profile: profileInfo.profile,
			ok: true
		};
	} catch (error) {
		console.error(`Error fetching details for user ${userId}:`, error.message);
		throw error;
	}
}

/**
 * Get detailed information about a specific channel
 * @param {string} channelId - The Slack channel ID
 * @returns {Promise<Object>} Channel details
 * @throws {Error} When API calls fail
 * @example
 * const details = await getChannelDetails('C1234567890');
 * console.log(details.channel.name, details.channel.topic);
 */
async function getChannelDetails(channelId) {
	await ensureSlackInitialized();

	try {
		const response = await slackUserClient.conversations.info({ channel: channelId });

		return {
			channel: response.channel,
			ok: true
		};
	} catch (error) {
		console.error(`Error fetching details for channel ${channelId}:`, error.message);
		throw error;
	}
}

/**
 * Get message analytics summary for a specific channel
 * @param {string} channelId - The Slack channel ID to analyze
 * @param {Object} [options] - Optional filtering parameters (same as getChannelMessages)
 * @returns {Promise<Object>} Analytics summary object
 * @example
 * const analytics = await getChannelMessageAnalytics('C1234567890', {
 *   startDate: '2024-01-01',
 *   endDate: '2024-01-31'
 * });
 * console.log(analytics.totalMessages, analytics.avgReactions);
 */
async function getChannelMessageAnalytics(channelId, options = {}) {
	const messages = await getChannelMessages(channelId, {
		...options,
		includeReactions: true,
		includeReplies: true
	});

	const analytics = {
		totalMessages: messages.length,
		totalReactions: 0,
		totalReplies: 0,
		avgReactions: 0,
		avgReplies: 0,
		mostReactedMessage: null,
		mostRepliedMessage: null,
		userDistribution: {},
		timeDistribution: {},
		topReactionTypes: {},
		activeUsers: 0
	};

	if (messages.length === 0) return analytics;

	const uniqueUsers = new Set();

	// Calculate aggregated metrics
	messages.forEach(message => {
		// User tracking
		if (message.user) {
			uniqueUsers.add(message.user);
		}

		// Reaction metrics
		// @ts-ignore - Properties may not be in type definition
		const reactionCount = message.reaction_count || 0;
		analytics.totalReactions += reactionCount;

		// @ts-ignore - Properties may not be in type definition
		if (!analytics.mostReactedMessage || reactionCount > (analytics.mostReactedMessage.reaction_count || 0)) {
			analytics.mostReactedMessage = message;
		}

		// Reply metrics
		const replyCount = message.reply_count || 0;
		analytics.totalReplies += replyCount;

		if (!analytics.mostRepliedMessage || replyCount > (analytics.mostRepliedMessage.reply_count || 0)) {
			analytics.mostRepliedMessage = message;
		}

		// User distribution
		const userId = message.user || 'unknown';
		analytics.userDistribution[userId] = (analytics.userDistribution[userId] || 0) + 1;

		// Time distribution (by hour of day)
		const hour = new Date(parseFloat(message.ts) * 1000).getHours();
		analytics.timeDistribution[hour] = (analytics.timeDistribution[hour] || 0) + 1;

		// Top reaction types
		if (message.reactions) {
			message.reactions.forEach(reaction => {
				analytics.topReactionTypes[reaction.name] = (analytics.topReactionTypes[reaction.name] || 0) + reaction.count;
			});
		}
	});

	// Calculate averages and unique counts
	analytics.avgReactions = Number((analytics.totalReactions / messages.length).toFixed(2));
	analytics.avgReplies = Number((analytics.totalReplies / messages.length).toFixed(2));
	analytics.activeUsers = uniqueUsers.size;

	return analytics;
}

// Initialize Slack tokens (non-blocking for faster server startup)
let slackInitialized = false;
let slackInitPromise = null;

// Start initialization in background (don't block module load)
function ensureSlackInitialized() {
	if (!slackInitPromise) {
		slackInitPromise = initializeSlack()
			.then(result => {
				slackInitialized = true;
				return result;
			})
			.catch(err => {
				console.error('SLACK: Initialization failed:', err.message);
				throw err;
			});
	}
	return slackInitPromise;
}

// Start initialization immediately but don't await it (non-blocking)
ensureSlackInitialized();

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
 * @property {Function} getUserDetails - User details fetcher (info + profile)
 * @property {Function} getChannelDetails - Channel details fetcher
 * @property {Function} getUserMessages - User messages fetcher
 * @property {Function} getUserMessageAnalytics - User message analytics calculator
 * @property {Function} getChannelMessages - Channel messages fetcher
 * @property {Function} getChannelMessageAnalytics - Channel message analytics calculator
 * @property {Function} testAuth - Auth tester
 */
const slackService = {
	name: 'slack-service',
	description: 'Service for interacting with Slack API',
	slackBotClient,
	slackUserClient,
	ensureSlackInitialized,
	analytics,
	getChannels,
	getUsers,
	getUserDetails,
	getChannelDetails,
	getUserMessages,
	getUserMessageAnalytics,
	getChannelMessages,
	getChannelMessageAnalytics,
	testAuth
};

// Direct execution capability for debugging
if (import.meta.url === `file://${process.argv[1]}`) {
	console.log('🔧 Running Slack service directly...');
	
	const { NODE_ENV = "unknown" } = process.env;
	
	try {
		// Ensure Slack is initialized
		await ensureSlackInitialized();
		console.log('✅ Slack initialized successfully');

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
		
		// Test user message functionality (commented out by default to avoid noise)
		// Uncomment and replace USER_ID to test the new message functionality
		/*
		const TEST_USER_ID = 'U1234567890'; // Replace with actual user ID
		console.log(`🔍 Testing user message functionality for ${TEST_USER_ID}`);

		const userMessages = await getUserMessages(TEST_USER_ID, {
			limit: 5,
			includeReactions: true,
			includeReplies: true
		});

		console.log(`📝 Found ${userMessages.length} recent messages`);

		if (userMessages.length > 0) {
			const analytics = await getUserMessageAnalytics(TEST_USER_ID, { limit: 10 });
			console.log('📊 User message analytics:', {
				totalMessages: analytics.totalMessages,
				avgReactions: analytics.avgReactions,
				avgReplies: analytics.avgReplies
			});
		}
		*/

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