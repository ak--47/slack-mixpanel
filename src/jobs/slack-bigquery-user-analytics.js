/**
 * @fileoverview Slack to BigQuery User Analytics Pipeline
 * Fetches user messages and analytics from Slack and stores them in BigQuery
 * @module SlackBigQueryUserAnalytics
 */

import 'dotenv/config';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import slackService from '../services/slack.js';
import bigQueryService from '../services/bigquery.js';

dayjs.extend(utc);

const {
	NODE_ENV = "unknown",
	SMARTERCHILD_USER_ID = "ATXAXLY00",
	BIGQUERY_DATASET = "smarterchild",
	BIGQUERY_TABLE = "user_message_analytics"
} = process.env;

/**
 * @typedef {Object} ProcessedMessage
 * @property {string} message_id - Message identifier (timestamp)
 * @property {string} slack_url - Unique Slack URL (used for deduplication)
 * @property {string} user_id - Slack user ID
 * @property {string} text - Message content
 * @property {string} channel_id - Channel ID where message was posted
 * @property {string} channel_name - Channel name
 * @property {string} timestamp - Message timestamp (ISO string)
 * @property {string} date - Message date (YYYY-MM-DD)
 * @property {number} reaction_count - Total number of reactions
 * @property {string} reactions_json - JSON string of reactions array
 * @property {number} reply_count - Number of replies to the message
 * @property {number} reply_users_count - Number of unique users who replied
 * @property {string} latest_reply - Timestamp of latest reply (if any)
 * @property {boolean} is_starred - Whether message is starred
 * @property {string} permalink - Permanent link to the message (for backward compatibility)
 * @property {number} score - Search relevance score
 * @property {string} created_at - Pipeline processing timestamp
 * @property {string} updated_at - Record last updated timestamp
 */

/**
 * Transform Slack message data into BigQuery-compatible format
 * @param {Object} message - Raw Slack message object
 * @returns {ProcessedMessage} Transformed message for BigQuery
 */
function transformMessageForBigQuery(message) {
	const timestamp = new Date(parseFloat(message.ts) * 1000);

	return {
		message_id: message.ts, // Keep timestamp as message ID
		slack_url: message.permalink || '', // Use permalink as unique deduplication key
		user_id: message.user,
		text: message.text || '',
		channel_id: message.channel?.id || '',
		channel_name: message.channel?.name || '',
		timestamp: timestamp.toISOString(),
		date: timestamp.toISOString().split('T')[0], // YYYY-MM-DD format
		reaction_count: message.reaction_count || 0,
		reactions_json: message.reactions ? JSON.stringify(message.reactions) : null,
		reply_count: message.reply_count || 0,
		reply_users_count: message.reply_users_count || 0,
		latest_reply: message.latest_reply ? new Date(parseFloat(message.latest_reply) * 1000).toISOString() : null,
		is_starred: message.is_starred || false,
		permalink: message.permalink || '', // Keep this for backward compatibility
		score: message.score || 0,
		created_at: new Date().toISOString(),
		updated_at: new Date().toISOString() // Track when this record was last updated
	};
}

/**
 * Main pipeline function to fetch Slack data and insert into BigQuery
 * @param {Object} [options] - Pipeline options
 * @param {string} [options.userId] - Slack user ID to fetch messages for
 * @param {number} [options.days] - Number of days to look back
 * @param {string} [options.dataset] - BigQuery dataset name
 * @param {string} [options.table] - BigQuery table name
 * @returns {Promise<Object>} Pipeline execution result
 */
async function runPipeline(options = {}) {
	const {
		userId = SMARTERCHILD_USER_ID,
		days = 7,
		dataset = BIGQUERY_DATASET,
		table = BIGQUERY_TABLE
	} = options;

	const startTime = Date.now();
	console.log(`🚀 Starting Slack→BigQuery pipeline for user ${userId}`);
	console.log(`📅 Fetching last ${days} days of messages`);
	console.log(`📊 Target: ${bigQueryService.projectId}.${dataset}.${table}`);

	try {
		// Verify services are ready
		if (!slackService.ready) {
			throw new Error('Slack service is not ready');
		}
		if (!bigQueryService.ready) {
			throw new Error('BigQuery service is not ready');
		}

		// Calculate date range
		const endDate = dayjs.utc();
		const startDate = endDate.subtract(days, 'day');

		console.log(`🔍 Fetching messages from ${startDate.format('YYYY-MM-DD')} to ${endDate.format('YYYY-MM-DD')}`);

		// Fetch messages from Slack
		const messages = await slackService.getUserMessages(userId, {
			startDate: startDate.format('YYYY-MM-DD'),
			endDate: endDate.format('YYYY-MM-DD'),
			includeReactions: true,
			includeReplies: true
		});

		console.log(`📝 Retrieved ${messages.length} messages from Slack`);

		if (messages.length === 0) {
			console.log('ℹ️ No messages found for the specified period');
			return {
				success: true,
				messagesProcessed: 0,
				duration: Date.now() - startTime,
				message: 'No messages found for the specified period'
			};
		}

		// Transform messages for BigQuery
		console.log('🔄 Transforming messages for BigQuery...');
		const transformedMessages = messages.map(transformMessageForBigQuery);

		// Insert into BigQuery with upsert by Slack URL (always replace with latest)
		console.log('💾 Inserting data into BigQuery with upsert...');
		const insertResult = await bigQueryService.insertData(dataset, table, transformedMessages, {
			dedupe: true,
			upsert: true, // Enable upsert mode to replace existing records
			idField: 'slack_url', // Use slack_url for deduplication
			lookbackDays: days * 2, // Look back twice as far for deduplication
			createTable: true,
			batchSize: 1000
		});

		const duration = Date.now() - startTime;
		console.log(`✅ Pipeline completed in ${duration}ms`);

		return {
			success: insertResult.success,
			messagesRetrieved: messages.length,
			messagesProcessed: insertResult.insertedRows,
			duplicatesSkipped: insertResult.duplicatesSkipped || 0,
			duration,
			insertResult
		};

	} catch (error) {
		const duration = Date.now() - startTime;
		console.error('❌ Pipeline failed:', error);

		return {
			success: false,
			error: error.message,
			duration
		};
	}
}

/**
 * Get analytics summary for the processed data
 * @param {Object} [options] - Analytics options
 * @param {string} [options.userId] - User ID to analyze
 * @param {number} [options.days] - Number of days to analyze
 * @param {string} [options.dataset] - BigQuery dataset name
 * @param {string} [options.table] - BigQuery table name
 * @returns {Promise<Object>} Analytics summary
 */
async function getAnalyticsSummary(options = {}) {
	const {
		userId = SMARTERCHILD_USER_ID,
		days = 7,
		dataset = BIGQUERY_DATASET,
		table = BIGQUERY_TABLE
	} = options;

	try {
		const lookbackDate = dayjs.utc().subtract(days, 'day').format('YYYY-MM-DD');

		const analyticsQuery = `
			SELECT
				user_id,
				COUNT(*) as total_messages,
				SUM(reaction_count) as total_reactions,
				AVG(reaction_count) as avg_reactions_per_message,
				SUM(reply_count) as total_replies,
				AVG(reply_count) as avg_replies_per_message,
				COUNT(DISTINCT channel_id) as channels_posted_in,
				COUNT(CASE WHEN reaction_count > 0 THEN 1 END) as messages_with_reactions,
				COUNT(CASE WHEN reply_count > 0 THEN 1 END) as messages_with_replies,
				MIN(timestamp) as first_message_time,
				MAX(timestamp) as last_message_time,
				COUNT(CASE WHEN is_starred THEN 1 END) as starred_messages
			FROM \`${bigQueryService.projectId}.${dataset}.${table}\`
			WHERE user_id = '${userId}'
			AND date >= '${lookbackDate}'
			GROUP BY user_id
		`;

		console.log('📊 Fetching analytics summary...');
		const results = await bigQueryService.query(analyticsQuery);

		if (results.length === 0) {
			return {
				user_id: userId,
				total_messages: 0,
				message: 'No data found for the specified period'
			};
		}

		const analytics = results[0];
		console.log('📈 Analytics summary:', {
			total_messages: analytics.total_messages,
			avg_reactions: Number(analytics.avg_reactions_per_message).toFixed(2),
			channels_posted_in: analytics.channels_posted_in
		});

		return analytics;

	} catch (error) {
		console.error('❌ Error fetching analytics summary:', error);
		throw error;
	}
}

export { runPipeline, getAnalyticsSummary };

// Direct execution capability
if (import.meta.url === `file://${process.argv[1]}`) {
	console.log('🔧 Running Slack→BigQuery User Analytics pipeline...');

	try {
		// Run the pipeline
		const result = await runPipeline();

		console.log('\n📊 Pipeline Result:');
		console.log(`✅ Success: ${result.success}`);
		console.log(`📝 Messages Retrieved: ${result.messagesRetrieved || 0}`);
		console.log(`💾 Messages Processed: ${result.messagesProcessed || 0}`);
		console.log(`🔄 Duplicates Skipped: ${result.duplicatesSkipped || 0}`);
		console.log(`⏱️ Duration: ${result.duration}ms`);

		if (result.error) {
			console.log(`❌ Error: ${result.error}`);
		}

		// Get analytics summary if pipeline was successful
		if (result.success && result.messagesProcessed > 0) {
			console.log('\n📈 Getting analytics summary...');
			const analytics = await getAnalyticsSummary();

			console.log('\n📊 User Analytics Summary:');
			console.log(`👤 User ID: ${analytics.user_id}`);
			console.log(`📝 Total Messages: ${analytics.total_messages}`);
			console.log(`👍 Total Reactions: ${analytics.total_reactions}`);
			console.log(`📊 Avg Reactions/Message: ${Number(analytics.avg_reactions_per_message).toFixed(2)}`);
			console.log(`💬 Total Replies: ${analytics.total_replies}`);
			console.log(`📊 Avg Replies/Message: ${Number(analytics.avg_replies_per_message).toFixed(2)}`);
			console.log(`📺 Channels Posted In: ${analytics.channels_posted_in}`);
			console.log(`⭐ Starred Messages: ${analytics.starred_messages}`);
		}

		// Debugger for dev inspection
		if (NODE_ENV === 'dev') debugger;

	} catch (error) {
		console.error('❌ Pipeline execution failed:', error);
		if (NODE_ENV === 'dev') debugger;
		process.exit(1);
	}
}