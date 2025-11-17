/**
 * @fileoverview Transform functions for channel data
 * @module ChannelTransforms
 *
 * NOTE: Analytics records now include an ENRICHED key with detailed channel information:
 * record.ENRICHED = {
 *   channel: {
 *     id, name, topic, purpose, num_members,
 *     is_private, is_archived, is_ext_shared,
 *     creator, created, ...
 *   },
 *   ok: true
 * }
 *
 * Enrichment is ONLY used for profiles. Events use basic analytics + lookup data.
 */

import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dayjs.extend(utc);

/**
 * Transform channel analytics record to Mixpanel event
 * @param {Object} record - Raw Slack analytics record (includes ENRICHED key)
 * @param {Object} context - Heavy objects (slackChannels, etc.)
 * @returns {Object|null} Mixpanel event or null to skip
 */
export function transformChannelEvent(record, context) {
	const { slackChannels, slack_prefix } = context;

	const channelDetails = slackChannels.find((c) => c.id === record.channel_id);

	const { ENRICHED = {}, ...recordWithoutEnriched } = record;
	// EVENTS DON'T GET ENRICHED, SO WE IGNORE the ENRICHED key here

	// Add basic channel details from lookup
	if (channelDetails) {
		recordWithoutEnriched.name = `#${channelDetails.name}`;
		recordWithoutEnriched['#  → SLACK'] = `${slack_prefix}/${record.channel_id}`;

		if (channelDetails.purpose?.value) {
			recordWithoutEnriched.purpose = channelDetails.purpose.value;
		}
		if (channelDetails.topic?.value) {
			recordWithoutEnriched.topic = channelDetails.topic.value;
		}
		if (channelDetails.is_ext_shared) {
			recordWithoutEnriched.external = true;
		}
		if (channelDetails.is_shared) {
			recordWithoutEnriched.external = true;
		}
		if (channelDetails.is_private) {
			recordWithoutEnriched.private = true;
		}
		if (channelDetails.num_members) {
			recordWithoutEnriched.members = channelDetails.num_members;
		}
	}

	const event = {
		event: 'daily channel activity',
		properties: {
			...recordWithoutEnriched,
			distinct_id: "",  // DO NOT ASSOCIATE WITH A USER!
			time: dayjs.utc(record.date).add(4, 'h').add(20, 'm').unix(),
			channel_id: record.channel_id,
			date: record.date
		}
	};

	return event;
}

/**
 * Transform channel analytics record to Mixpanel group profile
 * @param {Object} record - Raw Slack analytics record
 * @param {Object} context - Heavy objects (slackChannels, etc.)
 * @returns {Object|null} Mixpanel group profile or null to skip
 */
export function transformChannelProfile(record, context) {
	const { slackChannels, slack_prefix, channel_group_key } = context;

	const channelDetails = slackChannels.find((c) => c.id === record.channel_id);

	// ===== ENRICHED DATA HOOK =====
	// Extract ENRICHED data and merge all channel fields
	const ENRICHED = record.ENRICHED || {};
	const { channel: enrichedChannel = {} } = ENRICHED;
	const enrichedFields = { ...enrichedChannel };

	if (Object.keys(enrichedFields).length > 1) debugger;
	// Add channel details if available from the lookup
	if (channelDetails) {
		// Ensure channel name has # prefix (but don't double-prefix if it already has one)
		const channelName = channelDetails.name || '';
		enrichedFields.$name = channelName.startsWith('#') ? channelName : `#${channelName}`;
		enrichedFields.channel_name = channelName.replace(/^#/, ''); // Always store without # prefix
		enrichedFields['#  → SLACK'] = `${slack_prefix}/${record.channel_id}`;
		enrichedFields.$email = `${slack_prefix}/${record.channel_id}`;
		enrichedFields.created = dayjs.unix(channelDetails.created).format('YYYY-MM-DD');

		if (channelDetails.purpose?.value) {
			enrichedFields.purpose = channelDetails.purpose.value;
		}
		if (channelDetails.topic?.value) {
			enrichedFields.topic = channelDetails.topic.value;
		}
		if (channelDetails.is_ext_shared) {
			enrichedFields.external = true;
		}
		if (channelDetails.is_shared) {
			enrichedFields.external = true;
		}
		if (channelDetails.is_private) {
			enrichedFields.private = true;
		}
		if (channelDetails.num_members) {
			enrichedFields.members = channelDetails.num_members;
		}
	}

	// Make sure fields doesn't have any complex objects
	for (const key in enrichedFields) {
		const value = enrichedFields[key];
		// if (Array.isArray(value)) delete enrichedFields[key];
		// if (typeof value === "object" && value !== null) delete enrichedFields[key];
	}

	const profile = {
		$group_key: channel_group_key,
		$group_id: record.channel_id,
		$set: {
			...enrichedFields, // all the fields!
			// Override with required fields
			date: dayjs.utc(record.date).add(4, 'h').add(20, 'm').unix()
		}
	};

	return profile;
}

export default {
	transformChannelEvent,
	transformChannelProfile
};
