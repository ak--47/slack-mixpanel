/**
 * @fileoverview Transform functions for channel data
 * @module ChannelTransforms
 */

import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dayjs.extend(utc);

/**
 * Transform channel analytics record to Mixpanel event
 * @param {Object} record - Raw Slack analytics record
 * @param {Object} context - Heavy objects (slackChannels, etc.)
 * @returns {Object|null} Mixpanel event or null to skip
 */
export function transformChannelEvent(record, context) {
	const { slackChannels, slack_prefix } = context;

	const channelDetails = slackChannels.find((c) => c.id === record.channel_id);

	const event = {
		event: 'daily channel activity',
		properties: {
			...record,
			distinct_id: record.channel_id,
			time: dayjs.utc(record.date).add(4, 'h').add(20, 'm').unix(),
			channel_id: record.channel_id,
			date: record.date
		}
	};

	// Add channel details if available
	if (channelDetails) {
		event.properties.name = `#${channelDetails.name}`;
		event.properties['#  → SLACK'] = `${slack_prefix}/${record.channel_id}`;

		if (channelDetails.purpose?.value) {
			event.properties.purpose = channelDetails.purpose.value;
		}
		if (channelDetails.topic?.value) {
			event.properties.topic = channelDetails.topic.value;
		}
		if (channelDetails.is_ext_shared) {
			event.properties.external = true;
		}
		if (channelDetails.is_shared) {
			event.properties.external = true;
		}
		if (channelDetails.is_private) {
			event.properties.private = true;
		}
		if (channelDetails.num_members) {
			event.properties.members = channelDetails.num_members;
		}
	}

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

	const profile = {
		$group_key: channel_group_key,
		$group_id: record.channel_id,
		$set: {
			...record,
			distinct_id: record.channel_id,
			date: dayjs.utc(record.date).add(4, 'h').add(20, 'm').unix()
		}
	};

	// Add channel details if available
	if (channelDetails) {
		profile.$set.$name = `#${channelDetails.name}`;
		profile.$set['#  → SLACK'] = `${slack_prefix}/${record.channel_id}`;
		profile.$set.$email = `${slack_prefix}/${record.channel_id}`;
		profile.$set.created = dayjs.unix(channelDetails.created).format('YYYY-MM-DD');

		if (channelDetails.purpose?.value) {
			profile.$set.purpose = channelDetails.purpose.value;
		}
		if (channelDetails.topic?.value) {
			profile.$set.topic = channelDetails.topic.value;
		}
		if (channelDetails.is_ext_shared) {
			profile.$set.external = true;
		}
		if (channelDetails.is_shared) {
			profile.$set.external = true;
		}
		if (channelDetails.is_private) {
			profile.$set.private = true;
		}
		if (channelDetails.num_members) {
			profile.$set.members = channelDetails.num_members;
		}
	}

	return profile;
}

export default {
	transformChannelEvent,
	transformChannelProfile
};
