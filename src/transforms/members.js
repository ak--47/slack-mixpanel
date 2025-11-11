/**
 * @fileoverview Transform functions for member data
 * @module MemberTransforms
 */

import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dayjs.extend(utc);

/**
 * Transform member analytics record to Mixpanel event
 * @param {Object} record - Raw Slack analytics record
 * @param {Object} context - Heavy objects (slackMembers, etc.)
 * @returns {Object|null} Mixpanel event or null to skip
 */
export function transformMemberEvent(record, context) {
	const { slackMembers, slack_prefix } = context;

	// Skip if no email or not company domain
	if (!record.email_address) return null;

	const memberDetails = slackMembers.find((m) => m.id === record.user_id);

	const event = {
		event: 'daily user activity',
		distinct_id: record.email_address.toLowerCase(),
		time: dayjs.utc(record.date).add(4, 'h').add(20, 'm').unix(),
		properties: {
			...record,
			user_id: record.user_id,
			email: record.email_address,
			team_id: record.team_id,
			'#  → SLACK': `${slack_prefix}/${record.user_id}`,
			date: record.date
		}
	};

	// Add member details if available
	if (memberDetails) {
		event.properties.name = memberDetails.real_name;
		event.properties.display_name = memberDetails.profile?.display_name;
		event.properties.timezone = memberDetails.tz;
	}

	return event;
}

/**
 * Transform member analytics record to Mixpanel user profile
 * @param {Object} record - Raw Slack analytics record
 * @param {Object} context - Heavy objects (slackMembers, etc.)
 * @returns {Object|null} Mixpanel user profile or null to skip
 */
export function transformMemberProfile(record, context) {
	const { slackMembers, slack_prefix } = context;

	// Skip if no email
	if (!record.email_address) return null;

	const memberDetails = slackMembers.find((m) => m.id === record.user_id);

	const profile = {
		$distinct_id: record.email_address.toLowerCase(),
		$email: record.email_address,
		$set: {
			slack_id: record.user_id,
			slack_team_id: record.team_id,
			'#  → SLACK': `${slack_prefix}/${record.user_id}`,
			is_active: record.is_active
		}
	};

	// Add member details if available
	if (memberDetails && memberDetails.profile) {
		profile.$set.avatar = memberDetails.profile.image_512;
		profile.$set.title = memberDetails.profile.title;
		profile.$set.name = memberDetails.real_name;
		profile.$set.display_name = memberDetails.profile.display_name;
		profile.$set.timezone = memberDetails.tz;
	}

	return profile;
}

export default {
	transformMemberEvent,
	transformMemberProfile
};
