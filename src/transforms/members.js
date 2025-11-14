/**
 * @fileoverview Transform functions for member data
 * @module MemberTransforms
 *
 * NOTE: Analytics records now include an ENRICHED key with detailed user information:
 * record.ENRICHED = {
 *   user: { id, real_name, tz, is_admin, ... },
 *   profile: { email, phone, title, status_text, fields, ... },
 *   ok: true
 * }
 *
 * You can selectively pull fields from ENRICHED to include in events/profiles.
 * Example: record.ENRICHED?.profile?.title or record.ENRICHED?.user?.tz
 */

import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dayjs.extend(utc);

/**
 * Transform member analytics record to Mixpanel event
 * @param {Object} record - Raw Slack analytics record (includes ENRICHED key)
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
		properties: {
			...record,
			$user_id: record.user_id, // this is the primary UUID
			time: dayjs.utc(record.date).add(4, 'h').add(20, 'm').unix(),
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
		$distinct_id: record.user_id, // Use Slack user ID as distinct_id (must match events)
		$set: {
			slack_id: record.user_id,
			$email: record.email_address,
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
