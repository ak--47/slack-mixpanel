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
const { NODE_ENV = "unknown" } = process.env;

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


	const { ENRICHED = {}, ...recordWithoutEnriched } = record;
	// EVENTS DON'T GET ENRICHED, SO WE IGNORE the ENRICHED key here

	// but we still add some details
	if (memberDetails) {
		recordWithoutEnriched.name = memberDetails.real_name;
		recordWithoutEnriched.display_name = memberDetails.profile?.display_name;
		recordWithoutEnriched.timezone = memberDetails.tz;
	}

	const event = {
		event: 'daily user activity',
		properties: {
			...recordWithoutEnriched,
			$user_id: record.user_id, // this is the primary UUID
			time: dayjs.utc(record.date).add(4, 'h').add(20, 'm').unix(),
			email: record.email_address,
			team_id: record.team_id,
			'#  → SLACK': `${slack_prefix}/${record.user_id}`,
			date: record.date
		}
	};



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

	// ===== ENRICHED DATA HOOK =====
	// Extract ENRICHED data (we don't spread record here, so no need for rest)
	const { ENRICHED = {} } = record;
	const { user: enrichedUser = {}, profile: enrichedProfile = {} } = ENRICHED;
	const enrichedFields = { ...enrichedUser, ...enrichedProfile };

	// Add member details if available from the lookup
	if (memberDetails && memberDetails.profile) {
		enrichedFields.avatar = memberDetails.profile.image_512;
		enrichedFields.title = memberDetails.profile.title;
		enrichedFields.name = memberDetails.real_name;
		enrichedFields.display_name = memberDetails.profile.display_name;
		enrichedFields.timezone = memberDetails.tz;
	}


	// make sure fields doesn't have any complex objects
	loopProps: for (const key in enrichedFields) {
		const value = enrichedFields[key];
		if (Array.isArray(value)) delete enrichedFields[key];
		if (typeof value === "object") delete enrichedFields[key];
		if (key.startsWith("image_")) delete enrichedFields[key];
	}

	const profile = {
		$distinct_id: record.user_id, // Use Slack user ID as distinct_id (must match events)
		$set: {
			...enrichedFields, // all the fields!
			slack_id: record.user_id,
			$email: record.email_address,
			slack_team_id: record.team_id,
			'#  → SLACK': `${slack_prefix}/${record.user_id}`,
			is_active: record.is_active
		}
	};

	return profile;
}

export default {
	transformMemberEvent,
	transformMemberProfile
};
