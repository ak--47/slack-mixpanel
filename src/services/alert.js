/**
 * @fileoverview Slack webhook alerts for unrecoverable pipeline failures
 * @module Alert
 */

import logger from '../utils/logger.js';

/**
 * List Mixpanel load stages that still failed after all retries.
 * Stages skipped because an earlier stage failed have no error and are not listed.
 * @param {Object} result - runPipeline result
 * @returns {string[]} One line per failed stage, e.g. "members profiles: bad creds"
 */
export function loadFailures(result) {
	const failures = [];
	for (const [pipeline, load] of Object.entries(result?.load || {})) {
		for (const [stage, info] of Object.entries(load?.results || {})) {
			if (info && info.success === false && info.error) {
				failures.push(`${pipeline} ${stage}: ${info.error}`);
			}
		}
	}
	return failures;
}

/**
 * Post a failure message to the Slack webhook in `slack_failure_webhook`.
 * Never throws: an alert failure is logged and must not hide the original error.
 * @param {string} pipeline - Pipeline name (members, channels, all)
 * @param {string} message - What failed
 * @returns {Promise<boolean>} true if Slack accepted the message
 */
export async function notifyFailure(pipeline, message) {
	const webhook = process.env.slack_failure_webhook;
	if (!webhook) {
		logger.warn('[ALERT] slack_failure_webhook not set; failure alert not sent');
		return false;
	}

	const revision = process.env.K_REVISION || process.env.NODE_ENV || 'unknown';
	const text = `:rotating_light: *slack-mixpanel* pipeline \`${pipeline}\` failed (${revision})\n\`\`\`${message}\`\`\``;

	try {
		const res = await fetch(webhook, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ text })
		});
		if (!res.ok) {
			logger.error(`[ALERT] Slack webhook returned ${res.status}: ${await res.text()}`);
			return false;
		}
		return true;
	} catch (error) {
		logger.error(`[ALERT] Slack webhook request failed: ${error.message}`);
		return false;
	}
}
