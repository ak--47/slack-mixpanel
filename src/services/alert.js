/**
 * @fileoverview Slack webhook alerts for pipeline failures and ERROR logs.
 * Messages use the fleet-standard format in ../utils/alert-format.js.
 *
 * - `notifyFailure`: explicit alerts from the HTTP handlers (500s, Mixpanel
 *   uploads that still fail after retries). Sends whenever
 *   `slack_failure_webhook` is set.
 * - `alertFromLog`: every `logger.error` call routes here. Sends only in
 *   production on Cloud Run, so local dev and tests never post.
 *
 * Both share a dedupe (same title + component: one post per 30 min, repeats
 * counted) and a cap of 20 posts per rolling hour. This module never throws
 * and never imports the logger: a failed post writes one console.warn line, so
 * an alert failure cannot loop back into an alert.
 * @module Alert
 */

import { formatAlert, logsExplorerUrl, etTime } from '../utils/alert-format.js';

export const SERVICE_NAME = 'Slack Mixpanel';
export const GCP_PROJECT = 'mixpanel-gtm-training';
export const DEFAULT_CLOUD_RUN_SERVICE = 'slack-mixpanel-analytics';
export const DEDUPE_WINDOW_MS = 30 * 60_000;
export const CAP_PER_HOUR = 20;
const HOUR_MS = 60 * 60_000;
const POST_TIMEOUT_MS = 5000;

/** @type {Map<string, {lastSentAt: number, suppressed: number}>} */
let keys = new Map();
/** @type {number[]} */
let sentTimes = [];
let capSuppressed = 0;
/** @type {Set<Promise<unknown>>} */
const pending = new Set();
/** Errors whose alert is owned by an explicit notifyFailure call (or that must not alert). */
let handled = new WeakSet();

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
 * Tell the ERROR-log hook to skip this error: the caller alerts on it itself
 * (or it is a 400 that must not alert). Never throws.
 * @param {unknown} err
 */
export function markAlertHandled(err) {
	if (err && typeof err === 'object') handled.add(err);
}

/** @param {unknown} err @param {string} [url] */
function redact(err, url) {
	let msg = err instanceof Error ? err.message : String(err);
	if (url) msg = msg.split(url).join('<webhook>');
	return msg.replace(/https?:\/\/hooks\.slack\.com\/\S*/g, '<webhook>');
}

/**
 * Production + Cloud Run gate for the ERROR-log route.
 * @returns {boolean}
 */
export function logAlertsEnabled() {
	return process.env.NODE_ENV === 'production'
		&& Boolean(process.env.K_SERVICE || process.env.CLOUD_RUN_JOB)
		&& Boolean(process.env.slack_failure_webhook);
}

/**
 * Apply dedupe + cap; return the formatted text, or null when suppressed.
 * @param {Omit<import('../utils/alert-format.js').AlertInput, 'service' | 'atMs'>} input
 * @param {number} now
 * @returns {string | null}
 */
function admit(input, now) {
	const key = `${input.title} | ${input.component || ''}`;
	const state = keys.get(key);
	if (state && now - state.lastSentAt < DEDUPE_WINDOW_MS) {
		state.suppressed += 1;
		return null;
	}
	sentTimes = sentTimes.filter(t => now - t < HOUR_MS);
	if (sentTimes.length >= CAP_PER_HOUR) {
		capSuppressed += 1;
		return null;
	}
	const notes = [];
	if (state && state.suppressed > 0) notes.push(`+${state.suppressed} more since ${etTime(state.lastSentAt, false)}`);
	if (capSuppressed > 0) notes.push(`+${capSuppressed} other alerts held by the rate cap`);
	capSuppressed = 0;
	keys.set(key, { lastSentAt: now, suppressed: 0 });
	sentTimes.push(now);
	for (const [k, s] of keys) if (now - s.lastSentAt >= DEDUPE_WINDOW_MS && s.suppressed === 0) keys.delete(k);

	return formatAlert({
		...input,
		service: SERVICE_NAME,
		atMs: now,
		notes,
		// This service logs plain text to stderr, which Cloud Run stores with no
		// severity, so the standard `severity>=ERROR` clause would match nothing.
		logsUrl: logsExplorerUrl({
			project: GCP_PROJECT,
			service: process.env.K_SERVICE || DEFAULT_CLOUD_RUN_SERVICE,
			atMs: now
		}).replace(encodeURIComponent('\nseverity>=ERROR'), '')
	});
}

/**
 * POST text to the webhook. Never throws.
 * @param {string} webhook
 * @param {string} text
 * @returns {Promise<boolean>}
 */
async function post(webhook, text) {
	try {
		const res = await fetch(webhook, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ text }),
			signal: AbortSignal.timeout(POST_TIMEOUT_MS)
		});
		if (!res.ok) {
			console.warn(`[ALERT] Slack webhook returned ${res.status}`);
			return false;
		}
		return true;
	} catch (error) {
		console.warn(`[ALERT] Slack webhook request failed: ${redact(error, webhook)}`);
		return false;
	}
}

/** @param {Promise<unknown>} p */
function track(p) {
	pending.add(p);
	p.finally(() => pending.delete(p));
	return p;
}

/**
 * Split an error or message into title / detail / stack.
 * @param {unknown} errorOrMessage
 */
function describe(errorOrMessage) {
	if (errorOrMessage instanceof Error) {
		return { title: String(errorOrMessage.message || errorOrMessage.name).split('\n')[0], stack: errorOrMessage.stack };
	}
	const [title, ...rest] = String(errorOrMessage ?? '').split('\n');
	return { title, detail: rest.join('\n') || undefined };
}

/**
 * Post a pipeline failure to the Slack webhook in `slack_failure_webhook`.
 * Never throws: an alert failure is logged and must not hide the original error.
 * @param {string} pipeline - Pipeline name (members, channels, all)
 * @param {Error | string} errorOrMessage - What failed
 * @param {{path?: string, status?: number}} [opts] - Request route and HTTP status
 * @returns {Promise<boolean>} true if Slack accepted the message
 */
export async function notifyFailure(pipeline, errorOrMessage, opts = {}) {
	try {
		const webhook = process.env.slack_failure_webhook;
		if (!webhook) {
			console.warn('[ALERT] slack_failure_webhook not set; failure alert not sent');
			return false;
		}
		markAlertHandled(errorOrMessage);
		const text = admit({
			...describe(errorOrMessage),
			component: pipeline,
			path: opts.path || `POST /mixpanel-${pipeline}`,
			status: opts.status
		}, Date.now());
		if (!text) return false;
		return await track(post(webhook, text));
	} catch (error) {
		console.warn(`[ALERT] notify failed: ${redact(error, process.env.slack_failure_webhook)}`);
		return false;
	}
}

/**
 * Route one `logger.error(...args)` call to Slack. Fire-and-forget; never throws.
 * The first line of the log message is the title; an Error argument supplies
 * the stack. A leading `[TAG]` becomes the component.
 * @param {unknown[]} args
 */
export function alertFromLog(args) {
	try {
		if (!logAlertsEnabled()) return;
		const err = args.find(a => a instanceof Error);
		if (err && handled.has(err)) return;
		const text = args
			.filter(a => !(a instanceof Error))
			.map(describeArg)
			.join(' ')
			.trim();
		const [first = '', ...rest] = text.split('\n');
		const tag = first.match(/^\[([^\]]+)\]/);
		const normalized = normalizeTitle(first);
		const title = [normalized, err?.message?.split('\n')[0]].filter(Boolean).join(' ') || 'error';
		// The un-normalized line (progress counter, date) stays in WHY.
		const detail = [normalized !== first.trim() ? first.trim() : '', ...rest].filter(Boolean).join('\n');
		if (err) markAlertHandled(err);
		const out = admit({
			title,
			component: tag ? tag[1] : undefined,
			detail: detail || undefined,
			stack: err?.stack
		}, Date.now());
		if (out) track(post(/** @type {string} */ (process.env.slack_failure_webhook), out));
	} catch (error) {
		console.warn(`[ALERT] log alert failed: ${redact(error, process.env.slack_failure_webhook)}`);
	}
}

/**
 * Text for a non-Error log argument. Objects are named by constructor only, so
 * a rejection reason or response object cannot put its contents (headers,
 * config, tokens) into Slack. The full value stays in Cloud Logging.
 * @param {unknown} v
 */
function describeArg(v) {
	if (typeof v === 'string') return v;
	if (v === null || typeof v !== 'object') return String(v);
	const name = /** @type {any} */ (v).constructor?.name || 'Object';
	return `[${name}]`;
}

/**
 * Drop per-item noise from a log line so one outage dedupes to one key:
 * a `[n/N]` progress counter and ISO dates (`2026-11-20`, with a trailing ':').
 * @param {string} line
 */
export function normalizeTitle(line) {
	return line
		.replace(/\s*\[\d+\/\d+\]/g, '')
		.replace(/\s*\d{4}-\d{2}-\d{2}:?/g, '')
		.replace(/\s+/g, ' ')
		.trim();
}

/**
 * Production-only crash alerts. Alert only: the process keeps today's
 * behavior. mixpanel-import (loaded by src/jobs/load.js) installs its own
 * handlers: a log-only `unhandledRejection` (the service survives) and an
 * `uncaughtException` that logs and calls process.exit(1) synchronously.
 * - unhandledRejection: log + alert; survives while another listener (mixpanel-import's)
 *   is present, else flushes and exits 1 like Node's default.
 * - uncaughtException: prepended so it runs before mixpanel-import's exit (best effort:
 *   the POST may not land); exits 1 itself only if it is the only listener.
 * @param {{log: (...args: unknown[]) => void, nodeEnv?: string, proc?: NodeJS.EventEmitter, exit?: (code: number) => void}} o
 * @returns {boolean} true if handlers were installed
 */
export function installCrashAlerts({ log, nodeEnv = process.env.NODE_ENV, proc = process, exit = (code) => process.exit(code) }) {
	if (nodeEnv !== 'production') return false;
	// Never change whether the process survives. Today mixpanel-import (<3.5.1)
	// adds its own listeners: a log-only unhandledRejection (survive) and an
	// exiting uncaughtException. If ours is the only listener when the event
	// fires (e.g. after a mixpanel-import upgrade), registering it would have
	// suppressed Node's default crash — so crash ourselves, after the alert.
	const crashIfAlone = (event) => {
		if (proc.listenerCount(event) <= 1) flushAlerts().finally(() => exit(1));
	};
	// prepended so a once() listener elsewhere can't have removed itself before we count
	proc.prependListener('unhandledRejection', (reason) => {
		log('Unhandled Rejection:', reason);
		crashIfAlone('unhandledRejection');
	});
	proc.prependListener('uncaughtException', (error) => {
		log('Uncaught Exception:', error);
		crashIfAlone('uncaughtException');
	});
	return true;
}

/**
 * Wait for in-flight posts (bounded) before the process exits.
 * @param {number} [timeoutMs]
 */
export async function flushAlerts(timeoutMs = 3000) {
	if (pending.size === 0) return;
	/** @type {NodeJS.Timeout | undefined} */
	let timer;
	const timeout = new Promise(resolve => { timer = setTimeout(resolve, timeoutMs); });
	await Promise.race([Promise.allSettled([...pending]), timeout]);
	clearTimeout(timer);
}

/** Reset dedupe/cap state. Tests only. */
export function _resetAlertsForTests() {
	keys = new Map();
	sentTimes = [];
	capSuppressed = 0;
	handled = new WeakSet();
	pending.clear();
}
