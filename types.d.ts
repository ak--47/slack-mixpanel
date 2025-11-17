/**
 * @fileoverview Centralized TypeScript type definitions for slack-mixpanel pipeline
 *
 * This file contains all type definitions used across the codebase.
 * Import these types in JavaScript files using JSDoc:
 * @example
 * // @ts-check
 * /** @typedef {import('./types.d.ts').SlackChannel} SlackChannel *\/
 */

import { WebClient } from '@slack/web-api';

// ============================================================================
// SLACK API TYPES
// ============================================================================

/**
 * Response from Slack auth.test API
 */
export interface SlackAuthResponse {
	/** Whether the auth test was successful */
	ok: boolean;
	/** User ID (optional) */
	user?: string;
	/** Team ID */
	team?: string;
	/** Team URL */
	url?: string;
}

/**
 * Record from Slack Analytics API
 */
export interface SlackAnalyticsRecord {
	/** Date in YYYY-MM-DD format */
	date: string;
	/** Slack user ID */
	user_id: string;
	/** User email address */
	email_address: string;
	/** Slack team ID */
	team_id: string;
	/** Number of messages posted */
	messages_posted: number;
	/** Number of files uploaded */
	files_uploaded: number;
}

/**
 * Slack channel object from conversations.list or conversations.info
 */
export interface SlackChannel {
	/** Channel ID */
	id?: string;
	/** Channel name */
	name?: string;
	/** Whether channel is private */
	is_private?: boolean;
	/** Whether channel is archived */
	is_archived?: boolean;
	/** Whether channel is externally shared */
	is_ext_shared?: boolean;
	/** Whether channel is shared */
	is_shared?: boolean;
	/** Channel creation timestamp */
	created?: number;
	/** Number of channel members */
	num_members?: number;
	/** Channel purpose object */
	purpose?: {
		/** Channel purpose text */
		value?: string;
	};
	/** Channel topic object */
	topic?: {
		/** Channel topic text */
		value?: string;
	};
}

/**
 * Slack user object from users.list or users.info
 */
export interface SlackUser {
	/** User ID */
	id?: string;
	/** User's real name */
	real_name?: string;
	/** Whether user is deleted */
	deleted?: boolean;
	/** Whether user is a bot */
	is_bot?: boolean;
	/** User profile object */
	profile?: {
		/** Profile image URL */
		image_512?: string;
		/** User title */
		title?: string;
		/** Display name */
		display_name?: string;
	};
}

/**
 * User message from search.messages API
 */
export interface UserMessage {
	/** Message type */
	type: string;
	/** Message text content */
	text: string;
	/** Message timestamp */
	ts: string;
	/** User ID who sent the message */
	user: string;
	/** Channel information */
	channel: {
		/** Channel ID */
		id?: string;
		/** Channel name */
		name?: string;
	};
	/** Array of reactions to the message */
	reactions?: any[];
	/** Number of replies to the message */
	reply_count?: number;
	/** Whether message is starred */
	is_starred?: boolean;
	/** Total reaction count */
	reaction_count?: number;
	/** Permanent link to the message */
	permalink: any;
	/** Search relevance score */
	score: number;
}

/**
 * Channel message from conversations.history API
 */
export interface ChannelMessage {
	/** Message type */
	type: string;
	/** Message text content */
	text: string;
	/** Message timestamp */
	ts: string;
	/** User ID who sent the message */
	user: string;
	/** Channel ID */
	channel_id: string;
	/** Channel name */
	channel_name: string;
	/** Array of reactions to the message */
	reactions?: any[];
	/** Number of replies to the message */
	reply_count?: number;
	/** Total reaction count */
	reaction_count?: number;
	/** Whether message is starred */
	is_starred?: boolean;
	/** Permanent link to the message */
	permalink: string;
}

/**
 * Slack service interface
 */
export interface SlackService {
	/** Service name */
	name: string;
	/** Service description */
	description: string;
	/** Bot client instance */
	slackBotClient: WebClient;
	/** User client instance */
	slackUserClient: WebClient;
	/** Ensure Slack is initialized */
	ensureSlackInitialized: () => Promise<void>;
	/** Analytics data fetcher */
	analytics: (
		startDate?: string,
		endDate?: string,
		type?: 'member' | 'public_channel' | 'private_channel',
		streamResult?: boolean
	) => Promise<any | SlackAnalyticsRecord[]>;
	/** Channels fetcher */
	getChannels: () => Promise<SlackChannel[]>;
	/** Users fetcher */
	getUsers: () => Promise<SlackUser[]>;
	/** User details fetcher (info + profile) */
	getUserDetails: (userId: string) => Promise<any>;
	/** Channel details fetcher */
	getChannelDetails: (channelId: string) => Promise<any>;
	/** User messages fetcher */
	getUserMessages: (userId: string, options?: any) => Promise<UserMessage[]>;
	/** User message analytics calculator */
	getUserMessageAnalytics: (userId: string, options?: any) => Promise<any>;
	/** Channel messages fetcher */
	getChannelMessages: (channelId: string, options?: any) => Promise<ChannelMessage[]>;
	/** Channel message analytics calculator */
	getChannelMessageAnalytics: (channelId: string, options?: any) => Promise<any>;
	/** Auth tester */
	testAuth: (clientType?: 'bot' | 'user') => Promise<SlackAuthResponse>;
}

// ============================================================================
// MIXPANEL TYPES
// ============================================================================

/**
 * Mixpanel event record
 */
export interface MixpanelEvent {
	/** Event name */
	event: string;
	/** Properties object */
	properties: {
		/** Event timestamp (Unix) */
		time: number;
		/** Distinct ID (user identifier) */
		distinct_id: string;
		/** Additional event properties */
		[key: string]: any;
	};
}

/**
 * Mixpanel user profile record
 */
export interface MixpanelUserProfile {
	/** Distinct ID (user identifier) */
	$distinct_id: string;
	/** User email */
	$email?: string;
	/** Properties to set */
	$set: {
		[key: string]: any;
	};
}

/**
 * Mixpanel group profile record
 */
export interface MixpanelGroupProfile {
	/** Group key (e.g., 'channel_id') */
	$group_key: string;
	/** Group ID (e.g., channel ID) */
	$group_id: string;
	/** Properties to set */
	$set: {
		[key: string]: any;
	};
}

// ============================================================================
// PIPELINE TYPES
// ============================================================================

/**
 * Options for extract jobs
 */
export interface ExtractOptions {
	/** Start date in YYYY-MM-DD format */
	startDate: string;
	/** End date in YYYY-MM-DD format */
	endDate: string;
}

/**
 * Options for load jobs
 */
export interface LoadOptions {
	/** Record type to upload */
	type: 'event' | 'user' | 'group';
	/** Group key for group uploads */
	groupKey?: string;
	/** Transform function to apply to records */
	transformFunc?: (record: any, heavyObjects: any) => any;
	/** Heavy objects to pass to transform function */
	heavyObjects?: any;
	/** Maximum number of retries */
	maxRetries?: number;
}

/**
 * Result from extract job
 */
export interface ExtractResult {
	/** Number of files extracted */
	extracted: number;
	/** Number of files skipped (already exist) */
	skipped: number;
	/** List of file paths */
	files: string[];
}

/**
 * Result from load job
 */
export interface LoadResult {
	/** Number of records uploaded */
	uploaded: number;
	/** Number of files processed */
	files: number;
	/** List of file paths processed */
	filePaths: string[];
}

/**
 * Result from pipeline run
 */
export interface PipelineResult {
	/** Extract stage results */
	extract?: {
		members?: ExtractResult;
		channels?: ExtractResult;
	};
	/** Load stage results */
	load?: {
		members?: LoadResult;
		channels?: LoadResult;
	};
}
