/**
 * @fileoverview Logging utility with production-aware verbosity
 * @module Logger
 */

const { NODE_ENV = "unknown" } = process.env;
const IS_PRODUCTION = NODE_ENV === "production";

// ANSI color codes (no library needed!)
const colors = {
	reset: '\x1b[0m',
	cyan: '\x1b[36m',
	yellow: '\x1b[33m',
	red: '\x1b[31m',
	dim: '\x1b[2m'
};

/**
 * Colorize output in dev mode
 */
function colorize(color, ...args) {
	if (IS_PRODUCTION) {
		return args.join(' ');
	}
	return `${color}${args.join(' ')}${colors.reset}`;
}

/**
 * Production-aware logger
 * - info: Always logs (important information, summaries) - cyan in dev
 * - error: Always logs (errors) - red in dev
 * - verbose: Only logs in non-production (detailed progress) - dim in dev
 * - warn: Always logs (warnings) - yellow in dev
 * - summary: Structured data logging (JSON in production, formatted in dev)
 */
export const logger = {
	/**
	 * Log important information (always shown, cyan in dev)
	 */
	info: (...args) => {
		console.log(colorize(colors.cyan, ...args));
	},

	/**
	 * Log errors (always shown, red in dev)
	 */
	error: (...args) => {
		console.error(colorize(colors.red, ...args));
	},

	/**
	 * Log verbose/detailed information (hidden in production, dim in dev)
	 */
	verbose: (...args) => {
		if (!IS_PRODUCTION) {
			console.log(colorize(colors.dim, ...args));
		}
	},

	/**
	 * Log warnings (always shown, yellow in dev)
	 */
	warn: (...args) => {
		console.warn(colorize(colors.yellow, ...args));
	},

	/**
	 * Log structured data (JSON in production for parsing, formatted in dev for readability)
	 */
	summary: (message, data = {}) => {
		if (IS_PRODUCTION) {
			// Production: Single-line JSON for log aggregation/parsing
			console.log(JSON.stringify({ message, ...data, timestamp: new Date().toISOString() }));
		} else {
			// Dev: Human-readable format with colors
			console.log(colorize(colors.cyan, `\n${message}`));
			if (Object.keys(data).length > 0) {
				console.log(colorize(colors.dim, JSON.stringify(data, null, 2)));
			}
		}
	}
};

export default logger;
