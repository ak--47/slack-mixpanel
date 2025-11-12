/**
 * @fileoverview Logging utility with production-aware verbosity
 * @module Logger
 */

const { NODE_ENV = "production" } = process.env;
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
	}
};

export default logger;
