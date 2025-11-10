/**
 * @fileoverview Logging utility that tees output to both console and file
 * @module Logger
 */

import fs from 'fs';
import path from 'path';

/**
 * Create a logger that writes to both console and file
 * @param {string} logFilePath - Path to log file
 * @returns {Object} Logger functions
 */
export function createLogger(logFilePath) {
	// Ensure logs directory exists
	const logDir = path.dirname(logFilePath);
	if (!fs.existsSync(logDir)) {
		fs.mkdirSync(logDir, { recursive: true });
	}

	// Create write stream for log file
	const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });

	// Store original console methods
	const originalLog = console.log;
	const originalError = console.error;
	const originalWarn = console.warn;
	const originalInfo = console.info;

	// Helper to write to both console and file
	function tee(originalMethod, ...args) {
		// Write to original console
		originalMethod(...args);

		// Write to file
		const message = args.map(arg =>
			typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)
		).join(' ');
		logStream.write(message + '\n');
	}

	// Override console methods
	console.log = (...args) => tee(originalLog, ...args);
	console.error = (...args) => tee(originalError, ...args);
	console.warn = (...args) => tee(originalWarn, ...args);
	console.info = (...args) => tee(originalInfo, ...args);

	return {
		/**
		 * Restore original console methods and close log stream
		 */
		restore() {
			console.log = originalLog;
			console.error = originalError;
			console.warn = originalWarn;
			console.info = originalInfo;
			logStream.end();
		},

		/**
		 * Get the log file path
		 */
		getLogPath() {
			return logFilePath;
		}
	};
}

export default createLogger;
