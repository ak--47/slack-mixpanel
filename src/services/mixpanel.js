/**
 * @fileoverview Mixpanel upload service using mixpanel-import package
 * @module MixpanelService
 */

import mpImport from 'mixpanel-import';
import { Readable } from 'stream';
const { NODE_ENV = "unknown" } = process.env;

/**
 * @typedef {Object} UploadOptions
 * @property {('event'|'user'|'group')} [type='event'] - Type of data to upload
 * @property {string} [groupKey=''] - Group key for group uploads
 * @property {string} token - Mixpanel project token
 * @property {string} [secret] - Mixpanel project secret (optional)
 */

/**
 * @typedef {Object} UploadResult
 * @property {Object} meta - Upload metadata
 * @property {number} meta.rows_total - Total rows processed
 * @property {number} meta.rows_imported - Rows successfully imported
 */

/**
 * Mixpanel uploader using the mixpanel-import package
 */
class MixpanelUploader {
	constructor() {
		this.defaultOptions = {
			workers: 100,          // Increased from 30
			recordsPerBatch: 5000, // Increased from 2000
			compress: true,
			fixData: true,
			fixTime: true,
			verbose: false,
			strict: false,         // Allow malformed data
			timeout: 30000        // 30 second timeout
		};
	}

	/**
	 * Upload Node.js stream directly to Mixpanel using mixpanel-import
	 * @param {Stream} nodeStream - Node.js readable stream (object mode)
	 * @param {UploadOptions} options - Upload configuration
	 * @param {Function} [errorHandler=null] - Optional error handler function
	 * @param {Function} [responseMonitor=null] - Optional response monitoring function
	 * @returns {Promise<UploadResult>} Upload result with metadata
	 * @throws {Error} When token is missing or upload fails
	 * @example
	 * const nodeStream = highlandStream.toNodeStream({ objectMode: true });
	 * const result = await uploader.upload(nodeStream, {
	 *   type: 'event',
	 *   token: 'your-token'
	 * });
	 */
	async upload(nodeStream, options = {}, errorHandler = null, responseMonitor = null) {
		const { type = 'event', groupKey = '', token, secret, ...uploadOptions } = options;

		if (!token) {
			throw new Error('Mixpanel token is required');
		}

		// Prepare credentials
		const credentials = { token };
		if (secret) credentials.secret = secret;

		// Prepare mixpanel-import options
		/** @type {import('mixpanel-import').Options} */
		const mpOptions = {
			...this.defaultOptions,
			...uploadOptions,
			recordType: type,
			groupKey: groupKey || undefined,
			showProgress: NODE_ENV !== "production"  // Show progress in all non-production environments
		};

		try {
			// mixpanel-import handles Node.js streams directly
			const result = await mpImport(credentials, nodeStream, mpOptions);

			// Call response monitor if provided
			if (responseMonitor) {
				responseMonitor({
					num_records_imported: result.success || 0,
					num_good_events: result.success || 0
				});
			}

			return {
				meta: {
					rows_total: (result.success || 0) + (result.failed || 0),
					rows_imported: result.success || 0
				}
			};

		} catch (error) {
			// Handle errors using provided error handler
			if (errorHandler) {
				const handled = errorHandler(error);
				if (handled && handled.num_records_imported) {
					return {
						meta: {
							rows_total: handled.num_records_imported,
							rows_imported: handled.num_records_imported
						}
					};
				}
			}
			throw error;
		}
	}

	/**
	 * Upload array of objects directly to Mixpanel using mixpanel-import
	 * @param {Array} data - Array of objects to upload
	 * @param {UploadOptions} options - Upload configuration
	 * @returns {Promise<UploadResult>} Upload result with metadata
	 * @throws {Error} When token is missing or upload fails
	 * @example
	 * const result = await uploader.uploadArray([{event: 'test', distinct_id: '123'}], {
	 *   type: 'event',
	 *   token: 'your-token'
	 * });
	 */
	async uploadArray(data, options = {}) {
		const { type = 'event', groupKey = '', token, secret, ...uploadOptions } = options;

		if (!token) {
			throw new Error('Mixpanel token is required');
		}

		if (!Array.isArray(data) || data.length === 0) {
			return {
				meta: {
					rows_total: 0,
					rows_imported: 0
				}
			};
		}

		// Prepare credentials
		const credentials = { token };
		if (secret) credentials.secret = secret;

		// Prepare mixpanel-import options
		/** @type {import('mixpanel-import').Options} */
		const mpOptions = {
			...this.defaultOptions,
			...uploadOptions,
			recordType: type,
			groupKey: groupKey || undefined,
			showProgress: NODE_ENV !== "production"  // Show progress in all non-production environments
		};

		try {
			// mixpanel-import handles arrays directly - no stream conversion needed!
			const result = await mpImport(credentials, data, mpOptions);

			return {
				meta: {
					rows_total: (result.success || 0) + (result.failed || 0),
					rows_imported: result.success || 0
				}
			};

		} catch (error) {
			throw error;
		}
	}

	/**
	 * Test uploading sample data to validate configuration
	 * @param {UploadOptions} options - Upload configuration to test
	 * @returns {Promise<boolean>} Whether the test was successful
	 */
	async testUpload(options = {}) {
		const testData = [{
			event: 'Test Event',
			distinct_id: 'test-user',
			time: Math.floor(Date.now() / 1000),
			properties: {
				test_property: 'test_value',
				source: 'mixpanel-service-test'
			}
		}];

		const testStream = Readable.from(testData);

		try {
			const result = await mpImport(
				{ token: options.token, secret: options.secret },
				testStream,
				{
					recordType: 'event',
					dryRun: true, // Don't actually send data
					verbose: false
				}
			);
			return true;
		} catch (error) {
			console.error('Mixpanel test upload failed:', error.message);
			return false;
		}
	}
}

const mixpanel = new MixpanelUploader();

// Direct execution capability for debugging
if (import.meta.url === `file://${process.argv[1]}`) {
	console.log('🔧 Running Mixpanel service directly...');

	const { NODE_ENV = "unknown", mixpanel_token } = process.env;

	// Create test data
	const testEvents = [
		{
			event: 'Test Event',
			distinct_id: 'test-user-1',
			time: Math.floor(Date.now() / 1000),
			properties: {
				test_property: 'test_value',
				source: 'service_test'
			}
		},
		{
			event: 'Test Event 2',
			distinct_id: 'test-user-2',
			time: Math.floor(Date.now() / 1000),
			properties: {
				test_property: 'test_value_2',
				source: 'service_test'
			}
		}
	];

	try {
		console.log('🧪 Testing mixpanel-import integration...');

		// Test with dry run if token is available
		if (mixpanel_token) {
			console.log('🔑 Found token, testing dry run...');
			const testResult = await mixpanel.testUpload({ token: mixpanel_token });
			console.log(`✅ Dry run test: ${testResult ? 'PASSED' : 'FAILED'}`);
		} else {
			console.log('ℹ️  No token found, skipping live test');
		}

		// Test stream conversion
		console.log('🧪 Testing stream processing...');
		const testStream = Readable.from(testEvents);
		console.log('✅ Stream created with test data');

		console.log('🧪 Testing Highland stream integration...');
		const _ = await import('highland');
		const highlandStream = _(testEvents);
		const nodeStream = highlandStream.toNodeStream({ objectMode: true });
		console.log('✅ Highland stream converted to Node stream');

		console.log('✅ Mixpanel service test completed successfully!');
		console.log('ℹ️  Using mixpanel-import package for reliable uploads');

		// Debugger for dev inspection
		if (NODE_ENV === 'dev') debugger;

	} catch (error) {
		console.error('❌ Mixpanel service test failed:', error);
		if (NODE_ENV === 'dev') debugger;
		process.exit(1);
	}
}

export default mixpanel;