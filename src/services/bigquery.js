/**
 * @fileoverview BigQuery service for data warehousing and analytics operations
 * @module BigQueryService
 */

import { BigQuery } from '@google-cloud/bigquery';
import 'dotenv/config';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import * as akTools from 'ak-tools';
import pLimit from 'p-limit';

dayjs.extend(utc);
const { sleep } = akTools;

/**
 * @typedef {Object} BigQueryConfig
 * @property {string} projectId - BigQuery project ID
 * @property {string} [keyFilename] - Path to service account key file
 * @property {Object} [credentials] - Service account credentials object
 */

/**
 * @typedef {Object} TableSchema
 * @property {string} name - Field name
 * @property {string} type - BigQuery field type (STRING, INT64, FLOAT64, etc.)
 * @property {string} [mode] - Field mode (NULLABLE, REQUIRED, REPEATED)
 * @property {string} [description] - Field description
 */

/**
 * @typedef {Object} InsertResult
 * @property {boolean} success - Whether insertion was successful
 * @property {number} insertedRows - Number of rows successfully inserted
 * @property {number} failedRows - Number of rows that failed to insert
 * @property {Array} [errors] - Array of error messages if any failures occurred
 * @property {number} duration - Time taken for insertion in milliseconds
 */

const {
	gcs_project,
	NODE_ENV = "unknown",
	CONCURRENCY = NODE_ENV === "backfill" ? 5 : 2
} = process.env;

if (!gcs_project) throw new Error('No gcs_project in environment variables');

const limit = pLimit(parseInt(CONCURRENCY));
const initStartTime = Date.now();

/** @type {BigQuery} */
let bqClient = null;

/** @type {Object.<string, any>} */
const cache = {};

/**
 * Initialize BigQuery client with application default credentials
 * @returns {BigQuery} Configured BigQuery client instance
 */
function createBigQueryClient() {
	console.log('BIGQUERY: Using application default credentials');
	return new BigQuery({
		projectId: gcs_project
	});
}

/**
 * Test BigQuery authentication by running a simple query
 * @returns {Promise<{ready: boolean, projectId: string}>}
 * @throws {Error} When authentication fails
 */
async function testAuth() {
	try {
		if (!bqClient) {
			bqClient = createBigQueryClient();
		}

		// Test with a simple dry-run query
		await bqClient.createQueryJob({
			query: 'SELECT 1 as test_value',
			dryRun: true
		});

		console.log('BIGQUERY: Authentication successful');
		return { ready: true, projectId: gcs_project };
	} catch (error) {
		console.error('BIGQUERY: Authentication test failed:', error);
		throw error;
	}
}

/**
 * Initialize BigQuery service and test authentication
 * @returns {Promise<{ready: boolean, projectId: string}>}
 * @throws {Error} When authentication fails
 */
async function initializeBigQuery() {
	const auth = await testAuth();

	if (auth.ready) {
		const initTime = Date.now() - initStartTime;
		console.log(`BIGQUERY: service initialized in ${initTime}ms`);
		return auth;
	}

	throw new Error('BigQuery authentication failed');
}

/**
 * Ensure dataset exists, create if it doesn't
 * @param {string} datasetId - Dataset ID to create or verify
 * @param {Object} [options] - Dataset creation options
 * @param {string} [options.location='US'] - Dataset location
 * @param {string} [options.description] - Dataset description
 * @returns {Promise<string>} Dataset ID
 * @throws {Error} When dataset creation fails
 */
async function ensureDataset(datasetId, options = {}) {
	const { location = 'US', description } = options;

	try {
		const dataset = bqClient.dataset(datasetId);
		const [exists] = await dataset.exists();

		if (!exists) {
			console.log(`BIGQUERY: Creating dataset ${datasetId}`);
			const createOptions = { location };
			if (description) createOptions.description = description;

			await dataset.create(createOptions);
			console.log(`BIGQUERY: Dataset ${datasetId} created`);
		} else {
			console.log(`BIGQUERY: Dataset ${datasetId} already exists`);
		}

		return datasetId;
	} catch (error) {
		console.error(`BIGQUERY: Error ensuring dataset ${datasetId}:`, error);
		throw error;
	}
}

/**
 * Wait for table to be ready for operations with retry logic
 * @param {string} datasetId - Dataset ID
 * @param {string} tableId - Table ID
 * @param {number} [retries=20] - Number of existence check retries
 * @param {number} [maxInsertAttempts=20] - Maximum insert readiness attempts
 * @returns {Promise<boolean>} True if table is ready, false otherwise
 */
async function waitForTableToBeReady(datasetId, tableId, retries = 20, maxInsertAttempts = 20) {
	const table = bqClient.dataset(datasetId).table(tableId);

	console.log('BIGQUERY: Checking if table exists...');

	// First, wait for table to exist
	for (let i = 0; i < retries; i++) {
		const [exists] = await table.exists();
		if (exists) {
			console.log(`BIGQUERY: Table confirmed to exist on attempt ${i + 1}`);
			break;
		}

		const sleepTime = Math.random() * 4000 + 1000; // 1-5 seconds
		console.log(`BIGQUERY: Waiting for table to exist, attempt ${i + 1}, sleeping ${Math.round(sleepTime)}ms`);
		await sleep(sleepTime);

		if (i === retries - 1) {
			console.log(`BIGQUERY: Table does not exist after ${retries} attempts`);
			return false;
		}
	}

	console.log('BIGQUERY: Checking if table is ready for operations...');

	// Then, wait for table to be ready for operations
	for (let insertAttempt = 0; insertAttempt < maxInsertAttempts; insertAttempt++) {
		try {
			// Attempt a dummy insert that should fail gracefully
			const dummyRecord = { __test_field__: 'test_value_' + Date.now() };
			await table.insert([dummyRecord]);
			console.log('BIGQUERY: Table is ready for operations (unexpected success)');
			return true;
		} catch (error) {
			if (error.code === 404) {
				const sleepTime = Math.random() * 4000 + 1000; // 1-5 seconds
				console.log(`BIGQUERY: Table not ready for operations, sleeping ${Math.round(sleepTime)}ms, attempt ${insertAttempt + 1}`);
				await sleep(sleepTime);
			} else if (error.name === 'PartialFailureError') {
				console.log('BIGQUERY: Table is ready for operations');
				return true;
			} else {
				// Any other error indicates the table is ready but our dummy insert failed as expected
				console.log('BIGQUERY: Table is ready for operations (expected error)');
				return true;
			}
		}
	}

	console.log('BIGQUERY: Table is NOT ready after all attempts');
	return false;
}

/**
 * Create or recreate a table with the given schema
 * @param {string} datasetId - Dataset ID
 * @param {string} tableId - Table ID
 * @param {TableSchema[]} schema - Table schema definition
 * @param {Object} [options] - Table creation options
 * @param {boolean} [options.recreate=false] - Whether to delete and recreate existing table
 * @param {Object} [options.timePartitioning] - Time partitioning configuration
 * @param {Object} [options.clustering] - Clustering configuration
 * @returns {Promise<string>} Table ID
 * @throws {Error} When table creation fails
 */
async function ensureTable(datasetId, tableId, schema, options = {}) {
	const { recreate = false, timePartitioning, clustering } = options;

	try {
		await ensureDataset(datasetId);

		const dataset = bqClient.dataset(datasetId);
		const table = dataset.table(tableId);
		const [exists] = await table.exists();

		if (exists && recreate) {
			console.log(`BIGQUERY: Deleting existing table ${tableId} for recreation`);
			await table.delete();
		} else if (exists && !recreate) {
			console.log(`BIGQUERY: Table ${tableId} already exists`);
			return tableId;
		}

		console.log(`BIGQUERY: Creating table ${tableId}`);
		const createOptions = { schema };

		if (timePartitioning) {
			createOptions.timePartitioning = timePartitioning;
		}

		if (clustering) {
			createOptions.clustering = clustering;
		}

		await dataset.createTable(tableId, createOptions);
		console.log(`BIGQUERY: Table ${tableId} created`);

		// Wait for table to be ready
		const isReady = await waitForTableToBeReady(datasetId, tableId);
		if (!isReady) {
			throw new Error(`Table ${datasetId}.${tableId} was created but is not ready for operations`);
		}

		return tableId;
	} catch (error) {
		console.error(`BIGQUERY: Error ensuring table ${datasetId}.${tableId}:`, error);
		throw error;
	}
}

/**
 * Clean field name to be BigQuery compatible
 * @param {string} name - Original field name
 * @returns {string} Cleaned field name
 */
function cleanFieldName(name) {
	return name
		.replace(/[^a-zA-Z0-9_]/g, '_') // Replace invalid characters with underscore
		.replace(/^[^a-zA-Z_]/, '_') // Ensure starts with letter or underscore
		.toLowerCase();
}

/**
 * Infer BigQuery field type from JavaScript value
 * @param {any} value - JavaScript value to analyze
 * @returns {string} BigQuery field type
 */
function inferFieldType(value) {
	if (value === null || value === undefined) {
		return 'STRING'; // Default to STRING for null values
	}

	if (typeof value === 'boolean') {
		return 'BOOLEAN';
	}

	if (typeof value === 'number') {
		return Number.isInteger(value) ? 'INT64' : 'FLOAT64';
	}

	if (typeof value === 'string') {
		// Check if it's a date/timestamp
		const dateValue = new Date(value);
		if (!isNaN(dateValue.getTime()) && value.match(/^\d{4}-\d{2}-\d{2}/)) {
			return value.includes('T') || value.includes(' ') ? 'TIMESTAMP' : 'DATE';
		}
		return 'STRING';
	}

	if (Array.isArray(value) || typeof value === 'object') {
		return 'JSON';
	}

	return 'STRING';
}

/**
 * Generate BigQuery schema from array of JavaScript objects
 * @param {Object[]} objects - Array of objects to analyze
 * @param {Object} [options] - Schema generation options
 * @param {boolean} [options.strict=false] - Use strict typing (require all fields)
 * @param {number} [options.sampleSize=100] - Number of objects to sample for schema inference
 * @returns {TableSchema[]} BigQuery schema definition
 * @example
 * const messages = [
 *   { id: 1, text: 'Hello', created_at: '2024-01-01T10:00:00Z', reactions: ['👍', '❤️'] },
 *   { id: 2, text: 'World', created_at: '2024-01-01T11:00:00Z', reactions: [] }
 * ];
 * const schema = generateSchemaFromObjects(messages);
 */
function generateSchemaFromObjects(objects, options = {}) {
	const { strict = false, sampleSize = 100 } = options;

	if (!objects || objects.length === 0) {
		throw new Error('Cannot generate schema from empty array');
	}

	// Sample objects if array is large
	const sampleObjects = objects.length > sampleSize
		? objects.slice(0, sampleSize)
		: objects;

	// Collect all unique field names and their types
	const fieldMap = new Map();

	sampleObjects.forEach(obj => {
		if (obj && typeof obj === 'object') {
			Object.entries(obj).forEach(([key, value]) => {
				const cleanName = cleanFieldName(key);
				const type = inferFieldType(value);

				if (fieldMap.has(cleanName)) {
					const existing = fieldMap.get(cleanName);
					// If types differ, default to STRING or JSON for complex types
					if (existing.type !== type) {
						if (type === 'JSON' || existing.type === 'JSON') {
							existing.type = 'JSON';
						} else if (type === 'STRING' || existing.type === 'STRING') {
							existing.type = 'STRING';
						} else if ((type === 'INT64' && existing.type === 'FLOAT64') ||
								 (type === 'FLOAT64' && existing.type === 'INT64')) {
							existing.type = 'FLOAT64'; // Promote to float if mixed numbers
						}
					}
					existing.nullCount = value === null || value === undefined
						? existing.nullCount + 1
						: existing.nullCount;
				} else {
					fieldMap.set(cleanName, {
						name: cleanName,
						type,
						nullCount: value === null || value === undefined ? 1 : 0,
						originalName: key
					});
				}
			});
		}
	});

	// Generate schema with proper modes
	const schema = Array.from(fieldMap.values()).map(field => {
		const mode = strict || field.nullCount === 0 ? 'REQUIRED' : 'NULLABLE';

		return {
			name: field.name,
			type: field.type,
			mode,
			description: field.originalName !== field.name
				? `Original field name: ${field.originalName}`
				: undefined
		};
	});

	console.log(`BIGQUERY: Generated schema with ${schema.length} fields from ${sampleObjects.length} sample objects`);
	return schema;
}

/**
 * Convert JavaScript value to BigQuery-compatible value
 * @param {any} value - JavaScript value to convert
 * @param {string} type - Target BigQuery type
 * @returns {any} BigQuery-compatible value
 */
function convertValueToBigQuery(value, type) {
	if (value === null || value === undefined) {
		return null;
	}

	switch (type) {
		case 'STRING':
			return String(value);
		case 'INT64':
			return parseInt(value);
		case 'FLOAT64':
			return parseFloat(value);
		case 'BOOLEAN':
			if (typeof value === 'boolean') return value;
			if (typeof value === 'number') return value === 1;
			if (typeof value === 'string') return value.toLowerCase() === 'true';
			return Boolean(value);
		case 'TIMESTAMP':
			if (value instanceof Date) return value.toISOString();
			return new Date(value).toISOString();
		case 'DATE':
			if (value instanceof Date) return value.toISOString().split('T')[0];
			return new Date(value).toISOString().split('T')[0];
		case 'JSON':
			if (typeof value === 'string') return value;
			return JSON.stringify(value);
		default:
			return value;
	}
}

/**
 * Prepare rows for BigQuery insertion by converting types and cleaning fields
 * @param {Object[]} rows - Array of objects to prepare
 * @param {TableSchema[]} schema - Table schema definition
 * @returns {Object[]} Prepared rows for insertion
 */
function prepareRowsForInsertion(rows, schema) {
	return rows.map(row => {
		const cleanRow = {};

		schema.forEach(field => {
			const originalValue = row[field.name];

			// Skip undefined and empty string values
			if (originalValue !== undefined && originalValue !== '') {
				try {
					cleanRow[field.name] = convertValueToBigQuery(originalValue, field.type);
				} catch (error) {
					console.warn(`BIGQUERY: Error converting field ${field.name}:`, error);
					cleanRow[field.name] = null;
				}
			}
		});

		return cleanRow;
	});
}

/**
 * Check for existing records to enable deduplication
 * @param {string} datasetId - Dataset ID
 * @param {string} tableId - Table ID
 * @param {Object[]} records - Records to check for duplicates
 * @param {string} [idField='id'] - Field to use as unique identifier
 * @param {number} [lookbackDays=30] - Number of days to look back for duplicates
 * @returns {Promise<Object[]>} Records that don't already exist
 */
async function dedupeRecords(datasetId, tableId, records, idField = 'id', lookbackDays = 30) {
	if (!records || records.length === 0) {
		return [];
	}

	try {
		// Extract unique IDs from records
		const ids = records
			.map(record => record[idField])
			.filter(Boolean)
			.filter((id, index, arr) => arr.indexOf(id) === index); // Remove duplicates

		if (ids.length === 0) {
			console.log('BIGQUERY: No valid IDs found for deduplication, returning all records');
			return records;
		}

		// Create query to check for existing records
		const lookbackDate = dayjs().subtract(lookbackDays, 'day').format('YYYY-MM-DD');
		const idList = ids.map(id => `'${id}'`).join(',');

		const dedupeQuery = `
			SELECT DISTINCT ${idField}
			FROM \`${gcs_project}.${datasetId}.${tableId}\`
			WHERE ${idField} IN (${idList})
			AND DATE(_PARTITIONTIME) >= '${lookbackDate}'
		`;

		console.log(`BIGQUERY: Checking for duplicates in last ${lookbackDays} days`);
		const [existingRecords] = await bqClient.query({
			query: dedupeQuery,
			useLegacySql: false
		});

		const existingIds = new Set(existingRecords.map(row => row[idField]));

		// Filter out records that already exist
		const uniqueRecords = records.filter(record => {
			const recordId = record[idField];
			return recordId && !existingIds.has(recordId);
		});

		console.log(`BIGQUERY: Found ${existingRecords.length} existing records, returning ${uniqueRecords.length} unique records`);
		return uniqueRecords;

	} catch (error) {
		console.warn('BIGQUERY: Error checking for duplicates:', error.message);
		// On error, return all records to avoid blocking the pipeline
		return records;
	}
}

/**
 * Get existing record IDs for upsert operations
 * @param {string} datasetId - Dataset ID
 * @param {string} tableId - Table ID
 * @param {Object[]} records - Records to check for existing IDs
 * @param {string} [idField='id'] - Field to use as unique identifier
 * @param {number} [lookbackDays=30] - Number of days to look back
 * @returns {Promise<string[]>} Array of existing IDs
 */
async function getExistingRecordIds(datasetId, tableId, records, idField = 'id', lookbackDays = 30) {
	if (!records || records.length === 0) {
		return [];
	}

	try {
		// Extract unique IDs from records
		const ids = records
			.map(record => record[idField])
			.filter(Boolean)
			.filter((id, index, arr) => arr.indexOf(id) === index); // Remove duplicates

		if (ids.length === 0) {
			return [];
		}

		// Create query to get existing IDs
		const lookbackDate = dayjs().subtract(lookbackDays, 'day').format('YYYY-MM-DD');
		const idList = ids.map(id => `'${id}'`).join(',');

		const existingQuery = `
			SELECT DISTINCT ${idField}
			FROM \`${gcs_project}.${datasetId}.${tableId}\`
			WHERE ${idField} IN (${idList})
			AND DATE(_PARTITIONTIME) >= '${lookbackDate}'
		`;

		const [existingRecords] = await bqClient.query({
			query: existingQuery,
			useLegacySql: false
		});

		return existingRecords.map(row => row[idField]);

	} catch (error) {
		console.warn('BIGQUERY: Error getting existing record IDs:', error.message);
		return [];
	}
}

/**
 * Delete records by IDs for upsert operations
 * @param {string} datasetId - Dataset ID
 * @param {string} tableId - Table ID
 * @param {string[]} ids - Array of IDs to delete
 * @param {string} [idField='id'] - Field to use as unique identifier
 * @returns {Promise<void>}
 */
async function deleteRecordsByIds(datasetId, tableId, ids, idField = 'id') {
	if (!ids || ids.length === 0) {
		return;
	}

	try {
		const idList = ids.map(id => `'${id}'`).join(',');

		const deleteQuery = `
			DELETE FROM \`${gcs_project}.${datasetId}.${tableId}\`
			WHERE ${idField} IN (${idList})
		`;

		await bqClient.query({
			query: deleteQuery,
			useLegacySql: false
		});

		console.log(`BIGQUERY: Deleted ${ids.length} existing records`);

	} catch (error) {
		console.error('BIGQUERY: Error deleting existing records:', error);
		throw error;
	}
}

/**
 * Insert data into BigQuery table with optional deduplication
 * @param {string} datasetId - Dataset ID
 * @param {string} tableId - Table ID
 * @param {Object[]} data - Data to insert
 * @param {Object} [options] - Insert options
 * @param {TableSchema[]} [options.schema] - Table schema (auto-generated if not provided)
 * @param {boolean} [options.dedupe=false] - Whether to deduplicate before inserting
 * @param {string} [options.idField='id'] - Field to use for deduplication
 * @param {number} [options.lookbackDays=30] - Days to look back for deduplication
 * @param {boolean} [options.upsert=false] - Whether to replace existing records (true) or skip them (false)
 * @param {boolean} [options.createTable=true] - Whether to create table if it doesn't exist
 * @param {number} [options.batchSize=1000] - Number of rows per batch
 * @returns {Promise<InsertResult>} Insert result with statistics
 * @example
 * const result = await insertData('analytics', 'user_messages', messages, {
 *   dedupe: true,
 *   idField: 'message_id',
 *   lookbackDays: 7
 * });
 */
async function insertData(datasetId, tableId, data, options = {}) {
	const {
		schema: providedSchema,
		dedupe = false,
		idField = 'id',
		lookbackDays = 30,
		upsert = false,
		createTable = true,
		batchSize = 1000
	} = options;

	const startTime = Date.now();

	try {
		if (!data || data.length === 0) {
			return {
				success: true,
				insertedRows: 0,
				failedRows: 0,
				duration: Date.now() - startTime
			};
		}

		console.log(`BIGQUERY: Inserting ${data.length} records into ${datasetId}.${tableId}`);

		// Generate schema if not provided
		let schema = providedSchema;
		if (!schema) {
			console.log('BIGQUERY: Generating schema from data...');
			schema = generateSchemaFromObjects(data);
		}

		// Create table if needed
		if (createTable) {
			await ensureTable(datasetId, tableId, schema);
		}

		// Handle deduplication/upsert if requested
		let recordsToInsert = data;
		let duplicatesSkipped = 0;

		if (dedupe) {
			if (upsert) {
				// For upsert mode, delete existing records first, then insert all
				const existingIds = await getExistingRecordIds(datasetId, tableId, data, idField, lookbackDays);

				if (existingIds.length > 0) {
					console.log(`BIGQUERY: Deleting ${existingIds.length} existing records for upsert`);
					await deleteRecordsByIds(datasetId, tableId, existingIds, idField);
				}

				recordsToInsert = data; // Insert all records since we deleted duplicates
			} else {
				// Standard dedupe mode - filter out duplicates
				recordsToInsert = await dedupeRecords(datasetId, tableId, data, idField, lookbackDays);
				duplicatesSkipped = data.length - recordsToInsert.length;

				if (recordsToInsert.length === 0) {
					console.log('BIGQUERY: All records are duplicates, skipping insert');
					return {
						success: true,
						insertedRows: 0,
						failedRows: 0,
						duration: Date.now() - startTime,
						duplicatesSkipped: data.length
					};
				}
			}
		}

		// Prepare data for insertion
		const preparedRows = prepareRowsForInsertion(recordsToInsert, schema);

		// Get table reference
		const table = bqClient.dataset(datasetId).table(tableId);

		// Insert in batches
		const results = [];
		let totalInserted = 0;
		let totalFailed = 0;

		for (let i = 0; i < preparedRows.length; i += batchSize) {
			const batch = preparedRows.slice(i, i + batchSize);
			const batchStartTime = Date.now();

			try {
				const insertOptions = {
					skipInvalidRows: false,
					ignoreUnknownValues: false,
					raw: false,
					partialRetries: 3,
					schema: schema
				};

				await table.insert(batch, insertOptions);

				const batchDuration = Date.now() - batchStartTime;
				totalInserted += batch.length;

				results.push({
					status: 'success',
					insertedRows: batch.length,
					failedRows: 0,
					duration: batchDuration
				});

				console.log(`BIGQUERY: Inserted batch ${Math.floor(i / batchSize) + 1} (${batch.length} rows) in ${batchDuration}ms`);

			} catch (error) {
				const batchDuration = Date.now() - batchStartTime;

				if (error.name === 'PartialFailureError') {
					const failedRows = error.errors?.length || 0;
					const insertedRows = batch.length - failedRows;
					totalInserted += insertedRows;
					totalFailed += failedRows;

					const uniqueErrors = Array.from(new Set(
						error.errors?.map(e => e.errors?.map(e => e.message)).flat() || []
					));

					results.push({
						status: 'partial_failure',
						insertedRows,
						failedRows,
						errors: uniqueErrors,
						duration: batchDuration
					});

					console.warn(`BIGQUERY: Partial failure in batch ${Math.floor(i / batchSize) + 1}: ${failedRows} failed, ${insertedRows} succeeded`);
				} else {
					totalFailed += batch.length;
					results.push({
						status: 'error',
						insertedRows: 0,
						failedRows: batch.length,
						errors: [error.message],
						duration: batchDuration
					});

					console.error(`BIGQUERY: Batch ${Math.floor(i / batchSize) + 1} failed completely:`, error.message);
				}
			}

			// Rate limiting between batches
			if (i + batchSize < preparedRows.length) {
				await sleep(100);
			}
		}

		const totalDuration = Date.now() - startTime;
		console.log(`BIGQUERY: Insert completed - ${totalInserted} inserted, ${totalFailed} failed in ${totalDuration}ms`);

		return {
			success: totalFailed === 0,
			insertedRows: totalInserted,
			failedRows: totalFailed,
			duration: totalDuration,
			batchResults: results,
			...(dedupe && { duplicatesSkipped })
		};

	} catch (error) {
		const duration = Date.now() - startTime;
		console.error('BIGQUERY: Insert operation failed:', error);

		return {
			success: false,
			insertedRows: 0,
			failedRows: data.length,
			duration,
			errors: [error.message]
		};
	}
}

/**
 * Execute a BigQuery SQL query
 * @param {string} queryString - SQL query to execute
 * @param {Object} [options] - Query options
 * @param {boolean} [options.useLegacySql=false] - Whether to use legacy SQL
 * @param {number} [options.maxResults] - Maximum number of results to return
 * @param {boolean} [options.dryRun=false] - Whether to perform a dry run
 * @param {Object} [options.jobConfig] - Additional job configuration
 * @returns {Promise<Object[]>} Query results
 * @throws {Error} When query execution fails
 * @example
 * const results = await query('SELECT COUNT(*) as count FROM `project.dataset.table`');
 * console.log('Total rows:', results[0].count);
 */
async function query(queryString, options = {}) {
	const {
		useLegacySql = false,
		maxResults,
		dryRun = false,
		jobConfig = {}
	} = options;

	try {
		const queryOptions = {
			query: queryString,
			useLegacySql,
			...jobConfig
		};

		if (maxResults) {
			queryOptions.maxResults = maxResults;
		}

		if (dryRun) {
			queryOptions.dryRun = true;
		}

		console.log(`BIGQUERY: Executing query (dryRun: ${dryRun})`);

		const [results] = await bqClient.query(queryOptions);

		if (!dryRun) {
			console.log(`BIGQUERY: Query completed, ${results.length} rows returned`);
		} else {
			console.log('BIGQUERY: Dry run completed successfully');
		}

		return results;
	} catch (error) {
		console.error('BIGQUERY: Query execution failed:', error);
		throw error;
	}
}

/**
 * Get metadata about a dataset
 * @param {string} datasetId - Dataset ID to get metadata for
 * @returns {Promise<Object>} Dataset metadata
 * @throws {Error} If dataset doesn't exist or access is denied
 */
async function getDatasetInfo(datasetId) {
	try {
		const dataset = bqClient.dataset(datasetId);
		const [metadata] = await dataset.getMetadata();
		return metadata;
	} catch (error) {
		console.error(`BIGQUERY: Error getting dataset info for ${datasetId}:`, error);
		throw error;
	}
}

/**
 * Get metadata about a table including schema
 * @param {string} datasetId - Dataset ID
 * @param {string} tableId - Table ID
 * @returns {Promise<Object>} Table metadata including schema
 * @throws {Error} If table doesn't exist or access is denied
 */
async function getTableInfo(datasetId, tableId) {
	try {
		const table = bqClient.dataset(datasetId).table(tableId);
		const [metadata] = await table.getMetadata();
		return metadata;
	} catch (error) {
		console.error(`BIGQUERY: Error getting table info for ${datasetId}.${tableId}:`, error);
		throw error;
	}
}

/**
 * List all datasets in the project
 * @returns {Promise<Object[]>} Array of dataset metadata objects
 */
async function listDatasets() {
	try {
		const [datasets] = await bqClient.getDatasets();
		return datasets.map(dataset => ({
			id: dataset.id,
			projectId: dataset.projectId,
			location: dataset.location
		}));
	} catch (error) {
		console.error('BIGQUERY: Error listing datasets:', error);
		throw error;
	}
}

/**
 * List all tables in a dataset
 * @param {string} datasetId - Dataset ID to list tables from
 * @returns {Promise<Object[]>} Array of table metadata objects
 */
async function listTables(datasetId) {
	try {
		const dataset = bqClient.dataset(datasetId);
		const [tables] = await dataset.getTables();
		return tables.map(table => ({
			id: table.id,
			datasetId: table.dataset.id,
			projectId: table.dataset.projectId
		}));
	} catch (error) {
		console.error(`BIGQUERY: Error listing tables in ${datasetId}:`, error);
		throw error;
	}
}

/**
 * Delete a table
 * @param {string} datasetId - Dataset ID
 * @param {string} tableId - Table ID to delete
 * @returns {Promise<boolean>} True if deletion was successful
 */
async function deleteTable(datasetId, tableId) {
	try {
		const table = bqClient.dataset(datasetId).table(tableId);
		await table.delete();
		console.log(`BIGQUERY: Table ${datasetId}.${tableId} deleted successfully`);
		return true;
	} catch (error) {
		console.error(`BIGQUERY: Error deleting table ${datasetId}.${tableId}:`, error);
		throw error;
	}
}

// Initialize on module load
const { ready, projectId } = await initializeBigQuery();

/**
 * @typedef {Object} BigQueryService
 * @property {string} name - Service name
 * @property {string} description - Service description
 * @property {BigQuery} bqClient - BigQuery client instance
 * @property {string} projectId - BigQuery project ID
 * @property {boolean} ready - Whether service is ready
 * @property {Function} testAuth - Authentication tester
 * @property {Function} ensureDataset - Dataset creation/verification
 * @property {Function} ensureTable - Table creation/verification
 * @property {Function} generateSchemaFromObjects - Schema generation from objects
 * @property {Function} insertData - Data insertion with deduplication
 * @property {Function} dedupeRecords - Record deduplication
 * @property {Function} query - Query execution
 * @property {Function} getDatasetInfo - Dataset metadata retrieval
 * @property {Function} getTableInfo - Table metadata retrieval
 * @property {Function} listDatasets - List all datasets
 * @property {Function} listTables - List tables in dataset
 * @property {Function} deleteTable - Table deletion
 * @property {Function} prepareRowsForInsertion - Row preparation utility
 * @property {Function} convertValueToBigQuery - Value conversion utility
 */
const bigQueryService = {
	name: 'bigquery-service',
	description: 'Service for interacting with BigQuery for data warehousing and analytics',
	bqClient,
	projectId,
	ready,
	testAuth,
	ensureDataset,
	ensureTable,
	generateSchemaFromObjects,
	insertData,
	dedupeRecords,
	query,
	getDatasetInfo,
	getTableInfo,
	listDatasets,
	listTables,
	deleteTable,
	prepareRowsForInsertion,
	convertValueToBigQuery
};

// Direct execution capability for debugging
if (import.meta.url === `file://${process.argv[1]}`) {
	console.log('🔧 Running BigQuery service directly...');

	const { NODE_ENV = "unknown" } = process.env;

	try {
		// Test authentication
		console.log('✅ Authentication successful:', { ready, projectId });

		// Test dataset operations
		const testDatasetId = 'bigquery_service_test';
		console.log(`📦 Testing dataset operations with ${testDatasetId}`);

		await ensureDataset(testDatasetId, {
			description: 'Test dataset for BigQuery service validation'
		});

		// Test schema generation and table creation
		const testData = [
			{
				id: 1,
				name: 'Test Record 1',
				created_at: new Date().toISOString(),
				metadata: { type: 'test', active: true },
				count: 42
			},
			{
				id: 2,
				name: 'Test Record 2',
				created_at: new Date().toISOString(),
				metadata: { type: 'test', active: false },
				count: 84
			}
		];

		console.log('🔧 Testing schema generation...');
		const schema = generateSchemaFromObjects(testData);
		console.log('Generated schema:', schema.map(f => ({ name: f.name, type: f.type })));

		const testTableId = 'service_test_table';
		console.log(`📋 Testing table creation with ${testTableId}`);

		await ensureTable(testDatasetId, testTableId, schema, {
			recreate: true // Recreate for testing
		});

		console.log('📊 Testing data insertion...');
		const insertResult = await insertData(testDatasetId, testTableId, testData, {
			schema,
			createTable: false, // Table already exists
			batchSize: 100
		});

		console.log('Insert result:', {
			success: insertResult.success,
			insertedRows: insertResult.insertedRows,
			duration: insertResult.duration
		});

		// Test query
		console.log('🔍 Testing query execution...');
		const queryResult = await query(`
			SELECT COUNT(*) as total_rows
			FROM \`${gcs_project}.${testDatasetId}.${testTableId}\`
		`);

		console.log('Query result:', queryResult[0]);

		// Test deduplication
		console.log('🔄 Testing deduplication...');
		const dupeTestData = [
			...testData,
			{ id: 3, name: 'New Record', created_at: new Date().toISOString(), count: 100 }
		];

		const dedupeResult = await insertData(testDatasetId, testTableId, dupeTestData, {
			schema,
			dedupe: true,
			idField: 'id',
			lookbackDays: 1,
			createTable: false
		});

		console.log('Dedupe result:', {
			success: dedupeResult.success,
			insertedRows: dedupeResult.insertedRows,
			duplicatesSkipped: dedupeResult.duplicatesSkipped
		});

		// Cleanup test table
		console.log('🗑️ Cleaning up test table...');
		await deleteTable(testDatasetId, testTableId);

		console.log('✅ BigQuery service test completed successfully!');

		// Debugger for dev inspection
		if (NODE_ENV === 'dev') debugger;

	} catch (error) {
		console.error('❌ BigQuery service test failed:', error);
		if (NODE_ENV === 'dev') debugger;
		process.exit(1);
	}
}

export default bigQueryService;