/**
 * @fileoverview File storage service for writing/reading JSONL.gz files
 * @module Storage
 */

import fs from 'fs';
import path from 'path';
import { createWriteStream, createReadStream } from 'fs';
import { createGzip, createGunzip } from 'zlib';
import { pipeline } from 'stream/promises';
import { Storage as GCSStorage } from '@google-cloud/storage';
import 'dotenv/config';

const { gcs_path, gcs_project = 'mixpanel-gtm-training' } = process.env;

// Initialize GCS client if GCS path is configured
let gcsClient = null;
if (gcs_path) {
	gcsClient = new GCSStorage({ projectId: gcs_project });
}

/**
 * Get storage path (GCS or local tmp)
 * @returns {string} Storage base path
 */
export function getStoragePath() {
	if (gcs_path) {
		return gcs_path.replace(/\/$/, ''); // Remove trailing slash
	}
	return './tmp';
}

/**
 * Check if using GCS storage
 * @returns {boolean}
 */
export function isGCS() {
	return !!gcs_path;
}

/**
 * Parse GCS path into bucket and prefix
 * @param {string} gcsPath - GCS path (gs://bucket/path)
 * @returns {{bucket: string, prefix: string}}
 */
function parseGCSPath(gcsPath) {
	const match = gcsPath.match(/^gs:\/\/([^\/]+)\/(.*)$/);
	if (!match) {
		throw new Error(`Invalid GCS path: ${gcsPath}`);
	}
	return {
		bucket: match[1],
		prefix: match[2]
	};
}

/**
 * Write array of objects to JSONL.gz file
 * @param {string} filePath - Relative path (e.g., 'members/2024-01-01-members.jsonl.gz')
 * @param {Array<Object>} data - Array of objects to write
 * @returns {Promise<string>} Full path to written file
 */
export async function writeJSONLGz(filePath, data) {
	const basePath = getStoragePath();
	const fullPath = path.join(basePath, filePath);

	if (isGCS()) {
		// Write to GCS
		const { bucket, prefix } = parseGCSPath(basePath);
		const gcsFilePath = path.join(prefix, filePath);
		const file = gcsClient.bucket(bucket).file(gcsFilePath);

		// Create JSONL content
		const jsonlContent = data.map(obj => JSON.stringify(obj)).join('\n');

		// Upload with gzip compression
		const writeStream = file.createWriteStream({
			gzip: true,
			metadata: {
				contentType: 'application/x-ndjson',
				contentEncoding: 'gzip'
			}
		});

		return new Promise((resolve, reject) => {
			writeStream.on('error', reject);
			writeStream.on('finish', () => resolve(`gs://${bucket}/${gcsFilePath}`));
			writeStream.end(jsonlContent);
		});

	} else {
		// Write to local filesystem
		const dir = path.dirname(fullPath);
		if (!fs.existsSync(dir)) {
			fs.mkdirSync(dir, { recursive: true });
		}

		// Create JSONL content
		const jsonlContent = data.map(obj => JSON.stringify(obj)).join('\n');

		// Write with gzip compression
		const gzip = createGzip();
		const writeStream = createWriteStream(fullPath);

		await pipeline(
			async function* () {
				yield Buffer.from(jsonlContent);
			},
			gzip,
			writeStream
		);

		return fullPath;
	}
}

/**
 * Check if file exists
 * @param {string} filePath - Relative path
 * @returns {Promise<boolean>}
 */
export async function fileExists(filePath) {
	const basePath = getStoragePath();

	if (isGCS()) {
		const { bucket, prefix } = parseGCSPath(basePath);
		const gcsFilePath = path.join(prefix, filePath);
		const file = gcsClient.bucket(bucket).file(gcsFilePath);

		const [exists] = await file.exists();
		return exists;

	} else {
		const fullPath = path.join(basePath, filePath);
		return fs.existsSync(fullPath);
	}
}

/**
 * Read JSONL.gz file and return array of objects
 * @param {string} filePath - Relative path
 * @returns {Promise<Array<Object>>}
 */
export async function readJSONLGz(filePath) {
	const basePath = getStoragePath();

	if (isGCS()) {
		const { bucket, prefix } = parseGCSPath(basePath);
		const gcsFilePath = path.join(prefix, filePath);
		const file = gcsClient.bucket(bucket).file(gcsFilePath);

		// Download and decompress
		const [content] = await file.download();
		const decompressed = await new Promise((resolve, reject) => {
			const gunzip = createGunzip();
			const chunks = [];

			gunzip.on('data', chunk => chunks.push(chunk));
			gunzip.on('end', () => resolve(Buffer.concat(chunks).toString()));
			gunzip.on('error', reject);

			gunzip.end(content);
		});

		// Parse JSONL
		return decompressed
			.split('\n')
			.filter(line => line.trim())
			.map(line => JSON.parse(line));

	} else {
		const fullPath = path.join(basePath, filePath);

		// Read and decompress
		const decompressed = await new Promise((resolve, reject) => {
			const gunzip = createGunzip();
			const chunks = [];

			pipeline(
				createReadStream(fullPath),
				gunzip
			)
				.then(() => resolve(Buffer.concat(chunks).toString()))
				.catch(reject);

			gunzip.on('data', chunk => chunks.push(chunk));
		});

		// Parse JSONL
		return decompressed
			.split('\n')
			.filter(line => line.trim())
			.map(line => JSON.parse(line));
	}
}

/**
 * Delete file
 * @param {string} filePath - Relative path
 * @returns {Promise<void>}
 */
export async function deleteFile(filePath) {
	const basePath = getStoragePath();

	if (isGCS()) {
		const { bucket, prefix } = parseGCSPath(basePath);
		const gcsFilePath = path.join(prefix, filePath);
		const file = gcsClient.bucket(bucket).file(gcsFilePath);

		await file.delete();
		console.log(`🗑️  Deleted GCS file: gs://${bucket}/${gcsFilePath}`);

	} else {
		const fullPath = path.join(basePath, filePath);
		if (fs.existsSync(fullPath)) {
			fs.unlinkSync(fullPath);
			console.log(`🗑️  Deleted local file: ${fullPath}`);
		}
	}
}

/**
 * Get full path for file (for passing to mixpanel-import)
 * @param {string} filePath - Relative path
 * @returns {string} Full path (gs://... or absolute local path)
 */
export function getFullPath(filePath) {
	const basePath = getStoragePath();

	if (isGCS()) {
		const { bucket, prefix } = parseGCSPath(basePath);
		const gcsFilePath = path.join(prefix, filePath);
		return `gs://${bucket}/${gcsFilePath}`;
	} else {
		return path.resolve(basePath, filePath);
	}
}

/**
 * Clear all files in a directory
 * @param {string} dirPath - Relative directory path
 * @returns {Promise<number>} Number of files deleted
 */
export async function clearDirectory(dirPath) {
	const basePath = getStoragePath();
	let count = 0;

	if (isGCS()) {
		const { bucket, prefix } = parseGCSPath(basePath);
		const gcsDirPath = path.join(prefix, dirPath);
		const [files] = await gcsClient.bucket(bucket).getFiles({ prefix: gcsDirPath });

		for (const file of files) {
			await file.delete();
			count++;
		}

		console.log(`🗑️  Cleared ${count} files from GCS directory: gs://${bucket}/${gcsDirPath}`);

	} else {
		const fullPath = path.join(basePath, dirPath);
		if (fs.existsSync(fullPath)) {
			const files = fs.readdirSync(fullPath);
			for (const file of files) {
				fs.unlinkSync(path.join(fullPath, file));
				count++;
			}
			console.log(`🗑️  Cleared ${count} files from local directory: ${fullPath}`);
		}
	}

	return count;
}

export default {
	getStoragePath,
	isGCS,
	writeJSONLGz,
	fileExists,
	readJSONLGz,
	deleteFile,
	getFullPath,
	clearDirectory
};
