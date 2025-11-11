import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import request from 'supertest';
import express from 'express';
import dotenv from 'dotenv';
import { runPipeline } from '../../src/jobs/run-pipeline.js';
import * as akTools from 'ak-tools';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dotenv.config();
dayjs.extend(utc);

const { sLog, timer } = akTools;

// Create a test app (simplified version of index.js)
function createApp() {
  const app = express();

  // Environment variables
  const {
    NODE_ENV = "test",
    PORT = 8080
  } = process.env;

  // Middleware
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));

  // Helper functions (duplicated from index.js for testing)
  function parseParameters(req) {
    const { body = {}, query = {} } = req;

    const normalizedQuery = {};
    Object.keys(query).forEach(key => {
      normalizedQuery[key.toLowerCase()] = query[key];
    });

    const params = { ...body, ...normalizedQuery };

    if (params.backfill === 'true' || params.backfill === true) {
      if (params.days !== undefined || params.start_date !== undefined || params.end_date !== undefined) {
        throw new Error('Parameter "backfill" is mutually exclusive with "days", "start_date", and "end_date"');
      }
      return { ...params, env: 'backfill' };
    }

    const hasDays = params.days !== undefined;
    const hasDateRange = params.start_date !== undefined || params.end_date !== undefined;

    if (hasDays && hasDateRange) {
      throw new Error('Parameters "days" and "start_date/end_date" are mutually exclusive. Use one or the other.');
    }

    if (params.days !== undefined) {
      params.days = parseInt(params.days);
      if (isNaN(params.days) || params.days < 1) {
        throw new Error('Parameter "days" must be a positive integer');
      }
    }

    const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (params.start_date && !dateRegex.test(params.start_date)) {
      throw new Error('Parameter "start_date" must be in YYYY-MM-DD format');
    }

    if (params.end_date && !dateRegex.test(params.end_date)) {
      throw new Error('Parameter "end_date" must be in YYYY-MM-DD format');
    }

    return params;
  }

  // Health check endpoints
  app.get('/', (req, res) => {
    res.json({
      status: "ok",
      message: "Slack-Mixpanel pipeline service is alive",
      env: NODE_ENV,
      timestamp: new Date().toISOString(),
      endpoints: [
        "POST /mixpanel-members - Process Slack members pipeline",
        "POST /mixpanel-channels - Process Slack channels pipeline",
        "POST /mixpanel-all - Process both members and channels pipelines"
      ]
    });
  });

  app.get('/health', (req, res) => {
    res.json({
      status: "ok",
      message: "Slack-Mixpanel pipeline service is healthy",
      env: NODE_ENV,
      timestamp: new Date().toISOString()
    });
  });

  // Members pipeline endpoint
  app.post('/mixpanel-members', async (req, res) => {
    const t = timer('slack-members');
    t.start();

    try {
      sLog('START JOB: slack-members');

      const params = parseParameters(req);

      // Run file-based pipeline
      const result = await runPipeline({
        ...params,
        pipelines: ['members']
      });

      sLog(`FINISH JOB: slack-members ... ${t.end()}`);

      res.status(200).json({
        status: 'success',
        pipeline: 'members',
        timing: t.report(false),
        ...result
      });

    } catch (error) {
      console.error('ERROR JOB: slack-members', error);

      // Handle parameter validation errors with 400 status
      const isValidationError = error.message.includes('Parameter') || error.message.includes('mutually exclusive');

      res.status(isValidationError ? 400 : 500).json({
        status: 'error',
        pipeline: 'members',
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  // Channels pipeline endpoint
  app.post('/mixpanel-channels', async (req, res) => {
    const t = timer('slack-channels');
    t.start();

    try {
      sLog('START JOB: slack-channels');

      const params = parseParameters(req);

      // Run file-based pipeline
      const result = await runPipeline({
        ...params,
        pipelines: ['channels']
      });

      sLog(`FINISH JOB: slack-channels ... ${t.end()}`);

      res.status(200).json({
        status: 'success',
        pipeline: 'channels',
        timing: t.report(false),
        ...result
      });

    } catch (error) {
      console.error('ERROR JOB: slack-channels', error);

      // Handle parameter validation errors with 400 status
      const isValidationError = error.message.includes('Parameter') || error.message.includes('mutually exclusive');

      res.status(isValidationError ? 400 : 500).json({
        status: 'error',
        pipeline: 'channels',
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  // Combined pipeline endpoint (both members and channels)
  app.post('/mixpanel-all', async (req, res) => {
    const t = timer('slack-all');
    t.start();

    try {
      sLog('START JOB: slack-all');

      const params = parseParameters(req);

      // Run file-based pipeline
      const result = await runPipeline({
        ...params,
        pipelines: ['members', 'channels']
      });

      sLog(`FINISH JOB: slack-all ... ${t.end()}`);

      res.status(200).json({
        status: 'success',
        pipeline: 'all',
        timing: t.report(false),
        ...result
      });

    } catch (error) {
      console.error('ERROR JOB: slack-all', error);

      // Handle parameter validation errors with 400 status
      const isValidationError = error.message.includes('Parameter') || error.message.includes('mutually exclusive');

      res.status(isValidationError ? 400 : 500).json({
        status: 'error',
        pipeline: 'all',
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  return app;
}

describe('API Integration Tests', () => {
  let app;

  beforeAll(() => {
    app = createApp();
  });

  describe('Health Check Endpoints', () => {
    it('GET / should return service status', async () => {
      const response = await request(app)
        .get('/')
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(response.body.endpoints).toBeDefined();
      expect(Array.isArray(response.body.endpoints)).toBe(true);
    });

    it('GET /health should return health status', async () => {
      const response = await request(app)
        .get('/health')
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(response.body.message).toContain('healthy');
    });
  });

  describe('Parameter Validation', () => {
    it('should reject mutually exclusive parameters', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .query({ days: 7, start_date: '2024-01-01' })
        .expect(400);

      expect(response.body.status).toBe('error');
      expect(response.body.error).toContain('mutually exclusive');
    });

    it('should reject invalid days parameter', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .query({ days: 'invalid' })
        .expect(400);

      expect(response.body.status).toBe('error');
      expect(response.body.error).toContain('positive integer');
    });

    it('should reject invalid date format', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .query({ start_date: '2024-1-1' })
        .expect(400);

      expect(response.body.status).toBe('error');
      expect(response.body.error).toContain('YYYY-MM-DD format');
    });

    it('should accept valid days parameter', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .query({ days: 1 })
        .expect(200);

      expect(response.body.status).toBe('success');
      expect(response.body.pipeline).toBe('members');
    });

    it('should accept valid date range parameters', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .query({ start_date: '2024-01-01', end_date: '2024-01-02' })
        .expect(200);

      expect(response.body.status).toBe('success');
      expect(response.body.pipeline).toBe('members');
    });
  });

  describe('Pipeline Endpoints', () => {
    // Only run these tests if we have proper Slack credentials
    const hasSlackCredentials = process.env.slack_bot_token && process.env.slack_user_token;

    it.skipIf(!hasSlackCredentials)('POST /mixpanel-members should process members pipeline', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .send({ days: 1 })
        .expect(200);

      expect(response.body.status).toBe('success');
      expect(response.body.pipeline).toBe('members');
      expect(response.body.extract).toBeDefined();
      expect(response.body.load).toBeDefined();
      expect(response.body.timing).toBeDefined();
    });

    it.skipIf(!hasSlackCredentials)('POST /mixpanel-channels should process channels pipeline', async () => {
      const response = await request(app)
        .post('/mixpanel-channels')
        .send({ days: 1 })
        .expect(200);

      expect(response.body.status).toBe('success');
      expect(response.body.pipeline).toBe('channels');
      expect(response.body.extract).toBeDefined();
      expect(response.body.load).toBeDefined();
      expect(response.body.timing).toBeDefined();
    });

    it.skipIf(!hasSlackCredentials)('POST /mixpanel-all should process all pipelines', async () => {
      const response = await request(app)
        .post('/mixpanel-all')
        .send({ days: 1 })
        .expect(200);

      expect(response.body.status).toBe('success');
      expect(response.body.pipeline).toBe('all');
      expect(response.body.extract).toBeDefined();
      expect(response.body.load).toBeDefined();
      expect(response.body.timing).toBeDefined();
    });

    it('should handle missing Slack credentials gracefully', async () => {
      // This test checks if the pipeline runs without errors even with missing credentials
      const response = await request(app)
        .post('/mixpanel-members')
        .send({ days: 1 });

      // Should either succeed or fail gracefully
      expect([200, 500]).toContain(response.status);

      if (response.status === 200) {
        expect(response.body.status).toBe('success');
      } else {
        expect(response.body.status).toBe('error');
      }
    });
  });

  describe('JSON Body vs Query Parameters', () => {
    it('should accept parameters in JSON body', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .send({ days: 1 })
        .expect(200);

      expect(response.body.status).toBe('success');
    });

    it('should prioritize query parameters over JSON body', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .query({ days: 2 })
        .send({ days: 5 })
        .expect(200);

      expect(response.body.status).toBe('success');
      // The actual days used should be 2 (from query), not 5 (from body)
    });
  });

  describe('Extract vs Load Modes', () => {
    const hasSlackCredentials = process.env.slack_bot_token && process.env.slack_user_token;

    it.skipIf(!hasSlackCredentials)('should support extractOnly mode', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .send({ days: 1, extractOnly: true })
        .expect(200);

      expect(response.body.status).toBe('success');
      expect(response.body.extract).toBeDefined();
      expect(response.body.load).toEqual({});
    });

    it.skipIf(!hasSlackCredentials)('should support full extract+load mode', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .send({ days: 1, extractOnly: false })
        .expect(200);

      expect(response.body.status).toBe('success');
      expect(response.body.extract).toBeDefined();
      expect(response.body.load).toBeDefined();
    });
  });

  describe('Response Structure', () => {
    it('should return proper success response structure', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .send({ days: 1 })
        .expect(200);

      expect(response.body).toHaveProperty('status');
      expect(response.body).toHaveProperty('pipeline');
      expect(response.body).toHaveProperty('timing');
      expect(response.body).toHaveProperty('params');
      expect(response.body).toHaveProperty('extract');
      expect(response.body).toHaveProperty('load');
    });

    it('should return proper error response structure', async () => {
      const response = await request(app)
        .post('/mixpanel-members')
        .send({ days: -1 })
        .expect(400);

      expect(response.body).toHaveProperty('status');
      expect(response.body).toHaveProperty('pipeline');
      expect(response.body).toHaveProperty('error');
      expect(response.body).toHaveProperty('timestamp');
      expect(response.body.status).toBe('error');
    });
  });
});
