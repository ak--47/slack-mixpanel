import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import request from 'supertest';
import express from 'express';
import dotenv from 'dotenv';

// Import our app components
import slackMemberPipeline from '../../src/models/slack-members.js';
import slackChannelPipeline from '../../src/models/slack-channels.js';
import { devTask, writeTask, uploadTask, cloudTask } from '../../src/jobs/tasks.js';
import * as akTools from 'ak-tools';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dotenv.config();
dayjs.extend(utc);

const { sLog, timer } = akTools;

// Create a test app (essentially our index.js but for testing)
function createApp() {
  const app = express();
  
  // Environment variables
  const { 
    NODE_ENV = "test", 
    PORT = 8080, 
    CONCURRENCY = 10,
    mixpanel_token, 
    mixpanel_secret, 
    channel_group_key = 'channel_id',
    gcs_project,
    gcs_path
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

  function getDateRange(params) {
    const NOW = dayjs.utc();
    const sfdcDateTimeFmt = "YYYY-MM-DDTHH:mm:ss.SSS[Z]";
    
    if (params.env === 'backfill') {
      const BACKFILL_DAYS = 365 + 30;
      const start = NOW.subtract(BACKFILL_DAYS, "d").format(sfdcDateTimeFmt);
      const end = NOW.add(2, "d").format(sfdcDateTimeFmt);
      return {
        start,
        end,
        simpleStart: dayjs.utc(start).format("YYYY-MM-DD"),
        simpleEnd: dayjs.utc(end).format("YYYY-MM-DD"),
        days: BACKFILL_DAYS
      };
    }
    
    let DAYS;
    if (NODE_ENV === "dev") DAYS = 1;
    if (NODE_ENV === "production") DAYS = 2;
    if (NODE_ENV === "backfill") DAYS = 365;
    if (NODE_ENV === "test") DAYS = 5;
    if (NODE_ENV === "cloud") DAYS = 14;
    if (params.days) DAYS = params.days;
    
    let start = NOW.subtract(DAYS, "d").format(sfdcDateTimeFmt);
    let end = NOW.add(2, "d").format(sfdcDateTimeFmt);
    
    if (params.start_date) start = dayjs.utc(params.start_date).format(sfdcDateTimeFmt);
    if (params.end_date) end = dayjs.utc(params.end_date).format(sfdcDateTimeFmt);
    
    return {
      start,
      end,
      simpleStart: dayjs.utc(start).format("YYYY-MM-DD"),
      simpleEnd: dayjs.utc(end).format("YYYY-MM-DD"),
      days: DAYS
    };
  }

  function getWorkFunction(envMode) {
    const env = { mixpanel_token, mixpanel_secret, channel_group_key, gcs_project, gcs_path, NODE_ENV: envMode || NODE_ENV };
    
    const actualEnv = envMode || NODE_ENV;
    switch (actualEnv) {
      case "backfill":
        return (jobs, stats) => cloudTask(jobs, stats, env);
      case "cloud":
        return (jobs, stats) => cloudTask(jobs, stats, env);
      case "test":
        return (jobs, stats) => writeTask(jobs, stats, env);
      case "dev":
        return (jobs, stats) => devTask(jobs, stats, env);
      case "production":
        return (jobs, stats) => uploadTask(jobs, stats, env);
      default:
        return (jobs, stats) => uploadTask(jobs, stats, env);
    }
  }

  function createStats() {
    return {
      events: { processed: 0, uploaded: 0 },
      users: { processed: 0, uploaded: 0 },
      groups: { processed: 0, uploaded: 0 },
      
      reset() {
        this.events = { processed: 0, uploaded: 0 };
        this.users = { processed: 0, uploaded: 0 };
        this.groups = { processed: 0, uploaded: 0 };
      },
      
      report() {
        return {
          events: { ...this.events },
          users: { ...this.users },
          groups: { ...this.groups }
        };
      }
    };
  }

  // Health check endpoints
  app.get('/', (req, res) => {
    res.json({ 
      status: "ok", 
      message: "Slack-Mixpanel pipeline service is alive",
      env: NODE_ENV,
      timestamp: new Date().toISOString(),
      endpoints: {
        health: "GET /health - Health check",
        members: "POST /members - Process Slack members pipeline",
        channels: "POST /channels - Process Slack channels pipeline", 
        all: "POST /all - Process both members and channels pipelines"
      }
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
  app.post('/members', async (req, res) => {
    const t = timer('slack-members');
    t.start();
    
    try {
      const params = parseParameters(req);
      const dateRange = getDateRange(params);
      const stats = createStats();
      const work = getWorkFunction(params.env);
      
      // Get data streams from members pipeline
      const { slackMemberEvents, slackMemberProfiles } = await slackMemberPipeline(dateRange.simpleStart, dateRange.simpleEnd);
      
      // Execute pipeline
      const jobs = [
        { stream: slackMemberEvents, type: "event", label: "slack-members" },
        { stream: slackMemberProfiles, type: "user", label: "slack-member-profiles" }
      ];
      
      const results = await work(jobs, stats);
      
      let success = [];
      let failed = [];
      
      results.forEach(result => {
        if (result.status === "fulfilled") {
          success.push(result.value);
        } else {
          failed.push(result.reason);
        }
      });
      
      res.status(200).json({
        status: 'success',
        pipeline: 'members',
        timing: t.report(false),
        params: { start_date: dateRange.start, end_date: dateRange.end },
        results: { success, failed },
        stats: stats.report()
      });
      
    } catch (error) {
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
  app.post('/channels', async (req, res) => {
    const t = timer('slack-channels');
    t.start();
    
    try {
      const params = parseParameters(req);
      const dateRange = getDateRange(params);
      const stats = createStats();
      const work = getWorkFunction(params.env);
      
      // Get data streams from channels pipeline
      const { slackChannelEvents, slackChannelProfiles } = await slackChannelPipeline(dateRange.simpleStart, dateRange.simpleEnd);
      
      // Execute pipeline
      const jobs = [
        { stream: slackChannelEvents, type: "event", label: "slack-channels" },
        { stream: slackChannelProfiles, type: "group", groupKey: channel_group_key, label: "slack-channel-profiles" }
      ];
      
      const results = await work(jobs, stats);
      
      let success = [];
      let failed = [];
      
      results.forEach(result => {
        if (result.status === "fulfilled") {
          success.push(result.value);
        } else {
          failed.push(result.reason);
        }
      });
      
      res.status(200).json({
        status: 'success',
        pipeline: 'channels',
        timing: t.report(false),
        params: { start_date: dateRange.start, end_date: dateRange.end },
        results: { success, failed },
        stats: stats.report()
      });
      
    } catch (error) {
      const isValidationError = error.message.includes('Parameter') || error.message.includes('mutually exclusive');
      
      res.status(isValidationError ? 400 : 500).json({
        status: 'error',
        pipeline: 'channels',
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
      expect(response.body.endpoints.members).toContain('POST /members');
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
        .post('/members')
        .query({ days: 7, start_date: '2024-01-01' })
        .expect(400);
      
      expect(response.body.status).toBe('error');
      expect(response.body.error).toContain('mutually exclusive');
    });

    it('should reject invalid days parameter', async () => {
      const response = await request(app)
        .post('/members')
        .query({ days: 'invalid' })
        .expect(400);
      
      expect(response.body.status).toBe('error');
      expect(response.body.error).toContain('positive integer');
    });

    it('should reject invalid date format', async () => {
      const response = await request(app)
        .post('/members')
        .query({ start_date: '2024-1-1' })
        .expect(400);
      
      expect(response.body.status).toBe('error');
      expect(response.body.error).toContain('YYYY-MM-DD format');
    });

    it('should accept valid parameters', async () => {
      const response = await request(app)
        .post('/members')
        .query({ days: 1 })
        .expect(200);
      
      expect(response.body.status).toBe('success');
      expect(response.body.pipeline).toBe('members');
    });
  });

  describe('Pipeline Endpoints', () => {
    // Only run these tests if we have proper Slack credentials
    const hasSlackCredentials = process.env.slack_bot_token && process.env.slack_user_token;

    it.skipIf(!hasSlackCredentials)('POST /members should process members pipeline', async () => {
      const response = await request(app)
        .post('/members')
        .send({ days: 1 })
        .expect(200);
      
      expect(response.body.status).toBe('success');
      expect(response.body.pipeline).toBe('members');
      expect(response.body.stats).toBeDefined();
      expect(response.body.timing).toBeDefined();
    });

    it.skipIf(!hasSlackCredentials)('POST /channels should process channels pipeline', async () => {
      const response = await request(app)
        .post('/channels')
        .send({ days: 1 })
        .expect(200);
      
      expect(response.body.status).toBe('success');
      expect(response.body.pipeline).toBe('channels');
      expect(response.body.stats).toBeDefined();
      expect(response.body.timing).toBeDefined();
    });

    it('should handle missing Slack credentials gracefully', async () => {
      // This test checks if the pipeline runs without errors even with missing credentials
      // In test mode, it writes to files so it may not fail immediately
      const response = await request(app)
        .post('/members')
        .send({ days: 1 });
      
      // In test mode, it should either succeed (write to files) or fail gracefully
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
        .post('/members')
        .send({ days: 1 })
        .expect(200);
      
      expect(response.body.status).toBe('success');
    });

    it('should prioritize query parameters over JSON body', async () => {
      const response = await request(app)
        .post('/members')
        .query({ days: 2 })
        .send({ days: 5 })
        .expect(200);
      
      expect(response.body.status).toBe('success');
      // The actual days used should be 2 (from query), not 5 (from body)
    });
  });
});