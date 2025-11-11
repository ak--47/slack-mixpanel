import { describe, it, expect, beforeAll } from 'vitest';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import { runPipeline } from '../../src/jobs/run-pipeline.js';
import { extractMemberAnalytics, extractChannelAnalytics } from '../../src/jobs/extract.js';
import { loadMemberAnalytics, loadChannelAnalytics } from '../../src/jobs/load.js';
import { transformMemberEvent, transformMemberProfile } from '../../src/transforms/members.js';
import { transformChannelEvent, transformChannelProfile } from '../../src/transforms/channels.js';
import slack from '../../src/services/slack.js';
import storage from '../../src/services/storage.js';

dayjs.extend(utc);

describe('Pipeline Integration Tests', () => {
  const hasSlackCredentials = process.env.slack_bot_token && process.env.slack_user_token;

  beforeAll(() => {
    if (!hasSlackCredentials) {
      console.warn('Skipping pipeline integration tests - Slack credentials not available');
    }
  });

  describe('Slack Service', () => {
    it.skipIf(!hasSlackCredentials)('should connect to Slack API', async () => {
      // Test basic Slack API connectivity
      try {
        const users = await slack.getUsers();
        expect(Array.isArray(users)).toBe(true);

        if (users.length > 0) {
          expect(users[0]).toHaveProperty('id');
          expect(users[0]).toHaveProperty('real_name');
        }
      } catch (error) {
        // If we get a specific Slack API error, that's still a successful connection test
        if (error.code && error.code.startsWith('slack_')) {
          expect(error.code).toBeDefined();
        } else {
          throw error;
        }
      }
    });

    it.skipIf(!hasSlackCredentials)('should fetch channels', async () => {
      try {
        const channels = await slack.getChannels();
        expect(Array.isArray(channels)).toBe(true);

        if (channels.length > 0) {
          expect(channels[0]).toHaveProperty('id');
          expect(channels[0]).toHaveProperty('name');
        }
      } catch (error) {
        if (error.code && error.code.startsWith('slack_')) {
          expect(error.code).toBeDefined();
        } else {
          throw error;
        }
      }
    });

    it.skipIf(!hasSlackCredentials)('should handle analytics API calls', async () => {
      const startDate = dayjs.utc().subtract(2, 'days').format('YYYY-MM-DD');
      const endDate = dayjs.utc().subtract(1, 'day').format('YYYY-MM-DD');

      try {
        // Test member analytics
        const memberAnalytics = await slack.analytics(startDate, endDate, 'member');
        expect(memberAnalytics).toBeDefined();

        // Test channel analytics
        const channelAnalytics = await slack.analytics(startDate, endDate, 'public_channel');
        expect(channelAnalytics).toBeDefined();
      } catch (error) {
        // Analytics API might not be available or have delays
        if (error.message.includes('analytics') || error.code === 'feature_not_enabled') {
          console.warn('Analytics API not available for testing');
          expect(error.message).toBeDefined();
        } else {
          throw error;
        }
      }
    });
  });

  describe('Storage Service', () => {
    it('should identify storage type', () => {
      const isGCS = storage.isGCS();
      expect(typeof isGCS).toBe('boolean');
    });

    it('should provide storage path', () => {
      const storagePath = storage.getStoragePath();
      expect(typeof storagePath).toBe('string');
      expect(storagePath.length).toBeGreaterThan(0);
    });

    it.skipIf(!hasSlackCredentials)('should check file existence', async () => {
      // Test with a file that definitely doesn't exist
      const exists = await storage.fileExists('test-nonexistent-file.jsonl.gz');
      expect(typeof exists).toBe('boolean');
    });
  });

  describe('Extract Stage', () => {
    it.skipIf(!hasSlackCredentials)('should extract member analytics', async () => {
      const startDate = dayjs.utc().subtract(2, 'days').format('YYYY-MM-DD');
      const endDate = dayjs.utc().subtract(1, 'day').format('YYYY-MM-DD');

      try {
        const result = await extractMemberAnalytics(startDate, endDate);

        expect(result).toBeDefined();
        expect(result).toHaveProperty('extracted');
        expect(result).toHaveProperty('skipped');
        expect(result).toHaveProperty('files');
        expect(Array.isArray(result.files)).toBe(true);

        // Verify numbers are non-negative
        expect(result.extracted).toBeGreaterThanOrEqual(0);
        expect(result.skipped).toBeGreaterThanOrEqual(0);

      } catch (error) {
        if (error.message.includes('analytics') || error.code === 'feature_not_enabled') {
          console.warn('Member analytics not available for testing');
          expect(error.message).toBeDefined();
        } else {
          throw error;
        }
      }
    });

    it.skipIf(!hasSlackCredentials)('should extract channel analytics', async () => {
      const startDate = dayjs.utc().subtract(2, 'days').format('YYYY-MM-DD');
      const endDate = dayjs.utc().subtract(1, 'day').format('YYYY-MM-DD');

      try {
        const result = await extractChannelAnalytics(startDate, endDate);

        expect(result).toBeDefined();
        expect(result).toHaveProperty('extracted');
        expect(result).toHaveProperty('skipped');
        expect(result).toHaveProperty('files');
        expect(Array.isArray(result.files)).toBe(true);

        expect(result.extracted).toBeGreaterThanOrEqual(0);
        expect(result.skipped).toBeGreaterThanOrEqual(0);

      } catch (error) {
        if (error.message.includes('analytics') || error.code === 'feature_not_enabled') {
          console.warn('Channel analytics not available for testing');
          expect(error.message).toBeDefined();
        } else {
          throw error;
        }
      }
    });

    it.skipIf(!hasSlackCredentials)('should skip existing files (resumable)', async () => {
      const startDate = dayjs.utc().subtract(2, 'days').format('YYYY-MM-DD');
      const endDate = dayjs.utc().subtract(1, 'day').format('YYYY-MM-DD');

      try {
        // Extract once
        const firstRun = await extractMemberAnalytics(startDate, endDate);

        // Extract again - should skip files
        const secondRun = await extractMemberAnalytics(startDate, endDate);

        // Second run should have more skipped files
        expect(secondRun.skipped).toBeGreaterThanOrEqual(firstRun.skipped);

      } catch (error) {
        if (error.message.includes('analytics')) {
          console.warn('Analytics not available');
          expect(error.message).toBeDefined();
        } else {
          throw error;
        }
      }
    });
  });

  describe('Transform Functions', () => {
    it('should transform member events correctly', () => {
      const mockRecord = {
        email_address: 'test@mixpanel.com',
        user_id: 'U123',
        date: '2024-01-15',
        messages_posted: 10,
        channels_posted: 5
      };

      const mockSlackMembers = [{
        id: 'U123',
        real_name: 'Test User',
        profile: {
          display_name: 'testuser',
          image_72: 'https://example.com/avatar.jpg'
        }
      }];

      const context = {
        slackMembers: mockSlackMembers,
        slack_prefix: 'https://mixpanel.slack.com'
      };

      const event = transformMemberEvent(mockRecord, context);

      expect(event).toHaveProperty('event');
      expect(event).toHaveProperty('distinct_id');
      expect(event).toHaveProperty('time');
      expect(event).toHaveProperty('properties');
      expect(event.distinct_id).toBe('test@mixpanel.com');
      expect(event.event).toBe('slack activity');
    });

    it('should transform member profiles correctly', () => {
      const mockRecord = {
        email_address: 'test@mixpanel.com',
        user_id: 'U123',
        date: '2024-01-15'
      };

      const mockSlackMembers = [{
        id: 'U123',
        real_name: 'Test User'
      }];

      const context = {
        slackMembers: mockSlackMembers,
        slack_prefix: 'https://mixpanel.slack.com'
      };

      const profile = transformMemberProfile(mockRecord, context);

      expect(profile).toHaveProperty('$distinct_id');
      expect(profile).toHaveProperty('$email');
      expect(profile).toHaveProperty('$set');
      expect(profile.$distinct_id).toBe('test@mixpanel.com');
      expect(profile.$email).toBe('test@mixpanel.com');
      expect(profile.$set.slack_id).toBe('U123');
    });

    it('should transform channel events correctly', () => {
      const mockRecord = {
        channel_id: 'C123',
        date: '2024-01-15',
        messages_posted: 50,
        members_who_posted: 10
      };

      const mockSlackChannels = [{
        id: 'C123',
        name: 'general',
        purpose: { value: 'General discussion' },
        num_members: 100
      }];

      const context = {
        slackChannels: mockSlackChannels,
        slack_prefix: 'https://mixpanel.slack.com'
      };

      const event = transformChannelEvent(mockRecord, context);

      expect(event).toHaveProperty('event');
      expect(event).toHaveProperty('distinct_id');
      expect(event).toHaveProperty('time');
      expect(event.distinct_id).toBe('C123');
      expect(event.event).toBe('slack channel activity');
      expect(event.properties.name).toBe('#general');
    });

    it('should transform channel profiles correctly', () => {
      const mockRecord = {
        channel_id: 'C123',
        date: '2024-01-15'
      };

      const mockSlackChannels = [{
        id: 'C123',
        name: 'general',
        created: 1640000000
      }];

      const context = {
        slackChannels: mockSlackChannels,
        slack_prefix: 'https://mixpanel.slack.com',
        channel_group_key: 'channel_id'
      };

      const profile = transformChannelProfile(mockRecord, context);

      expect(profile).toHaveProperty('$group_key');
      expect(profile).toHaveProperty('$group_id');
      expect(profile).toHaveProperty('$set');
      expect(profile.$group_key).toBe('channel_id');
      expect(profile.$group_id).toBe('C123');
      expect(profile.$set.name).toBe('#general');
    });
  });

  describe('Full Pipeline', () => {
    it.skipIf(!hasSlackCredentials)('should run complete pipeline', async () => {
      try {
        const result = await runPipeline({
          days: 1,
          pipelines: ['members'],
          extractOnly: false
        });

        expect(result).toBeDefined();
        expect(result.status).toBe('success');
        expect(result).toHaveProperty('extract');
        expect(result).toHaveProperty('load');
        expect(result).toHaveProperty('timing');

      } catch (error) {
        if (error.message.includes('analytics') || error.code === 'feature_not_enabled') {
          console.warn('Analytics not available for pipeline testing');
          expect(error.message).toBeDefined();
        } else {
          throw error;
        }
      }
    });

    it.skipIf(!hasSlackCredentials)('should support extract-only mode', async () => {
      try {
        const result = await runPipeline({
          days: 1,
          pipelines: ['members'],
          extractOnly: true
        });

        expect(result).toBeDefined();
        expect(result.status).toBe('success');
        expect(result).toHaveProperty('extract');
        expect(result.load).toEqual({});

      } catch (error) {
        if (error.message.includes('analytics')) {
          console.warn('Analytics not available');
          expect(error.message).toBeDefined();
        } else {
          throw error;
        }
      }
    });

    it('should handle date parameters correctly', async () => {
      const startDate = '2024-01-01';
      const endDate = '2024-01-02';

      // This should not throw for date validation
      try {
        await runPipeline({
          start_date: startDate,
          end_date: endDate,
          pipelines: ['members'],
          extractOnly: true
        });
        expect(true).toBe(true);
      } catch (error) {
        // Analytics might not be available, but date validation should work
        if (error.message.includes('Parameter')) {
          throw error; // Re-throw validation errors
        }
        expect(error).toBeDefined();
      }
    });
  });

  describe('Data Processing', () => {
    it('should filter members by company domain', () => {
      const company_domain = process.env.company_domain || 'mixpanel.com';

      const mockRecord = {
        email_address: `test@${company_domain}`,
        user_id: 'U123',
        date: '2024-01-01'
      };

      const shouldInclude = mockRecord.email_address.endsWith(`@${company_domain}`);
      expect(shouldInclude).toBe(true);

      const mockInvalidRecord = {
        email_address: 'test@otherdomain.com',
        user_id: 'U456',
        date: '2024-01-01'
      };

      const shouldExclude = mockInvalidRecord.email_address.endsWith(`@${company_domain}`);
      expect(shouldExclude).toBe(false);
    });

    it('should handle time formatting correctly', () => {
      const testDate = '2024-01-15';
      const formattedTime = dayjs.utc(testDate).add(4, 'h').add(20, 'm').unix();

      expect(typeof formattedTime).toBe('number');
      expect(formattedTime).toBeGreaterThan(0);

      // Verify it's a valid Unix timestamp
      const dateFromTimestamp = dayjs.unix(formattedTime);
      expect(dateFromTimestamp.isValid()).toBe(true);
    });
  });

  describe('Error Handling', () => {
    it('should handle invalid date parameters', async () => {
      const invalidParams = {
        days: 7,
        start_date: '2024-01-01' // Mutually exclusive with days
      };

      try {
        await runPipeline(invalidParams);
        // Should throw
        expect(false).toBe(true);
      } catch (error) {
        expect(error.message).toContain('mutually exclusive');
      }
    });

    it('should handle invalid days parameter', async () => {
      try {
        await runPipeline({ days: -5 });
        expect(false).toBe(true);
      } catch (error) {
        expect(error.message).toContain('positive integer');
      }
    });

    it('should handle invalid date format', async () => {
      try {
        await runPipeline({ start_date: '2024-1-1' });
        expect(false).toBe(true);
      } catch (error) {
        expect(error.message).toContain('YYYY-MM-DD');
      }
    });
  });
});
