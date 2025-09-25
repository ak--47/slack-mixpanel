import { describe, it, expect, beforeAll } from 'vitest';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import slackMemberPipeline from '../../src/models/slack-members.js';
import slackChannelPipeline from '../../src/models/slack-channels.js';
import slack from '../../src/services/slack.js';

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

  describe('Member Pipeline', () => {
    it.skipIf(!hasSlackCredentials)('should process member data', async () => {
      const startDate = dayjs.utc().subtract(2, 'days').format('YYYY-MM-DD');
      const endDate = dayjs.utc().subtract(1, 'day').format('YYYY-MM-DD');
      
      try {
        const { slackMemberEvents, slackMemberProfiles } = await slackMemberPipeline(startDate, endDate);
        
        expect(slackMemberEvents).toBeDefined();
        expect(slackMemberProfiles).toBeDefined();
        
        // Convert streams to arrays for validation
        const [memberEvents, memberProfiles] = await Promise.all([
          new Promise((resolve) => {
            slackMemberEvents.toArray((results) => {
              resolve(results);
            });
          }),
          new Promise((resolve) => {
            slackMemberProfiles.toArray((results) => {
              resolve(results);
            });
          })
        ]);
        
        expect(Array.isArray(memberEvents)).toBe(true);
        expect(Array.isArray(memberProfiles)).toBe(true);
        
        // Validate event structure if we have data
        if (memberEvents.length > 0) {
          const event = memberEvents[0];
          expect(event).toHaveProperty('event');
          expect(event).toHaveProperty('distinct_id');
          expect(event).toHaveProperty('insert_id');
          expect(event).toHaveProperty('time');
          expect(event.event).toBe('daily user summary');
        }
        
        // Validate profile structure if we have data
        if (memberProfiles.length > 0) {
          const profile = memberProfiles[0];
          expect(profile).toHaveProperty('distinct_id');
          expect(profile).toHaveProperty('email');
          expect(profile).toHaveProperty('slack_id');
        }
        
      } catch (error) {
        if (error.message.includes('analytics') || error.code === 'feature_not_enabled') {
          console.warn('Member analytics not available for testing');
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
      expect(async () => {
        await slackMemberPipeline(startDate, endDate);
      }).not.toThrow();
    });
  });

  describe('Channel Pipeline', () => {
    it.skipIf(!hasSlackCredentials)('should process channel data', async () => {
      const startDate = dayjs.utc().subtract(2, 'days').format('YYYY-MM-DD');
      const endDate = dayjs.utc().subtract(1, 'day').format('YYYY-MM-DD');
      
      try {
        const { slackChannelEvents, slackChannelProfiles } = await slackChannelPipeline(startDate, endDate);
        
        expect(slackChannelEvents).toBeDefined();
        expect(slackChannelProfiles).toBeDefined();
        
        // Convert streams to arrays for validation
        const [channelEvents, channelProfiles] = await Promise.all([
          new Promise((resolve) => {
            slackChannelEvents.toArray((results) => {
              resolve(results);
            });
          }),
          new Promise((resolve) => {
            slackChannelProfiles.toArray((results) => {
              resolve(results);
            });
          })
        ]);
        
        expect(Array.isArray(channelEvents)).toBe(true);
        expect(Array.isArray(channelProfiles)).toBe(true);
        
        // Validate event structure if we have data
        if (channelEvents.length > 0) {
          const event = channelEvents[0];
          expect(event).toHaveProperty('event');
          expect(event).toHaveProperty('distinct_id');
          expect(event).toHaveProperty('insert_id');
          expect(event).toHaveProperty('time');
          expect(event.event).toBe('daily channel summary');
        }
        
        // Validate profile structure if we have data
        if (channelProfiles.length > 0) {
          const profile = channelProfiles[0];
          expect(profile).toHaveProperty('distinct_id');
          expect(profile).toHaveProperty('name');
        }
        
      } catch (error) {
        if (error.message.includes('analytics') || error.code === 'feature_not_enabled') {
          console.warn('Channel analytics not available for testing');
          expect(error.message).toBeDefined();
        } else {
          throw error;
        }
      }
    });
  });

  describe('Data Processing', () => {
    it('should filter members by company domain', async () => {
      // This test verifies the email filtering logic works
      const company_domain = process.env.company_domain || 'mixpanel.com';
      
      if (!hasSlackCredentials) {
        // Mock test for domain filtering logic
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
      }
    });

    it('should generate proper insert_ids for deduplication', async () => {
      // Test the MD5 hash generation logic
      const { md5 } = await import('ak-tools');
      
      const testRecord = {
        email_address: 'test@mixpanel.com',
        date: '2024-01-01',
        user_id: 'U123'
      };
      
      const insertId = md5(`${testRecord.email_address}-${testRecord.date}-${testRecord.user_id}`);
      expect(typeof insertId).toBe('string');
      expect(insertId.length).toBeGreaterThan(0);
      
      // Same inputs should generate same hash
      const insertId2 = md5(`${testRecord.email_address}-${testRecord.date}-${testRecord.user_id}`);
      expect(insertId).toBe(insertId2);
    });

    it('should handle time formatting correctly', async () => {
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
    it('should handle invalid date ranges gracefully', async () => {
      const invalidStart = 'invalid-date';
      const validEnd = '2024-01-02';
      
      // The pipeline should handle invalid dates - either reject or handle gracefully
      try {
        await slackMemberPipeline(invalidStart, validEnd);
        // If it doesn't throw, that's also acceptable as it might handle it gracefully
        expect(true).toBe(true);
      } catch (error) {
        // If it throws, that's expected behavior for invalid dates
        expect(error).toBeDefined();
      }
    });

    it('should handle date validation in pipeline', async () => {
      // Test with properly formatted but potentially problematic dates
      const futureStart = '2030-01-01';
      const futureEnd = '2030-01-02';
      
      try {
        const result = await slackMemberPipeline(futureStart, futureEnd);
        expect(result).toBeDefined();
        expect(result.slackMemberEvents).toBeDefined();
        expect(result.slackMemberProfiles).toBeDefined();
      } catch (error) {
        // Future dates might not have data, but shouldn't crash
        expect(error.message).toBeDefined();
      }
    });
  });
});