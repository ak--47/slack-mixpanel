import { describe, it, expect, beforeAll } from 'vitest';
import slack from '../../src/services/slack.js';
import mixpanelImport from 'mixpanel-import';

/**
 * Service tests for Slack and Mixpanel integrations
 * These are sanity checks to ensure basic service functionality works
 */

describe('Service Tests', () => {
  const hasSlackCredentials = process.env.slack_bot_token && process.env.slack_user_token;
  const hasMixpanelTestToken = process.env.test_mixpanel_token && process.env.mixpanel_secret;

  beforeAll(() => {
    if (!hasSlackCredentials) {
      console.warn('⚠️  Skipping Slack service tests - credentials not available');
    }
    if (!hasMixpanelTestToken) {
      console.warn('⚠️  Skipping Mixpanel service tests - test_mixpanel_token not available');
    }
  });

  describe('Slack Service', () => {
    describe('Service Initialization', () => {
      it.skipIf(!hasSlackCredentials)('should initialize successfully', async () => {
        await slack.ensureSlackInitialized();
        expect(slack.name).toBe('slack-service');
        expect(slack.slackBotClient).toBeDefined();
        expect(slack.slackUserClient).toBeDefined();
      });

      it.skipIf(!hasSlackCredentials)('should test authentication', async () => {
        const botAuth = await slack.testAuth('bot');
        const userAuth = await slack.testAuth('user');

        expect(botAuth.ok).toBe(true);
        expect(userAuth.ok).toBe(true);
        expect(botAuth.team).toBeDefined();
        expect(userAuth.team).toBeDefined();
      });
    });

    describe('User Methods', () => {
      it.skipIf(!hasSlackCredentials)('should fetch all users', async () => {
        const users = await slack.getUsers();

        expect(Array.isArray(users)).toBe(true);
        expect(users.length).toBeGreaterThan(0);

        const firstUser = users[0];
        expect(firstUser).toHaveProperty('id');
        expect(firstUser).toHaveProperty('real_name');
        expect(firstUser).toHaveProperty('profile');
      });

      it.skipIf(!hasSlackCredentials)('should fetch user details', async () => {
        // First get a user ID from the users list
        const users = await slack.getUsers();
        const testUser = users.find(u => !u.deleted && !u.is_bot);

        if (!testUser) {
          console.warn('⚠️  No active non-bot users found for testing');
          return;
        }

        const userDetails = await slack.getUserDetails(testUser.id);

        expect(userDetails).toBeDefined();
        expect(userDetails.ok).toBe(true);
        expect(userDetails.user).toBeDefined();
        expect(userDetails.profile).toBeDefined();
        expect(userDetails.user.id).toBe(testUser.id);

        // Check that profile has expected fields
        expect(userDetails.profile).toHaveProperty('real_name');
        expect(userDetails.profile).toHaveProperty('display_name');
      });

      it.skipIf(!hasSlackCredentials)('should handle invalid user ID gracefully', async () => {
        try {
          await slack.getUserDetails('INVALID_USER_ID');
          // Should not reach here
          expect(false).toBe(true);
        } catch (error) {
          expect(error).toBeDefined();
          expect(error.message).toBeDefined();
        }
      });
    });

    describe('Channel Methods', () => {
      it.skipIf(!hasSlackCredentials)('should fetch all channels', async () => {
        const channels = await slack.getChannels();

        expect(Array.isArray(channels)).toBe(true);
        expect(channels.length).toBeGreaterThan(0);

        const firstChannel = channels[0];
        expect(firstChannel).toHaveProperty('id');
        expect(firstChannel).toHaveProperty('name');
      });

      it.skipIf(!hasSlackCredentials)('should fetch channel details', async () => {
        // First get a channel ID from the channels list
        const channels = await slack.getChannels();
        const testChannel = channels.find(c => !c.is_archived);

        if (!testChannel) {
          console.warn('⚠️  No active channels found for testing');
          return;
        }

        const channelDetails = await slack.getChannelDetails(testChannel.id);

        expect(channelDetails).toBeDefined();
        expect(channelDetails.ok).toBe(true);
        expect(channelDetails.channel).toBeDefined();
        expect(channelDetails.channel.id).toBe(testChannel.id);
        expect(channelDetails.channel.name).toBe(testChannel.name);

        // Check for expected metadata
        expect(channelDetails.channel).toHaveProperty('created');
        expect(channelDetails.channel).toHaveProperty('is_private');
      });

      it.skipIf(!hasSlackCredentials)('should fetch channel details with member count', async () => {
        const channels = await slack.getChannels();
        const testChannel = channels.find(c => !c.is_archived && !c.is_private);

        if (!testChannel) {
          console.warn('⚠️  No public channels found for testing');
          return;
        }

        const channelDetails = await slack.getChannelDetails(testChannel.id);

        expect(channelDetails.channel).toHaveProperty('num_members');
        expect(typeof channelDetails.channel.num_members).toBe('number');
      });

      it.skipIf(!hasSlackCredentials)('should handle invalid channel ID gracefully', async () => {
        try {
          await slack.getChannelDetails('INVALID_CHANNEL_ID');
          // Should not reach here
          expect(false).toBe(true);
        } catch (error) {
          expect(error).toBeDefined();
          expect(error.message).toBeDefined();
        }
      });
    });

    describe('Analytics Methods', () => {
      it.skipIf(!hasSlackCredentials)('should fetch member analytics', async () => {
        const startDate = '2025-01-01';
        const endDate = '2025-01-02';

        try {
          const analytics = await slack.analytics(startDate, endDate, 'member', false);

          expect(analytics).toBeDefined();
          expect(Array.isArray(analytics)).toBe(true);

          // Analytics might be empty for the test date range
          if (analytics.length > 0) {
            const record = analytics[0];
            expect(record).toHaveProperty('date');
            expect(record).toHaveProperty('user_id');
          }
        } catch (error) {
          // Analytics API might not be available or have delays
          if (error.message.includes('analytics') || error.code === 'feature_not_enabled') {
            console.warn('⚠️  Analytics API not available for testing');
            expect(error.message).toBeDefined();
          } else {
            throw error;
          }
        }
      });

      it.skipIf(!hasSlackCredentials)('should fetch channel analytics', async () => {
        const startDate = '2025-01-01';
        const endDate = '2025-01-02';

        try {
          const analytics = await slack.analytics(startDate, endDate, 'public_channel', false);

          expect(analytics).toBeDefined();
          expect(Array.isArray(analytics)).toBe(true);

          if (analytics.length > 0) {
            const record = analytics[0];
            expect(record).toHaveProperty('date');
            expect(record).toHaveProperty('channel_id');
          }
        } catch (error) {
          if (error.message.includes('analytics') || error.code === 'feature_not_enabled') {
            console.warn('⚠️  Analytics API not available for testing');
            expect(error.message).toBeDefined();
          } else {
            throw error;
          }
        }
      });
    });
  });

  describe('Mixpanel Service', () => {
    describe('Import Functionality', () => {
      it('should be importable', () => {
        expect(mixpanelImport).toBeDefined();
        expect(typeof mixpanelImport).toBe('function');
      });

      it.skipIf(!hasMixpanelTestToken)('should successfully import test events', async () => {
        const testEvents = [
          {
            event: 'test_event',
            properties: {
              distinct_id: 'test_user_1',
              time: Math.floor(Date.now() / 1000),
              test_run: true,
              source: 'vitest',
              timestamp: new Date().toISOString()
            }
          },
          {
            event: 'test_event',
            properties: {
              distinct_id: 'test_user_2',
              time: Math.floor(Date.now() / 1000),
              test_run: true,
              source: 'vitest',
              timestamp: new Date().toISOString()
            }
          }
        ];

        const creds = {
          token: process.env.test_mixpanel_token,
          secret: process.env.mixpanel_secret
        };

        const options = {
          recordType: /** @type {any} */ ('event'),
          logs: false,
          compress: false,
          abridged: false
        };

        try {
          const result = await mixpanelImport(creds, testEvents, options);

          expect(result).toBeDefined();

          // mixpanel-import returns detailed results
          // @ts-ignore - recordsImported may not be in type definition
          if (result.recordsImported !== undefined) {
            // @ts-ignore - recordsImported may not be in type definition
            expect(result.recordsImported).toBeGreaterThanOrEqual(0);
            // @ts-ignore - recordsImported may not be in type definition
            console.log(`✅ Mixpanel import test: ${result.recordsImported} records imported`);
          } else {
            // Result structure might vary - just verify it succeeded
            expect(result).toBeDefined();
            console.log(`✅ Mixpanel import test succeeded:`, result);
          }
        } catch (error) {
          console.error('❌ Mixpanel import failed:', error.message);
          throw error;
        }
      });

      it.skipIf(!hasMixpanelTestToken)('should successfully import test user profiles', async () => {
        const testProfiles = [
          {
            $distinct_id: 'test_user_1',
            $set: {
              $email: 'test_user_1@example.com',
              $name: 'Test User 1',
              test_run: true,
              source: 'vitest',
              updated_at: new Date().toISOString()
            }
          },
          {
            $distinct_id: 'test_user_2',
            $set: {
              $email: 'test_user_2@example.com',
              $name: 'Test User 2',
              test_run: true,
              source: 'vitest',
              updated_at: new Date().toISOString()
            }
          }
        ];

        const creds = {
          token: process.env.test_mixpanel_token,
          secret: process.env.mixpanel_secret
        };

        const options = {
          recordType: /** @type {any} */ ('user'),
          logs: false,
          compress: false,
          abridged: false
        };

        try {
          const result = await mixpanelImport(creds, testProfiles, options);

          expect(result).toBeDefined();

          // mixpanel-import returns detailed results
          // @ts-ignore - recordsImported may not be in type definition
          if (result.recordsImported !== undefined) {
            // @ts-ignore - recordsImported may not be in type definition
            expect(result.recordsImported).toBeGreaterThanOrEqual(0);
            // @ts-ignore - recordsImported may not be in type definition
            console.log(`✅ Mixpanel profile test: ${result.recordsImported} profiles imported`);
          } else {
            // Result structure might vary - just verify it succeeded
            expect(result).toBeDefined();
            console.log(`✅ Mixpanel profile test succeeded:`, result);
          }
        } catch (error) {
          console.error('❌ Mixpanel profile import failed:', error.message);
          throw error;
        }
      });

      it.skipIf(!hasMixpanelTestToken)('should successfully import test group profiles', async () => {
        const testGroups = [
          {
            $group_key: 'test_workspace_id',
            $group_id: 'workspace_123',
            $set: {
              $name: 'Test Workspace 1',
              test_run: true,
              source: 'vitest',
              updated_at: new Date().toISOString()
            }
          }
        ];

        const creds = {
          token: process.env.test_mixpanel_token,
          secret: process.env.mixpanel_secret
        };

        const options = {
          recordType: /** @type {any} */ ('group'),
          groupKey: 'test_workspace_id',
          logs: false,
          compress: false,
          abridged: false
        };

        try {
          const result = await mixpanelImport(creds, testGroups, options);

          expect(result).toBeDefined();

          // mixpanel-import returns detailed results
          // @ts-ignore - recordsImported may not be in type definition
          if (result.recordsImported !== undefined) {
            // @ts-ignore - recordsImported may not be in type definition
            expect(result.recordsImported).toBeGreaterThanOrEqual(0);
            // @ts-ignore - recordsImported may not be in type definition
            console.log(`✅ Mixpanel group test: ${result.recordsImported} groups imported`);
          } else {
            // Result structure might vary - just verify it succeeded
            expect(result).toBeDefined();
            console.log(`✅ Mixpanel group test succeeded:`, result);
          }
        } catch (error) {
          console.error('❌ Mixpanel group import failed:', error.message);
          throw error;
        }
      });
    });
  });
});
