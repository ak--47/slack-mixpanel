import dotenv from 'dotenv';
import { beforeAll, afterAll } from 'vitest';

// Load environment variables for testing
dotenv.config();

// Global test setup
beforeAll(async () => {
  // Ensure test environment
  process.env.NODE_ENV = 'test';
  
  // Set default test values if not provided
  if (!process.env.slack_bot_token) {
    console.warn('Warning: slack_bot_token not set for integration tests');
  }
  
  if (!process.env.slack_user_token) {
    console.warn('Warning: slack_user_token not set for integration tests');
  }
  
  if (!process.env.mixpanel_token) {
    console.warn('Warning: mixpanel_token not set for integration tests');
  }
});

afterAll(async () => {
  // Cleanup after tests
});