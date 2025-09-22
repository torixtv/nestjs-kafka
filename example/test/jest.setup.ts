// Jest setup for E2E tests
import { Logger } from '@nestjs/common';

// Increase timeout for integration tests
jest.setTimeout(60000);

// Suppress log output during tests unless explicitly needed
const logger = new Logger('Test');
logger.log('Setting up E2E test environment...');

// Global test setup
beforeAll(async () => {
  logger.log('E2E test suite starting...');
});

afterAll(async () => {
  logger.log('E2E test suite completed');
});