import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dayjs.extend(utc);

// We need to extract the helper functions from index.js to test them
// For now, we'll create standalone versions for testing

// Helper function to parse and validate parameters (extracted from index.js)
function parseParameters(req) {
  const { body = {}, query = {} } = req;
  
  // Merge query params into body, with query taking precedence
  const normalizedQuery = {};
  Object.keys(query).forEach(key => {
    normalizedQuery[key.toLowerCase()] = query[key];
  });
  
  const params = { ...body, ...normalizedQuery };
  
  // Handle backfill parameter (mutually exclusive with date params)
  if (params.backfill === 'true' || params.backfill === true) {
    if (params.days !== undefined || params.start_date !== undefined || params.end_date !== undefined) {
      throw new Error('Parameter "backfill" is mutually exclusive with "days", "start_date", and "end_date"');
    }
    // Set environment to backfill mode
    return { ...params, env: 'backfill' };
  }
  
  // Validate mutually exclusive parameters
  const hasDays = params.days !== undefined;
  const hasDateRange = params.start_date !== undefined || params.end_date !== undefined;
  
  if (hasDays && hasDateRange) {
    throw new Error('Parameters "days" and "start_date/end_date" are mutually exclusive. Use one or the other.');
  }
  
  // Convert days to number if provided
  if (params.days !== undefined) {
    params.days = parseInt(params.days);
    if (isNaN(params.days) || params.days < 1) {
      throw new Error('Parameter "days" must be a positive integer');
    }
  }
  
  // Validate date format if provided
  const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
  if (params.start_date && !dateRegex.test(params.start_date)) {
    throw new Error('Parameter "start_date" must be in YYYY-MM-DD format');
  }
  
  if (params.end_date && !dateRegex.test(params.end_date)) {
    throw new Error('Parameter "end_date" must be in YYYY-MM-DD format');
  }
  
  return params;
}

// Helper function to determine date range (extracted from index.js)
function getDateRange(params) {
  const NOW = dayjs.utc();
  const sfdcDateTimeFmt = "YYYY-MM-DDTHH:mm:ss.SSS[Z]";
  
  // Handle backfill mode (13 months)
  if (params.env === 'backfill') {
    const BACKFILL_DAYS = 365 + 30; // ~13 months
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
  
  // Determine default days based on environment
  let DAYS;
  const NODE_ENV = process.env.NODE_ENV || 'test';
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

describe('Parameter Parsing', () => {
  describe('parseParameters', () => {
    it('should merge query and body parameters with query taking precedence', () => {
      const req = {
        query: { DAYS: '7' },
        body: { days: 3 }
      };
      
      const result = parseParameters(req);
      expect(result.days).toBe(7); // query takes precedence and is normalized to lowercase
    });

    it('should handle case-insensitive query parameters', () => {
      const req = {
        query: { DAYS: '5' },
        body: {}
      };
      
      const result = parseParameters(req);
      expect(result.days).toBe(5);
    });

    it('should throw error for mutually exclusive days and date range', () => {
      const req = {
        query: { days: '7', start_date: '2024-01-01' },
        body: {}
      };
      
      expect(() => parseParameters(req)).toThrow('mutually exclusive');
    });

    it('should throw error for backfill with other parameters', () => {
      const req = {
        query: { backfill: 'true', days: '7' },
        body: {}
      };
      
      expect(() => parseParameters(req)).toThrow('mutually exclusive');
    });

    it('should handle backfill parameter correctly', () => {
      const req = {
        query: { backfill: 'true' },
        body: {}
      };
      
      const result = parseParameters(req);
      expect(result.env).toBe('backfill');
    });

    it('should validate days parameter as positive integer', () => {
      const invalidCases = [
        { query: { days: '0' }, body: {} },
        { query: { days: '-5' }, body: {} },
        { query: { days: 'abc' }, body: {} }
      ];
      
      invalidCases.forEach(req => {
        expect(() => parseParameters(req)).toThrow('positive integer');
      });
    });

    it('should validate date format', () => {
      const invalidDates = [
        { query: { start_date: '2024-1-1' }, body: {} },
        { query: { start_date: '24-01-01' }, body: {} },
        { query: { end_date: '2024/01/01' }, body: {} }
      ];
      
      invalidDates.forEach(req => {
        expect(() => parseParameters(req)).toThrow('YYYY-MM-DD format');
      });
    });

    it('should accept valid date formats', () => {
      const req = {
        query: { start_date: '2024-01-01', end_date: '2024-01-31' },
        body: {}
      };
      
      const result = parseParameters(req);
      expect(result.start_date).toBe('2024-01-01');
      expect(result.end_date).toBe('2024-01-31');
    });
  });

  describe('getDateRange', () => {
    let originalDayjsUtc;
    
    beforeEach(() => {
      // Store original dayjs.utc function
      originalDayjsUtc = dayjs.utc;
      
      // Mock dayjs.utc() to return a consistent date for testing
      const mockDate = originalDayjsUtc('2024-01-15T12:00:00Z');
      dayjs.utc = vi.fn().mockImplementation((date) => {
        if (date) {
          return originalDayjsUtc(date); // Allow specific dates to pass through
        }
        return mockDate; // Return mock for NOW
      });
    });
    
    afterEach(() => {
      // Restore original function
      dayjs.utc = originalDayjsUtc;
    });

    it('should handle backfill mode correctly', () => {
      const params = { env: 'backfill' };
      const result = getDateRange(params);
      
      expect(result.days).toBe(395); // 365 + 30
      expect(result.simpleStart).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      expect(result.simpleEnd).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    });

    it('should use custom days parameter', () => {
      const params = { days: 10 };
      const result = getDateRange(params);
      
      expect(result.days).toBe(10);
    });

    it('should use default days for test environment', () => {
      const params = {};
      const result = getDateRange(params);
      
      expect(result.days).toBe(5); // Default for test environment
    });

    it('should handle custom date range', () => {
      const params = {
        start_date: '2024-01-01',
        end_date: '2024-01-31'
      };
      
      const result = getDateRange(params);
      
      expect(result.simpleStart).toBe('2024-01-01');
      expect(result.simpleEnd).toBe('2024-01-31');
    });

    it('should return properly formatted dates', () => {
      const params = { days: 1 };
      const result = getDateRange(params);
      
      expect(result.start).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
      expect(result.end).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
      expect(result.simpleStart).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      expect(result.simpleEnd).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    });
  });
});