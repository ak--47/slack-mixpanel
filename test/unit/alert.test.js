import { describe, it, expect, vi, afterEach } from 'vitest';
import { notifyFailure, loadFailures } from '../../src/services/alert.js';

describe('alert service', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    delete process.env.slack_failure_webhook;
  });

  it('posts a Slack message to the webhook', async () => {
    process.env.slack_failure_webhook = 'https://hooks.example.test/abc';
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, text: async () => 'ok' });
    vi.stubGlobal('fetch', fetchMock);

    const sent = await notifyFailure('all', 'boom');

    expect(sent).toBe(true);
    expect(fetchMock).toHaveBeenCalledOnce();
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe('https://hooks.example.test/abc');
    expect(init.method).toBe('POST');
    const body = JSON.parse(init.body);
    expect(body.text).toContain('all');
    expect(body.text).toContain('boom');
  });

  it('does not call fetch when the webhook is not configured', async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);

    expect(await notifyFailure('all', 'boom')).toBe(false);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('returns false and does not throw when Slack rejects the post', async () => {
    process.env.slack_failure_webhook = 'https://hooks.example.test/abc';
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 404, text: async () => 'no_service' }));

    expect(await notifyFailure('all', 'boom')).toBe(false);
  });

  it('lists load stages that failed after retries', () => {
    const result = {
      load: {
        members: { uploaded: 1, failed: 1, results: { events: { success: true }, profiles: { success: false, error: 'bad creds' } } },
        channels: { uploaded: 2, failed: 0, results: { events: { success: true }, profiles: { success: true } } }
      }
    };
    expect(loadFailures(result)).toEqual(['members profiles: bad creds']);
    expect(loadFailures({ load: {} })).toEqual([]);
    expect(loadFailures({})).toEqual([]);
  });
});
