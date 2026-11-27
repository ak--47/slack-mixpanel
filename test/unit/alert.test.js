import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest';
import {
  notifyFailure, loadFailures, alertFromLog, markAlertHandled, flushAlerts,
  logAlertsEnabled, _resetAlertsForTests, CAP_PER_HOUR, installCrashAlerts, normalizeTitle
} from '../../src/services/alert.js';
import logger from '../../src/utils/logger.js';
import { EventEmitter } from 'node:events';
import { readFileSync } from 'node:fs';
import { formatAlert } from '../../src/utils/alert-format.js';

const HOOK = 'https://hooks.example.test/abc';
const ENV_KEYS = ['slack_failure_webhook', 'NODE_ENV', 'K_SERVICE', 'CLOUD_RUN_JOB'];
let savedEnv;

/** @param {any} fetchMock */
function bodies(fetchMock) {
  return fetchMock.mock.calls.map(([, init]) => JSON.parse(init.body).text);
}

describe('alert service', () => {
  beforeEach(() => {
    savedEnv = Object.fromEntries(ENV_KEYS.map(k => [k, process.env[k]]));
    _resetAlertsForTests();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
    for (const k of ENV_KEYS) {
      if (savedEnv[k] === undefined) delete process.env[k];
      else process.env[k] = savedEnv[k];
    }
  });

  it('posts a standard-format Slack message to the webhook', async () => {
    process.env.slack_failure_webhook = HOOK;
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, text: async () => 'ok' });
    vi.stubGlobal('fetch', fetchMock);

    const sent = await notifyFailure('all', new Error('boom'), { status: 500 });

    expect(sent).toBe(true);
    expect(fetchMock).toHaveBeenCalledOnce();
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(HOOK);
    expect(init.method).toBe('POST');
    const text = JSON.parse(init.body).text;
    const lines = text.split('\n');
    expect(lines[0]).toBe(':rotating_light: *Slack Mixpanel — boom*');
    expect(lines[1]).toBe('*WHO:* —');
    expect(lines[2]).toBe('*WHAT:* all / POST /mixpanel-all / 500');
    expect(lines[3]).toMatch(/^\*WHERE:\* test\/unit\/alert\.test\.js @ \d+$/);
    expect(lines[4]).toMatch(/^\*WHEN:\* \d\d\/\d\d\/\d\d @ \d{1,2}:\d\d [AP]M ET$/);
    expect(lines[5]).toBe('*WHY:*');
    expect(lines[6]).toBe('> Error: boom');
    expect(lines.at(-1)).toMatch(/^<https:\/\/console\.cloud\.google\.com\/logs\/query.*project=mixpanel-gtm-training\|View logs in GCP>$/);
    expect(text).toContain(encodeURIComponent('resource.labels.service_name="slack-mixpanel-analytics"'));
    expect(text).not.toMatch(/K_REVISION|```|\(test\)/);
  });

  it('formats a load failure message: first line is the title, the rest is WHY', async () => {
    process.env.slack_failure_webhook = HOOK;
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal('fetch', fetchMock);

    await notifyFailure('members', 'Mixpanel upload failed after retries:\nmembers profiles: bad creds');
    const text = bodies(fetchMock)[0];
    expect(text).toContain(':rotating_light: *Slack Mixpanel — Mixpanel upload failed after retries:*');
    expect(text).toContain('*WHAT:* members / POST /mixpanel-members\n');
    expect(text).toContain('*WHERE:* —');
    expect(text).toContain('*WHY:*\n> members profiles: bad creds');
  });

  it('matches the reference formatter byte for byte', () => {
    const atMs = Date.UTC(2026, 10, 27, 4, 15);
    expect(formatAlert({
      service: 'Slack Mixpanel', title: 'boom', component: 'all', path: 'POST /mixpanel-all', status: 500,
      file: 'src/index.js', line: 12, detail: 'Error: boom', atMs, notes: ['+3 more since 11:02 PM']
    })).toBe([
      ':rotating_light: *Slack Mixpanel — boom*',
      '*WHO:* —',
      '*WHAT:* all / POST /mixpanel-all / 500',
      '*WHERE:* src/index.js @ 12',
      '*WHEN:* 11/26/26 @ 11:15 PM ET · +3 more since 11:02 PM',
      '*WHY:*',
      '> Error: boom'
    ].join('\n'));
  });

  it('does not call fetch when the webhook is not configured', async () => {
    delete process.env.slack_failure_webhook;
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);

    expect(await notifyFailure('all', 'boom')).toBe(false);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('returns false and does not throw when Slack rejects the post', async () => {
    process.env.slack_failure_webhook = HOOK;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 404, text: async () => 'no_service' }));

    expect(await notifyFailure('all', 'boom')).toBe(false);
  });

  it('redacts the webhook URL when the post throws', async () => {
    process.env.slack_failure_webhook = 'https://hooks.slack.com/services/T/B/secret';
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('connect failed to https://hooks.slack.com/services/T/B/secret')));
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

    expect(await notifyFailure('all', 'boom')).toBe(false);
    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0][0]).not.toContain('secret');
    expect(warn.mock.calls[0][0]).toContain('<webhook>');
    warn.mockRestore();
  });

  it('dedupes repeats for 30 minutes and reports the count on the next post', async () => {
    process.env.slack_failure_webhook = HOOK;
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal('fetch', fetchMock);
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.UTC(2026, 10, 27, 4, 2));

    expect(await notifyFailure('all', 'boom')).toBe(true);
    expect(await notifyFailure('all', 'boom')).toBe(false);
    expect(await notifyFailure('all', 'boom')).toBe(false);
    expect(await notifyFailure('channels', 'boom')).toBe(true); // different component
    vi.setSystemTime(Date.UTC(2026, 10, 27, 4, 33));
    expect(await notifyFailure('all', 'boom')).toBe(true);

    const texts = bodies(fetchMock);
    expect(texts).toHaveLength(3);
    expect(texts[2]).toContain('*WHEN:* 11/26/26 @ 11:33 PM ET · +2 more since 11:02 PM');
  });

  it(`caps posts at ${CAP_PER_HOUR} per rolling hour and reports the held count`, async () => {
    process.env.slack_failure_webhook = HOOK;
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal('fetch', fetchMock);
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.UTC(2026, 10, 27, 4, 0));

    for (let i = 0; i < CAP_PER_HOUR + 3; i++) await notifyFailure('all', `boom ${i}`);
    expect(fetchMock).toHaveBeenCalledTimes(CAP_PER_HOUR);

    vi.setSystemTime(Date.UTC(2026, 10, 27, 5, 1));
    await notifyFailure('all', 'later');
    expect(bodies(fetchMock).at(-1)).toContain('+3 other alerts held by the rate cap');
  });

  it('gates ERROR-log alerts to production on Cloud Run', () => {
    process.env.slack_failure_webhook = HOOK;
    process.env.NODE_ENV = 'test';
    delete process.env.K_SERVICE;
    delete process.env.CLOUD_RUN_JOB;
    expect(logAlertsEnabled()).toBe(false);
    process.env.NODE_ENV = 'production';
    expect(logAlertsEnabled()).toBe(false);
    process.env.K_SERVICE = 'slack-mixpanel-analytics';
    expect(logAlertsEnabled()).toBe(true);
    delete process.env.slack_failure_webhook;
    expect(logAlertsEnabled()).toBe(false);

    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    alertFromLog(['[EXTRACT] boom']);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('routes ERROR logs to Slack, skipping errors a handler already owns', async () => {
    process.env.slack_failure_webhook = HOOK;
    process.env.NODE_ENV = 'production';
    process.env.K_SERVICE = 'slack-mixpanel-analytics';
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal('fetch', fetchMock);

    alertFromLog(['[EXTRACT] 3/5 ❌ 2026-11-20: rate limited']);
    const owned = new Error('handled elsewhere');
    markAlertHandled(owned);
    alertFromLog(['ERROR JOB: slack-all', owned]);
    alertFromLog(['Uncaught Exception:', new Error('kaboom')]);
    await flushAlerts();

    const texts = bodies(fetchMock);
    expect(texts).toHaveLength(2);
    expect(texts[0]).toContain(':rotating_light: *Slack Mixpanel — [EXTRACT] 3/5 ❌ rate limited*');
    expect(texts[0]).toContain('*WHAT:* EXTRACT\n');
    expect(texts[1]).toContain(':rotating_light: *Slack Mixpanel — Uncaught Exception: kaboom*');
    expect(texts[1]).toContain('> Error: kaboom');
    expect(texts[1]).toMatch(/\*WHERE:\* test\/unit\/alert\.test\.js @ \d+/);
  });

  describe('review fixes', () => {
    /** @param {any} fetchMock */
    function prodOnCloudRun(fetchMock) {
      process.env.slack_failure_webhook = HOOK;
      process.env.NODE_ENV = 'production';
      process.env.K_SERVICE = 'slack-mixpanel-analytics';
      vi.stubGlobal('fetch', fetchMock);
    }

    it('installs crash alerts only when NODE_ENV is production', () => {
      for (const nodeEnv of ['dev', 'test', 'unknown', undefined]) {
        const proc = new EventEmitter();
        expect(installCrashAlerts({ log: () => {}, nodeEnv, proc })).toBe(false);
        expect(proc.listenerCount('unhandledRejection')).toBe(0);
        expect(proc.listenerCount('uncaughtException')).toBe(0);
      }
      const proc = new EventEmitter();
      expect(installCrashAlerts({ log: () => {}, nodeEnv: 'production', proc })).toBe(true);
      expect(proc.listenerCount('unhandledRejection')).toBe(1);
      expect(proc.listenerCount('uncaughtException')).toBe(1);
    });

    it('unhandledRejection logs + alerts and never exits', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
      prodOnCloudRun(fetchMock);
      const exit = vi.spyOn(process, 'exit').mockImplementation(/** @type {any} */ (() => {}));
      const err = vi.spyOn(console, 'error').mockImplementation(() => {});
      const proc = new EventEmitter();
      // mixpanel-import@3.1.x's own log-only listener, as in production today
      proc.on('unhandledRejection', () => {});
      installCrashAlerts({ log: logger.error, nodeEnv: 'production', proc });

      proc.emit('unhandledRejection', new Error('slack auth.test failed'), Promise.resolve());
      await flushAlerts();

      expect(exit).not.toHaveBeenCalled();
      expect(bodies(fetchMock)[0]).toContain(':rotating_light: *Slack Mixpanel — Unhandled Rejection: slack auth.test failed*');
      exit.mockRestore();
      err.mockRestore();
    });

    it('does not exit while another listener owns the outcome (mixpanel-import today)', async () => {
      const exit = vi.fn();
      const proc = new EventEmitter();
      proc.on('unhandledRejection', () => {});
      proc.on('uncaughtException', () => {});
      installCrashAlerts({ log: () => {}, nodeEnv: 'production', proc, exit });
      proc.emit('unhandledRejection', new Error('r'), Promise.resolve());
      proc.emit('uncaughtException', new Error('x'));
      await flushAlerts();
      await new Promise((r) => setTimeout(r, 0));
      expect(exit).not.toHaveBeenCalled();
    });

    it('exits 1 when it is the only listener (keeps Node default crash semantics)', async () => {
      for (const event of ['unhandledRejection', 'uncaughtException']) {
        const exit = vi.fn();
        const proc = new EventEmitter();
        installCrashAlerts({ log: () => {}, nodeEnv: 'production', proc, exit });
        proc.emit(event, new Error('alone'), Promise.resolve());
        await flushAlerts();
        await new Promise((r) => setTimeout(r, 0));
        expect(exit).toHaveBeenCalledWith(1);
      }
    });

    it('logs link has no severity clause (stderr text carries no severity)', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
      prodOnCloudRun(fetchMock);
      await notifyFailure('all', new Error('boom'), { status: 500 });
      const text = bodies(fetchMock)[0];
      expect(text).toContain('View logs in GCP');
      expect(text).not.toContain(encodeURIComponent('severity>=ERROR'));
      expect(text).toContain(encodeURIComponent('resource.labels.service_name='));
    });

    it('uncaughtException alert runs before an existing exiting listener', () => {
      const order = [];
      const proc = new EventEmitter();
      proc.on('uncaughtException', () => order.push('mixpanel-import exit'));
      installCrashAlerts({ log: () => order.push('alert'), nodeEnv: 'production', proc });
      proc.emit('uncaughtException', new Error('x'));
      expect(order).toEqual(['alert', 'mixpanel-import exit']);
    });

    it('errorNoAlert logs but does not post; load.js upload errors use it', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
      prodOnCloudRun(fetchMock);
      const err = vi.spyOn(console, 'error').mockImplementation(() => {});
      logger.errorNoAlert('[MIXPANEL] ❌ Events: bad creds');
      await flushAlerts();
      expect(err).toHaveBeenCalledOnce();
      expect(fetchMock).not.toHaveBeenCalled();
      logger.error('[MIXPANEL] ❌ Events: bad creds');
      await flushAlerts();
      expect(fetchMock).toHaveBeenCalledOnce();
      err.mockRestore();

      const loadSrc = readFileSync(new URL('../../src/jobs/load.js', import.meta.url), 'utf8');
      expect(loadSrc).not.toMatch(/logger\.error\(/);
    });

    it('normalizes extract progress + date out of the title and dedupes an outage', async () => {
      expect(normalizeTitle('[EXTRACT] [3/30] ❌ 2026-11-20: rate limited')).toBe('[EXTRACT] ❌ rate limited');
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
      prodOnCloudRun(fetchMock);
      for (let i = 1; i <= 30; i++) {
        alertFromLog([`[EXTRACT] [${i}/30] ❌ 2026-11-${String(i).padStart(2, '0')}: rate limited`]);
      }
      await flushAlerts();
      const texts = bodies(fetchMock);
      expect(texts).toHaveLength(1);
      expect(texts[0]).toContain(':rotating_light: *Slack Mixpanel — [EXTRACT] ❌ rate limited*');
      expect(texts[0]).toContain('*WHY:*\n> [EXTRACT] [1/30] ❌ 2026-11-01: rate limited');
    });

    it('never serializes non-Error objects into Slack', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
      prodOnCloudRun(fetchMock);
      class AxiosLikeError { constructor() { this.config = { headers: { Authorization: 'Bearer sekrit' } }; } }
      alertFromLog(['Unhandled Rejection:', new AxiosLikeError()]);
      alertFromLog(['[X] plain', { token: 'sekrit2' }]);
      await flushAlerts();
      const all = bodies(fetchMock).join('\n');
      expect(all).not.toMatch(/sekrit|Authorization|token/);
      expect(all).toContain('Unhandled Rejection: [AxiosLikeError]');
      expect(all).toContain('[X] plain [Object]');
    });
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
