const fs = require('fs');
const os = require('os');
const path = require('path');
const EventEmitter = require('events');
const request = require('supertest');
const { createApp, normalizeCluster } = require('../app');

function makeFakeBridge(overrides = {}) {
  const bridge = new EventEmitter();
  bridge.start = jest.fn().mockResolvedValue(undefined);
  bridge.stop = jest.fn().mockResolvedValue(undefined);
  bridge.getStatus = jest.fn().mockReturnValue({
    status: 'stopped',
    stats: { consumed: 0, produced: 0, errors: 0, startedAt: null },
    lastError: null,
  });
  bridge.getRecentMessages = jest.fn().mockReturnValue([]);
  return Object.assign(bridge, overrides);
}

const validPayload = {
  source: { brokers: 'localhost:9092', topic: 'orders' },
  target: { brokers: 'localhost:9093', topic: 'orders-mirror' },
};

describe('createApp', () => {
  let configPath;

  beforeEach(() => {
    configPath = path.join(os.tmpdir(), `kafka-bridge-test-${Date.now()}-${Math.random()}.json`);
  });

  afterEach(() => {
    if (fs.existsSync(configPath)) fs.unlinkSync(configPath);
  });

  test('GET /api/config returns an empty shell when no config file exists', async () => {
    const app = createApp({ bridge: makeFakeBridge(), configPath });
    const res = await request(app).get('/api/config');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ source: {}, target: {} });
  });

  test('GET /api/config falls back to empty shell on invalid JSON', async () => {
    fs.writeFileSync(configPath, 'not-json');
    const app = createApp({ bridge: makeFakeBridge(), configPath });
    const res = await request(app).get('/api/config');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ source: {}, target: {} });
  });

  test('POST /api/config rejects when source or target missing', async () => {
    const app = createApp({ bridge: makeFakeBridge(), configPath });
    const res = await request(app).post('/api/config').send({ source: {} });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/source and target/);
  });

  test('POST /api/config rejects when source brokers/topic missing', async () => {
    const app = createApp({ bridge: makeFakeBridge(), configPath });
    const res = await request(app)
      .post('/api/config')
      .send({ source: {}, target: { brokers: 'localhost:9093', topic: 't' } });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/Source brokers/);
  });

  test('POST /api/config rejects when target brokers/topic missing', async () => {
    const app = createApp({ bridge: makeFakeBridge(), configPath });
    const res = await request(app)
      .post('/api/config')
      .send({ source: { brokers: 'localhost:9092', topic: 'orders' }, target: {} });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/Target brokers/);
  });

  test('POST /api/config normalizes, persists, and echoes the config; GET reflects it', async () => {
    const app = createApp({ bridge: makeFakeBridge(), configPath });
    const payload = {
      source: {
        brokers: ' localhost:9092 , localhost:9192 ',
        topic: 'orders',
        groupId: 'g1',
        fromBeginning: true,
        ssl: true,
        sasl: { mechanism: 'plain', username: 'u', password: 'p' },
      },
      target: { brokers: 'localhost:9093', topic: 'orders-mirror' },
    };

    const postRes = await request(app).post('/api/config').send(payload);
    expect(postRes.status).toBe(200);
    expect(postRes.body.config.source.brokers).toEqual(['localhost:9092', 'localhost:9192']);
    expect(postRes.body.config.source.sasl.mechanism).toBe('plain');
    expect(postRes.body.config.target.sasl.mechanism).toBe('none');

    const getRes = await request(app).get('/api/config');
    expect(getRes.body.source.topic).toBe('orders');
    expect(getRes.body.source.fromBeginning).toBe(true);
    expect(fs.existsSync(configPath)).toBe(true);
  });

  test('POST /api/start returns 400 when no config has been saved', async () => {
    const app = createApp({ bridge: makeFakeBridge(), configPath });
    const res = await request(app).post('/api/start');
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/No configuration/);
  });

  test('POST /api/start starts the bridge when a config is present', async () => {
    fs.writeFileSync(configPath, JSON.stringify(validPayload));
    const bridge = makeFakeBridge();
    const app = createApp({ bridge, configPath });

    const res = await request(app).post('/api/start');

    expect(res.status).toBe(200);
    expect(bridge.start).toHaveBeenCalledWith(validPayload);
    expect(res.body.ok).toBe(true);
  });

  test('POST /api/start returns 500 when bridge.start rejects', async () => {
    fs.writeFileSync(configPath, JSON.stringify(validPayload));
    const bridge = makeFakeBridge({ start: jest.fn().mockRejectedValue(new Error('conn fail')) });
    const app = createApp({ bridge, configPath });

    const res = await request(app).post('/api/start');

    expect(res.status).toBe(500);
    expect(res.body.error).toBe('conn fail');
  });

  test('POST /api/stop stops the bridge and returns its status', async () => {
    const bridge = makeFakeBridge();
    const app = createApp({ bridge, configPath });

    const res = await request(app).post('/api/stop');

    expect(res.status).toBe(200);
    expect(bridge.stop).toHaveBeenCalledTimes(1);
    expect(res.body.status.status).toBe('stopped');
  });

  test('GET /api/status proxies bridge.getStatus()', async () => {
    const bridge = makeFakeBridge({
      getStatus: jest.fn().mockReturnValue({ status: 'running', stats: { consumed: 5 }, lastError: null }),
    });
    const app = createApp({ bridge, configPath });

    const res = await request(app).get('/api/status');

    expect(res.status).toBe(200);
    expect(res.body.status).toBe('running');
    expect(res.body.stats.consumed).toBe(5);
  });

  test('GET /api/logs collects lines emitted via the bridge "log" event', async () => {
    const bridge = makeFakeBridge();
    const app = createApp({ bridge, configPath });

    bridge.emit('log', '[ts] first');
    bridge.emit('log', '[ts] second');

    const res = await request(app).get('/api/logs');
    expect(res.body.logs).toEqual(['[ts] first', '[ts] second']);
  });

  test('GET /api/logs caps the buffer at 300 lines, dropping the oldest', async () => {
    const bridge = makeFakeBridge();
    const app = createApp({ bridge, configPath });

    for (let i = 0; i < 305; i += 1) {
      bridge.emit('log', `line-${i}`);
    }

    const res = await request(app).get('/api/logs');
    expect(res.body.logs).toHaveLength(300);
    expect(res.body.logs[0]).toBe('line-5');
    expect(res.body.logs[299]).toBe('line-304');
  });

  test('GET /api/messages proxies bridge.getRecentMessages()', async () => {
    const bridge = makeFakeBridge({
      getRecentMessages: jest.fn().mockReturnValue([{ key: 'k', value: 'v', status: 'forwarded' }]),
    });
    const app = createApp({ bridge, configPath });

    const res = await request(app).get('/api/messages');

    expect(res.status).toBe(200);
    expect(res.body.messages).toEqual([{ key: 'k', value: 'v', status: 'forwarded' }]);
  });

  test('serves static files from staticDir when provided', async () => {
    const staticDir = fs.mkdtempSync(path.join(os.tmpdir(), 'kafka-bridge-static-'));
    fs.writeFileSync(path.join(staticDir, 'hello.txt'), 'hi there');
    const app = createApp({ bridge: makeFakeBridge(), configPath, staticDir });

    const res = await request(app).get('/hello.txt');

    expect(res.status).toBe(200);
    expect(res.text).toBe('hi there');
  });
});

describe('normalizeCluster', () => {
  test('applies defaults for a minimal input', () => {
    const result = normalizeCluster({});
    expect(result).toEqual({
      brokers: [],
      clientId: 'kafka-bridge',
      topic: '',
      groupId: undefined,
      fromBeginning: false,
      ssl: false,
      sasl: { mechanism: 'none', username: '', password: '' },
    });
  });

  test('parses comma separated brokers and trims whitespace', () => {
    const result = normalizeCluster({ brokers: ' a:1 ,b:2,, c:3 ' });
    expect(result.brokers).toEqual(['a:1', 'b:2', 'c:3']);
  });
});
