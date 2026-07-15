jest.mock('kafkajs');

const KafkaBridge = require('../bridge');
const {
  __producerInstances,
  __consumerInstances,
  __kafkaConstructorCalls,
  __setNextProducerConnectError,
  __setNextConsumerConnectError,
  __reset,
} = require('kafkajs');

const baseConfig = {
  source: { brokers: ['localhost:9092'], topic: 'orders', groupId: 'g1' },
  target: { brokers: ['localhost:9093'], topic: 'orders-mirror' },
};

describe('KafkaBridge', () => {
  let bridge;

  beforeEach(() => {
    __reset();
    bridge = new KafkaBridge();
  });

  test('starts in stopped state with empty stats and no messages', () => {
    expect(bridge.getStatus()).toEqual({
      status: 'stopped',
      stats: { consumed: 0, produced: 0, errors: 0, startedAt: null },
      lastError: null,
    });
    expect(bridge.getRecentMessages()).toEqual([]);
  });

  test('log() emits a timestamped line', () => {
    const handler = jest.fn();
    bridge.on('log', handler);
    bridge.log('hello world');
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0][0]).toMatch(/^\[.+\] hello world$/);
  });

  test('recordMessage caps the buffer at 20 entries, dropping the oldest', () => {
    for (let i = 0; i < 25; i += 1) {
      bridge.recordMessage({ i });
    }
    const messages = bridge.getRecentMessages();
    expect(messages).toHaveLength(20);
    expect(messages[0].i).toBe(5);
    expect(messages[19].i).toBe(24);
  });

  test('start() connects producer/consumer, subscribes, and becomes running', async () => {
    await bridge.start(baseConfig);

    expect(bridge.status).toBe('running');
    expect(bridge.stats.startedAt).not.toBeNull();

    const producer = __producerInstances[0];
    const consumer = __consumerInstances[0];
    expect(producer.connect).toHaveBeenCalledTimes(1);
    expect(consumer.connect).toHaveBeenCalledTimes(1);
    expect(consumer.subscribe).toHaveBeenCalledWith({ topic: 'orders', fromBeginning: false });

    expect(__kafkaConstructorCalls[0]).toMatchObject({ brokers: ['localhost:9092'] });
    expect(__kafkaConstructorCalls[1]).toMatchObject({ brokers: ['localhost:9093'] });
  });

  test('start() defaults groupId and honors fromBeginning', async () => {
    await bridge.start({
      source: { brokers: ['localhost:9092'], topic: 'orders', fromBeginning: true },
      target: { brokers: ['localhost:9093'], topic: 'orders-mirror' },
    });
    const consumer = __consumerInstances[0];
    expect(consumer.subscribe).toHaveBeenCalledWith({ topic: 'orders', fromBeginning: true });
  });

  test('start() applies ssl and sasl options to the kafka config', async () => {
    await bridge.start({
      source: {
        brokers: ['localhost:9092'],
        topic: 'orders',
        ssl: true,
        sasl: { mechanism: 'plain', username: 'u', password: 'p' },
      },
      target: { brokers: ['localhost:9093'], topic: 'orders-mirror' },
    });
    expect(__kafkaConstructorCalls[0]).toMatchObject({
      ssl: true,
      sasl: { mechanism: 'plain', username: 'u', password: 'p' },
    });
    // target has no ssl/sasl configured
    expect(__kafkaConstructorCalls[1].ssl).toBeUndefined();
    expect(__kafkaConstructorCalls[1].sasl).toBeUndefined();
  });

  test('start() rejects when already running', async () => {
    await bridge.start(baseConfig);
    await expect(bridge.start(baseConfig)).rejects.toThrow('Bridge is already running');
  });

  test('start() rejects when already starting', async () => {
    const promise = bridge.start(baseConfig);
    expect(bridge.status).toBe('starting');
    await expect(bridge.start(baseConfig)).rejects.toThrow('Bridge is already running');
    await promise;
  });

  test('eachMessage forwards the message and records a "forwarded" entry', async () => {
    await bridge.start(baseConfig);
    const consumer = __consumerInstances[0];
    const producer = __producerInstances[0];

    await consumer.eachMessage({
      topic: 'orders',
      partition: 0,
      message: { offset: '1', key: Buffer.from('order-1'), value: Buffer.from('{"a":1}'), headers: {} },
    });

    expect(bridge.stats.consumed).toBe(1);
    expect(bridge.stats.produced).toBe(1);
    expect(bridge.stats.errors).toBe(0);
    expect(producer.send).toHaveBeenCalledWith({
      topic: 'orders-mirror',
      messages: [{ key: Buffer.from('order-1'), value: Buffer.from('{"a":1}'), headers: {} }],
    });

    const [entry] = bridge.getRecentMessages();
    expect(entry).toMatchObject({
      partition: 0,
      offset: '1',
      key: 'order-1',
      value: '{"a":1}',
      status: 'forwarded',
    });
  });

  test('eachMessage handles a null key gracefully', async () => {
    await bridge.start(baseConfig);
    const consumer = __consumerInstances[0];

    await consumer.eachMessage({
      topic: 'orders',
      partition: 0,
      message: { offset: '2', key: null, value: Buffer.from('v'), headers: {} },
    });

    const [entry] = bridge.getRecentMessages();
    expect(entry.key).toBeNull();
  });

  test('eachMessage truncates long values in the preview', async () => {
    await bridge.start(baseConfig);
    const consumer = __consumerInstances[0];
    const longValue = 'x'.repeat(600);

    await consumer.eachMessage({
      topic: 'orders',
      partition: 0,
      message: { offset: '3', key: null, value: Buffer.from(longValue), headers: {} },
    });

    const [entry] = bridge.getRecentMessages();
    expect(entry.value.length).toBe(501); // 500 chars + ellipsis marker
    expect(entry.value.endsWith('…')).toBe(true);
  });

  test('eachMessage records an "error" entry when forwarding fails', async () => {
    await bridge.start(baseConfig);
    const consumer = __consumerInstances[0];
    const producer = __producerInstances[0];
    producer.send.mockRejectedValueOnce(new Error('send failed'));

    await consumer.eachMessage({
      topic: 'orders',
      partition: 1,
      message: { offset: '4', key: Buffer.from('k'), value: Buffer.from('v'), headers: {} },
    });

    expect(bridge.stats.consumed).toBe(1);
    expect(bridge.stats.produced).toBe(0);
    expect(bridge.stats.errors).toBe(1);
    expect(bridge.lastError).toBe('send failed');

    const [entry] = bridge.getRecentMessages();
    expect(entry.status).toBe('error');
    expect(entry.error).toBe('send failed');
  });

  test('start() failure on producer.connect leaves the bridge stopped with lastError set', async () => {
    __setNextProducerConnectError(new Error('producer conn fail'));
    await expect(bridge.start(baseConfig)).rejects.toThrow('producer conn fail');
    expect(bridge.status).toBe('stopped');
    expect(bridge.lastError).toBe('producer conn fail');
  });

  test('start() failure on consumer.connect leaves the bridge stopped with lastError set', async () => {
    __setNextConsumerConnectError(new Error('consumer conn fail'));
    await expect(bridge.start(baseConfig)).rejects.toThrow('consumer conn fail');
    expect(bridge.status).toBe('stopped');
    expect(bridge.lastError).toBe('consumer conn fail');
    // producer was connected successfully before the consumer failed, so it must be cleaned up
    expect(__producerInstances[0].disconnect).toHaveBeenCalledTimes(1);
  });

  test('stop() disconnects consumer and producer and returns to stopped', async () => {
    await bridge.start(baseConfig);
    const consumer = __consumerInstances[0];
    const producer = __producerInstances[0];

    await bridge.stop();

    expect(consumer.disconnect).toHaveBeenCalledTimes(1);
    expect(producer.disconnect).toHaveBeenCalledTimes(1);
    expect(bridge.status).toBe('stopped');
    expect(bridge.consumer).toBeNull();
    expect(bridge.producer).toBeNull();
  });

  test('stop() is a no-op when already stopped', async () => {
    await bridge.stop();
    expect(bridge.status).toBe('stopped');
  });

  test('stop() skips producer disconnect when producer reference is already cleared', async () => {
    await bridge.start(baseConfig);
    const consumer = __consumerInstances[0];
    bridge.producer = null;

    await bridge.stop();

    expect(consumer.disconnect).toHaveBeenCalledTimes(1);
    expect(bridge.status).toBe('stopped');
  });

  test('stop() skips consumer disconnect when consumer reference is already cleared', async () => {
    await bridge.start(baseConfig);
    const producer = __producerInstances[0];
    bridge.consumer = null;

    await bridge.stop();

    expect(producer.disconnect).toHaveBeenCalledTimes(1);
    expect(bridge.status).toBe('stopped');
  });

  test('stop() records lastError when disconnect fails but still ends stopped', async () => {
    await bridge.start(baseConfig);
    const consumer = __consumerInstances[0];
    consumer.disconnect.mockRejectedValueOnce(new Error('disconnect fail'));

    await bridge.stop();

    expect(bridge.lastError).toBe('disconnect fail');
    expect(bridge.status).toBe('stopped');
  });
});
