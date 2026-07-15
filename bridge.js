const { Kafka, logLevel } = require('kafkajs');
const EventEmitter = require('events');

const MAX_RECENT_MESSAGES = 20;
const MAX_PREVIEW_LENGTH = 500;

function toPreview(buffer) {
  if (buffer === null || buffer === undefined) return null;
  const str = buffer.toString('utf-8');
  return str.length > MAX_PREVIEW_LENGTH ? str.slice(0, MAX_PREVIEW_LENGTH) + '…' : str;
}

function buildKafkaConfig(clusterCfg) {
  const kafkaConfig = {
    clientId: clusterCfg.clientId || 'kafka-bridge',
    brokers: clusterCfg.brokers,
    logLevel: logLevel.NOTHING,
  };

  if (clusterCfg.ssl) {
    kafkaConfig.ssl = true;
  }

  if (clusterCfg.sasl && clusterCfg.sasl.mechanism && clusterCfg.sasl.mechanism !== 'none') {
    kafkaConfig.sasl = {
      mechanism: clusterCfg.sasl.mechanism,
      username: clusterCfg.sasl.username,
      password: clusterCfg.sasl.password,
    };
  }

  return kafkaConfig;
}

class KafkaBridge extends EventEmitter {
  constructor() {
    super();
    this.status = 'stopped'; // stopped | starting | running | stopping | error
    this.stats = { consumed: 0, produced: 0, errors: 0, startedAt: null };
    this.lastError = null;
    this.consumer = null;
    this.producer = null;
    this.recentMessages = [];
  }

  log(message) {
    const line = `[${new Date().toISOString()}] ${message}`;
    this.emit('log', line);
  }

  recordMessage(entry) {
    this.recentMessages.push(entry);
    if (this.recentMessages.length > MAX_RECENT_MESSAGES) this.recentMessages.shift();
  }

  getStatus() {
    return {
      status: this.status,
      stats: this.stats,
      lastError: this.lastError,
    };
  }

  getRecentMessages() {
    return this.recentMessages;
  }

  async start(config) {
    if (this.status === 'running' || this.status === 'starting') {
      throw new Error('Bridge is already running');
    }

    this.status = 'starting';
    this.lastError = null;
    this.stats = { consumed: 0, produced: 0, errors: 0, startedAt: null };
    this.recentMessages = [];

    const { source, target } = config;

    try {
      const sourceKafka = new Kafka(buildKafkaConfig(source));
      const targetKafka = new Kafka(buildKafkaConfig(target));

      this.producer = targetKafka.producer();
      await this.producer.connect();
      this.log(`Connected producer to target cluster (${target.brokers.join(', ')})`);

      this.consumer = sourceKafka.consumer({ groupId: source.groupId || 'kafka-bridge-group' });
      await this.consumer.connect();
      await this.consumer.subscribe({ topic: source.topic, fromBeginning: !!source.fromBeginning });
      this.log(`Subscribed to source topic "${source.topic}" on (${source.brokers.join(', ')})`);

      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          this.stats.consumed += 1;
          const entry = {
            timestamp: new Date().toISOString(),
            partition,
            offset: message.offset,
            key: toPreview(message.key),
            value: toPreview(message.value),
            status: 'consumed',
          };
          try {
            await this.producer.send({
              topic: target.topic,
              messages: [
                {
                  key: message.key,
                  value: message.value,
                  headers: message.headers,
                },
              ],
            });
            this.stats.produced += 1;
            entry.status = 'forwarded';
          } catch (err) {
            this.stats.errors += 1;
            this.lastError = err.message;
            entry.status = 'error';
            entry.error = err.message;
            this.log(`ERROR forwarding message (partition ${partition}): ${err.message}`);
          }
          this.recordMessage(entry);
        },
      });

      this.status = 'running';
      this.stats.startedAt = new Date().toISOString();
      this.log('Bridge is running');
    } catch (err) {
      this.status = 'error';
      this.lastError = err.message;
      this.log(`ERROR starting bridge: ${err.message}`);
      await this.stop().catch(() => {});
      throw err;
    }
  }

  async stop() {
    if (this.status === 'stopped') return;
    this.status = 'stopping';
    try {
      if (this.consumer) {
        await this.consumer.disconnect();
        this.consumer = null;
      }
      if (this.producer) {
        await this.producer.disconnect();
        this.producer = null;
      }
      this.log('Bridge stopped');
    } catch (err) {
      this.lastError = err.message;
      this.log(`ERROR stopping bridge: ${err.message}`);
    } finally {
      this.status = 'stopped';
    }
  }
}

module.exports = KafkaBridge;
