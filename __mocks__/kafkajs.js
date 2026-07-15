const producerInstances = [];
const consumerInstances = [];
const kafkaConstructorCalls = [];

let nextProducerConnectError = null;
let nextConsumerConnectError = null;

function createProducer() {
  const producer = {
    connect: jest.fn(() => {
      if (nextProducerConnectError) {
        const err = nextProducerConnectError;
        nextProducerConnectError = null;
        return Promise.reject(err);
      }
      return Promise.resolve();
    }),
    disconnect: jest.fn().mockResolvedValue(undefined),
    send: jest.fn().mockResolvedValue(undefined),
  };
  producerInstances.push(producer);
  return producer;
}

function createConsumer() {
  const consumer = {
    connect: jest.fn(() => {
      if (nextConsumerConnectError) {
        const err = nextConsumerConnectError;
        nextConsumerConnectError = null;
        return Promise.reject(err);
      }
      return Promise.resolve();
    }),
    disconnect: jest.fn().mockResolvedValue(undefined),
    subscribe: jest.fn().mockResolvedValue(undefined),
    run: jest.fn().mockImplementation(async ({ eachMessage }) => {
      consumer.eachMessage = eachMessage;
    }),
  };
  consumerInstances.push(consumer);
  return consumer;
}

class Kafka {
  constructor(config) {
    kafkaConstructorCalls.push(config);
    this.config = config;
  }

  producer() {
    return createProducer();
  }

  consumer() {
    return createConsumer();
  }
}

module.exports = {
  Kafka,
  logLevel: { NOTHING: 0 },
  __producerInstances: producerInstances,
  __consumerInstances: consumerInstances,
  __kafkaConstructorCalls: kafkaConstructorCalls,
  __setNextProducerConnectError: (err) => {
    nextProducerConnectError = err;
  },
  __setNextConsumerConnectError: (err) => {
    nextConsumerConnectError = err;
  },
  __reset: () => {
    producerInstances.length = 0;
    consumerInstances.length = 0;
    kafkaConstructorCalls.length = 0;
    nextProducerConnectError = null;
    nextConsumerConnectError = null;
  },
};
