const { Kafka } = require('kafkajs');

const items = ['widget', 'gadget', 'gizmo', 'sprocket', 'doohickey'];
const rand = (n) => Math.floor(Math.random() * n);

(async () => {
  const kafka = new Kafka({ clientId: 'continuous-producer', brokers: ['localhost:9092'] });
  const producer = kafka.producer();
  await producer.connect();
  console.log('connected, sending 10 msg/sec to topic "orders"...');

  let total = 0;
  setInterval(async () => {
    const messages = Array.from({ length: 10 }, () => {
      const p = { id: rand(1000000), item: items[rand(items.length)], qty: rand(50) + 1, ts: new Date().toISOString() };
      return { key: `order-${p.id}`, value: JSON.stringify(p) };
    });
    try {
      await producer.send({ topic: 'orders', messages });
      total += messages.length;
      console.log(`[${new Date().toISOString()}] sent 10 (total: ${total})`);
    } catch (err) {
      console.error('send error:', err.message);
    }
  }, 1000);
})();
