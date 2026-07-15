const path = require('path');
const KafkaBridge = require('./bridge');
const { createApp } = require('./app');

const bridge = new KafkaBridge();
const app = createApp({
  bridge,
  configPath: path.join(__dirname, 'config.json'),
  staticDir: path.join(__dirname, 'public'),
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Kafka bridge web UI listening on http://localhost:${PORT}`);
});
