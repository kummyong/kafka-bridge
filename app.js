const express = require('express');
const fs = require('fs');

const MAX_LOGS = 300;

function normalizeCluster(raw) {
  return {
    brokers: String(raw.brokers || '')
      .split(',')
      .map((b) => b.trim())
      .filter(Boolean),
    clientId: raw.clientId || 'kafka-bridge',
    topic: raw.topic || '',
    groupId: raw.groupId || undefined,
    fromBeginning: !!raw.fromBeginning,
    ssl: !!raw.ssl,
    sasl: {
      mechanism: raw.sasl && raw.sasl.mechanism ? raw.sasl.mechanism : 'none',
      username: (raw.sasl && raw.sasl.username) || '',
      password: (raw.sasl && raw.sasl.password) || '',
    },
  };
}

function createApp({ bridge, configPath, staticDir }) {
  const app = express();
  const logs = [];

  bridge.on('log', (line) => {
    logs.push(line);
    if (logs.length > MAX_LOGS) logs.shift();
  });

  app.use(express.json());
  if (staticDir) {
    app.use(express.static(staticDir));
  }

  function loadConfig() {
    if (!fs.existsSync(configPath)) return null;
    try {
      return JSON.parse(fs.readFileSync(configPath, 'utf-8'));
    } catch {
      return null;
    }
  }

  function saveConfig(config) {
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf-8');
  }

  app.get('/api/config', (req, res) => {
    const config = loadConfig();
    res.json(config || { source: {}, target: {} });
  });

  app.post('/api/config', (req, res) => {
    const { source, target } = req.body;
    if (!source || !target) {
      return res.status(400).json({ error: 'source and target are required' });
    }
    const config = {
      source: normalizeCluster(source),
      target: normalizeCluster(target),
    };
    if (config.source.brokers.length === 0 || !config.source.topic) {
      return res.status(400).json({ error: 'Source brokers and topic are required' });
    }
    if (config.target.brokers.length === 0 || !config.target.topic) {
      return res.status(400).json({ error: 'Target brokers and topic are required' });
    }
    saveConfig(config);
    res.json({ ok: true, config });
  });

  app.post('/api/start', async (req, res) => {
    const config = loadConfig();
    if (!config) {
      return res.status(400).json({ error: 'No configuration saved yet' });
    }
    try {
      await bridge.start(config);
      res.json({ ok: true, status: bridge.getStatus() });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/stop', async (req, res) => {
    await bridge.stop();
    res.json({ ok: true, status: bridge.getStatus() });
  });

  app.get('/api/status', (req, res) => {
    res.json(bridge.getStatus());
  });

  app.get('/api/logs', (req, res) => {
    res.json({ logs });
  });

  app.get('/api/messages', (req, res) => {
    res.json({ messages: bridge.getRecentMessages() });
  });

  return app;
}

module.exports = { createApp, normalizeCluster };
