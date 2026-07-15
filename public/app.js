const $ = (id) => document.getElementById(id);

function readForm() {
  return {
    source: {
      brokers: $('src-brokers').value,
      topic: $('src-topic').value,
      groupId: $('src-group').value,
      fromBeginning: $('src-from-beginning').checked,
      ssl: $('src-ssl').checked,
      sasl: {
        mechanism: $('src-sasl-mechanism').value,
        username: $('src-sasl-username').value,
        password: $('src-sasl-password').value,
      },
    },
    target: {
      brokers: $('tgt-brokers').value,
      topic: $('tgt-topic').value,
      ssl: $('tgt-ssl').checked,
      sasl: {
        mechanism: $('tgt-sasl-mechanism').value,
        username: $('tgt-sasl-username').value,
        password: $('tgt-sasl-password').value,
      },
    },
  };
}

function fillForm(config) {
  if (!config) return;
  const s = config.source || {};
  const t = config.target || {};
  $('src-brokers').value = (s.brokers || []).join(',');
  $('src-topic').value = s.topic || '';
  $('src-group').value = s.groupId || '';
  $('src-from-beginning').checked = !!s.fromBeginning;
  $('src-ssl').checked = !!s.ssl;
  $('src-sasl-mechanism').value = (s.sasl && s.sasl.mechanism) || 'none';
  $('src-sasl-username').value = (s.sasl && s.sasl.username) || '';
  $('src-sasl-password').value = (s.sasl && s.sasl.password) || '';

  $('tgt-brokers').value = (t.brokers || []).join(',');
  $('tgt-topic').value = t.topic || '';
  $('tgt-ssl').checked = !!t.ssl;
  $('tgt-sasl-mechanism').value = (t.sasl && t.sasl.mechanism) || 'none';
  $('tgt-sasl-username').value = (t.sasl && t.sasl.username) || '';
  $('tgt-sasl-password').value = (t.sasl && t.sasl.password) || '';
}

function showMsg(text, isError) {
  const el = $('msg');
  el.textContent = text;
  el.className = 'msg ' + (isError ? 'error' : 'ok');
  setTimeout(() => { el.textContent = ''; }, 4000);
}

async function loadConfig() {
  const res = await fetch('/api/config');
  const config = await res.json();
  fillForm(config);
}

async function saveConfig() {
  const res = await fetch('/api/config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(readForm()),
  });
  const data = await res.json();
  if (!res.ok) {
    showMsg('저장 실패: ' + data.error, true);
  } else {
    showMsg('설정이 저장되었습니다.', false);
  }
}

async function startBridge() {
  const res = await fetch('/api/start', { method: 'POST' });
  const data = await res.json();
  if (!res.ok) showMsg('시작 실패: ' + data.error, true);
  else showMsg('브릿지를 시작했습니다.', false);
  refreshStatus();
}

async function stopBridge() {
  await fetch('/api/stop', { method: 'POST' });
  refreshStatus();
}

async function refreshStatus() {
  const res = await fetch('/api/status');
  const data = await res.json();
  const badge = $('statusBadge');
  badge.textContent = data.status;
  badge.className = 'st-' + data.status;

  const s = data.stats || {};
  $('stats').textContent =
    `consumed: ${s.consumed || 0}  |  produced: ${s.produced || 0}  |  errors: ${s.errors || 0}` +
    (s.startedAt ? `  |  started: ${s.startedAt}` : '');

  if (data.lastError) {
    showMsg('마지막 오류: ' + data.lastError, true);
  }
}

function escapeHtml(str) {
  if (str === null || str === undefined) return '<i>null</i>';
  return String(str).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

async function refreshMessages() {
  const res = await fetch('/api/messages');
  const data = await res.json();
  const body = $('msgTableBody');
  body.innerHTML = data.messages
    .slice()
    .reverse()
    .map((m) => `<tr>
        <td>${escapeHtml(m.timestamp)}</td>
        <td class="status-${m.status}">${escapeHtml(m.status)}${m.error ? ': ' + escapeHtml(m.error) : ''}</td>
        <td>${escapeHtml(m.partition)}/${escapeHtml(m.offset)}</td>
        <td class="key" title="${escapeHtml(m.key)}">${escapeHtml(m.key)}</td>
        <td class="val" title="${escapeHtml(m.value)}">${escapeHtml(m.value)}</td>
      </tr>`)
    .join('');
}

async function refreshLogs() {
  const res = await fetch('/api/logs');
  const data = await res.json();
  const box = $('logBox');
  const atBottom = box.scrollTop + box.clientHeight >= box.scrollHeight - 10;
  box.textContent = data.logs.join('\n');
  if (atBottom) box.scrollTop = box.scrollHeight;
}

$('saveBtn').addEventListener('click', saveConfig);
$('startBtn').addEventListener('click', startBridge);
$('stopBtn').addEventListener('click', stopBridge);

loadConfig();
refreshStatus();
refreshLogs();
refreshMessages();
setInterval(refreshStatus, 3000);
setInterval(refreshLogs, 3000);
setInterval(refreshMessages, 3000);
