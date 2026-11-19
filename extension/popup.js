const $ = (id) => document.getElementById(id);
let ws = null;
let cfg = null;
let lastExtract = null;   // 待确认的分析结果（确认前不下发）
let filenameDirty = false; // 用户是否手改过保存文件名：改过则切清晰度时不覆盖
let hbTimer = null;     // 应用层心跳定时器
let pongTimer = null;   // 等待 PONG 的超时定时器
let retryTimer = null;  // 重连定时器
let retry = 0;
let retryAt = 0;        // 下次自动重试的时间戳（ms），用于按钮倒计时
let retryTick = null;   // 按钮倒计时刷新定时器

// ---- 调试日志：内存 + session 持久化，popup 关掉再开还能看到最近记录 ----
// 用 session 不落盘、浏览器关闭自动清空；只按条数封顶，保留最近 200 条。
const LOG_KEY = 'bili-dl-log';
const LOG_MAX = 200;
let logs = [];

// 只按条数裁剪，保留最近 LOG_MAX 条。
function capLogs(arr) {
  return arr.length > LOG_MAX ? arr.slice(-LOG_MAX) : arr;
}

function log(level, msg) {
  const t = new Date();
  const ts = [t.getHours(), t.getMinutes(), t.getSeconds()].map((n) => String(n).padStart(2, '0')).join(':')
    + '.' + String(t.getMilliseconds()).padStart(3, '0');
  logs.push({ at: t.getTime(), ts, level, msg: String(msg) });
  logs = capLogs(logs);
  renderLog();
  chrome.storage.session.set({ [LOG_KEY]: logs }, () => {
    if (chrome.runtime.lastError) { /* session 不可用时静默 */ }
  });
}

function renderLog() {
  const box = $('log');
  if (!box) return;
  box.innerHTML = logs.map((l) =>
    `<div class="l ${l.level}"><span class="t">${l.ts}</span> <span class="m">${esc(l.msg)}</span></div>`
  ).join('');
  $('logcount').textContent = logs.length;
  box.scrollTop = box.scrollHeight;
}

(async () => {
  try {
    const r = await chrome.storage.session.get(LOG_KEY);
    if (r && Array.isArray(r[LOG_KEY])) {
      logs = capLogs(r[LOG_KEY]);
      renderLog();
    }
  } catch (_) { /* 忽略 */ }
  log('info', 'popup 已打开');
})();

$('loghead').onclick = (e) => {
  // 点按钮不切换折叠，避免和复制/清空冲突。
  if (e.target.tagName === 'BUTTON') return;
  const box = $('log');
  box.classList.toggle('open');
  $('loghead').querySelector('b').textContent = (box.classList.contains('open') ? '▾' : '▸') + ' 调试日志';
};
$('logclear').onclick = () => {
  logs = [];
  chrome.storage.session.remove(LOG_KEY);
  renderLog();
};
$('logcopy').onclick = async () => {
  const text = capLogs(logs).map((l) => `${l.ts} [${l.level}] ${l.msg}`).join('\n');
  try {
    await navigator.clipboard.writeText(text);
    $('logcopy').textContent = '已复制';
    setTimeout(() => { $('logcopy').textContent = '复制'; }, 1200);
  } catch (_) {
    $('logcopy').textContent = '复制失败';
  }
};

const sendTab = (msg) => new Promise((resolve) => {
  chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
    if (!tabs[0]) return resolve(null);
    chrome.tabs.sendMessage(tabs[0].id, msg, (r) => resolve(r));
  });
});

// endpoint 统一规范化：只取 origin（scheme://host:port），剥掉用户误填的路径/query。
// 探活/REST 走 httpBase，WS 走 wsBase（daemon 的 WS 端点固定为 /ws）。
function endpointOrigin(endpoint) {
  try {
    const u = new URL(String(endpoint || '').trim());
    return u.protocol + '//' + u.host;
  } catch (_) {
    return String(endpoint || '').trim().replace(/[/?#].*$/, '');
  }
}
function httpBase(endpoint) {
  return endpointOrigin(endpoint).replace(/^ws/, 'http');
}
function wsBase(endpoint) {
  return endpointOrigin(endpoint).replace(/^http/, 'ws') + '/ws';
}

async function loadCfg() {
  return chrome.storage.sync.get({ endpoint: 'ws://127.0.0.1:18080', token: '', speed_limit_kbps: 0 });
}

// 打开 popup 时先显示 SW 后台探活的缓存，避免等一次网络请求才看到状态。
async function showCachedProbe() {
  const r = await chrome.storage.local.get('bili-dl-probe');
  const p = r['bili-dl-probe'];
  if (!p || !p.at) return;
  const age = Math.max(0, Math.round((Date.now() - p.at) / 1000));
  const when = age < 60 ? '刚刚' : age < 3600 ? Math.floor(age / 60) + ' 分钟前' : Math.floor(age / 3600) + ' 小时前';
  $('msg').textContent = (p.ok ? '上次探活：在线' : '上次探活：' + p.reason) + '（' + when + '）';
  log('info', `缓存探活：${p.ok ? '在线' : p.reason}（${when}）`);
}

// ---- 探活：用 health 明确区分「服务不可达」与「token 错误」 ----
async function probe(c) {
  const base = httpBase(c.endpoint);
  let res;
  if (!base) {
    res = { ok: false, reason: 'endpoint 未配置' };
  } else {
    try {
      const r = await fetch(base + '/api/health', {
        headers: { Authorization: 'Bearer ' + (c.token || '') },
        cache: 'no-store',
      });
      if (r.ok) res = { ok: true, reason: '服务正常' };
      else if (r.status === 401) res = { ok: false, reason: '服务拒绝：token 不正确' };
      else res = { ok: false, reason: '服务异常：HTTP ' + r.status };
    } catch (e) {
      res = { ok: false, reason: '服务不可达：检查地址/端口/网络/权限' };
      log('err', `探活 fetch 异常：${e.message}`);
    }
  }
  log(res.ok ? 'ok' : 'warn', `探活 ${base} → ${res.ok ? 'OK' : res.reason}`);
  // 实时结果写回缓存，下次开 popup 或后台 alarm 前都能直接显示。
  chrome.storage.local.set({ 'bili-dl-probe': { ok: res.ok, reason: res.reason, at: Date.now() } });
  return res;
}

function setConn(kind, detail) {
  const el = $('conn');
  el.className = kind === 'online' ? 'on' : 'off';
  el.textContent = kind === 'online' ? '在线' : (kind === 'connecting' ? '连接中…' : '离线');
  if (detail) $('msg').textContent = detail;
  // 「重试连接」只在明确离线时出现；在线/连接中隐藏（离线探活失败会走到这里）。
  const btn = $('retry');
  if (!btn) return;
  if (kind === 'off') {
    btn.hidden = false;
    btn.disabled = false;
    updateRetryButton();
  } else {
    btn.hidden = true;
    btn.disabled = false;
    stopRetryCountdown();
    retryAt = 0;
    btn.textContent = '重试连接';
    btn.title = '立即重连，不等待自动退避';
  }
}

function stopRetryCountdown() {
  if (retryTick) {
    clearInterval(retryTick);
    retryTick = null;
  }
}

// 像验证码按钮一样展示自动重试倒计时；倒计时期间仍可点击立即重试。
function updateRetryButton() {
  const btn = $('retry');
  if (!btn) return;
  const left = retryAt ? Math.max(0, Math.ceil((retryAt - Date.now()) / 1000)) : 0;
  if (left > 0) {
    btn.textContent = `重试连接 · ${left}s`;
    btn.title = `点击立即重试；${left}s 后自动重试（第 ${retry} 次）`;
  } else {
    btn.textContent = '重试连接';
    btn.title = '立即重连，不等待自动退避';
  }
}

function stopHeartbeat() {
  clearInterval(hbTimer);
  clearTimeout(pongTimer);
  hbTimer = null;
  pongTimer = null;
}

// 关闭当前 socket 且不触发回调，避免重连时旧连接干扰状态。
function dropSocket() {
  if (!ws) return;
  const old = ws;
  ws = null;
  old.onclose = null;
  old.onmessage = null;
  old.onerror = null;
  old.onopen = null;
  stopHeartbeat();
  try { old.close(); } catch (_) {}
}

function scheduleReconnect() {
  clearTimeout(retryTimer);
  retry = Math.min(retry + 1, 6);
  const delay = Math.min(1000 * (2 ** (retry - 1)), 30000);
  retryAt = Date.now() + delay;
  log('info', `${delay / 1000}s 后重连（第 ${retry} 次）`);
  retryTimer = setTimeout(connect, delay);
  stopRetryCountdown();
  retryTick = setInterval(() => {
    if (Date.now() >= retryAt) stopRetryCountdown();
    updateRetryButton();
  }, 250);
  updateRetryButton();
}

function startHeartbeat() {
  stopHeartbeat();
  hbTimer = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setConn('off', '连接已断开，重连中…');
      dropSocket();
      scheduleReconnect();
      return;
    }
    try {
      ws.send(JSON.stringify({ type: 'PING' }));
    } catch (_) {
      setConn('off', '心跳发送失败，重连中…');
      dropSocket();
      scheduleReconnect();
      return;
    }
    clearTimeout(pongTimer);
    pongTimer = setTimeout(async () => {
      // 兼容旧版 daemon（不回 PONG）：先用 health 复核，服务正常就保持连接。
      log('warn', '心跳超时，等待 PONG 未收到，复核 health…');
      const p = await probe(cfg || await loadCfg());
      if (p.ok) {
        log('info', 'health 正常，判定为旧版 daemon 不回 PONG，保持连接');
        return;
      }
      setConn('off', p.reason + '，重连中…');
      dropSocket();
      scheduleReconnect();
    }, 6000);
  }, 15000);
}

// 手动/设置变更后立即重连：取消退避等待与倒计时，重置计数，断开旧连接。
function reconnectNow(reason) {
  log('info', `立即重连（${reason}）`);
  clearTimeout(retryTimer);
  retryTimer = null;
  stopRetryCountdown();
  retryAt = 0;
  retry = 0;
  dropSocket();
  setConn('connecting');
  $('msg').textContent = '正在重新连接…';
  connect();
}

// ---- WS 连接 ----
async function connect() {
  cfg = await loadCfg();
  log('info', `连接目标 ${cfg.endpoint}，token ${cfg.token ? '已设置(' + cfg.token.length + '字符)' : '为空'}`);
  setConn('connecting'); // 不写 msg，保留上次探活的缓存显示
  const p = await probe(cfg);
  if (!p.ok) {
    setConn('off', p.reason + '（自动重试中）');
    scheduleReconnect();
    return;
  }
  let url;
  try {
    url = wsBase(cfg.endpoint) + '?token=' + encodeURIComponent(cfg.token || '');
    log('info', `WS 连接地址 ${url.replace(/token=[^&]*/, 'token=***')}`);
    ws = new WebSocket(url);
    log('info', 'WS 对象已创建，等待 onopen…');
  } catch (e) {
    log('err', 'WS 构造失败：' + e.message);
    setConn('off', '地址非法：' + e.message);
    scheduleReconnect();
    return;
  }
  ws.onopen = () => {
    retry = 0;
    setConn('online', '服务已连接');
    log('ok', 'WS onopen：连接建立');
    startHeartbeat();
    fetchTasks();
    fetchDiag();
    // daemon 可能刚重启，连接建立后同步一次本地限速配置，确保设置持续生效。
    loadCfg().then((c) => pushSpeedLimit(c.speed_limit_kbps || 0));
  };
  ws.onmessage = (e) => {
    try {
      const ev = JSON.parse(e.data);
      if (ev.type === 'PONG') {
        clearTimeout(pongTimer);
        return;
      }
      if (ev.type === 'snapshot' && ev.tasks) {
        log('info', `WS snapshot：${ev.tasks.length} 个任务`);
        render(ev.tasks);
      }
      if (ev.type === 'task.updated') {
        log('info', `任务更新 ${ev.task ? ev.task.id + ' → ' + ev.task.state : ''}`);
        fetchTasks();
      }
      if (ev.type === 'task.deleted') {
        log('info', '任务删除');
        fetchTasks();
      }
    } catch (err) {
      log('warn', 'WS 消息解析失败：' + err.message);
    }
  };
  ws.onclose = (ev) => {
    stopHeartbeat();
    log('warn', `WS onclose：code=${ev.code} reason=${ev.reason || '-'}`);
    setConn('off', '连接已断开，重连中…');
    scheduleReconnect();
  };
  ws.onerror = () => {
    // 不覆盖状态：health 预检已给出具体原因，onclose 统一切到重连态。
    log('err', 'WS onerror（浏览器未给出细节，通常是 401 或网络中断）');
  };
}

// 拉取 daemon 依赖摘要（ffmpeg/输出目录），关键排障信息进日志。
async function fetchDiag() {
  const c = cfg || await loadCfg();
  const base = httpBase(c.endpoint);
  try {
    const r = await fetch(base + '/api/diag', {
      headers: { Authorization: 'Bearer ' + (c.token || '') },
    });
    if (!r.ok) {
      log('warn', `diag 不可用：HTTP ${r.status}`);
      return;
    }
    const d = await r.json();
    log('ok', `daemon 自检：ffmpeg=${d.ffmpeg || '-'}`);
    log('info', `输出目录=${d.output_root || '-'}，合并worker=${d.merge_workers ?? '-'}，下载重试=${d.download_tries ?? '-'}`);
  } catch (e) {
    log('warn', 'diag 请求失败：' + e.message);
  }
}

function fetchTasks() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  // daemon 握手时推一次 snapshot；这里主动 REST 拉最新队列。
  const c = cfg || { endpoint: '', token: '' };
  fetch(httpBase(c.endpoint) + '/api/tasks', {
    headers: { Authorization: 'Bearer ' + (c.token || '') },
  })
    .then((r) => (r.ok ? r.json() : null))
    .then((tasks) => { if (tasks) render(tasks); })
    .catch(() => {});
}

function render(tasks) {
  const ul = $('tasks');
  ul.innerHTML = '';
  if (!tasks || !tasks.length) {
    ul.innerHTML = '<li style="color:#888">暂无任务</li>';
    return;
  }
  for (const t of tasks) {
    const li = document.createElement('li');
    const stateCls = t.state === 'done' ? 'done' : (t.state === 'failed' ? 'failed' : '');
    const pct = Math.round(t.progress || 0);
    const stage = t.stage ? esc(t.stage) : stateText(t.state);

    // 下载中：分三阶段展示，视频/音频各自进度条 + 合并阶段。
    let stageHtml = '';
    if (t.state === 'downloading') {
      const v = Math.round(t.video_pct || 0);
      const a = Math.round(t.audio_pct || 0);
      const vs = esc(t.video_speed || '0 B/s');
      const as = esc(t.audio_speed || '0 B/s');
      stageHtml = `
        <div class="substage">
          <div class="subrow"><span class="lab">▸ 视频流</span><span class="num">${v}% · ${vs}</span></div>
          <div class="bar thin"><i style="width:${v}%"></i></div>
          <div class="subrow"><span class="lab">▸ 音频流</span><span class="num">${a}% · ${as}</span></div>
          <div class="bar thin"><i style="width:${a}%"></i></div>
          <div class="subrow"><span class="lab pending">○ 合并</span><span class="num">等待</span></div>
        </div>`;
    } else if (t.state === 'merging') {
      stageHtml = `
        <div class="substage">
          <div class="subrow"><span class="lab done">✓ 视频流</span><span class="num">100%</span></div>
          <div class="subrow"><span class="lab done">✓ 音频流</span><span class="num">100%</span></div>
          <div class="subrow"><span class="lab">▸ 合并</span><span class="num">进行中</span></div>
        </div>`;
    }

    // 操作按钮：进行中可暂停，暂停/失败可继续，任何状态可移除。
    const active = t.state === 'pending' || t.state === 'downloading' || t.state === 'merging';
    const resumable = t.state === 'paused' || t.state === 'failed';
    let actions = '';
    if (active) actions += `<button class="mini" data-act="pause" data-id="${esc(t.id)}">暂停</button>`;
    if (resumable) actions += `<button class="mini" data-act="resume" data-id="${esc(t.id)}">继续</button>`;
    actions += `<button class="mini warn" data-act="remove" data-id="${esc(t.id)}">移除</button>`;

    li.innerHTML = `
      <div class="title">${esc(t.title || t.id)}</div>
      <div class="meta ${stateCls}">${stage} · ${pct}%${t.stage_text ? ' · ' + esc(t.stage_text) : ''}${t.error ? ' · ' + esc(t.error) : ''}</div>
      ${t.state === 'downloading' || t.state === 'merging' ? `<div class="bar"><i style="width:${pct}%"></i></div>` : ''}
      ${stageHtml}
      ${t.output_path && t.state !== 'done' ? `<div class="meta">目标：${esc(t.output_path)}</div>` : ''}
      ${t.output ? `<div class="meta done">✓ 已保存：${esc(t.output)}</div>` : ''}
      <div class="acts">${actions}</div>
    `;
    ul.appendChild(li);
  }
}

function stateText(s) {
  return { pending: '排队中', downloading: '下载中', paused: '已暂停', merging: '合并中', done: '完成', failed: '失败', canceled: '已取消' }[s] || s;
}

function esc(s) {
  return String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

// ---- 分析结果确认 ----
function humanSize(n) {
  n = Number(n) || 0;
  if (n <= 0) return '未知';
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return (i === 0 ? n : n.toFixed(1)) + ' ' + units[i];
}

function fmtDuration(sec) {
  sec = Number(sec) || 0;
  if (sec <= 0) return '未知';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = Math.floor(sec % 60);
  return h > 0
    ? `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`
    : `${m}:${String(s).padStart(2, '0')}`;
}

// 默认文件名沿用 daemon 原规则，但用户可在确认卡片里改成任意相对路径。
function defaultFileName(task, c) {
  const clean = String(task.title || 'video')
    .replace(/[\\/:*?"<>|\x00-\x1f]/g, '_').trim().slice(0, 80) || 'video';
  return `${clean}-${task.bvid || 'video'}-${(c && c.qn) || task.qn || 0}.mp4`;
}

// 切换清晰度：替换实际下发的直链、qn、任务 ID，并刷新尺寸展示与默认文件名。
function selectQuality(qn, initial) {
  const task = lastExtract;
  if (!task) return;
  const list = task.candidates || [];
  const c = list.find((x) => x.qn === qn) || list[0];
  if (!c) return;
  task.qn = c.qn;
  task.selectedQN = c.qn;
  task.id = `${task.bvid || 'unknown'}-c${task.cid || 1}-qn${c.qn}`;
  const oldHeaders = (task.video && task.video.headers) || {};
  task.video = { url: c.video.url, headers: oldHeaders };
  task.audio = { url: c.audio.url, headers: oldHeaders };
  task.analysis = c; // 摘要展示直接用候选本身；下发前剔除

  document.querySelectorAll('#c-qualities .qrow').forEach((el) => {
    const on = Number(el.dataset.qn) === c.qn;
    el.classList.toggle('on', on);
    const r = el.querySelector('input');
    if (r && !initial) r.checked = on;
  });

  const quality = c.qualityName ? `${esc(c.qualityName)} (QN ${c.qn})` : `QN ${c.qn || '未知'}`;
  const res = c.width && c.height ? `${c.width}×${c.height}` : '未知';
  const fps = c.frameRate ? String(c.frameRate).replace(/fps$/i, '') + ' fps' : '未知';
  const codec = c.codecs ? esc(String(c.codecs).split('.')[0]) : '未知';
  const rows = [
    ['画质', quality],
    ['分辨率', res],
    ['帧率 / 编码', `${fps} · ${codec}`],
    ['时长', fmtDuration(task.duration)],
    ['视频大小', humanSize(c.videoSize)],
    ['音频大小', humanSize(c.audioSize)],
    ['合计预估', humanSize(c.totalSize)],
  ];
  $('c-rows').innerHTML = rows.map(([k, v], i) =>
    `<div class="crow"><span class="k">${k}</span><span class="v${i === rows.length - 1 ? ' big' : ''}">${v}</span></div>`
  ).join('');
  $('c-note').textContent = c.estimated
    ? '大小由码率×时长估算，仅供参考；实际以下载为准。'
    : '大小为页面提供的预估值，实际可能有差异。';
  if (!filenameDirty) $('c-filename').value = defaultFileName(task, c);
  log('info', `选择清晰度 QN ${c.qn} · ${res} · ${humanSize(c.totalSize)}`);
}

// 展示分析结果卡片，等待用户确认；此时不下发任何任务。
function showConfirm(task) {
  lastExtract = task; // 暂存，确认时才 submit
  filenameDirty = false;
  $('c-title').textContent = task.title || task.id;
  const list = (task.candidates && task.candidates.length)
    ? task.candidates
    : [{ qn: task.qn, ...(task.analysis || {}), video: task.video, audio: task.audio }];
  $('c-qualities').innerHTML = list.map((c) => {
    const name = c.qualityName ? esc(c.qualityName) : 'QN ' + c.qn;
    const res = c.width && c.height ? `${c.width}×${c.height}` : '-';
    return `<label class="qrow" data-qn="${c.qn}">
      <input type="radio" name="c-qn" value="${c.qn}"${c.qn === task.qn ? ' checked' : ''}>
      <span class="qn">${name}</span>
      <span class="qmeta">QN ${c.qn} · ${res}</span>
      <span class="qsize">${humanSize(c.totalSize)}</span>
    </label>`;
  }).join('');
  $('c-qualities').onchange = (e) => {
    if (e.target && e.target.name === 'c-qn') selectQuality(parseInt(e.target.value, 10));
  };
  $('c-filename').oninput = () => { filenameDirty = true; };
  selectQuality(task.qn, true);
  $('confirm').classList.add('show');
  $('msg').textContent = '分析完成，请确认清晰度与保存文件名';
  log('ok', `分析完成：${task.title || task.id} · ${list.length} 个清晰度 · 默认 QN ${task.qn}`);
}

function hideConfirm() {
  $('confirm').classList.remove('show');
}

// ---- 解析并下发 ----
let extracting = false;

$('extract').onclick = async () => {
  if (extracting) return;
  hideConfirm();
  await runExtract();
};

// 单次分析：失败就是失败，不做自重试（重试同一页面不会改变结果）。
async function runExtract() {
  extracting = true;
  $('msg').textContent = '正在分析当前视频…';
  log('info', '请求分析当前页面…');
  await chrome.runtime.sendMessage({ type: 'CLEAR_EXTRACT' });
  const tabReply = await sendTab({ type: 'DO_EXTRACT' });
  log('info', `DO_EXTRACT 已发送，content 应答=${tabReply ? JSON.stringify(tabReply).slice(0, 120) : '无'}`);

  // 主世界结果经 SW 落到 storage.session，轮询读取。
  let tries = 0;
  const timer = setInterval(async () => {
    tries++;
    const r = await chrome.runtime.sendMessage({ type: 'GET_LAST_EXTRACT' });
    if (r && r.ok) {
      clearInterval(timer);
      extracting = false;
      log('ok', `提取成功：${r.payload.id || '?'}，qn=${r.payload.qn ?? '?'}，等待确认`);
      // 不立即下发：先展示分析结果，由用户确认。storage 里的结果保留到确认/取消时再清。
      showConfirm(r.payload);
      return;
    }
    if (r && r.ok === false) {
      clearInterval(timer);
      extracting = false;
      log('err', '提取失败：' + (r.error || '未知错误'));
      $('msg').textContent = r.error || '提取失败';
      return;
    }
    if (tries > 50) {
      clearInterval(timer);
      extracting = false;
      log('err', '提取超时（10s 内无结果）');
      $('msg').textContent = '提取超时：请先播放视频几秒；已播放仍失败请刷新页面后重试';
    }
  }, 200);
}

async function submit(task) {
  if (!task) return;
  // 用户指定的保存路径（相对 daemon 输出根目录，可含子目录）。
  const out = String($('c-filename').value || '').trim();
  task.output_path = out || defaultFileName(task, task.analysis || {});
  // analysis / candidates 仅供确认展示，不下发给 daemon。
  delete task.analysis;
  delete task.candidates;
  delete task.selectedQN;
  const headers = await chrome.runtime.sendMessage({ type: 'GET_HEADERS' });
  if (headers && headers.headers) {
    task.video.headers = headers.headers;
    task.audio.headers = headers.headers;
  }
  const c = cfg || await loadCfg();
  const httpBaseUrl = httpBase(c.endpoint);
  $('msg').textContent = '提交任务中…';
  log('info', `提交任务 ${task.id} · qn=${task.qn} · 输出=${task.output_path} → ${httpBaseUrl}/api/tasks`);
  try {
    const r = await fetch(httpBaseUrl + '/api/tasks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + (c.token || '') },
      body: JSON.stringify(task),
    });
    const data = await r.json();
    if (r.ok || r.status === 202) {
      $('msg').textContent = '已下发：' + task.id;
      log('ok', `任务 ${task.id} 下发成功（HTTP ${r.status}）`);
      fetchTasks();
    } else {
      $('msg').textContent = '下发失败：' + (data.error || r.status);
      log('err', `任务下发失败 HTTP ${r.status}：${data.error || ''}`);
    }
  } catch (e) {
    $('msg').textContent = '下发失败：' + e.message;
    log('err', '任务下发异常：' + e.message);
  }
}

// 确认后才真正下发任务；取消则丢弃并清理暂存。
$('confirm-yes').onclick = async () => {
  const task = lastExtract;
  hideConfirm();
  if (!task) { $('msg').textContent = '分析已失效，请重新分析'; return; }
  await submit(task);
  // 下发后消费掉，防止重复确认导致重复任务。
  lastExtract = null;
  await chrome.runtime.sendMessage({ type: 'CLEAR_EXTRACT' });
};
$('confirm-no').onclick = async () => {
  hideConfirm();
  lastExtract = null;
  $('msg').textContent = '已取消，未下发任务';
  log('info', '用户取消，未下发任务');
  await chrome.runtime.sendMessage({ type: 'CLEAR_EXTRACT' });
};

// ---- 任务操作：暂停 / 继续 / 移除 ----
$('tasks').addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-act]');
  if (!btn) return;
  const act = btn.dataset.act;
  const id = btn.dataset.id;
  btn.disabled = true;
  try {
    await taskAction(act, id);
  } finally {
    btn.disabled = false;
  }
});

async function taskAction(act, id) {
  const c = cfg || await loadCfg();
  const base = httpBase(c.endpoint);
  let path = '/api/task/' + encodeURIComponent(id);
  let method = 'DELETE';
  if (act === 'pause') { path += '/pause'; method = 'POST'; }
  else if (act === 'resume') { path += '/resume'; method = 'POST'; }
  const label = { pause: '暂停', resume: '继续', remove: '移除' }[act];
  $('msg').textContent = `正在${label}任务…`;
  log('info', `${label}任务 ${id}`);
  try {
    const r = await fetch(base + path, {
      method,
      headers: { Authorization: 'Bearer ' + (c.token || '') },
    });
    const data = await r.json().catch(() => ({}));
    if (r.ok || r.status === 202) {
      $('msg').textContent = `已${label}：${id}`;
      log('ok', `任务 ${id} ${label}成功`);
      fetchTasks();
    } else {
      $('msg').textContent = `${label}失败：${data.error || r.status}`;
      log('err', `任务 ${id} ${label}失败 HTTP ${r.status}：${data.error || ''}`);
    }
  } catch (e) {
    $('msg').textContent = `${label}失败：${e.message}`;
    log('err', `任务 ${act} 异常：${e.message}`);
  }
}

// ---- 顶部设置手风琴：改动即生效，无保存按钮 ----
function cfgStatus(text) {
  const el = $('cfgstatus');
  el.textContent = text;
  if (text) setTimeout(() => { if (el.textContent === text) el.textContent = ''; }, 2500);
}

// 把当前限速推给 daemon（异步，不阻塞 UI）。
async function pushSpeedLimit(kbps) {
  const c = cfg || await loadCfg();
  const base = httpBase(c.endpoint);
  if (!base) return;
  try {
    const r = await fetch(base + '/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + (c.token || '') },
      body: JSON.stringify({ speed_limit_kbps: kbps }),
    });
    if (r.ok) {
      cfgStatus(`限速已生效：${kbps || '不限'}`);
      log('ok', `限速已推送：${kbps} KB/s`);
      fetchDiag();
    } else {
      cfgStatus(`限速推送失败：HTTP ${r.status}`);
      log('err', `限速推送失败 HTTP ${r.status}`);
    }
  } catch (e) {
    cfgStatus('限速推送失败：服务不可达');
    log('err', '限速推送异常：' + e.message);
  }
}

// endpoint 改动后要补权限（http/ws 两种 scheme），逻辑与旧 options 页一致。
async function ensurePermissions(endpoint) {
  if (!endpoint) return true;
  let u;
  try { u = new URL(endpoint); } catch (_) { return false; }
  const host = u.host;
  const wanted = [`http://${host}/*`, `ws://${host}/*`, `https://${host}/*`, `wss://${host}/*`];
  const have = new Set((await chrome.permissions.getAll()).origins || []);
  const missing = wanted.filter((o) => !have.has(o));
  if (!missing.length) return true;
  return chrome.permissions.request({ origins: missing });
}

// 输入即保存：endpoint/token 触发重连，限速推送 daemon。
function bindCfgInput(id, key, transform) {
  $(id).addEventListener('change', async () => {
    const raw = $(id).value;
    const val = transform ? transform(raw) : raw.trim();
    const patch = {};
    patch[key] = val;
    await chrome.storage.sync.set(patch);
    if (key === 'endpoint') {
      const ok = await ensurePermissions(val);
      cfgStatus(ok ? '地址已更新，正在重连' : '需要授权该地址');
    } else if (key === 'token') {
      cfgStatus('令牌已更新，正在重连');
    } else if (key === 'speed_limit_kbps') {
      await pushSpeedLimit(val);
    }
  });
}

(async () => {
  const c = await loadCfg();
  $('endpoint').value = c.endpoint || '';
  $('token').value = c.token || '';
  $('speedlimit').value = (c.speed_limit_kbps !== undefined ? c.speed_limit_kbps : 0);
})();

bindCfgInput('endpoint', 'endpoint');
bindCfgInput('token', 'token');
bindCfgInput('speedlimit', 'speed_limit_kbps', (v) => {
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n >= 0 ? n : 0;
});

$('retry').onclick = () => {
  const btn = $('retry');
  btn.disabled = true;
  btn.textContent = '重试中…';
  reconnectNow('手动点击');
};

// 配置变更：endpoint/token 重连；限速变化推送到 daemon。
chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== 'sync') return;
  if (changes.endpoint || changes.token) reconnectNow('设置变更');
  if (changes.speed_limit_kbps && !changes.endpoint) {
    pushSpeedLimit(changes.speed_limit_kbps.newValue || 0);
  }
});

showCachedProbe();
connect();
