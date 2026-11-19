// Service Worker：抓请求头（webRequest）+ 消息中转 + 后台定时探活。
// WS 长连接不放这里（MV3 空闲会被杀），由 popup 持有。

const HEADER_KEY = 'bili-dl-cdn-headers';
const TASK_KEY = 'bili-dl-tasks';
const PROBE_KEY = 'bili-dl-probe';
const PROBE_ALARM = 'bili-dl-probe';

// ---- 后台定时探活：alarms 会唤醒休眠的 SW，popup 关闭时也能持续检查 ----
// periodInMinutes 最小为 1（MV3 限制）。同名 create 是覆盖，不会重复注册。
chrome.alarms.create(PROBE_ALARM, { periodInMinutes: 1 });
chrome.runtime.onInstalled.addListener(() => {
  chrome.alarms.create(PROBE_ALARM, { periodInMinutes: 1 });
});

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === PROBE_ALARM) runProbe();
});

// 设置页改了 endpoint/token，立即探一次，不用等下一分钟。
chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'sync' && (changes.endpoint || changes.token)) runProbe();
});

// 只取 origin，剥掉误填的路径/query，和 popup 的探活保持一致。
function httpBase(endpoint) {
  try {
    const u = new URL(String(endpoint || '').trim());
    return (u.protocol + '//' + u.host).replace(/^ws/, 'http');
  } catch (_) {
    return String(endpoint || '').trim().replace(/[/?#].*$/, '').replace(/^ws/, 'http');
  }
}

async function runProbe() {
  const c = await chrome.storage.sync.get({ endpoint: 'ws://127.0.0.1:18080', token: '' });
  const base = httpBase(c.endpoint);
  let result;
  if (!base) {
    result = { ok: false, reason: 'endpoint 未配置', at: Date.now() };
  } else {
    try {
      const r = await fetch(base + '/api/health', {
        headers: { Authorization: 'Bearer ' + (c.token || '') },
        cache: 'no-store',
      });
      if (r.ok) result = { ok: true, reason: '服务正常', at: Date.now() };
      else if (r.status === 401) result = { ok: false, reason: 'token 不正确', at: Date.now() };
      else result = { ok: false, reason: '服务异常：HTTP ' + r.status, at: Date.now() };
    } catch (_) {
      result = { ok: false, reason: '服务不可达：检查地址/端口/网络/权限', at: Date.now() };
    }
  }
  await chrome.storage.local.set({ [PROBE_KEY]: result });
  return result;
}

// 供 popup 主动触发探活（如连接失败后复核）。
chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg.type === 'PROBE_NOW') {
    runProbe().then(sendResponse);
    return true;
  }
  return false;
});

// ---- webRequest 抓完整请求头（含 Cookie/Referer，需 extraHeaders）----
const headerFilter = {
  urls: [
    '*://*.bilibili.com/x/player/*playurl*',
    '*://*.bilibili.com/x/player/wbi/playurl*',
    '*://*.bilivideo.com/*',
    '*://*.hdslb.com/*',
  ],
};

// MV3 只读观察，不做 blocking 修改。
chrome.webRequest.onSendHeaders.addListener(
  (details) => {
    if (!details.requestHeaders) return;
    const headers = {};
    for (const h of details.requestHeaders) {
      headers[h.name] = h.value;
    }
    // 只关心 CDN 媒体请求或 playurl 的头，存最近一组。
    const isMedia = /bilivideo\.com|hdslb\.com/.test(details.url);
    const isPlayurl = /playurl/i.test(details.url);
    if (!isMedia && !isPlayurl) return;
    chrome.storage.session.set({ [HEADER_KEY]: { headers, url: details.url, at: Date.now() } });
  },
  headerFilter,
  ['requestHeaders', 'extraHeaders']
);

// ---- 消息中转 ----
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.type === 'GET_HEADERS') {
    chrome.storage.session.get(HEADER_KEY, (r) => {
      sendResponse(r[HEADER_KEY] || null);
    });
    return true;
  }
  if (msg.type === 'CLEAR_EXTRACT') {
    chrome.storage.session.remove('bili-dl-last-extract', () => sendResponse({ ok: true }));
    return true;
  }
  if (msg.type === 'EXTRACTED') {
    // 提取结果暂存，popup 打开时读取并下发。
    chrome.storage.session.set({ 'bili-dl-last-extract': msg.payload });
    sendResponse({ ok: true });
    return true;
  }
  if (msg.type === 'GET_LAST_EXTRACT') {
    chrome.storage.session.get('bili-dl-last-extract', (r) => {
      sendResponse(r['bili-dl-last-extract'] || null);
    });
    return true;
  }
  if (msg.type === 'SAVE_TASKS') {
    chrome.storage.session.set({ [TASK_KEY]: msg.tasks });
    sendResponse({ ok: true });
    return true;
  }
  if (msg.type === 'GET_TASKS') {
    chrome.storage.session.get(TASK_KEY, (r) => sendResponse(r[TASK_KEY] || []));
    return true;
  }
  return false;
});
