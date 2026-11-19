// 页面主世界：复用页面 Cookie 与运行环境，提取 playurl 响应。
(function () {
  const CHANNEL = 'bili-dl';
  const captured = { playurl: [], headers: {} };

  function post(type, payload) {
    window.postMessage({ channel: CHANNEL, from: 'extractor', type, payload }, '*');
  }

  // ---- 1. hook XHR，截获 /playurl 响应 ----
  const OrigXHR = window.XMLHttpRequest;
  function HookedXHR() {
    const xhr = new OrigXHR();
    let url = '';
    const origOpen = xhr.open;
    xhr.open = function (method, u, ...rest) {
      url = String(u);
      return origOpen.call(this, method, u, ...rest);
    };
    xhr.addEventListener('load', function () {
      if (!/playurl/i.test(url)) return;
      try {
        const data = JSON.parse(xhr.responseText);
        if (data && data.data) {
          captured.playurl.push({ url, data: data.data, at: Date.now() });
        }
      } catch (_) { /* 非 JSON，忽略 */ }
    });
    return xhr;
  }
  HookedXHR.prototype = OrigXHR.prototype;
  Object.defineProperty(HookedXHR, 'UNSENT', { value: OrigXHR.UNSENT });
  Object.defineProperty(HookedXHR, 'OPENED', { value: OrigXHR.OPENED });
  Object.defineProperty(HookedXHR, 'HEADERS_RECEIVED', { value: OrigXHR.HEADERS_RECEIVED });
  Object.defineProperty(HookedXHR, 'LOADING', { value: OrigXHR.LOADING });
  Object.defineProperty(HookedXHR, 'DONE', { value: OrigXHR.DONE });
  window.XMLHttpRequest = HookedXHR;

  // ---- 2. hook fetch，截获 /playurl 响应 ----
  const origFetch = window.fetch;
  window.fetch = async function (input, init) {
    const url = typeof input === 'string' ? input : (input && input.url) || '';
    const resp = await origFetch.apply(this, arguments);
    if (/playurl/i.test(url)) {
      try {
        const clone = resp.clone();
        const data = await clone.json();
        if (data && data.data) captured.playurl.push({ url, data: data.data, at: Date.now() });
      } catch (_) { /* 忽略 */ }
    }
    return resp;
  };

  // ---- 3. 页面 state 读取（部分页面把 playurl 注入全局变量）----
  function readPageState() {
    const candidates = [
      window.__PLAYURL_HYDRATE_DATA__,
      window.__INITIAL_STATE__ && window.__INITIAL_STATE__?.playurl,
      window.__NEXT_DATA__ && window.__NEXT_DATA__?.props?.pageProps?.playurl,
    ];
    for (const c of candidates) {
      if (c && (c.data?.dash || c.dash || c.data?.durl)) return c.data || c;
    }
    return null;
  }

  // ---- 4. 从最接近的 playurl 响应解析 dash / durl ----
  function hasDash(d) {
    return !!(d && d.dash && Array.isArray(d.dash.video) && d.dash.video.length);
  }

  function pickPlayurl() {
    // 倒着找最近一条「带 dash」的响应：页面初次预请求可能只有 durl，后一次才有 dash。
    for (let i = captured.playurl.length - 1; i >= 0; i--) {
      const d = captured.playurl[i].data;
      if (hasDash(d)) return d;
    }
    const state = readPageState();
    if (hasDash(state)) return state;
    return captured.playurl.length ? captured.playurl[captured.playurl.length - 1].data : null;
  }

  // hook 漏抓时（如脚本注入晚于请求、播放器在 iframe），从 performance 里找到原请求
  // 并重放一次：URL 自带 wbi 签名，浏览器环境里直接 fetch 即可，无需重实现签名。
  function findPlayurlURL() {
    try {
      const entries = performance.getEntriesByType('resource') || [];
      for (let i = entries.length - 1; i >= 0; i--) {
        const u = String(entries[i].name || '');
        if (/playurl/i.test(u) && /bilibili\.com/.test(u)) return u;
      }
    } catch (_) { /* 忽略 */ }
    return '';
  }

  async function replayPlayurl() {
    const url = findPlayurlURL();
    if (!url) return null;
    try {
      const r = await origFetch(url, { credentials: 'include', cache: 'no-store' });
      const j = await r.json();
      return j && j.data ? j.data : null;
    } catch (_) { return null; }
  }

  function streamURL(s) {
    return s && (s.baseUrl || s.base_url
      || (Array.isArray(s.backupUrl) && s.backupUrl[0])
      || (Array.isArray(s.backup_url) && s.backup_url[0])) || '';
  }

  // 生成单一清晰度的完整候选：视频流可不同，音频轨共用第一条。
  function makeCandidate(video, audio, data, info, timelength) {
    const qn = video.id || info.qn || 0;
    const vSize = streamSize(video, timelength);
    const aSize = streamSize(audio, timelength);
    return {
      qn,
      qualityName: qualityName(qn, data),
      width: video.width || 0,
      height: video.height || 0,
      frameRate: video.frameRate || '',
      codecs: video.codecs || '',
      audioCodecs: audio.codecs || '',
      videoSize: vSize,
      audioSize: aSize,
      totalSize: vSize + aSize,
      estimated: !(video.size || video.bytes),
      container: 'dash',
      video: { url: streamURL(video) },
      audio: { url: streamURL(audio) },
    };
  }

  function pickBestAudio(list) {
    return list.reduce((best, a) =>
      (!best || (Number(a.bandwidth) || 0) > (Number(best.bandwidth) || 0)) ? a : best, null);
  }

  // 供 popup 以 radio 切换的清晰度候选。同名 / 同 QN 只保留码率最高的一条：
  // dash.video 常含同一清晰度的 AVC/HEVC/AV1 多版本，support_formats 也可能让
  // 不同 QN 都叫「1080P 高码率」，不去重就会出现一堆同名但尺寸不同的选项。
  function buildCandidates(data, info) {
    if (!data) return [];
    const dash = data.dash;
    if (!dash || !Array.isArray(dash.video) || !dash.video.length) return [];
    const audioList = Array.isArray(dash.audio) ? dash.audio : [];
    if (!audioList.length) return []; // 需要独立音频轨，durl 单路流暂不支持
    const audio = pickBestAudio(audioList);
    const tl = data.timelength || 0;
    const byKey = new Map();
    for (const v of dash.video) {
      if (!streamURL(v)) continue;
      const c = makeCandidate(v, audio, data, info, tl);
      if (!c.qn || !c.video.url || !c.audio.url) continue;
      const key = c.qualityName || ('QN ' + c.qn);
      const prev = byKey.get(key);
      if (!prev || (c.videoSize || 0) > (prev.videoSize || 0)) byKey.set(key, c);
    }
    const list = Array.from(byKey.values());
    // 尺寸大的排前面，高画质优先；同尺寸再按 QN 降序。
    list.sort((a, b) => (b.totalSize || 0) - (a.totalSize || 0) || b.qn - a.qn);
    return list;
  }

  // 供确认卡片展示的摘要，不含直链（下发前会整体剔除）。
  function analysisOf(c) {
    const keys = ['qn', 'qualityName', 'width', 'height', 'frameRate', 'codecs',
      'audioCodecs', 'videoSize', 'audioSize', 'totalSize', 'estimated', 'container'];
    const a = {};
    for (const k of keys) a[k] = c[k];
    return a;
  }

  function meta() {
    const titleEl = document.querySelector('h1.video-title, .video-title, h1[title]');
    let title = titleEl ? (titleEl.getAttribute('title') || titleEl.textContent || '') : document.title || '';
    title = title.replace(/[_｜|]?\s*哔哩哔哩.*$/i, '').trim();
    const bvidMatch = location.pathname.match(/(BV[0-9A-Za-z]{10})/);
    const bvid = bvidMatch ? bvidMatch[1] : '';
    const cidMatch = location.href.match(/[?&]p=(\d+)/);
    const p = cidMatch ? parseInt(cidMatch[1], 10) : 1;
    let qn = 0;
    try {
      const s = JSON.parse(localStorage.getItem('bilibili_player_settings') || '{}');
      qn = s.video_status?.quality || s.quality || 0;
    } catch (_) { /* 忽略 */ }
    return { title: title || 'video', bvid, p, qn };
  }

  // 画质编号 → 可读名称（1080P 高清 / 4K 等），来自 playurl 的 support_formats。
  function qualityName(qn, data) {
    const fmts = (data && data.support_formats) || [];
    const f = fmts.find((x) => x.quality === qn);
    return f ? (f.new_description || f.description || f.format || '') : '';
  }

  // 单条流大小估算：优先 size 字段，否则 bandwidth(bps) × 时长(ms) / 8000。
  function streamSize(s, timelengthMs) {
    const direct = Number(s && (s.size || s.bytes));
    if (direct > 0) return direct;
    const bw = Number(s && s.bandwidth) || 0;
    if (bw > 0 && timelengthMs > 0) return Math.round(bw * timelengthMs / 8000);
    return 0;
  }

  // 接收提取指令
  window.addEventListener('message', async (e) => {
    if (e.source !== window) return;
    const d = e.data;
    if (!d || d.channel !== CHANNEL || d.to !== 'extractor' || d.type !== 'DO_EXTRACT') return;
    const info = meta();
    let data = pickPlayurl();
    if (!hasDash(data)) {
      // hook 没拿到 dash 时，回放 performance 里已发出的 playurl 请求兜底。
      data = await replayPlayurl() || data;
    }
    const candidates = buildCandidates(data, info);
    if (!candidates.length) {
      post('EXTRACT_RESULT', { ok: false, error: '未找到可用的 DASH playurl 数据：请播放视频几秒，或刷新页面后重试' });
      return;
    }
    // 默认选当前播放画质；popup 里可改选其它清晰度后再下发。
    const sel = candidates.find(c => c.qn === info.qn) || candidates[0];
    const tl = (data && data.timelength) || 0;
    const task = {
      id: `${info.bvid || 'unknown'}-c${info.p}-qn${sel.qn}`,
      bvid: info.bvid,
      cid: info.p,
      qn: sel.qn,
      title: info.title,
      duration: tl ? Math.round(tl / 1000) : 0,
      video: { url: sel.video.url, headers: {} },
      audio: { url: sel.audio.url, headers: {} },
      analysis: analysisOf(sel), // 摘要仅用于展示，下发前剔除
      candidates,                 // 其它清晰度的直链与尺寸，供 radio 切换
      selectedQN: sel.qn,
    };
    post('EXTRACT_RESULT', { ok: true, payload: task });
  });

  // 接收 SW 抓到的请求头（SW 通过 content script 转发进来）
  window.addEventListener('message', (e) => {
    if (e.source !== window) return;
    const d = e.data;
    if (!d || d.channel !== CHANNEL || d.to !== 'extractor' || d.type !== 'SET_HEADERS') return;
    captured.headers.cdn = d.payload || {};
  });

  post('READY', { ts: Date.now() });
})();
