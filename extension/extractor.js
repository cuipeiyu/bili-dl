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

    try {
      const r = await origFetch(window.location.href, { credentials: 'include', cache: 'no-store' });
      const html = await r.text();
      let m = html.match(/<script>window\.__INITIAL_STATE__=(.+?)<\/script>/);
      if (m && m[1]) {
        // 页面在 JSON 尾部拼了一段自清理脚本，必须先剥离再解析（注意 removeChild(s); 后有个分号）。
        const CLEANUP = ";(function(){var s;(s=document.currentScript||document.scripts[document.scripts.length-1]).parentNode.removeChild(s);}());";
        const txt = m[1].replace(CLEANUP, '');
        try { return JSON.parse(txt); } catch (_) { /* 继续试下一个入口 */ }
      }
      m = html.match(/<script id="__NEXT_DATA__" type="application\/json">(.+?)<\/script>/);
      if (m && m[1]) {
        const q = JSON.parse(m[1]);
        const st = q && q.props && q.props.dehydratedState
          && q.props.dehydratedState.queries && q.props.dehydratedState.queries[0]
          && q.props.dehydratedState.queries[0].state;
        if (st && st.status === 'success') return st;
      }
    } catch (_) { /* 忽略，走兜底 */ }
    return window.__INITIAL_STATE__ || null;
  }

  // 从 __INITIAL_STATE__ 拿真实 aid/bvid/cid；分集按 ?p=N 从 pages 里取。
  function readMetaFromState(state) {
    const d = (state && state.data) || {};
    const vd = d.videoData || (state && state.videoData) || {};
    let aid = Number(vd.aid) || 0;
    let bvid = vd.bvid || (location.pathname.match(/(BV[0-9A-Za-z]{10})/) || [])[1] || '';
    let cid = Number(vd.cid) || 0;
    let title = vd.title || '';
    let p = 1;
    const pm = location.href.match(/[?&]p=(\d+)/);
    if (pm) p = parseInt(pm[1], 10);
    const pages = Array.isArray(vd.pages) ? vd.pages : [];
    if (pages.length > 1) {
      const sp = pages.find((x) => Number(x.page) === p) || pages[0];
      if (sp) {
        p = Number(sp.page) || p;
        cid = Number(sp.cid) || cid;
        title = `${title}_P${sp.page}_${sp.part || ''}`.replace(/_$/, '');
      }
    }
    return { aid, bvid, cid, p, title };

  // 从播放器 active 画质菜单读取当前清晰度，读不到再用 localStorage 默认值。
  function readQuality() {
    let qn = 0;
    const el = document.querySelector('.squirtle-quality-select-list li.active, .bpx-player-ctrl-quality-menu .bpx-state-active');
    if (el) {
      qn = Number(el.dataset.value) || 0;
      if (!qn) {
        const first = el.parentElement && el.parentElement.querySelector('li:first-child');
        if (first) qn = Number(first.dataset.value) || 0;
      }
    }
    if (!qn) {
      try {
        const s = JSON.parse(localStorage.getItem('bilibili_player_settings') || '{}');
        qn = Number(s.setting_config && s.setting_config.defquality) || 0;
        if (!qn) qn = Number(s.video_status && s.video_status.quality) || 0;
      } catch (_) { /* 忽略 */ }
    }
    return qn || 120;

  // 主动请求 playurl，复用页面 Cookie（credentials: include）。
  // 用原始 fetch，避免被本脚本的 hook 再次捕获。
  async function fetchPlayurl(info, qn) {
    if (!info.aid && !info.bvid) return null;
    const url = `//api.bilibili.com/x/player/wbi/playurl?qn=${qn}&fnver=0&fnval=4048&fourk=1`
      + `&avid=${info.aid || 0}&bvid=${encodeURIComponent(info.bvid)}&cid=${info.cid || 0}`;
    try {
      const r = await origFetch(url, { credentials: 'include', cache: 'no-store' });
      const j = await r.json();
      if (j && j.code === 0 && j.data) return j.data;
      return { __error: j && (j.message || j.code) || 'playurl 返回异常' };
    } catch (e) {
      return { __error: 'playurl 请求失败：' + (e && e.message ? e.message : '网络错误') };
    }
  }

  // ---- 3b. 页面 state 读取（兼容入口，非主路径）----
  function readPageState() {
    const candidates = [
      window.__PLAYURL_HYDRATE_DATA__,
      // B 站新版页面把预取 playurl 放在 __playinfo__，播放器代码里明确读它；
      // __INITIAL_STATE__.playurl 并不存在，只留作兼容旧版。
      window.__playinfo__,
      window.__INITIAL_STATE__ && window.__INITIAL_STATE__?.playurl,
      window.__INITIAL_STATE__ && window.__INITIAL_STATE__?.videoData?.playinfo,
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

  // 画质编号 → 可读名称：support_formats 优先，
  // 其次 accept_quality[i] ↔ accept_description[i] 映射。
  function qualityName(qn, data) {
    const fmts = (data && data.support_formats) || [];
    const f = fmts.find((x) => Number(x.quality) === Number(qn));
    if (f) return f.new_description || f.display_desc || f.description || f.format || '';
    const aq = (data && data.accept_quality) || [];
    const ad = (data && data.accept_description) || [];
    const i = aq.findIndex((x) => Number(x) === Number(qn));
    return i >= 0 ? (ad[i] || '') : '';
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
    // 主路径：取 state / 真实 cid / 清晰度，再主动请求 playurl。
    const state = await readInitialState();
    const info = readMetaFromState(state);
    info.qn = readQuality();
    if (!info.title) {
      info.title = (document.title || 'video').replace(/[_｜|]?\s*哔哩哔哩.*$/i, '').trim() || 'video';
    }
    let data = await fetchPlayurl(info, info.qn);
    if (!hasDash(data)) {
      // 主动请求被拦/失败时，才回退到 hook 到的响应或 performance 回放。
      const fallback = pickPlayurl() || await replayPlayurl();
      if (fallback && hasDash(fallback)) data = fallback;
    }
    const candidates = hasDash(data) ? buildCandidates(data, info) : [];
    if (!candidates.length) {
      const err = data && data.__error ? String(data.__error) : '';
      post('EXTRACT_RESULT', {
        ok: false,
        error: err ? ('playurl 请求失败：' + err) : '未找到可用的 DASH playurl 数据：请登录 B 站并刷新页面后重试',
      });
      return;
    }
    // 默认选当前播放画质；popup 里可改选其它清晰度后再下发。
    const sel = candidates.find(c => c.qn === info.qn) || candidates[0];
    const tl = (data && data.timelength) || 0;
    const task = {
      id: `${info.bvid || 'unknown'}-c${info.p}-qn${sel.qn}`,
      bvid: info.bvid,
      cid: info.cid || info.p,
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
