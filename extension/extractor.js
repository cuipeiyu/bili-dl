// 页面主世界：复用页面 Cookie 与运行环境，提取 DASH 音视频直链。
(function () {
  const CHANNEL = 'bili-dl';
  const CLEANUP = ";(function(){var s;(s=document.currentScript||document.scripts[document.scripts.length-1]).parentNode.removeChild(s);}());";
  const QUALITY_NAMES = {
    16: '360P 流畅', 32: '480P 标清', 64: '720P 准高清', 80: '1080P 高清',
    112: '1080P 高码率', 120: '4K 超高清', 125: 'HDR 真彩',
  };
  const captured = { playurl: [], headers: {} };

  function post(type, payload) {
    window.postMessage({ channel: CHANNEL, from: 'extractor', type, payload }, '*');
  }

  // 播放接口按路径判断：埋点 URL 的查询串里也会出现 playurl 字样。
  function isPlayurlURL(u) {
    try {
      const x = new URL(String(u || ''), location.href);
      if (!/^https?:$/.test(x.protocol)) return false;
      if (!/(^|\.)bilibili\.com$/.test(x.hostname)) return false;
      return x.pathname.indexOf('/playurl') >= 0;
    } catch (_) { return false; }
  }

  // 播放接口响应有两种结构：投稿视频 {data:{dash}}、番剧 {result:{video_info:{dash}}}。
  // 同时从 URL 查询串回填 avid/cid/bvid/ep_id，番剧页 state 里没有这些字段。
  function playurlPayload(url, json) {
    if (!json) return null;
    let data = json.data;
    if (!data && json.result) data = json.result.video_info || json.result;
    if (!data || (!data.dash && !data.durl)) return null;
    try {
      const q = new URL(String(url), location.href).searchParams;
      if (q.get('avid')) data.avid = Number(q.get('avid')) || 0;
      if (q.get('cid')) data.cid = Number(q.get('cid')) || 0;
      if (q.get('bvid')) data.bvid = q.get('bvid');
      if (q.get('ep_id')) data.ep_id = q.get('ep_id');
    } catch (_) { /* 参数缺失不影响 dash */ }
    return { url: String(url), data, at: Date.now() };
  }

  // ---- 1. hook XHR：番剧页播放请求走 XHR，注入在 document_start ----
  const OrigXHR = window.XMLHttpRequest;
  function HookedXHR() {
    const xhr = new OrigXHR();
    const origOpen = xhr.open;
    xhr.open = function (method, u, ...rest) {
      this.__biliDlURL = String(u);
      return origOpen.call(this, method, u, ...rest);
    };
    xhr.addEventListener('load', function () {
      const u = this.responseURL || this.__biliDlURL || '';
      if (!isPlayurlURL(u)) return;
      try {
        const hit = playurlPayload(u, JSON.parse(this.responseText));
        if (hit) captured.playurl.push(hit);
      } catch (_) { /* 非 JSON，忽略 */ }
    }, { capture: true, passive: true });
    return xhr;
  }
  HookedXHR.prototype = OrigXHR.prototype;
  for (const k of ['UNSENT', 'OPENED', 'HEADERS_RECEIVED', 'LOADING', 'DONE']) {
    Object.defineProperty(HookedXHR, k, { value: OrigXHR[k] });
  }
  window.XMLHttpRequest = HookedXHR;

  // ---- 2. hook fetch：投稿视频播放请求走 fetch ----
  const origFetch = window.fetch;
  window.fetch = async function (input, init) {
    const url = typeof input === 'string' ? input : (input && input.url) || '';
    const resp = await origFetch.apply(this, arguments);
    if (isPlayurlURL(url)) {
      try {
        const hit = playurlPayload(url, await resp.clone().json());
        if (hit) captured.playurl.push(hit);
      } catch (_) { /* 忽略 */ }
    }
    return resp;
  };

  // ---- 3. W()：抓当前页 HTML 里的 __INITIAL_STATE__ / __NEXT_DATA__ ----
  async function readInitialState(mode) {
    if (mode === 'next2025' && window.__PLAYURL_HYDRATE_DATA__) {
      return { code: 0, data: window.__PLAYURL_HYDRATE_DATA__ };
    }
    try {
      const html = await (await origFetch(window.location.href, { credentials: 'include' })).text();
      let m = html.match(/<script>window\.__INITIAL_STATE__=(.+?)<\/script>/);
      if (m && m[1]) {
        return { code: 0, data: JSON.parse(m[1].replace(CLEANUP, '')) };
      }
      m = html.match(/<script id="__NEXT_DATA__" type="application\/json">(.+?)<\/script>/);
      if (m && m[1]) {
        const st = JSON.parse(m[1]).props.pageProps.dehydratedState.queries[0].state;
        if (st && st.status === 'success') return { code: 0, data: st };
      }
    } catch (_) { /* 失败走兜底 */ }
    return { code: -2 };
  }

  // ---- 4. z()：清晰度，播放器 DOM 优先，其次 localStorage，默认 120 ----
  function readQuality() {
    try {
      const el = document.querySelector('.squirtle-quality-select-list li.active, .bpx-player-ctrl-quality-menu .bpx-state-active');
      if (el) {
        let qn = Number(el.dataset.value) || 0;
        if (!qn && el.parentElement) {
          const first = el.parentElement.querySelector('li:first-child');
          qn = Number(first && first.dataset.value) || 0;
        }
        if (qn) return qn;
      }
    } catch (_) { /* 忽略 */ }
    try {
      const s = JSON.parse(localStorage.getItem('bilibili_player_settings') || '{}');
      return Number(s.setting_config && s.setting_config.defquality) || 120;
    } catch (_) { return 120; }
  }

  // ---- 5. d0()：主动请求 playurl（投稿视频主路径）----
  async function fetchPlayurl(aid, bvid, cid, qn) {
    if (!aid && !bvid) return null;
    const url = `//api.bilibili.com/x/player/wbi/playurl?qn=${qn}&fnver=0&fnval=4048&fourk=1`
      + `&avid=${aid || 0}&bvid=${encodeURIComponent(bvid || '')}&cid=${cid || 0}`;
    try {
      const j = await (await origFetch(url, { credentials: 'include' })).json();
      if (j && j.code === 0) return j.data;
      return { __error: j && (j.message || j.code) || 'playurl 返回异常' };
    } catch (e) {
      return { __error: 'playurl 请求失败：' + (e && e.message ? e.message : '网络错误') };
    }
  }

  // ---- 6. U() 的画质名映射 ----
  function qualityName(qn, data) {
    const map = Object.assign({}, QUALITY_NAMES);
    const aq = (data && data.accept_quality) || [];
    const ad = (data && data.accept_description) || [];
    aq.forEach((q, i) => { if (ad[i]) map[q] = ad[i]; });
    const fmts = (data && data.support_formats) || [];
    fmts.forEach((f) => {
      if (f && f.quality) map[f.quality] = f.new_description || f.display_desc || f.description || map[f.quality];
    });
    return map[qn] || '';
  }

  function streamURL(s) {
    return s && (s.baseUrl || s.base_url
      || (Array.isArray(s.backupUrl) && s.backupUrl[0])
      || (Array.isArray(s.backup_url) && s.backup_url[0])) || '';
  }

  function streamSize(s, timelengthMs) {
    const direct = Number(s && (s.size || s.bytes));
    if (direct > 0) return direct;
    const bw = Number(s && s.bandwidth) || 0;
    if (bw > 0 && timelengthMs > 0) return Math.round(bw * timelengthMs / 8000);
    return 0;
  }

  function pickBestAudio(list) {
    return list.reduce((best, a) =>
      (!best || (Number(a.bandwidth) || 0) > (Number(best.bandwidth) || 0)) ? a : best, null);
  }

  // 全部清晰度候选；同名/同 QN 只留码率最高的一条。
  function buildCandidates(data, info) {
    if (!data) return [];
    const dash = data.dash;
    if (!dash || !Array.isArray(dash.video) || !dash.video.length) return [];
    const audioList = Array.isArray(dash.audio) ? dash.audio : [];
    if (!audioList.length) return [];
    const audio = pickBestAudio(audioList);
    const tl = data.timelength || 0;
    const byKey = new Map();
    for (const v of dash.video) {
      if (!streamURL(v)) continue;
      const qn = Number(v.id) || info.qn || 0;
      const vSize = streamSize(v, tl);
      const c = {
        qn,
        qualityName: qualityName(qn, data) || QUALITY_NAMES[qn] || '',
        width: v.width || 0,
        height: v.height || 0,
        frameRate: v.frameRate || '',
        codecs: v.codecs || '',
        audioCodecs: audio.codecs || '',
        videoSize: vSize,
        audioSize: streamSize(audio, tl),
        totalSize: vSize + streamSize(audio, tl),
        estimated: !(v.size || v.bytes),
        container: 'dash',
        video: { url: streamURL(v) },
        audio: { url: streamURL(audio) },
      };
      if (!c.qn || !c.video.url || !c.audio.url) continue;
      const key = c.qualityName || ('QN ' + c.qn);
      const prev = byKey.get(key);
      if (!prev || (c.videoSize || 0) > (prev.videoSize || 0)) byKey.set(key, c);
    }
    const list = Array.from(byKey.values());
    list.sort((a, b) => (b.totalSize || 0) - (a.totalSize || 0) || b.qn - a.qn);
    return list;
  }

  function analysisOf(c) {
    const keys = ['qn', 'qualityName', 'width', 'height', 'frameRate', 'codecs',
      'audioCodecs', 'videoSize', 'audioSize', 'totalSize', 'estimated', 'container'];
    const a = {};
    for (const k of keys) a[k] = c[k];
    return a;
  }

  function hasDash(d) {
    return !!(d && d.dash && Array.isArray(d.dash.video) && d.dash.video.length
      && Array.isArray(d.dash.audio) && d.dash.audio.length);
  }

  // 等 hook 抓到播放响应（注入晚于请求时兜底，最多 6 秒）。
  function waitCaptured(ms) {
    return new Promise((resolve) => {
      const hit = () => captured.playurl.slice().reverse().find((x) => hasDash(x.data));
      if (hit()) return resolve(hit());
      const t0 = Date.now();
      const timer = setInterval(() => {
        const r = hit();
        if (r || Date.now() - t0 > ms) {
          clearInterval(timer);
          resolve(r || null);
        }
      }, 200);
    });
  }

  function pageType() {
    const path = location.pathname;
    if (/\/bangumi\/play\/\S+/i.test(path)) return 'bangumi';
    if (/\/watch/i.test(path) || /[?&]ep_id=/.test(location.search || '')) return 'bangumi';
    if (/\/video\/(av|bv)[0-9a-zA-Z]+/i.test(path)) return 'video';
    return 'other';
  }

  function makeTask(data, info, qn, title) {
    const candidates = buildCandidates(data, info);
    if (!candidates.length) return null;
    const sel = candidates.find((c) => c.qn === qn) || candidates[0];
    const tl = data.timelength || 0;
    return {
      id: `${info.bvid || 'unknown'}-c${info.cid || info.p || 1}-qn${sel.qn}`,
      bvid: info.bvid || '',
      cid: info.cid || 0,
      qn: sel.qn,
      title: title || 'video',
      duration: tl ? Math.round(tl / 1000) : 0,
      video: { url: sel.video.url, headers: {} },
      audio: { url: sel.audio.url, headers: {} },
      analysis: analysisOf(sel),
      candidates,
      selectedQN: sel.qn,
    };
  }

  // 接收提取指令
  window.addEventListener('message', async (e) => {
    if (e.source !== window) return;
    const d = e.data;
    if (!d || d.channel !== CHANNEL || d.to !== 'extractor' || d.type !== 'DO_EXTRACT') return;

    const type = pageType();
    const qn = readQuality();
    const info = { aid: 0, bvid: '', cid: 0, p: 1 };
    let data = null;
    let title = '';
    let api = 'null';

    try {
      if (type === 'video' || type === 'other') {
        const w = await readInitialState();
        if (w.code) throw new Error('页面信息读取失败');
        const vd = (w.data && w.data.videoData) || {};
        info.aid = Number(vd.aid) || 0;
        info.bvid = vd.bvid || (location.pathname.match(/(BV[0-9A-Za-z]{10})/i) || [])[1] || '';
        info.cid = Number(vd.cid) || 0;
        title = vd.title || '';
        const pm = location.search.match(/p=(\d+)/);
        if (pm) info.p = Number(pm[1]) || 1;
        const pages = Array.isArray(vd.pages) ? vd.pages : [];
        if (pages.length > 1) {
          const sp = pages.find((x) => String(x.page) === String(info.p)) || pages[0];
          if (sp) {
            info.p = Number(sp.page) || info.p;
            info.cid = Number(sp.cid) || info.cid;
            title = `${title}_P${sp.page}_${sp.part || ''}`.replace(/_$/, '');
          }
        }
        const fetched = await fetchPlayurl(info.aid, info.bvid, info.cid, qn);
        if (fetched && hasDash(fetched)) data = fetched;
        else api = fetched && fetched.__error ? String(fetched.__error).slice(0, 80) : 'no-dash';
      } else if (type === 'bangumi') {
        // 与播放器一致：等 XHR hook 抓到 /playurl 响应，响应体在 result.video_info。
        const hit = await waitCaptured(6000);
        if (hit && hasDash(hit.data)) {
          data = hit.data;
          info.aid = Number(hit.data.avid) || 0;
          info.cid = Number(hit.data.cid) || 0;
          info.bvid = hit.data.bvid || '';
          info.epId = hit.data.ep_id || '';
          api = 'hook';
        } else {
          // 没抓到响应时，走页面预取数据。
          const w = await readInitialState('next2025');
          const root = w.data && w.data.result;
          const vi = root && (root.video_info || (root.supplement && root.supplement.video_info));
          if (vi && hasDash(vi)) {
            data = vi;
            info.aid = Number(root.arc && root.arc.aid) || 0;
            info.cid = Number(root.arc && root.arc.cid) || 0;
            api = 'hydrate';
          } else {
            api = 'no-capture';
          }
        }
      }
    } catch (err) {
      api = 'error:' + (err && err.message ? err.message : 'unknown');
    }

    // 所有页面统一兜底：hook 里任何带 dash 的响应。
    if (!hasDash(data)) {
      const hit = await waitCaptured(0);
      if (hit && hasDash(hit.data)) {
        data = hit.data;
        info.aid = info.aid || Number(hit.data.avid) || 0;
        info.cid = info.cid || Number(hit.data.cid) || 0;
        info.bvid = info.bvid || hit.data.bvid || '';
      }
    }
    if (!title) title = (document.title || 'video').replace(/[_｜|]?\s*哔哩哔哩.*$/i, '').trim() || 'video';

    const task = hasDash(data) ? makeTask(data, info, qn, title) : null;
    if (!task) {
      const diag = `page=${type} aid=${info.aid} bvid=${info.bvid || '-'} cid=${info.cid} qn=${qn} api=${api} dash=${hasDash(data) ? 'y' : 'n'} hook=${captured.playurl.length}`;
      post('EXTRACT_RESULT', { ok: false, error: '未找到可用的 DASH playurl 数据 [' + diag + ']' });
      return;
    }
    post('EXTRACT_RESULT', { ok: true, payload: task });
  });

  // 接收 SW 抓到的请求头
  window.addEventListener('message', (e) => {
    if (e.source !== window) return;
    const d = e.data;
    if (!d || d.channel !== CHANNEL || d.to !== 'extractor' || d.type !== 'SET_HEADERS') return;
    captured.headers.cdn = d.payload || {};
  });

  post('READY', { ts: Date.now() });
})();
