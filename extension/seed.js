// 隔离世界：把 extractor.js 注入页面主世界，才能读页面 state / hook XHR。
(function () {
  const ID = 'bili-dl-extractor';
  if (document.getElementById(ID)) return;

  const s = document.createElement('script');
  s.id = ID;
  s.src = chrome.runtime.getURL('extractor.js');
  s.dataset.channel = 'bili-dl';
  (document.head || document.documentElement).appendChild(s);
  s.remove();

  // 接收主世界提取结果，转发给 SW。
  window.addEventListener('message', (e) => {
    if (e.source !== window) return;
    if (!e.data || e.data.channel !== 'bili-dl' || e.data.from !== 'extractor') return;
    chrome.runtime.sendMessage({ type: 'EXTRACTED', payload: e.data.payload });
  });

  // 接收 popup 的提取指令，转发给主世界。
  chrome.runtime.onMessage.addListener((msg) => {
    if (msg.type === 'DO_EXTRACT') {
      window.postMessage({ channel: 'bili-dl', to: 'extractor', type: 'DO_EXTRACT' }, '*');
    }
  });
})();
