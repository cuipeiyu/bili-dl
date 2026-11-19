const $ = (id) => document.getElementById(id);

(async () => {
  const cfg = await chrome.storage.sync.get({ endpoint: 'ws://127.0.0.1:18080', token: '' });
  $('endpoint').value = cfg.endpoint;
  $('token').value = cfg.token;
  // 打开设置页时补一次权限：集群晖地址切换后，http/ws 两种权限都要有。
  await ensurePermissions(cfg.endpoint);
})();

// endpoint 换地址后，探活/提交走 http，WS 推送走 ws，两者权限都要申请。
async function ensurePermissions(endpoint) {
  if (!endpoint) return true;
  let u;
  try {
    u = new URL(endpoint);
  } catch (_) {
    return false;
  }
  const host = u.host; // 含端口，如 127.0.0.1:18080
  const wanted = [
    `http://${host}/*`,
    `ws://${host}/*`,
    `https://${host}/*`,
    `wss://${host}/*`,
  ];
  const have = new Set((await chrome.permissions.getAll()).origins || []);
  const missing = wanted.filter((o) => !have.has(o));
  if (!missing.length) return true;
  return chrome.permissions.request({ origins: missing });
}

$('save').onclick = async () => {
  const endpoint = $('endpoint').value.trim().replace(/\/+$/, '');
  const token = $('token').value.trim();
  if (!/^wss?:\/\//.test(endpoint)) {
    $('status').textContent = '地址需以 ws:// 或 wss:// 开头';
    return;
  }
  const granted = await ensurePermissions(endpoint);
  if (!granted) {
    $('status').textContent = '需要授权该地址才能连接（http 与 ws 都要允许）';
    return;
  }
  await chrome.storage.sync.set({ endpoint, token });
  $('status').textContent = '已保存';
};
