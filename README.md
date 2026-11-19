# bili-dl

浏览器扩展提取 B 站视频/音频直链与请求头，提交到本地 Go 服务下载，ffmpeg 无损合并。
下载全局串行、单任务内音视频两路并发、合并独立并行。

## 组件

- `extension/`：MV3 扩展（原生 JS，无构建）。加载方式：Chrome 开发者模式 → 加载已解压的扩展程序。
- `daemon/`：Go 服务，仅标准库。
- `Dockerfile` / `docker-compose.yml`：群晖部署。

## daemon 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `BILIDL_ADDR` | `:18080` | 监听地址 |
| `BILIDL_TOKEN` | （必填） | 访问令牌，为空拒绝启动 |
| `BILIDL_OUTPUT_ROOT` | `./data` | 输出根目录 |
| `BILIDL_STATE_FILE` | `./data/tasks.json` | 任务持久化文件 |
| `BILIDL_FFMPEG` | `ffmpeg` | ffmpeg 路径 |
| `BILIDL_MERGE_WORKERS` | `2` | 合并并发数 |
| `BILIDL_DOWNLOAD_TRIES` | `3` | 下载重试次数 |
| `BILIDL_HTTP_IDLE_TIMEOUT_SEC` | `30` | 下载空闲超时：连续 N 秒收不到数据才判定卡死（不是整个下载的总时限）。兼容旧名 `BILIDL_HTTP_TIMEOUT_SEC` |
| `BILIDL_LOG_LEVEL` | `info` | 日志级别：debug/info/warn/error（logrus 文本日志，带时间戳） |
| `BILIDL_SPEED_LIMIT_KBPS` | `0` | 下载总限速 KB/s（两路共享），0=不限速；运行时可用 API 改，扩展设置区改完即生效 |

## API

- `GET /api/health`
- `GET /api/tasks` — 任务列表（按创建时间稳定排序）
- `POST /api/tasks` — 提交任务（见下方字段）
- `GET|DELETE /api/task/{id}` — 查询 / 移除（移除会中断下载并清理分片）
- `POST /api/task/{id}/pause` — 暂停（保留分片供继续）
- `POST /api/task/{id}/resume` — 继续（从已下载分片断点续传）
- `GET /api/settings` — 读取运行配置（当前限速）
- `POST /api/settings` — 修改限速，body `{"speed_limit_kbps": 512}`，0=不限速；正在下载的任务立即生效
- `GET /ws?token=...` — WS 推送（snapshot / task.updated / task.deleted）

全部接口需 `Authorization: Bearer <token>`；WS 用 query 传 token。

### 提交字段

```json
{
  "id": "BVxxx-c1-qn80",
  "bvid": "BVxxx", "cid": 1, "qn": 80,
  "title": "标题", "duration": 241,
  "output_path": "视频03.mp4",
  "video": { "url": "https://...", "headers": { "Referer": "...", "Cookie": "...", "User-Agent": "..." } },
  "audio": { "url": "https://...", "headers": { } }
}
```

`id`/`video.url`/`audio.url` 必填。headers 由扩展从页面真实请求抓取并透传。

`output_path` 可选：相对 `BILIDL_OUTPUT_ROOT` 的保存路径，可含子目录（如 `视频03.mp4`）；缺省时按 `标题-bvid-qn.mp4` 生成。daemon 会丢弃绝对路径前缀与 `..` 段、替换非法字符、自动补 `.mp4` 后缀，并创建所需子目录。扩展在分析结果卡片里以 radio 列出全部清晰度，切换会替换对应直链与预估尺寸，文件名输入框可随时修改。

## 部署到群晖

```bash
# 1. 构建多架构镜像（需 docker buildx）
docker buildx build --platform linux/amd64,linux/arm64 -t bili-dl:latest --load .

# 2. 改 docker-compose.yml 里的 BILIDL_TOKEN 和 volume 路径

# 3. 启动
docker compose up -d
```

扩展设置页填 `ws://<群晖IP>:18080` 和相同 token，并授权该 origin。

## 已知边界（MVP）

- 仅 DASH 音画分离流；durl 单路暂不支持。
- 直链签名有时效，过期（403/410）需重新解析页面后再次下发。
- 文件名自动生成：`标题-bvid-qn.mp4`，非法字符替换为 `_`，用户下载后自行归类。
- 进程重启后未完成任务标记为失败（直链已失效），需重新下发。
