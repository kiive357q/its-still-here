# It's still here

自动化文件无感转存中继工具 —— 实时捕获本地监听目录与 IM 客户端（如微信 PC 端）收到的文件，在完成稳定性校验后按规则分流上传至 Google Drive、OneDrive、百度网盘/夸克网盘（基于 AList WebDAV）等远端存储，并在服务端校验大小一致后清理本地暂存。

---

## 项目背景与痛点

日常办公与项目协作中，大量核心文档、设计包和视频素材通过微信等 IM 工具流转，但传统工作流长期面临两个硬性痛点：

1. **文件 14 天过期失效**：微信接收的文件若未及时打开或另存，过期后将无法再次下载，回溯历史资料时经常看到令人崩溃的「文件已过期或已被清理」。
2. **PC 端 200MB 自动下载限制**：微信 PC 客户端默认仅对小文件开启自动下载，面对超过 200MB 的压缩包、录像或数据集往往需要手动点击接收；且本地留存过多大文件会持续膨胀 `WeChat Files` 磁盘空间。

**It's still here** 的目标就是把「被动过期」变成「无感留存」：文件到达即捕获、落盘即校验、按规则自动归档至你指定的云盘，再也不用担心「文件已过期」。

---

## 核心特性

- **输入源解耦（Source Adapters）**：
  - `folder`：基于 `watchdog` 的本地文件夹监听适配器，支持临时后缀过滤（`.tmp`、`.crdownload`、`.part`、`.downloading`）与防抖稳定检测（`stabilize_seconds`），第一层子目录自动映射为 `chat_name`。
  - `wechat`：基于 `wxauto` / `wxauto4` 的微信 PC 端（3.9 / 4.0）监听适配器，使用独立线程池卸载大文件下载，不阻塞消息轮询。
- **多网盘支持（Storage Providers）**：
  - `rclone` 后端：原生对接 Google Drive、OneDrive、S3、Dropbox 等数十种云存储，支持分片参数（`--drive-chunk-size`）、速率限制（`--tpslimit`）与远端大小校验（`rclone lsf`）。
  - `webdav` 后端：基于 `httpx` 实现 1 MiB 流式分块上传（`PUT`），大文件（200MB+）无需全量加载进内存；自动递归 `MKCOL` 建目录并支持 `HEAD Content-Length` 校验，可无缝对接 **AList** 挂载的百度网盘、夸克网盘、阿里云盘等。
- **进程级独立代理隔离**：
  - 不同存储后端可单独开启或关闭代理（`proxy.enabled` / `proxy.url`）。
  - `RcloneProvider` 仅复制环境变量并注入子进程 `env`，`WebDAVProvider` 仅注入单个 `httpx.Client` 实例，**绝不修改全局 `os.environ`**，确保微信直连、国内网盘直连与海外网盘代理互不干扰。
- **SQLite 断点状态机与去重**：
  - 基于 SQLite（`StateDB`）持久化每条转存任务，完整追踪 `DETECTED → DOWNLOADING → STAGED → UPLOADING → VERIFIED → CLEANED`（及异常态 `PENDING_RETRY` / `FAILED`）。
  - 支持基于 SHA-256 哈希与目标存储的幂等去重，意外中断或网络波动后按 `retry_interval_seconds` 自动退避重传。

---

## 系统架构

```text
+-----------------------------------------------------------------------------+
|                             It's still here                                 |
|                                                                             |
|  [ 输入源 Sources ]                                                          |
|   ├── WatchFolderAdapter (watchdog) ──┐                                     |
|   └── WeChatAdapter (wxauto/wxauto4) ─┼─> FileEvent (稳定大小校验后触发)      |
|                                       v                                     |
|  [ 路由与状态机 Core ]                                                       |
|   ├── RuleEngine (按来源/会话/扩展名/体积匹配规则，渲染远端路径模板)          |
|   └── StateDB (SQLite: SHA-256 去重 + 状态机持久化 + 失败重试调度)           |
|                                       │                                     |
|          ┌────────────────────────────┴────────────────────────────┐        |
|          v (独立子进程 env 注入代理)                                v (直连) |
|  [ RcloneProvider ]                                        [ WebDAVProvider ]|
|   ├── Google Drive (HTTP/SOCKS 代理)                        └── AList WebDAV |
|   └── OneDrive     (直连)                                       ├── 百度网盘  |
|                                                                 └── 夸克网盘  |
+-----------------------------------------------------------------------------+
```

---

## 快速开始

### 1. 环境准备与安装

要求 Python `>= 3.10`。建议在虚拟环境中安装：

```bash
git clone https://github.com/kiive357q/its-still-here.git
cd its-still-here

python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
# Linux / macOS:
# source .venv/bin/activate

# 基础安装（包含文件夹监听、rclone / WebDAV 驱动与 CLI）
pip install -e .

# 如需启用微信 PC 客户端监听（仅 Windows），追加安装 wechat 可选依赖：
pip install -e ".[wechat]"
# 若使用微信 4.0 版本，请确保环境中已安装 wxauto4（例如：pip install wxauto4）
```

> **开发测试安装**：`pip install -e ".[dev]"`，随后可运行 `pytest` 执行单元测试。

### 2. 配置远端存储

#### 方式 A：使用 `rclone` 登录 Google Drive / OneDrive

1. 下载并安装 [rclone](https://rclone.org/downloads/)，确保 `rclone` 已加入系统 `PATH`。
2. 运行交互式配置向导创建对应名称的 remote：

```bash
rclone config
```

- 创建 Google Drive remote 时，建议命名为 `gdrive`（与 `config.example.yaml` 中的 `remote_name: "gdrive"` 对应）。
- 创建 OneDrive remote 时，建议命名为 `onedrive`（与 `remote_name: "onedrive"` 对应）。
- 若国内网络环境需要通过代理完成 Google OAuth 授权，可在运行 `rclone config` 前临时在终端设置代理变量。

#### 方式 B：使用 AList WebDAV 对接百度网盘 / 夸克网盘

1. 在本地或局域网部署 [AList](https://alist.nn.ci/)，并在 AList 后台添加「百度网盘」或「夸克网盘」存储，分别挂载到路径 `/百度网盘` 或 `/夸克网盘`。
2. 确保 AList 的 WebDAV 功能已开启（默认端口 `5244`，WebDAV 根地址为 `http://127.0.0.1:5244/dav/`）。
3. 在配置文件中将 `type` 设为 `"webdav"`，并填入对应的挂载 URL 与账号密码：

```yaml
- name: "alist_domestic"
  type: "webdav"
  enabled: true
  base_url: "http://127.0.0.1:5244/dav/百度网盘" # 夸克网盘可设为 .../dav/夸克网盘
  username: "admin"
  password: "your-alist-password"
  root_path: "ItIsStillHere"
  proxy:
    enabled: false
```

### 3. 启用微信适配器（可选，仅 Windows）

1. 在 Windows 上登录微信 PC 客户端（支持 `3.9` 或 `4.0`）。
2. 确保已安装对应自动化依赖（`wxauto` 对应 `3.9`，`wxauto4` 对应 `4.0`）。
3. 在 `config.yaml` 的 `sources` 中将 `wechat_pc` 的 `enabled` 设为 `true`，并按需填写 `listen_chats`（例如 `["文件传输助手", "核心项目群"]`）。

### 4. 初始化、自检与运行

```bash
# 1. 生成或初始化配置文件与本地工作目录（如 config.yaml）
its-still-here init

# 2. 运行健康检查：验证输入源依赖、代理端口 TCP 连通性、rclone/WebDAV 远端可达性
its-still-here check

# 3. 启动主转存服务（持续监听并按规则执行上传与校验清理）
its-still-here run

# 4. 查看 SQLite 状态数据库中的任务队列、已完成数量与重试状态
its-still-here status
```

---

## 独立代理隔离配置说明

在国内网络环境下，常见需求是：**微信客户端与国内网盘（AList / 百度 / 夸克）保持直连，同时仅让 Google Drive 走本地代理**。如果直接设置全局系统代理或全局 `HTTP_PROXY` 环境变量，往往会导致国内服务绕行甚至连接异常。

**It's still here** 在存储驱动层实现了严格的进程与客户端级代理隔离：

- **微信适配器**：不注入任何代理变量，始终走宿主机直连通道。
- **Google Drive (`RcloneProvider`)**：当 `proxy.enabled: true` 时，`RcloneProvider._build_env()` 会复制当前环境变量字典并在**仅限本次 `rclone` 子进程**的环境中注入 `HTTP_PROXY`、`HTTPS_PROXY` 和 `ALL_PROXY`，主进程 `os.environ` 保持完全纯净。
- **WebDAV (`WebDAVProvider`)**：当需要为特定 WebDAV 走代理时，仅在创建 `httpx.Client(proxy=...)` 时传入代理参数；设为 `enabled: false` 时即强制直连。

示例片段（节选自 [config.example.yaml](config.example.yaml)）：

```yaml
storages:
  # Google Drive：仅该后端的 rclone 子进程走 7890 代理
  - name: "google_drive"
    type: "rclone"
    remote_name: "gdrive"
    proxy:
      enabled: true
      url: "http://127.0.0.1:7890"

  # AList WebDAV（百度网盘/夸克网盘）：完全直连，不受 Google Drive 代理影响
  - name: "alist_domestic"
    type: "webdav"
    base_url: "http://127.0.0.1:5244/dav/百度网盘"
    proxy:
      enabled: false
      url: null
```

`its-still-here check` 会优先使用 TCP 探测检查所配置代理端口（如 `127.0.0.1:7890`）是否可达，再执行 `rclone about gdrive:` 验活，方便快速定位网络与认证问题。

---

## License

本项目基于 [MIT License](LICENSE) 开源。
