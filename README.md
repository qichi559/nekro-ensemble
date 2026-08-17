# 三 Bot AI 角色语音系统

> **本项目基于 [NekroAgent](https://github.com/KroMiose/nekro-agent) 构建**，并遵循 Nekro Agent 开源协议 V1.1。详见 [`NEKRO_LICENSE`](./NEKRO_LICENSE) 与 [`THIRD_PARTY_NOTICES.md`](./THIRD_PARTY_NOTICES.md)。

一套基于 [NekroAgent](https://github.com/KroMiose/nekro-agent) 的**多实例 AI 角色语音系统**。你可以同时运行任意多个 AI 角色，每个角色拥有独立的人格、记忆和模型，并同时接入 **QQ**、**网页聊天** 和 **实体语音设备（Stackchan）** 三条链路，再配上一个实时监控面板统一管理。

> 典型场景：想让多个不同人设的 AI 角色同时在线陪你聊天，一个在 QQ 群里、一个在网页上、一个对着实体语音设备说话——这套系统就是为此设计的。

---

## 功能特性

### 多 Bot 并行（数量不限）
- N 个独立 NekroAgent 实例，各自独立人格、独立记忆、独立模型组
- **数量、端口、容器名、服务名全部可自定义**，想跑 1 个还是 10 个都行
- 缺省按序号自动递增（端口 8021/8022/…、bridge 8090/8091/…），无需手动逐个配置

### QQ 语音回复
- QQ 收到消息后，文字回复 ≤ 250 字时自动合成语音一并发出（文字 + 语音两条消息）
- 支持 **LLM 显式情感标注**：角色在回复时输出 `[[emotion:xx]]` 情感标签，语音合成时按该情感选择语气，比关键词猜测更准确
- 面板提供 **QQ 语音开关**，一键开启/关闭自动语音

### 三链路接入
| 链路 | 接入方式 | 说明 |
|---|---|---|
| QQ | NekroAgent ↔ NapCat ↔ QQ | OneBot v11 协议，支持群聊/私聊 |
| 网页 | 浏览器 ↔ 面板 `/chat` | 内置网页聊天页，多 Bot 切换 |
| 设备 | Stackchan ↔ bridge ↔ NekroAgent | 实体语音设备（小智 ESP32） |

### 监控面板
- 容器运行状态、CPU/内存实时监控
- 容器日志查看、重启控制
- **掉线检测**：监控 NapCat 日志，检测到被踢下线后自动重启并通过其他在线 Bot 发 QQ 通知
- **一键切换模型组**：运行时切换所有 Bot 的模型，无需重启容器
- 网页聊天、TTS 音色试听与配置

---

## 核心概念

| 名称 | 是什么 |
|---|---|
| **NekroAgent** | 底层 AI 角色框架（KroMiose 开源），负责角色人设、LLM 调用、记忆、插件 |
| **NapCat** | 第三方 QQ 协议端，把 QQ 消息桥接给 NekroAgent（OneBot v11） |
| **bridge** | 本项目的桥接服务，把 Stackchan 设备的消息转发给 NekroAgent，并提供 TTS 合成 API |
| **Stackchan** | 实体语音机器人（小智 ESP32），通过 SSE 与 bridge 通信 |
| **bot_monitor** | 本项目的一体化监控面板 + 网页聊天 |

---

## 架构

```
                          ┌──────────────────────────────────────────┐
                          │            bot_monitor.py 监控面板        │
                          │              http://0.0.0.0:8080          │
                          │   容器控制 · 日志 · 掉线通知 · 模型切换   │
                          │   网页聊天(/chat) · TTS配置               │
                          └────────────────┬─────────────────────────┘
                                           │ 控制 / 监控 / 切换模型
              ┌────────────────┬───────────┴───────────┬───────────────┐
              ▼                ▼                       ▼               ▼
        NekroAgent #1     NekroAgent #2          NekroAgent #3      bridge
         :8021            :8022                   :8023              :8090/8091/8092
              │                │                       │               │
              ▼                ▼                       ▼               ▼
         NapCat #1         NapCat #2              NapCat #3        Stackchan
         (QQ 登录)         (QQ 登录)              (QQ 登录)         (实体设备)
```

三条链路：

1. **QQ**：QQ 消息 → NapCat（协议端）→ NekroAgent（OneBot v11）→ LLM 回复 → 语音合成 → 发回 QQ
2. **网页**：浏览器打开面板的 `/chat`，直接通过面板转发到对应 Bot 的 NekroAgent
3. **设备**：Stackchan 语音 → bridge → NekroAgent（SSE 适配器）→ 回复 → bridge TTS → 设备播放

---

## 环境要求

| 依赖 | 版本/说明 |
|---|---|
| 服务器 | Linux（推荐 Ubuntu），需可运行 Docker |
| Docker | 20.10+，需 `docker compose` v2 |
| Python | 3.9+（运行 `bot_monitor.py` 与 `bridge.py`） |
| QQ 账号 | 每个 Bot 一个（建议实名、等级别太低，机房 IP 有风控风险） |
| 模型 API | DeepSeek / Gemini / OpenAI 或任意中转站 API Key |
| 豆包 TTS（可选） | 火山引擎语音合成 API Key（仅语音链路需要） |

---

## 目录结构

```
.
├── bot_monitor.py              # 监控面板（含网页聊天），单文件无依赖
├── bots.example.json           # Bot 配置模板（复制为 bots.json）
├── .env.example                # 环境变量模板（复制为 .env）
├── patches/
│   ├── adapter.py              # QQ 语音补丁：语音回复 + 情感标注 + 语音开关
│   └── commands.py             # SSE 补丁：修复 channel_type 被硬编码为 group 的问题
├── bridge/
│   └── bridge.py               # NekroAgent ↔ Stackchan 桥接（含豆包 TTS API）
├── tts/
│   └── doubao_v3.py            # 豆包 TTS provider（小智语音引擎调用）
└── deploy/
    ├── docker-compose.yml      # 单实例 NekroAgent + NapCat + Postgres + Qdrant 编排模板
    └── nekro-agent.yaml.example  # NekroAgent 系统配置模板
```

---

## 部署指南

### 0. 端口规划

以 3 个 Bot 为例（数量自定，规律可类推）：

| Bot | NekroAgent | NapCat WebUI | bridge | Postgres 容器 |
|---|---|---|---|---|
| #1 | 8021 | 6099 | 8090 | `nekro_postgres` |
| #2 | 8022 | 6100 | 8091 | `nekro2_nekro_postgres` |
| #3 | 8023 | 6101 | 8092 | `nekro3_nekro_postgres` |

### 1. 部署 NekroAgent 多实例

**方式 A：官方一键脚本（单实例起步）**

```bash
sudo -E bash -c "$(curl -fsSL https://raw.githubusercontent.com/KroMiose/nekro-agent/main/quick_start_x_napcat.sh)"
```

**方式 B：用本仓库的编排模板**

参考 `deploy/docker-compose.yml`。多实例时复制数据目录，并修改 `.env` 中的端口与 `INSTANCE_NAME`：

```bash
# 实例 1
export NEKRO_DATA_DIR=$HOME/srv/nekro_agent
export INSTANCE_NAME=""            # 单实例留空
export NEKRO_EXPOSE_PORT=8021
export NAPCAT_EXPOSE_PORT=6099

# 实例 2（复制后改前缀）
export NEKRO_DATA_DIR=$HOME/srv/nekro_agent2
export INSTANCE_NAME="nekro2_"     # 容器名会变成 nekro2_nekro_agent 等
export NEKRO_EXPOSE_PORT=8022
export NAPCAT_EXPOSE_PORT=6100
```

> ⚠️ 容器名与端口务必和 `bots.json` 里填的一致。

### 2. NapCat 登录

```bash
sudo docker logs napcat | grep "二维码解码URL"
# 或取出二维码图片
sudo docker cp napcat:/app/napcat/cache/qrcode.png .
```

用对应 Bot 的 QQ 扫码登录。三个 Bot 重复三次。

> 提示：本模板使用 `mlikiowa/napcat-docker:v4.15.0`（旧架构，社区公认较稳）。若换新版注意登录数据不兼容。

### 3. 应用补丁

把 `patches/` 下两个文件放到每个实例的数据目录，并在 compose 中挂载：

```yaml
volumes:
  - ${NEKRO_DATA_DIR}/patches/adapter.py:/app/nekro_agent/adapters/onebot_v11/adapter.py:ro
  - ${NEKRO_DATA_DIR}/patches/commands.py:/app/nekro_agent/adapters/sse/commands.py:ro
```

- `adapter.py`：QQ 语音回复 + 情感标注 + 语音开关
- `commands.py`：修复设备（SSE）频道被误标为群聊的问题

> 补丁是 bind mount 覆盖框架源码，**镜像升级后需重新打补丁**。

### 4. 配置模型组与人设

模型组和角色人设都可以直接在 **NekroAgent 后台（WebUI）** 里配置，无需改配置文件：

- **模型组**：后台「模型组」页添加 `CHAT_MODEL` / `BASE_URL` / `API_KEY`
- **角色人设**：后台「人设（Preset）」页创建

> 面板支持运行时切换模型组，切完立即生效、无需重启。

### 5. 配置监控面板

```bash
cp bots.example.json bots.json        # 填入 Bot 的容器名、URL、token、密码
cp .env.example .env                  # 按需填环境变量
python3 bot_monitor.py
```

访问 `http://服务器IP:8080` 打开监控面板，`/chat` 打开网页聊天。

### 6. 桥接（可选，Stackchan 设备需要）

每个 Bot 一个 bridge 实例。先安装依赖：

```bash
pip install -r bridge/requirements.txt
```

然后启动（每个 Bot 一个实例，改端口和地址）：

```bash
export LISTEN_PORT=8090          # 对应第 1 个 Bot
export NEKRO_BASE_URL=http://127.0.0.1:8021
export ACCESS_KEY=你的SSE访问密钥
export TTS_API_KEY=你的豆包TTS密钥
export TTS_VOICE=你的音色ID
python3 bridge/bridge.py
```

### 7. 小智设备（可选）

设备链路依赖小智 ESP32 server（`xiaozhi-server`），`tts/doubao_v3.py` 是其豆包语音引擎的 provider。若不需要设备链路，跳过本步，并在环境变量中设 `DEVICE_BOT_INDEX=-1` 隐藏设备栏。

---

## 配置详解

### bots.json 字段

想跑几个 Bot 就往数组里加几个对象。除标注「是」的外，其余字段缺省都会自动推导：

| 字段 | 必填 | 说明 |
|---|---|---|
| `role` | 是 | 角色名，面板/聊天页显示 |
| `nekro_container` | 是 | NekroAgent 容器名 |
| `napcat_container` | 是 | NapCat 容器名 |
| `nekro_url` | 是 | NekroAgent WebUI 地址 |
| `napcat_url` | 是 | NapCat WebUI 地址 |
| `napcat_token` | 是 | NapCat WebUI 登录 token |
| `nekro_token` | 是 | NekroAgent access token |
| `admin_password` | 是 | NekroAgent 管理员密码 |
| `name` | 否 | 显示名（用于排序） |
| `qq` | 否 | Bot 的 QQ 号（头像用） |
| `nekro_username` | 否 | 默认 `admin` |
| `napcat_http_port` | 否 | NapCat 容器内 HTTP 端口 |
| `bridge_port` | 否 | bridge 端口，缺省 `8090 + (序号-1)` |
| `bridge_service` | 否 | bridge systemd 服务名，缺省 `nekro-bridge`/`nekro-bridge2`/… |
| `postgres_container` | 否 | postgres 容器名，缺省由 `nekro_container` 推导 |
| `data_dir` | 否 | 宿主机数据目录（用于面板读写 `voice_switch` 开关和读模型组，不填则相关功能跳过） |

### 环境变量（.env）

| 变量 | 说明 | 默认 |
|---|---|---|
| `SITE_PASSWORD` | 面板访问密码 | 留空则随机生成 |
| `USER_QQ` | 你的 QQ 号（网页聊天用户头像） | 空 |
| `DEVICE_BOT_INDEX` | 设备栏显示在第几个 Bot 卡片（0-based） | `2` |
| `TTS_API_KEY` | 豆包 TTS 密钥（bridge 用） | 空 |
| `TTS_VOICE` | 豆包 TTS 音色 ID | 空（需自己填） |
| `TTS_RESOURCE_ID` | 豆包 TTS 资源 ID（音色克隆 `seed-icl` / 通用 `seed-tts`） | 空 |
| `TTS_BRIDGE_URL` | adapter 调用的 TTS 接口地址 | `http://172.21.0.1:8090/api/tts` |
| `OWNER_QQ` / `OWNER_NAME` | bridge 里设备消息发送者的 QQ/名字 | 空 |
| `LISTEN_PORT` | bridge 监听端口（每个实例不同） | `8090` |
| `BRIDGE_DATA_DIR` | bridge 数据目录（TTS 配置与缓存） | `/opt/nekro-bridge` |

### 模型组

模型组在 **NekroAgent 后台（WebUI）** 里配置。系统支持**运行时切换**：

- 在后台「模型组」页预定义多个组（如 `deepseek`、`gemini`）
- 面板下拉一键切换，**无需重启容器**
- 所有 Bot 会一起切换

### QQ 语音开关

每个 Bot 数据目录下放一个 `voice_switch` 文件：

- 内容为 `0` → 关闭自动语音回复
- 内容为其他 / 不存在 → 开启

面板上也有对应开关，操作后即时生效。

### 语音情感标注

角色人设在回复时输出 `[[emotion:情感]]` 标签（17 种预设情感之一）。`adapter.py` 会：

1. 剥离标签，纯文字部分正常发送
2. 把情感传给 bridge 的 TTS 接口，合成对应语气的语音

这样语音的情感就不再靠关键词瞎猜，而是由 LLM 显式指定。

---

## 监控面板说明

面板（8080 端口）提供：

- **系统信息**：服务器 CPU/内存/磁盘、Docker 状态
- **Bot 卡片**：每个 Bot 的 NapCat / NekroAgent 运行状态、CPU/内存、日志、重启按钮
- **模型栏**：当前模型组 + 下拉切换（所有 Bot 一起切）
- **掉线通知**：检测到 Bot 被踢下线，自动重启并通过其他在线 Bot 发 QQ 消息
- **语音开关**：每个 Bot 的 QQ 语音回复开关
- **网页聊天**（`/chat`）：直接和任意 Bot 文字聊天，支持语音播放

---

## 免责声明

本项目基于 NekroAgent 与 NapCat 构建。NapCat 为第三方 QQ 协议实现，**请遵守相关平台服务条款，仅用于学习交流，不得用于任何商业用途或违反平台规定的行为**。使用本项目产生的任何后果由使用者自行承担。

## 许可证

[MIT](LICENSE)
