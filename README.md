# 多 Bot AI 角色语音系统

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

> 面板完整功能清单见文末「[监控面板说明](#监控面板说明)」。

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

三条链路的接入方式和消息流向见上表「功能特性」。

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
│   ├── bridge.py               # NekroAgent ↔ Stackchan 桥接（含豆包 TTS API）
│   ├── requirements.txt        # bridge 依赖
│   └── nekro-bridge.service.example  # bridge systemd 服务模板
├── tts/
│   └── doubao_v3.py            # 豆包 TTS provider（小智语音引擎调用）
└── deploy/
    ├── docker-compose.yml      # 单实例 NekroAgent + NapCat + Postgres + Qdrant 编排模板
    └── nekro-agent.yaml.example  # NekroAgent 系统配置参考模板（可选，NekroAgent 会自动生成默认配置）
```

---

## 部署指南

### 0. 部署前准备清单

开始前，先确认下面这些都有了（按部署顺序排）：

| # | 要准备的东西 | 哪里获取 | 必选？ |
|---|---|---|---|
| 1 | 一台 Linux 服务器 | 云厂商 VPS / 自建机，需可跑 Docker | ✅ |
| 2 | 每个 Bot 一个 QQ 账号 | 自己的 QQ（建议实名、等级别太低） | ✅ |
| 3 | 模型 API Key | DeepSeek / Gemini / OpenAI 或中转站 | ✅ |
| 4 | 豆包 TTS API Key + 音色 ID | 火山引擎（仅语音链路需要） | ⭕ 语音才要 |
| 5 | 服务器 SSH 登录方式 | 密钥或密码 | ✅ |

部署过程中**需要你亲手填/改的配置**（按步骤）：
- **Step 1**：每个实例的环境变量（端口、token、密码）
- **Step 2**：NapCat WebUI 里的 WebSocket 地址（`ws://容器名:8021`）
- **Step 3**：compose 里多实例的 `TTS_BRIDGE_URL`（仅 2 个以上实例）
- **Step 4/4.5**：NekroAgent 后台的模型组、人设、适配器
- **Step 5**：`bots.json`（每个 Bot 的容器名/token/密码）和 `.env`
- **Step 6**：bridge systemd 服务的环境变量（仅设备语音要）

> 所有配置**都不需要改代码**，改完重启对应服务即可生效。

### 0.5. 端口规划

以 3 个 Bot 为例（数量自定，规律可类推）：

| Bot | NekroAgent | NapCat WebUI | bridge | Postgres 容器 |
|---|---|---|---|---|
| #1 | 8021 | 6099 | 8090 | `nekro_postgres` |
| #2 | 8022 | 6100 | 8091 | `nekro2_nekro_postgres` |
| #3 | 8023 | 6101 | 8092 | `nekro3_nekro_postgres` |

### 1. 部署 NekroAgent 多实例

**方式 A：官方一键脚本（推荐，含 NapCat）**

```bash
sudo -E bash -c "$(curl -fsSL https://raw.githubusercontent.com/KroMiose/nekro-agent/main/docker/install.sh)" - --with-napcat
```

脚本会安装 Docker、生成 `.env`（自动填充随机 token/密码）、拉取官方 compose 并启动。**单实例默认即可用**；多实例时在数据目录的 `.env` 里改 `INSTANCE_NAME` 前缀和端口后重新运行（每个实例一个数据目录）。

> ⚠️ 官方 compose **不含本项目的补丁挂载**，用方式 A 部署后仍需按下方 Step 3 手动追加补丁 volumes。

**方式 B：用本仓库的编排模板**

`deploy/docker-compose.yml` 与官方 compose 几乎一致，差异只有两点：**预置了补丁挂载**（Step 3 的两行 volumes 已写好）+ **固定 NapCat 老版本镜像 `v4.15.0`**（社区公认较稳，见 Step 2 提示）。适合想一步到位、不愿手动改官方 compose 的用户。

复制模板后，按需设置实例前缀与端口即可：

```bash
# 实例 1（数据目录和端口按需改）
export NEKRO_DATA_DIR=$HOME/srv/nekro_agent
export INSTANCE_NAME=""            # 实例 2 用 nekro2_，实例 3 用 nekro3_
export NEKRO_EXPOSE_PORT=8021      # 实例 2 用 8022，依此类推
export NAPCAT_EXPOSE_PORT=6099     # 实例 2 用 6100，依此类推
export ONEBOT_ACCESS_TOKEN=你的QQ协议token   # 与 NA 后台 OneBot 配置一致
export NEKRO_ADMIN_PASSWORD=你的管理员密码    # 首次部署设，之后可在后台改
sudo -E docker compose -f deploy/docker-compose.yml up -d
```

> 多实例 = 复制数据目录后，改 `INSTANCE_NAME` / `NEKRO_EXPOSE_PORT` / `NAPCAT_EXPOSE_PORT` 三个变量再执行一次。容器名与端口务必和 `bots.json` 里填的一致。

> 💡 **`deploy/nekro-agent.yaml.example` 是可选参考**：NekroAgent 首次启动会在数据目录的 `configs/` 下自动生成 `nekro-agent.yaml` 默认配置，**无需手动复制**。只有当你需要预置系统级参数（如记忆开关、日志级别）时才参考此文件手动调整。

### 2. NapCat 登录

```bash
# 实例 1
sudo docker logs nekro_napcat | grep "二维码解码URL"
# 或取出二维码图片
sudo docker cp nekro_napcat:/app/napcat/cache/qrcode.png .

# 实例 2（容器名前缀为 nekro2_）
sudo docker logs nekro2_nekro_napcat | grep "二维码解码URL"
```

用对应 Bot 的 QQ 扫码登录，每个 Bot 重复一次。

> 提示：本模板使用 `mlikiowa/napcat-docker:v4.15.0`（旧架构，社区公认较稳）。若换新版注意登录数据不兼容。

> ⚠️ **NapCat WebSocket 端口十分关键**：扫码登录后，需在 NapCat WebUI 的「网络配置」里新建/修改 OneBot 11 WebSocket 连接，**地址必须填写 `ws://<NekroAgent容器名>:8021/onebot/v11/ws`**（实例 2 为 `ws://nekro2_nekro_agent:8021/...`），即**容器内网络端口 8021**，而不是宿主机端口（`8021` 是容器内端口，宿主映射为 `NEKRO_EXPOSE_PORT`）。填成宿主机 IP + 宿主端口会报 `ECONNREFUSED`。
>
> 另外，豆包 TTS 语音合成需要 NekroAgent 容器能访问宿主机 bridge，详见 [第 6 步](#6-桥接可选-stackchan-设备需要) 的 `TTS_BRIDGE_URL` 说明。

### 3. 应用补丁

把 `patches/` 下两个文件放到每个实例的数据目录下的 `patches/` 子目录中：

```bash
# 实例 1
mkdir -p $HOME/srv/nekro_agent/patches
cp patches/adapter.py patches/commands.py $HOME/srv/nekro_agent/patches/

# 实例 2
mkdir -p $HOME/srv/nekro_agent2/patches
cp patches/adapter.py patches/commands.py $HOME/srv/nekro_agent2/patches/
```

然后在 compose 中挂载（`deploy/docker-compose.yml` 已自带这两行，确认路径正确即可）：

```yaml
volumes:
  - ${NEKRO_DATA_DIR}/patches/adapter.py:/app/nekro_agent/adapters/onebot_v11/adapter.py:ro
  - ${NEKRO_DATA_DIR}/patches/commands.py:/app/nekro_agent/adapters/sse/commands.py:ro
```

- `adapter.py`：QQ 语音回复 + 情感标注 + 语音开关
- `commands.py`：修复设备（SSE）频道被误标为群聊的问题

> 补丁是 bind mount 覆盖框架源码，**镜像升级后需重新打补丁**。

> 💡 **TTS 桥接地址（多实例必看）**：`adapter.py` 的 QQ 语音合成会调用宿主机的 bridge HTTP API，地址读取环境变量 `TTS_BRIDGE_URL`，**默认值 `http://172.21.0.1:8090/api/tts` 只对实例 1（bridge 8090）有效**。实例 2 的 bridge 在 8091、实例 3 在 8092，必须在对应 compose 的 `environment` 中追加（`172.21.0.1` 是 Docker 默认网关，指向宿主机）：
>
> ```bash
> export TTS_BRIDGE_URL=http://172.21.0.1:8091/api/tts   # 实例 2
> # 实例 3 用 8092，依此类推
> ```
>
> 若所有实例共用同一 bridge 端口也可全部指定同一地址。**不设置时语音功能静默失败（返回空白，不影响文字）**，排查语音问题时先确认此变量。

### 4. 配置模型组与人设

模型组和角色人设都可以直接在 **NekroAgent 后台（WebUI）** 里配置，无需改配置文件：

- **模型组**：后台「模型组」页添加 `CHAT_MODEL` / `BASE_URL` / `API_KEY`
- **角色人设**：后台「人设（Preset）」页创建

> 面板支持运行时切换模型组，切完立即生效、无需重启。

### 4.5. 配置适配器（关键！）

首次部署后，需要进 NekroAgent 后台启用两个适配器，否则 QQ 链路和设备链路都不工作：

**① OneBot 适配器（QQ 链路）**

进入后台「适配器」→「OneBot V11」配置：
- `BOT_QQ`：填这个 Bot 的 QQ 号
- `access_token`：填你在 Step 1 中 `export ONEBOT_ACCESS_TOKEN=` 设的值，两者必须一致

**② SSE 适配器（设备链路，桥接需要）**

进入后台「适配器」→「SSE」配置：
- 确认适配器已启用（`ENABLED=true`）
- 设置一个 `access_key`（任意字符串），**记下这个值**，后面配置 bridge 的 systemd 服务时要用到

> 两个适配器配置后可能需要重启容器生效：`sudo docker restart <容器名>`

### 5. 配置监控面板

```bash
cp bots.example.json bots.json        # 填入 Bot 的容器名、URL、token、密码
cp .env.example .env                  # 按需填环境变量
setsid nohup python3 bot_monitor.py > bot_monitor.log 2>&1 < /dev/null &
```

> ⚠️ 面板启动后不要关闭终端。上面的 `setsid nohup ... &` 命令会让面板在 SSH 断开后持续运行。若用普通 `nohup ... &`，SSH 退出后进程可能被 SIGHUP 杀掉。如需重启，先 `kill $(pgrep -f bot_monitor.py)` 再重新执行上述命令。

访问 `http://服务器IP:8080` 打开监控面板，`/chat` 打开网页聊天。

### 6. 桥接（可选，Stackchan 设备需要）

每个 Bot 一个 bridge 实例，建议用 systemd 常驻（监控面板通过 `systemctl is-active` 检测 bridge 状态）。

**① 准备目录与依赖**

```bash
sudo mkdir -p /opt/nekro-bridge
# 把 bridge/bridge.py 复制到 /opt/nekro-bridge/
# 注意：以下命令在仓库根目录执行
sudo cp bridge/bridge.py /opt/nekro-bridge/
sudo pip install -r bridge/requirements.txt
```

**② 配置 systemd 服务**

参考 `bridge/nekro-bridge.service.example`，每个实例复制一份，改端口、地址、音色、历史文件：

```bash
sudo cp bridge/nekro-bridge.service.example /etc/systemd/system/nekro-bridge.service
sudo vim /etc/systemd/system/nekro-bridge.service
sudo systemctl daemon-reload
sudo systemctl enable --now nekro-bridge.service
```

关键环境变量（多实例的差异，写在 `.service` 文件的 `Environment=` 行中）：

| 变量 | 说明 | 实例1 / 实例2 / 实例3 |
|---|---|---|
| `NEKRO_BASE_URL` | 对应 NekroAgent 地址 | 8021 / 8022 / 8023 |
| `LISTEN_PORT` | bridge 监听端口 | 8090 / 8091 / 8092 |
| `ACCESS_KEY` | SSE 访问密钥（与 NekroAgent 后台一致） | 各实例相同 |
| `CHANNEL_ID` | SSE 频道名（固定） | `private_stackchan` |
| `CHAT_HISTORY_FILE` | 聊天历史文件 | `chat_history_1` / `_2` / … |
| `TTS_API_KEY` | 豆包 TTS 密钥 | 各实例可相同 |
| `TTS_RESOURCE_ID` | 豆包 TTS 资源 ID | `seed-icl-2.0` 或 `seed-tts-1.0` |
| `TTS_VOICE` | 豆包音色 ID | 每实例可不同 |
| `OWNER_QQ` / `OWNER_NAME` | 设备消息发送者的 QQ/名字 | 各实例相同 |
| `BRIDGE_DATA_DIR` | bridge 数据目录（TTS 配置/缓存/历史文件） | 默认 `/opt/nekro-bridge`，一般不用改 |

> ⚠️ 服务名务必与 `bots.json` 里的 `bridge_service` 字段一致（默认 `nekro-bridge` / `nekro-bridge2` / …），否则面板读不到 bridge 状态。

> 🔑 **TTS 密钥只放环境变量，别改代码**：`bridge.py` 里 `TTS_API_KEY` 的默认值应为**空字符串**，你的真实密钥**必须**写在 systemd 的 `Environment=TTS_API_KEY=你的密钥` 一行（同样 `TTS_RESOURCE_ID`、`TTS_VOICE` 都通过环境变量传入）。**请勿把真实密钥硬编码进 `bridge.py`**——本仓库是开源的，写进代码等于公开密钥。如果是从旧版本升级，先检查 `/opt/nekro-bridge/bridge.py` 开头有没有残留的硬编码密钥，有的话删掉并改用环境变量，然后 `sudo systemctl restart nekro-bridge`。

### 7. 小智设备（可选，实体语音）

设备链路 = 小智后端（xiaozhi-server）+ bridge + 豆包 TTS，三者配合：

**① 部署 xiaozhi-server**

参考小智官方文档，用 docker-compose 部署：

```yaml
services:
  xiaozhi-esp32-server:
    image: ghcr.nju.edu.cn/xinnan-tech/xiaozhi-esp32-server:server_latest
    container_name: xiaozhi-esp32-server
    restart: always
    security_opt:
      - seccomp:unconfined
    ports:
      - "8000:8000"   # WebSocket 服务
      - "8003:8003"   # OTA / 视觉分析接口
    volumes:
      - ./data:/opt/xiaozhi-esp32-server/data
      - ./models/SenseVoiceSmall/model.pt:/opt/xiaozhi-esp32-server/models/SenseVoiceSmall/model.pt
```

**② 放置豆包 TTS provider**

`tts/doubao_v3.py` 是基于 xiaozhi-server TTS 机制编写的豆包 provider，需放入小智容器的 TTS provider 目录（`core/providers/tts/`），并在小智配置里将 TTS 指向豆包、填入 `voice` 与 `api_key`（具体配置项见小智官方文档）。

**③ 对接 bridge**

小智后端（xiaozhi-server）把 bridge 当作一个 OpenAI 兼容的 LLM 接口调用（`POST /v1/chat/completions`），bridge 再把消息通过 SSE 转发给 NekroAgent。因此：
- 小智配置里的 LLM 接口地址指向 bridge：`http://<bridge地址>:<LISTEN_PORT>/v1/chat/completions`
- bridge 的 `ACCESS_KEY` 与 NekroAgent 后台一致、`CHANNEL_ID=private_stackchan`
- 小智侧无需关心 SSE 细节，由 bridge 内部处理

**④ 面板配置**

环境变量 `DEVICE_BOT_INDEX` 指定设备栏显示在第几个 Bot 卡片。若不需要设备链路，设 `DEVICE_BOT_INDEX=-1` 隐藏设备栏。

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
| `NOTIFY_QQ` | 掉线通知发到哪个 QQ 号 | 空（不通知） |
| `EXTRA_DOCKER_CONTAINERS` | 额外监控的 Docker 容器名（逗号分隔） | 空 |
| `EXTRA_NAV_LINKS` | 面板顶部额外导航链接（JSON 数组） | 空 |
| `XIAOZHI_CONFIG_PATH` | 小智配置文件路径（面板编辑用） | 空 |

> bridge 的 TTS 密钥、音色等配置放在 systemd 服务文件中，见第 6 步。

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

> **提醒**：需要在每个角色的人设（Preset）里加入下面这段指令，LLM 才会输出情感标签：

> 【语音情感标注】每次回复必须先从以下 17 种类型中选一个，在回复正文最前面用 `[[emotion:类型]]` 开头标注（仅用于语音合成，不显示给用户），然后再写正文：tsundere傲娇、charming娇媚、happy开心、sad难过、angry生气、scare害怕、surprise惊讶、tear哭腔、lovey-dovey撒娇、comfort安慰、energetic元气、annoyed嗔怪、pleased愉悦、sorry抱歉、conniving绿茶、storytelling讲故事、novel_dialog平和。即使没有强烈情感，也必须选最接近的一种标注，禁止不标注；严格禁止使用列表以外的任何类型（如 shy、sleepy 等一律禁用），禁止自创。回复分多段时，每段都要标注。

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
