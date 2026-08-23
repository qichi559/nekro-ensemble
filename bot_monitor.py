#!/usr/bin/env python3
"""
小栖bot - NekroAgent & NapCat 多Bot实时监控面板
功能: 容器控制 / 日志查看 / Token免输入 / PWA
"""

import subprocess
import json
import sys
import os
import re
import hashlib
import threading
import time
import urllib.request
import urllib.error
import http.client
import select
import socket
from queue import Queue, Empty
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def _load_dotenv():
    """加载同目录 .env 文件到环境变量（不覆盖已存在的环境变量）"""
    env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_file):
        return
    try:
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception:
        pass


_load_dotenv()

HOST = "0.0.0.0"
PORT = 8080
COLLECT_INTERVAL = 15

# 面板访问密码：优先从环境变量 SITE_PASSWORD 读取，未设置则随机生成并在启动日志打印一次
SITE_PASSWORD = os.getenv("SITE_PASSWORD", "")
if not SITE_PASSWORD:
    import secrets
    SITE_PASSWORD = secrets.token_urlsafe(12)
    print(f"[Init] 未设置 SITE_PASSWORD，已生成随机密码: {SITE_PASSWORD}", flush=True)

# Bot 配置：从同目录 bots.json 读取（可用环境变量 BOTS_CONFIG_FILE 指定路径）
# bots.json 不存在时 BOTS 为空，面板会提示先配置。参考 bots.example.json 填写。
BOTS_CONFIG_FILE = os.getenv(
    "BOTS_CONFIG_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bots.json"),
)


def _load_bots():
    if os.path.exists(BOTS_CONFIG_FILE):
        try:
            with open(BOTS_CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[Init] 读取 {BOTS_CONFIG_FILE} 失败: {e}", flush=True)
    return []


BOTS = _load_bots()

ALLOWED_CONTAINERS = set()
for b in BOTS:
    ALLOWED_CONTAINERS.add(b["nekro_container"])
    ALLOWED_CONTAINERS.add(b["napcat_container"])

# 额外监控的Docker容器（环境变量逗号分隔，如 "container1,container2"）
EXTRA_DOCKER_CONTAINERS = [x.strip() for x in os.getenv("EXTRA_DOCKER_CONTAINERS", "").split(",") if x.strip()]
ALLOWED_CONTAINERS.update(EXTRA_DOCKER_CONTAINERS)

# 额外监控的服务
EXTRA_SERVICES = []

# 额外导航链接（环境变量 EXTRA_NAV_LINKS，JSON 数组格式）
try:
    EXTRA_NAV_LINKS = json.loads(os.getenv("EXTRA_NAV_LINKS", "[]"))
except Exception:
    EXTRA_NAV_LINKS = []

# 掉线通知配置：接收掉线通知的QQ号，留空则不通知
NOTIFY_QQ = os.getenv("NOTIFY_QQ", "")

# 小智LLM模式切换配置（可选，无小智部署时留空）
XIAOZHI_CONFIG_PATH = os.getenv("XIAOZHI_CONFIG_PATH", "")
DIRECT_LLM_NAME = "DirectLLM"          # 直连LLM配置名称（需在.config.yaml的LLM段中定义）
# 设备（小智 stackchan）绑定到第几个 bot 的卡片（0-based，如 2 表示第 3 个 bot；-1 表示无设备）
DEVICE_BOT_INDEX = int(os.getenv("DEVICE_BOT_INDEX", "2"))

# 模型切换配置（本地预设文件，已废弃保留兼容）
MODEL_PRESETS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model_presets.json")


def get_bot_data_dir(bot_index):
    """bot_index 1-based：返回该 bot 的宿主机数据目录（bots.json 的 data_dir 字段，缺省空）"""
    try:
        idx = int(bot_index)
        if 1 <= idx <= len(BOTS):
            return BOTS[idx - 1].get("data_dir", "") or ""
    except Exception:
        pass
    return ""

MONITOR_API_ROUTES = {"/api/status", "/api/refresh", "/api/container", "/api/nekro-go", "/api/nekro-back", "/api/kick-restart", "/api/service-control", "/api/note-sync-toggle", "/api/llm-mode", "/api/toggle-llm-mode", "/api/model-presets", "/api/apply-model", "/api/test-model", "/api/delete-model-group", "/api/create-model-group", "/api/fetch-models", "/api/qq-voice", "/api/chat/history", "/api/chat/status", "/api/chat/send", "/api/chat/tts", "/api/generate-prompt", "/api/chat/generate-prompt", "/api/chat-context", "/api/visual-benchmarks", "/api/visual-benchmarks/reset"}

def is_monitor_route(path):
    if path in MONITOR_API_ROUTES:
        return True
    if path.startswith("/api/logs/"):
        return True
    if path.startswith("/api/svc-logs/"):
        return True
    if is_chat_route(path):
        return True
    return False


# ===== Bridge 聊天代理 =====
# bridge 端口：优先读 bots.json 每个 bot 的 bridge_port 字段，缺省按 8090 + (index-1) 递增


def get_bridge_port(bot_index):
    """bot_index 为 1-based 的 bot 序号，返回对应 bridge 端口"""
    try:
        idx = int(bot_index)
        if 1 <= idx <= len(BOTS):
            b = BOTS[idx - 1]
            if b.get("bridge_port"):
                return int(b["bridge_port"])
    except Exception:
        pass
    try:
        return 8090 + int(bot_index) - 1
    except Exception:
        return 8090


def get_bridge_port_from_query(qs):
    """从 query string 解析 bot 参数, 返回对应 bridge 端口"""
    try:
        from urllib.parse import parse_qs
        d = parse_qs(qs)
        if 'bot' in d:
            return get_bridge_port(int(d['bot'][0]))
    except Exception:
        pass
    return get_bridge_port(1)


def get_bridge_port_from_body(body):
    """从 JSON body 解析 bot 参数, 返回 (端口, 去掉bot字段后的body)"""
    port = get_bridge_port(1)
    new_body = body
    try:
        payload = json.loads(body) if body else {}
        if 'bot' in payload:
            port = get_bridge_port(int(payload['bot']))
            payload.pop('bot', None)
            new_body = json.dumps(payload, ensure_ascii=False)
    except Exception:
        pass
    return port, new_body


def proxy_to_bridge(method, path, body=None, port=8090):
    """代理请求到 bridge.py"""
    import urllib.request
    import urllib.error
    url = f"http://127.0.0.1:{port}" + path
    try:
        req = urllib.request.Request(url, method=method)
        if body:
            req.data = body.encode("utf-8")
            req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=130) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:
        return 502, json.dumps({"ok": False, "msg": str(e)}, ensure_ascii=False).encode()

def is_chat_route(path):
    """检查是否是聊天相关路由"""
    return path.startswith("/api/chat/")

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr, flush=True)

def run_cmd(cmd, timeout=10):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            err = result.stderr.strip()
            if err:
                log(f"CMD FAILED: {cmd[:80]} | stderr: {err[:200]}")
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        log(f"CMD TIMEOUT ({timeout}s): {cmd[:80]}")
        return ""
    except Exception as e:
        log(f"CMD ERROR: {cmd[:80]} | {e}")
        return ""

def check_docker():
    result = run_cmd("docker info --format '{{.ServerVersion}}' 2>&1", timeout=5)
    if result and not result.startswith("Cannot"):
        return True, result
    return False, result or "docker not available"

def get_container_statuses():
    cmd = "docker inspect --format='{{.Name}}|{{.State.Status}}' $(docker ps -aq) 2>/dev/null"
    output = run_cmd(cmd, timeout=8)
    statuses = {}
    for line in output.split("\n"):
        line = line.strip()
        if "|" in line:
            parts = line.split("|")
            name = parts[0].lstrip("/")
            status = parts[1] if len(parts) > 1 else "unknown"
            statuses[name] = status
    return statuses

def get_container_stats_fast():
    all_containers = [b["nekro_container"] for b in BOTS] + [b["napcat_container"] for b in BOTS] + EXTRA_DOCKER_CONTAINERS
    cmd = "docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}' " + " ".join(all_containers)
    output = run_cmd(cmd, timeout=15)
    stats = {}
    for line in output.split("\n"):
        line = line.strip()
        if "|" in line:
            parts = line.split("|")
            if len(parts) >= 4:
                name = parts[0].strip()
                stats[name] = {"cpu": parts[1].strip(), "mem": parts[2].strip(), "mem_pct": parts[3].strip()}
    return stats

# NapCat WebUI API 凭证缓存 {bot_index: {"credential": "...", "time": timestamp}}
napcat_webui_credential_cache = {}

def get_napcat_credential(bot_index, bot):
    """获取 NapCat WebUI API 凭证（带缓存，50分钟内复用）"""
    cached = napcat_webui_credential_cache.get(bot_index)
    if cached and (time.time() - cached["time"]) < 3000:
        return cached["credential"]

    port_match = re.search(r":(\d+)", bot["napcat_url"])
    if not port_match:
        return None
    port = port_match.group(1)
    token = bot.get("napcat_token", "")
    if not token:
        return None

    # NapCat WebUI 认证: hash = sha256(token + '.napcat') 的十六进制
    password_hash = hashlib.sha256((token + '.napcat').encode()).hexdigest()
    login_url = f"http://localhost:{port}/api/auth/login"
    login_data = json.dumps({"hash": password_hash}).encode("utf-8")
    try:
        req = urllib.request.Request(login_url, data=login_data,
            headers={"Content-Type": "application/json"}, method="POST")
        resp = urllib.request.urlopen(req, timeout=5)
        result = json.loads(resp.read().decode("utf-8"))
        if result.get("code") == 0:
            credential = result.get("data", {}).get("Credential")
            if credential:
                napcat_webui_credential_cache[bot_index] = {"credential": credential, "time": time.time()}
                return credential
        else:
            # 可能启用了2FA或token不匹配，清除缓存
            napcat_webui_credential_cache.pop(bot_index, None)
    except Exception as e:
        log(f"napcat webui login failed (bot {bot_index}): {e}")
    return None

def get_napcat_online_via_webui(bot_index, bot):
    """通过 NapCat WebUI API 精确检查 QQ 在线状态（权威方法）"""
    credential = get_napcat_credential(bot_index, bot)
    if not credential:
        return None, None

    port_match = re.search(r":(\d+)", bot["napcat_url"])
    if not port_match:
        return None, None
    port = port_match.group(1)

    # 调用 CheckLoginStatus API
    status_url = f"http://localhost:{port}/api/QQLogin/CheckLoginStatus"
    try:
        req = urllib.request.Request(status_url, data=b"{}",
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {credential}"}, method="POST")
        resp = urllib.request.urlopen(req, timeout=5)
        result = json.loads(resp.read().decode("utf-8"))
        if result.get("code") == 0:
            data = result.get("data", {})
            is_login = data.get("isLogin", False)
            is_offline = data.get("isOffline", False)
            login_error = data.get("loginError", "")
            if is_login:
                return True, "在线"
            elif is_offline:
                return False, "掉线"
            elif login_error:
                return False, f"离线"
            else:
                return False, "未登录"
        elif result.get("code") != 0 and "Unauthorized" in result.get("message", ""):
            # 凭证过期，清除缓存
            napcat_webui_credential_cache.pop(bot_index, None)
    except Exception as e:
        log(f"napcat webui check status failed (bot {bot_index}): {e}")
    return None, None

def get_napcat_online_via_logs(napcat_container):
    """通过日志分析检查 QQ 在线状态（fallback 方法）"""
    output = run_cmd(f"docker logs {napcat_container} --tail 100 2>&1", timeout=8)
    if not output:
        return None, "no logs"
    lines = output.split("\n")

    last_offline_pos = -1
    last_offline_msg = ""
    last_online_pos = -1

    offline_patterns = [
        ("KickedOffLine", "被踢下线"),
        ("下线通知", "被踢下线"),
        ("账号状态变更为离线", "离线"),
        ("被挤下线", "被踢下线"),
        ("账号被冻结", "被冻结"),
        ("登录失效", "离线"),
    ]

    online_keywords = ["登录成功", "ServerTime", "时间同步", "同步时间"]

    for i, line in enumerate(lines):
        for kw, msg in offline_patterns:
            if kw in line:
                last_offline_pos = i
                last_offline_msg = msg
        for kw in online_keywords:
            if kw in line:
                last_online_pos = i
        if "上线" in line and not any(x in line for x in ["重新", "尝试", "失败"]):
            last_online_pos = i

    if last_offline_pos >= 0 and last_offline_pos >= last_online_pos:
        return False, last_offline_msg
    if last_online_pos >= 0 and last_online_pos > last_offline_pos:
        return True, "在线"

    last_20_text = "\n".join(lines[-20:]) if len(lines) >= 20 else "\n".join(lines)
    if "离线" in last_20_text and "上线" not in last_20_text:
        return False, "离线"

    return None, "未知"

def get_napcat_online(bot_index, bot):
    """检查 NapCat QQ 在线状态: 优先 WebUI API，失败回退日志分析"""
    # 方法1: WebUI API（权威，直接查询 QQ 登录状态）
    online, msg = get_napcat_online_via_webui(bot_index, bot)
    if online is not None:
        return online, msg
    # 方法2: 日志分析（fallback）
    return get_napcat_online_via_logs(bot["napcat_container"])

# 掉线通知
notify_cooldown = {}  # {bot_index: last_notify_time} - 仅用于发送失败时1分钟重试
notified_offline = set()  # 已通知掉线的bot索引集合，一次掉线只通知一次
NOTIFY_COOLDOWN = 600  # 冷却时间（仅用于失败重试间隔）

def send_qq_notify_via_napcat(napcat_container, http_port, target_qq, message):
    """通过 NapCat 的 OneBot HTTP API 发送 QQ 私聊消息（端口直接从配置读取，不依赖配置文件解析）"""
    url = f"http://127.0.0.1:{http_port}/send_private_msg"
    data = json.dumps({"user_id": int(target_qq), "message": message})

    cmd = ["docker", "exec", napcat_container, "curl", "-s", "-X", "POST", url,
           "-H", "Content-Type: application/json", "-d", data]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.stdout:
            resp = json.loads(result.stdout)
            if resp.get("status") == "ok":
                return True, "ok"
            return False, resp.get("msg", "send failed")
    except Exception as e:
        log(f"send notify failed: {e}")
    return False, "send failed"

def check_and_notify(bots_data):
    """检测掉线并通过其他在线 Bot 发送通知"""
    if not NOTIFY_QQ:
        return

    now = time.time()
    for i, bot_data in enumerate(bots_data):
        online = bot_data.get("online")
        online_msg = bot_data.get("online_msg", "")

        if online is False:
            # 一次掉线只通知一次，已通知过的跳过
            if i in notified_offline:
                continue
            # 发送失败后1分钟内不重试
            last_notify = notify_cooldown.get(i, 0)
            if (now - last_notify) < 60:
                continue

            # 找其他在线 Bot 发送通知
            notified = False
            for j, other_bot in enumerate(bots_data):
                if i == j:
                    continue
                if other_bot.get("online") is not True:
                    continue
                if not other_bot.get("napcat_running", False):
                    continue

                napcat_container = other_bot.get("napcat_container", "")
                http_port = BOTS[j].get("napcat_http_port", 0)
                if not http_port:
                    continue
                bot_name = bot_data.get("name", "")
                bot_role = bot_data.get("role", "")
                bot_qq = bot_data.get("qq", "")
                # 通知消息统一显示"掉线"，避免"未登录了喵~"
                display_msg = "掉线" if online_msg == "未登录" else online_msg
                message = f"{bot_role}{display_msg}了喵~"

                ok, msg = send_qq_notify_via_napcat(napcat_container, http_port, NOTIFY_QQ, message)
                if ok:
                    log(f"notify sent: {bot_name} offline, via {other_bot.get('name', '')}")
                    notified = True
                    break
                else:
                    log(f"notify failed via {other_bot.get('name', '')}: {msg}")

            if notified:
                # 通知成功，标记为已通知，不再重复发送
                notified_offline.add(i)
                notify_cooldown[i] = now
            else:
                # 没有可用的在线 Bot，1分钟后重试
                notify_cooldown[i] = now
        elif online is True:
            # 重启后5分钟内不清除通知标记，避免重启后短暂在线又掉线导致重复通知
            last_restart = kick_restart_cooldown.get(i, 0)
            if (now - last_restart) < 300:
                continue
            if i in notified_offline:
                log(f"{bot_data.get('name', '')} back online, notify flag cleared")
                notified_offline.discard(i)
            if i in notify_cooldown:
                del notify_cooldown[i]

def get_nekro_connection(nekro_container):
    output = run_cmd(f"docker logs {nekro_container} --tail 200 2>&1", timeout=8)
    if not output:
        return "无法获取日志"
    meaningful_lines = []
    has_health_check = False
    for line in output.split("\n"):
        if "/api/health" in line or ("GET /api/" in line and "health" in line):
            has_health_check = True
            continue
        meaningful_lines.append(line)
    meaningful = "\n".join(meaningful_lines[-30:]) if meaningful_lines else output
    last_status = None
    for line in meaningful.split("\n"):
        low = line.lower()
        if "closed by peer" in low or "connection closed" in low or "disconnected" in low:
            last_status = "disconnected"
        elif "connected" in low and "closed" not in low:
            last_status = "connected"
        elif "connection open" in low:
            last_status = "connected"
    if last_status == "connected":
        return "已连接"
    elif last_status == "disconnected":
        return "已断开"
    if has_health_check and "closed by peer" not in meaningful.lower():
        return "已连接"
    return "未知"

def get_system_info():
    mem_info = run_cmd("free -b | awk '/Mem:/ {printf \"%.1f/%.1f\", $3/1073741824, $2/1073741824}'", timeout=5)
    mem_pct = run_cmd("free | awk '/Mem:/ {printf \"%.0f\", $3/$2*100}'", timeout=5)
    disk_info = run_cmd("df -h / | awk 'NR==2 {printf \"%s/%s (%s)\", $3, $2, $5}'", timeout=5)
    cpu_info = run_cmd("top -bn1 | awk '/Cpu/ {printf \"%.1f%%\", $2+$4}'", timeout=5)
    return {
        "mem": mem_info + " GB" if mem_info else "N/A",
        "mem_pct": mem_pct + "%" if mem_pct else "N/A",
        "disk": disk_info if disk_info else "N/A",
        "cpu": cpu_info if cpu_info else "N/A",
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

def collect_bot_data(bot, bot_index, statuses, stats):
    napcat_name = bot["napcat_container"]
    nekro_name = bot["nekro_container"]
    napcat_running = statuses.get(napcat_name) == "running"
    nekro_running = statuses.get(nekro_name) == "running"
    napcat_stats = stats.get(napcat_name, {"cpu": "N/A", "mem": "N/A", "mem_pct": "N/A"})
    nekro_stats = stats.get(nekro_name, {"cpu": "N/A", "mem": "N/A", "mem_pct": "N/A"})
    online, online_msg = get_napcat_online(bot_index, bot)
    nekro_conn = get_nekro_connection(nekro_name)
    napcat_webui_url = bot["napcat_url"] + "/webui?token=" + bot.get("napcat_token", "")
    preset_name = get_bot_preset_name(bot_index + 1)
    return {
        "name": bot["name"], "role": bot["role"], "qq": bot["qq"],
        "preset_name": preset_name,
        "napcat_url": napcat_webui_url,
        "napcat_container": napcat_name, "nekro_container": nekro_name,
        "napcat_running": napcat_running, "nekro_running": nekro_running,
        "napcat_stats": napcat_stats, "nekro_stats": nekro_stats,
        "online": online, "online_msg": online_msg, "nekro_conn": nekro_conn,
    }

def collect_data():
    errors = []
    docker_ok, docker_ver = check_docker()
    if not docker_ok:
        return {"system": {"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "mem": "N/A", "mem_pct": "N/A", "disk": "N/A", "cpu": "N/A"}, "bots": [], "error": f"Docker unavailable: {docker_ver}"}
    with ThreadPoolExecutor(max_workers=3) as pool:
        future_statuses = pool.submit(get_container_statuses)
        future_stats = pool.submit(get_container_stats_fast)
        future_system = pool.submit(get_system_info)
        statuses = future_statuses.result()
        stats = future_stats.result()
        system = future_system.result()
    if not statuses:
        errors.append("container status empty")
    if not stats:
        errors.append("container stats timeout")
    bots_data = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        future_bots = {pool.submit(collect_bot_data, bot, i, statuses, stats): bot for i, bot in enumerate(BOTS)}
        for future in as_completed(future_bots):
            try:
                bots_data.append(future.result())
            except Exception as e:
                bot = future_bots[future]
                errors.append(f"{bot['name']} error: {e}")
                _bi = 1
                try:
                    _bi = BOTS.index(bot) + 1
                except Exception:
                    pass
                bots_data.append({"name": bot["name"], "role": bot["role"], "qq": bot["qq"], "preset_name": get_bot_preset_name(_bi), "napcat_url": bot["napcat_url"], "napcat_container": bot["napcat_container"], "nekro_container": bot["nekro_container"], "napcat_running": False, "nekro_running": False, "napcat_stats": {"cpu": "N/A", "mem": "N/A", "mem_pct": "N/A"}, "nekro_stats": {"cpu": "N/A", "mem": "N/A", "mem_pct": "N/A"}, "online": None, "online_msg": "采集失败", "nekro_conn": "采集失败"})
    # 按 BOTS 配置顺序排序，确保索引与 BOTS 一致
    order = {b["name"]: i for i, b in enumerate(BOTS)}
    bots_data.sort(key=lambda x: order.get(x["name"], 99))
    # 为每个bot附加踢下线重启开关状态
    for i, bot_data in enumerate(bots_data):
        bot_data['kick_restart'] = kick_restart_enabled.get(i, True)
    # 掉线通知（通过其他在线Bot发送QQ消息，先于重启执行，使用原始掉线原因）
    check_and_notify(bots_data)
    # 被踢下线自动重启检测（可能修改 online_msg 为"重启"）
    check_kick_restart(bots_data)
    # 桥接服务状态（服务名优先读 bots.json 的 bridge_service 字段，缺省按 nekro-bridge/nekro-bridge2/... 递增）
    bridge_services = [b.get("bridge_service") or ("nekro-bridge" if i == 0 else f"nekro-bridge{i+1}") for i, b in enumerate(BOTS)]
    bridge_out = run_cmd("systemctl is-active " + " ".join(bridge_services), timeout=5)
    bridge_states = [s.strip() == "active" for s in bridge_out.strip().split("\n") if s.strip()]
    for i, bot_data in enumerate(bots_data):
        bot_data["bridge_active"] = bridge_states[i] if i < len(bridge_states) else False
        bot_data["bridge_service"] = bridge_services[i] if i < len(bridge_services) else ""
    # 额外服务监控（已精简）
    extra_services = collect_extra_services(statuses, stats)
    # 笔记同步状态（各 bot）
    note_sync = check_note_sync()
    # 小智LLM模式 + 设备状态
    llm_mode = get_llm_mode()
    device = {"running": statuses.get("xiaozhi-esp32-server") == "running", "llm_mode": llm_mode, "container": "xiaozhi-esp32-server"}
    error_msg = "; ".join(errors) if errors else None
    return {"system": system, "bots": bots_data, "extra_services": extra_services, "extra_nav": EXTRA_NAV_LINKS, "error": error_msg, "auto_restart_records": auto_restart_records[-10:], "note_sync": note_sync, "llm_mode": llm_mode, "device": device}

class DataCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._data = None
        self._collecting = False
    def get(self):
        with self._lock:
            if self._data is None:
                return None
            return self._data.copy() if isinstance(self._data, dict) else self._data
    def collect_now(self):
        with self._lock:
            if self._collecting:
                return
            self._collecting = True
        try:
            log("collecting...")
            t0 = time.time()
            data = collect_data()
            elapsed = time.time() - t0
            log(f"done ({elapsed:.1f}s), bots={len(data.get('bots', []))}")
            with self._lock:
                self._data = data
        except Exception as e:
            log(f"collect error: {e}")
            with self._lock:
                self._data = {"system": {"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "mem": "N/A", "mem_pct": "N/A", "disk": "N/A", "cpu": "N/A"}, "bots": [], "error": f"error: {e}"}
        finally:
            with self._lock:
                self._collecting = False
    def background_loop(self):
        while True:
            try:
                self.collect_now()
            except Exception as e:
                log(f"bg error: {e}")
            time.sleep(COLLECT_INTERVAL)
    def start(self):
        t = threading.Thread(target=self.background_loop, daemon=True)
        t.start()
        log(f"bg thread started ({COLLECT_INTERVAL}s)")

cache = DataCache()

# 重启记录（踢下线自动重启使用）
auto_restart_records = []  # [{time, container, reason}]

# 被踢下线自动重启 NapCat
kick_restart_enabled = {i: True for i in range(len(BOTS))}  # 每个Bot的踢下线重启开关
kick_restart_cooldown = {}  # {bot_index: last_restart_time} 冷却记录
KICK_RESTART_INTERVAL = 180  # 被踢下线后3分钟才重启，避免频繁重启
KICK_RESTART_MAX_PER_HOUR = 5  # 每小时最多重启5次
kick_restart_counts = {}  # {bot_index: [timestamp, ...]} 每小时重启次数记录

def control_container(container_name, action, is_manual=True):
    if container_name not in ALLOWED_CONTAINERS:
        return False, f"container {container_name} not allowed"
    if action not in ("start", "stop", "restart"):
        return False, f"action {action} not supported"
    run_cmd(f"docker {action} {container_name}", timeout=30)
    am = {"start": "启动", "stop": "停止", "restart": "重启"}
    msg = f"{container_name} 已{am.get(action, action)}"
    log(f"ctrl: {container_name} {action}")
    return True, msg

def check_kick_restart(bots_data):
    now = time.time()
    for i, bot_data in enumerate(bots_data):
        if not kick_restart_enabled.get(i, True):
            continue
        online_msg = bot_data.get('online_msg', '')
        online = bot_data.get('online')
        # 处理掉线、被踢下线或离线状态（不处理"未登录"，避免重启循环）
        if online is False and ('踢下线' in online_msg or '离线' in online_msg or '掉线' in online_msg):
            napcat_container = bot_data.get('napcat_container', '')
            napcat_running = bot_data.get('napcat_running', False)
            if not napcat_running:
                continue
            # 检查冷却时间
            last_restart = kick_restart_cooldown.get(i, 0)
            if (now - last_restart) < KICK_RESTART_INTERVAL:
                continue
            # 检查每小时重启次数
            counts = kick_restart_counts.get(i, [])
            counts = [t for t in counts if (now - t) < 3600]
            if len(counts) >= KICK_RESTART_MAX_PER_HOUR:
                log(f"kick-restart skipped: bot {i} reached max {KICK_RESTART_MAX_PER_HOUR}/hour")
                continue
            # 执行重启
            run_cmd(f"docker restart {napcat_container}", timeout=30)
            kick_restart_cooldown[i] = now
            counts.append(now)
            kick_restart_counts[i] = counts
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            reason = f"QQ{online_msg}"
            auto_restart_records.append({"time": ts, "container": napcat_container, "reason": reason})
            if len(auto_restart_records) > 50:
                auto_restart_records.pop(0)
            log(f"kick-restart: {napcat_container} ({reason})")
            bot_data['online_msg'] = '重启'
            time.sleep(2)

def get_container_logs(container_name, lines=150):
    if container_name not in ALLOWED_CONTAINERS:
        return False, "not allowed", ""
    output = run_cmd(f"docker logs {container_name} --tail {lines} 2>&1", timeout=15)
    return True, "ok", output

def get_systemd_status(service_name):
    output = run_cmd(f"systemctl is-active {service_name} 2>/dev/null", timeout=5)
    return output.strip() == "active"

def get_systemd_logs(service_name, lines=150):
    output = run_cmd(f"journalctl -u {service_name} --no-pager -n {lines} 2>&1", timeout=15)
    return True, "ok", output

def collect_extra_services(statuses, stats):
    result = []
    for svc in EXTRA_SERVICES:
        if svc["type"] == "systemd":
            running = get_systemd_status(svc["service"])
            item = {
                "name": svc["name"], "type": "systemd",
                "service": svc["service"], "desc": svc["desc"],
                "running": running,
                "stats": {"cpu": "N/A", "mem": "N/A", "mem_pct": "N/A"},
            }
            if "url" in svc:
                item["url"] = svc["url"]
            result.append(item)
        elif svc["type"] == "docker":
            container = svc["container"]
            running = statuses.get(container) == "running"
            container_stats = stats.get(container, {"cpu": "N/A", "mem": "N/A", "mem_pct": "N/A"})
            item = {
                "name": svc["name"], "type": "docker",
                "container": container, "desc": svc["desc"],
                "running": running, "stats": container_stats,
            }
            if "url" in svc:
                item["url"] = svc["url"]
            result.append(item)
    return result

def get_pg_container(bot_index):
    """bot_index 1-based：优先读 bots.json 的 postgres_container 字段，缺省由 nekro_container 推导"""
    try:
        idx = int(bot_index)
        if 1 <= idx <= len(BOTS):
            b = BOTS[idx - 1]
            if b.get("postgres_container"):
                return b["postgres_container"]
            return b.get("nekro_container", "").replace("nekro_agent", "nekro_postgres")
    except Exception:
        pass
    return ""


def get_bot_preset_name(bot_index):
    """读取某 bot 的 Nekro 默认人设名字（AI_CHAT_DEFAULT_PRESET_ID 指向的 presets 表 name）。

    bot_index 1-based。读取 yaml 的 AI_CHAT_DEFAULT_PRESET_ID（缺省 1），
    再 docker exec psql 查 presets 表对应 id 的 name。
    带进程内缓存（预设不常变，缓存 60s），查询失败返回空串。
    """
    try:
        idx = int(bot_index)
        if not (1 <= idx <= len(BOTS)):
            return ""
        key = idx
        now = time.time()
        cached = _preset_name_cache.get(key)
        if cached and now - cached["time"] < 60:
            return cached["name"]
        # 1) 读 yaml 拿 AI_CHAT_DEFAULT_PRESET_ID（缺省 1）
        dd = get_bot_data_dir(idx)
        preset_id = "1"
        if dd:
            p = os.path.join(dd, "configs", "nekro-agent.yaml")
            txt = run_cmd(f"cat {p}", timeout=5) or ""
            m = re.search(r"^AI_CHAT_DEFAULT_PRESET_ID:\s*(.+)", txt, re.MULTILINE)
            if m:
                val = m.group(1).strip().split("#")[0].strip()
                if val:
                    preset_id = val
        # 2) 查数据库 presets 表
        pg = get_pg_container(idx)
        if not pg:
            _preset_name_cache[key] = {"name": "", "time": now}
            return ""
        sql = "SELECT name FROM presets WHERE id = %s" % preset_id
        cmd = 'docker exec %s psql -U nekro_agent -d nekro_agent -t -A -c "%s"' % (pg, sql.replace('"', '\\"'))
        out = run_cmd(cmd, timeout=10)
        name = out.strip()
        _preset_name_cache[key] = {"name": name, "time": now}
        return name
    except Exception as e:
        log(f"preset name check error (bot {bot_index}): {e}")
        return ""


_preset_name_cache = {}


def check_note_sync_one(pg):
    """检查单个库的笔记插件双向同步状态"""
    try:
        trigger_cmd = 'docker exec %s psql -U nekro_agent -d nekro_agent -t -c "SELECT tgname, tgenabled FROM pg_trigger WHERE tgname LIKE \'sync_note%%\'"' % pg
        trigger_output = run_cmd(trigger_cmd, timeout=10)
        triggers = {}
        for line in trigger_output.split("\n"):
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 2:
                triggers[parts[0]] = (parts[1] == "O")  # O=enabled, D=disabled

        qq_to_sse = triggers.get("sync_note_qq_to_sse", False)
        sse_to_qq = triggers.get("sync_note_sse_to_qq", False)

        qq_private_key = ("onebot_v11-private_" + NOTIFY_QQ) if NOTIFY_QQ else "onebot_v11-private_"
        content_cmd = 'docker exec %s psql -U nekro_agent -d nekro_agent -A -t -c "SELECT target_chat_key, md5(data_value), CASE WHEN jsonb_typeof(data_value::jsonb->\'notes\')=\'object\' THEN (SELECT count(*) FROM jsonb_each(data_value::jsonb->\'notes\')) ELSE 0 END FROM plugin_data WHERE plugin_key = \'KroMiose.note\' AND target_chat_key IN (\'%s\', \'sse-private_stackchan\')"' % (pg, qq_private_key)
        content_output = run_cmd(content_cmd, timeout=10)

        qq_notes = 0
        sse_notes = 0
        qq_hash = ""
        sse_hash = ""
        for line in content_output.split("\n"):
            line = line.strip()
            if not line or "|" not in line:
                continue
            parts = line.split("|")
            chat_key = parts[0].strip()
            h = parts[1].strip() if len(parts) > 1 else ""
            try:
                note_count = int(parts[2].strip()) if len(parts) > 2 else 0
            except Exception:
                note_count = 0
            if "onebot_v11-private" in chat_key:
                qq_notes = note_count
                qq_hash = h
            elif "sse-private" in chat_key:
                sse_notes = note_count
                sse_hash = h

        content_synced = qq_hash == sse_hash
        return {
            "running": content_synced and qq_to_sse and sse_to_qq,
            "triggers": {"qq_to_sse": qq_to_sse, "sse_to_qq": sse_to_qq},
            "qq_notes": qq_notes, "sse_notes": sse_notes,
            "content_synced": content_synced,
        }
    except Exception as e:
        log(f"note sync check error ({pg}): {e}")
        return {"running": False, "error": str(e), "triggers": {"qq_to_sse": False, "sse_to_qq": False}, "qq_notes": 0, "sse_notes": 0, "content_synced": False}


def check_note_sync():
    """检查各 bot 的笔记同步状态"""
    result = {}
    for i, _ in enumerate(BOTS):
        pg = get_pg_container(i + 1)
        if pg:
            result[str(i + 1)] = check_note_sync_one(pg)
    return result

def toggle_note_sync_trigger(direction, enabled, bot=1):
    """启用或禁用指定 bot 的笔记同步触发器"""
    trigger_map = {"qq_to_sse": "sync_note_qq_to_sse", "sse_to_qq": "sync_note_sse_to_qq"}
    trigger_name = trigger_map.get(direction)
    if not trigger_name:
        return False, "invalid direction"
    pg = get_pg_container(bot)
    if not pg:
        return False, "未找到对应数据库容器"
    action = "ENABLE" if enabled else "DISABLE"
    cmd = f'docker exec {pg} psql -U nekro_agent -d nekro_agent -c "ALTER TABLE plugin_data {action} TRIGGER {trigger_name}"'
    output = run_cmd(cmd, timeout=10)
    if "ERROR" in output:
        return False, output
    log(f"note sync trigger {trigger_name}: {action}")
    return True, f"{'开启' if enabled else '关闭'} {direction} 同步"


def get_llm_mode():
    """读取小智当前LLM模式: nekro(NekroAgent) / direct(直连LLM) / unknown"""
    config = run_cmd(f"cat {XIAOZHI_CONFIG_PATH}", timeout=5)
    if not config:
        return "unknown"
    lines = config.split("\n")
    in_selected = False
    for line in lines:
        if line.startswith("selected_module:"):
            in_selected = True
            continue
        if in_selected:
            if line and not line[0].isspace():
                break
            stripped = line.strip()
            if stripped.startswith("LLM:"):
                val = stripped.split(":", 1)[1].strip()
                if val == "ChatGLMLLM":
                    return "nekro"
                elif val == DIRECT_LLM_NAME:
                    return "direct"
                return "unknown"
    return "unknown"


def prettify_model(name):
    """模型名美化显示"""
    if not name:
        return ""
    table = {
        "deepseek-v4-flash": "DeepSeek V4 Flash",
        "deepseek-v4-pro": "DeepSeek V4 Pro",
        "gemini-3.7-flash": "Gemini 3.7 Flash",
        "gemini-3.7-flash-high@按次": "Gemini 3.7 Flash",
        "gemini-2.5-flash-image-preview": "Gemini 2.5 Flash Image",
    }
    if name in table:
        return table[name]
    # 兜底：去 @ 后缀、分隔符转空格、词首大写
    n = name.split("@")[0]
    n = n.replace("-", " ").replace("_", " ")
    return " ".join(w[:1].upper() + w[1:] for w in n.split() if w)


def fetch_nekro_model_groups():
    """从各 bot 拉取并合并聊天模型组列表（按 group_name 去重），返回 ([...], err)"""
    merged = {}
    err = None
    for bot_index in range(len(BOTS)):
        ok, result = _nekro_config_request(bot_index, "/model-groups")
        if not ok:
            err = f"bot{bot_index + 1} 拉取失败: {result}"
            continue
        for gname, g in (result or {}).items():
            if g.get("MODEL_TYPE") != "chat":
                continue
            chat_model = g.get("CHAT_MODEL", "")
            if not chat_model:
                continue  # 跳过空组（default）
            if gname not in merged:
                merged[gname] = {
                    "group_name": gname,
                    "chat_model": chat_model,
                    "base_url": g.get("BASE_URL", ""),
                    "api_key": g.get("API_KEY", ""),
                    "pretty": prettify_model(chat_model),
                }
    return list(merged.values()), err


def save_model_presets(presets):
    """保存模型预设列表"""
    with open(MODEL_PRESETS_FILE, "w", encoding="utf-8") as f:
        json.dump({"presets": presets}, f, ensure_ascii=False, indent=2)
    return True


def get_current_model():
    """读取各 bot 当前 USE_MODEL_GROUP（模型组名）"""
    for i, _ in enumerate(BOTS):
        dd = get_bot_data_dir(i + 1)
        if not dd:
            continue
        p = os.path.join(dd, "configs", "nekro-agent.yaml")
        txt = run_cmd(f"cat {p}", timeout=5) or ""
        m = re.search(r"USE_MODEL_GROUP:\s*(.+)", txt)
        if m:
            return m.group(1).strip().split(" #")[0].strip()
    return "unknown"


def get_voice_switch_states():
    """读取各 bot 的 QQ 语音回复开关状态（voice_switch 文件，内容 "0" 为关，缺省为开）"""
    states = []
    for i, b in enumerate(BOTS):
        enabled = True
        dd = b.get("data_dir", "") or ""
        if dd:
            p = os.path.join(dd, "voice_switch")
            txt = run_cmd(f"cat {p}", timeout=5)
            if txt is not None:
                enabled = txt.strip() != "0"
        states.append({"bot_index": i, "enabled": enabled})
    return states


def set_voice_switch_state(bot_index, enabled):
    """写某个 bot 的 QQ 语音开关（"1" 开 / "0" 关），sudo 写防 root 属主"""
    if bot_index < 0 or bot_index >= len(BOTS):
        return False, "无效的 Bot 索引"
    dd = get_bot_data_dir(bot_index + 1)
    if not dd:
        return False, "该 Bot 未配置 data_dir"
    p = os.path.join(dd, "voice_switch")
    val = "1" if enabled else "0"
    tmp = f"/tmp/voice_switch_{bot_index}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(val)
        run_cmd(f"sudo -n cp {tmp} {p}", timeout=10)
        txt = run_cmd(f"cat {p}", timeout=5)
        if txt is not None and txt.strip() == val:
            return True, "已更新"
        return False, "写入校验失败"
    except Exception as e:
        return False, f"写入失败: {e}"


def _set_default_model_group(path, chat_model, base_url, api_key):
    """替换 yaml 里 default 模型组的 CHAT_MODEL/BASE_URL/API_KEY（sudo 写，防 root 属主）"""
    txt = run_cmd(f"cat {path}", timeout=5)
    if not txt:
        log(f"read {path} failed (empty)")
        return False
    lines = txt.split("\n")
    in_groups = False
    in_default = False
    for i, line in enumerate(lines):
        if line.rstrip() == "MODEL_GROUPS:":
            in_groups = True
            continue
        if in_groups and line == "  default:":
            in_default = True
            continue
        if in_default:
            if line and not line[0].isspace():
                break
            if line.startswith("  ") and not line.startswith("    "):
                break
            stripped = line.strip()
            if stripped.startswith("CHAT_MODEL:"):
                lines[i] = "    CHAT_MODEL: " + chat_model
            elif stripped.startswith("BASE_URL:"):
                lines[i] = "    BASE_URL: " + base_url
            elif stripped.startswith("API_KEY:"):
                lines[i] = "    API_KEY: " + api_key
    tmp = "/tmp/model_patch_" + os.path.basename(os.path.dirname(path)) + ".yaml"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
    except Exception as e:
        log(f"write tmp {tmp} failed: {e}")
        return False
    run_cmd(f"sudo -n cp {tmp} {path}", timeout=10)
    verify = run_cmd(f"cat {path}", timeout=5) or ""
    if chat_model in verify:
        return True
    log(f"sudo cp to {path} verify failed")
    return False


def _nekro_config_request(bot_index, path, method="GET", body=None):
    """带 401 自动重登的 nekro config API 请求，返回 (ok, result_or_err)"""
    token, err = get_nekro_jwt(bot_index)
    if not token:
        return False, f"登录失败: {err}"
    m = re.search(r":(\d+)", BOTS[bot_index].get("nekro_url", ""))
    if not m:
        return False, "端口解析失败"
    url = f"http://localhost:{m.group(1)}/api/config{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Authorization": f"Bearer {token}"}
    if data is not None:
        headers["Content-Type"] = "application/json"

    def do_req(tok):
        h = dict(headers)
        h["Authorization"] = f"Bearer {tok}"
        req = urllib.request.Request(url, data=data, method=method, headers=h)
        return urllib.request.urlopen(req, timeout=10)

    try:
        resp = do_req(token)
        return True, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as he:
        if he.code == 401:
            # token 失效（容器重启过），清缓存重登重试一次
            nekro_jwt_cache.pop(bot_index, None)
            token, err = get_nekro_jwt(bot_index)
            if not token:
                return False, f"重登失败: {err}"
            try:
                resp = do_req(token)
                return True, json.loads(resp.read().decode("utf-8"))
            except Exception as e:
                return False, f"重试异常: {e}"
        return False, f"HTTP {he.code}: {he}"
    except Exception as e:
        return False, f"请求异常: {e}"


def _sync_xiaozhi_model_group(group_name, chat_model, base_url, api_key):
    """同步小智 .config.yaml：把 LLM 段下 DirectLLM 模块的
    base_url/model_name/api_key 更新为目标模型组的值（自适应缩进，不写死空格数）。

    注意：只同步 DirectLLM（直连模式）。ChatGLMLLM 走 bridge→NekroAgent，
    由面板切换 USE_MODEL_GROUP 自动跟随，不可改为直连目标，避免破坏桥接链路。
    失败不抛异常，只 log。返回 (ok, msg)。
    """
    if not XIAOZHI_CONFIG_PATH or not os.path.exists(XIAOZHI_CONFIG_PATH):
        return True, "未配置小智 XIAOZHI_CONFIG_PATH，跳过同步"
    try:
        with open(XIAOZHI_CONFIG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        log(f"[xiaozhi-sync] read config failed: {e}")
        return False, f"读小智配置失败: {e}"

    # 找到 LLM: 顶层段范围（到下一个顶层键为止）
    llm_start = None
    for i, line in enumerate(lines):
        if line.rstrip() == "LLM:" or line.rstrip().startswith("LLM: "):
            llm_start = i
            break
    if llm_start is None:
        log("[xiaozhi-sync] LLM section not found")
        return False, "小智配置中未找到 LLM 段"

    # 解析段内模块/字段缩进宽度（自适应）
    mod_indent = None  # 模块名缩进（如 '  ChatGLMLLM:' 的 2 空格）
    field_indent = None  # 字段缩进（如 '    type:' 的 4 空格）
    mods = []  # [(模块缩进行, 模块名), ...]
    for i in range(llm_start + 1, len(lines)):
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            continue
        if not line[0].isspace() and stripped:
            break  # 顶层键 → 段结束
        indent = len(line) - len(line.lstrip())
        if line[indent:].rstrip().endswith(":"):
            # 模块名行（冒号结尾、无空格分隔）
            if mod_indent is None:
                mod_indent = indent
            if field_indent is None and mod_indent is not None and indent > mod_indent:
                field_indent = indent
            if indent <= (field_indent or 10**9):
                # 记录顶层模块（缩进 == mod_indent）
                mods.append((indent, stripped[:-1]))
                continue
            # 更深层（可能是子配置），忽略
            continue
        # 普通字段行：若还没确定 field_indent，取第一个字段的缩进
        if field_indent is None and mod_indent is not None and indent > mod_indent:
            field_indent = indent
    if mod_indent is None:
        log("[xiaozhi-sync] no modules under LLM section")
        return False, "LLM 段下未找到任何模块"

    # 用 field_indent 作为字段缩进；若多个模块层级一致，直接取 mod_indent+2 兜底
    if field_indent is None:
        field_indent = mod_indent + 2

    changed = 0
    current_mod = None
    for i in range(llm_start + 1, len(lines)):
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            continue
        if not line[0].isspace() and stripped:
            break
        indent = len(line) - len(line.lstrip())
        if indent == mod_indent and stripped.endswith(":"):
            current_mod = stripped[:-1]
            continue
        if current_mod == "DirectLLM" and indent == field_indent:
            indent_str = line[:indent]
            if stripped.startswith("base_url:"):
                lines[i] = f"{indent_str}base_url: {base_url}\n"
                changed += 1
            elif stripped.startswith("model_name:"):
                lines[i] = f"{indent_str}model_name: {chat_model}\n"
                changed += 1
            elif stripped.startswith("api_key:") and api_key:
                lines[i] = f"{indent_str}api_key: {api_key}\n"
                changed += 1

    if changed == 0:
        log("[xiaozhi-sync] no fields matched, nothing to write")
        return False, "小智配置未匹配到可同步字段（DirectLLM）"

    try:
        with open(XIAOZHI_CONFIG_PATH, "w", encoding="utf-8") as f:
            f.writelines(lines)
    except Exception as e:
        log(f"[xiaozhi-sync] write config failed: {e}")
        return False, f"写小智配置失败: {e}"
    log(f"[xiaozhi-sync] {group_name} -> DirectLLM ({chat_model} @ {base_url}) 已同步")
    return True, f"小智配置已同步为 {group_name}"


def apply_model_to_all(group_name, chat_model=None, base_url=None, api_key=None):
    """切换各 bot 的 USE_MODEL_GROUP（目标组缺失则自动创建），不重启"""
    import urllib.parse as _up
    _gq = _up.quote(group_name, safe="")
    # 1. 先找目标组的完整配置（从任意有该组的 bot）
    group_config = None
    for bi in range(len(BOTS)):
        ok, result = _nekro_config_request(bi, "/model-groups")
        if ok and group_name in (result or {}):
            group_config = result[group_name]
            break
    if group_config is None:
        # 各 bot 都没有，用传入参数构造
        if not chat_model or not base_url:
            return False, f"组 {group_name} 不存在且缺少模型信息"
        group_config = {
            "GROUP_NAME": "", "CHAT_MODEL": chat_model, "CHAT_PROXY": "",
            "BASE_URL": base_url, "API_KEY": api_key or "",
            "MODEL_TYPE": "chat", "ENABLE_VISION": True, "ENABLE_COT": False,
            "TOKEN_INPUT_RATE": 1.0, "TOKEN_COMPLETION_RATE": 1.0, "MODEL_PRICE_RATE": 1.0,
            "TEMPERATURE": None, "TOP_P": None, "TOP_K": None,
            "PRESENCE_PENALTY": None, "FREQUENCY_PENALTY": None, "EXTRA_BODY": None
        }
    # 2. 对每个 bot：确保有目标组（缺失则用找到的配置创建），切换 + 持久化
    for bot_index in range(len(BOTS)):
        ok, result = _nekro_config_request(bot_index, "/model-groups")
        if not ok:
            return False, f"bot{bot_index + 1} {result}"
        if group_name not in (result or {}):
            ok, r = _nekro_config_request(bot_index, f"/model-groups/{_gq}", method="POST", body=group_config)
            if not ok or not (r or {}).get("ok"):
                return False, f"bot{bot_index + 1} 创建组失败: {r}"
        # 切换 USE_MODEL_GROUP（内存即时生效）
        ok, r = _nekro_config_request(bot_index, f"/set/system/USE_MODEL_GROUP?value={_gq}", method="POST")
        if not ok or not (r or {}).get("ok"):
            return False, f"bot{bot_index + 1} 切换失败: {r}"
        # 持久化到 yaml
        ok, r = _nekro_config_request(bot_index, "/save/system", method="POST")
        if not ok or not (r or {}).get("ok"):
            return False, f"bot{bot_index + 1} 保存失败: {r}"
    # 3. 同步小智语音（若有）：更新 DirectLLM 指向目标模型组 + 后台重启小智
    if XIAOZHI_CONFIG_PATH and os.path.exists(XIAOZHI_CONFIG_PATH):
        ok_sync, sync_msg = _sync_xiaozhi_model_group(group_name, group_config["CHAT_MODEL"], group_config["BASE_URL"], group_config["API_KEY"])
        if ok_sync:
            def _bg_restart_xz():
                run_cmd("docker restart xiaozhi-esp32-server", timeout=30)
                log(f"xiaozhi restarted after model group: {group_name}")
            threading.Thread(target=_bg_restart_xz, daemon=True).start()
            return True, f"已切换到 {group_name}（即时生效；{sync_msg}，小智重启中）"
    return True, f"已切换到 {group_name}（即时生效，无需重启）"


def delete_model_group_from_all(group_name):
    """删除所有 bot 的同名模型组（与『切换即全量创建』对称：加是全加，删也应全删）。

    NA 接口：DELETE /api/config/model-groups/{group_name}
    对每个 bot 执行删除，跳过该 bot 上不存在的组（404，不算失败），汇总结果。
    """
    import urllib.parse as _up
    _gq = _up.quote(group_name, safe="")
    ok_count = 0
    skip_count = 0
    fail_list = []
    for bot_index in range(len(BOTS)):
        # 直接发 DELETE，把 404(不存在) 当 skip，其余失败计入
        ok, result = _nekro_config_raw_request(bot_index, f"/model-groups/{_gq}", method="DELETE")
        if ok:
            ok_count += 1
        elif result == 404:
            skip_count += 1
        else:
            fail_list.append(f"bot{bot_index + 1}: {result}")
    msg = f"已从 {ok_count} 个 bot 删除 {group_name}"
    if skip_count:
        msg += f"（{skip_count} 个 bot 无此组，跳过）"
    if fail_list:
        msg += f"；失败: {'; '.join(fail_list)}"
    return (len(fail_list) == 0), msg


def _nekro_config_raw_request(bot_index, path, method="GET", body=None):
    """类似 _nekro_config_request，但失败时返回 (ok, HTTP状态码或错误串)。
    用于需要对 404 单独处理的场景（如删除不存在资源）。
    """
    token, err = get_nekro_jwt(bot_index)
    if not token:
        return False, f"登录失败: {err}"
    m = re.search(r":(\d+)", BOTS[bot_index].get("nekro_url", ""))
    if not m:
        return False, "端口解析失败"
    url = f"http://localhost:{m.group(1)}/api/config{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        resp = urllib.request.urlopen(req, timeout=10)
        return True, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as he:
        if he.code == 401:
            nekro_jwt_cache.pop(bot_index, None)
            token, err = get_nekro_jwt(bot_index)
            if not token:
                return False, f"重登失败: {err}"
            try:
                req = urllib.request.Request(url, data=data, headers={**headers, "Authorization": f"Bearer {token}"}, method=method)
                resp = urllib.request.urlopen(req, timeout=10)
                return True, json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as he2:
                return False, he2.code
            except Exception as e2:
                return False, f"重试异常: {e2}"
        return False, he.code
    except Exception as e:
        return False, f"请求异常: {e}"


def create_model_group_on_all(group_name, chat_model, base_url, api_key):
    """在所有 bot 上创建同名模型组（与删除对称：加是全加）。

    NA 接口：POST /api/config/model-groups/{group_name}
    对每个 bot 执行创建，跳过已存在的组（409/400，不算失败），汇总结果。
    """
    if not group_name or not chat_model or not base_url:
        return False, "模型组名、模型名、接口地址不能为空"
    import urllib.parse as _up
    _gq = _up.quote(group_name, safe="")
    body = {
        "GROUP_NAME": group_name,
        "CHAT_MODEL": chat_model,
        "BASE_URL": base_url,
        "API_KEY": api_key or "",
        "MODEL_TYPE": "chat",
        "ENABLE_VISION": True,
        "ENABLE_COT": False,
        "TOKEN_INPUT_RATE": 1.0,
        "TOKEN_COMPLETION_RATE": 1.0,
        "MODEL_PRICE_RATE": 1.0,
        "TEMPERATURE": None,
        "TOP_P": None,
        "TOP_K": None,
        "PRESENCE_PENALTY": None,
        "FREQUENCY_PENALTY": None,
        "EXTRA_BODY": None,
    }
    ok_count = 0
    skip_count = 0
    fail_list = []
    for bot_index in range(len(BOTS)):
        ok, result = _nekro_config_raw_request(bot_index, f"/model-groups/{_gq}", method="POST", body=body)
        if ok:
            ok_count += 1
        elif result in (400, 409):
            skip_count += 1
        else:
            fail_list.append(f"bot{bot_index + 1}: {result}")
    msg = f"已在 {ok_count} 个 bot 创建模型组 {group_name}"
    if skip_count:
        msg += f"（{skip_count} 个 bot 已存在该组，跳过）"
    if fail_list:
        msg += f"；失败: {'; '.join(fail_list)}"
    return (len(fail_list) == 0), msg


def fetch_models_from_api(base_url, api_key):
    """调用 OpenAI 兼容接口 /models 获取模型列表，返回 (ok, models_or_err)。
    base_url 可能带 /v1（如 https://xxx/v1）也可能不带（如 https://xxx），自动识别。
    """
    u = base_url.rstrip("/")
    if not u.endswith("/v1"):
        u = u + "/v1"
    url = u + "/models"
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read().decode("utf-8"))
        models = []
        for m in (data.get("data") or []):
            mid = m.get("id") or m.get("model") or ""
            if mid:
                models.append(mid)
        if not models:
            return False, "接口返回的模型列表为空"
        models = sorted(set(models))
        return True, models
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8")[:200]
        except Exception:
            detail = str(e)
        return False, f"HTTP {e.code}: {detail}"
    except Exception as e:
        return False, str(e)



# ===== AI 场景生图 Prompt 提炼模块 =====

BOT_PERSONA_VISUAL_BENCHMARKS = {
    1: {
        "name": "雾岛澪",
        "role": "魅魔死宅同居女友",
        "tags": "- 核心外貌: 1girl, solo, succubus, gothic, dark hair, jet black hair, very long hair, straight hair, blunt bangs, low twintails, cold blue eyes, calm eyes, mole under right eye, beauty mark under eye, voluptuous, large breasts, deep cleavage, tiny waist, wide hips, slender legs\n- 服饰: black off-shoulder sweater, off-shoulder knit, bare shoulders, black miniskirt, black thighhighs, zettai ryouiki, cross necklace, choker\n- 魅魔特征: small devil horns, demon horns, black tail, heart-shaped tail tip\n- 场景: 现代温馨公寓、客厅沙发、昏暗温馨暖光、游戏手柄、慵懒死宅氛围",
        "fallback_positive": "masterpiece, best quality, ultra-detailed, anime artwork, 1girl, solo, succubus, gothic, jet black hair, very long hair, straight hair, blunt bangs, low twintails, cold blue eyes, half-closed eyes, mole under right eye, beauty mark, voluptuous, large breasts, deep cleavage, tiny waist, wide hips, slender legs, black off-shoulder sweater, off-shoulder knit, bare shoulders, black miniskirt, black thighhighs, cross necklace, choker, small devil horns, demon horns, black tail, heart-shaped tail tip, sitting on couch, holding game controller, cozy modern apartment living room, sofa, warm ambient lighting, dim room, soft light",
        "fallback_summary": "深夜的温馨公寓里，暖黄色的台灯静静亮着。澪慵懒地蜷缩在客厅沙发上，抱着抱枕靠着你，墨黑色的低双马尾散落下来。头顶微小的恶魔角与心形尾巴尖在放松时悄悄探出，冰蓝色的眼眸带着微醺般的娇懒与依恋，正专注地看着屏幕与你依偎在一起。",
    },
    2: {
        "name": "霁月",
        "role": "情感过敏症犬系依恋少女/合租人",
        "tags": "- 核心外貌: 1girl, solo, dog-like dependent girl, sweet face, gentle expression, light flax hair, light brown hair, very long wavy hair, airy bangs, white flower hair ornament behind ear, amber eyes, light brown eyes, sweet expression, contrasting voluptuous body, gigantic breasts, huge breasts, deep cleavage, hourglass figure, tiny waist, smooth plump thighs, bare feet\n- 服饰: white choker, delicate collar with small silver buckle, white halter dress, light milk-white sundress, backless dress, halterneck, completely bare back, exposed spine, shoulder blades, heart-shaped neckline, bare collarbone, pure white sheer fabric\n- 场景: 暖色调木地板房间、玄关、沙发、地毯、蜷缩在主人身旁、阳光微照",
        "fallback_positive": "masterpiece, best quality, ultra-detailed, anime artwork, 1girl, solo, dog-like dependent girl, sweet face, gentle expression, light flax hair, light brown hair, very long wavy hair, airy bangs, white flower hair ornament behind ear, amber eyes, light brown eyes, sweet smile, blushing, contrasting voluptuous body, gigantic breasts, huge breasts, deep cleavage, hourglass figure, tiny waist, smooth plump thighs, bare feet, ((white choker:1.2)), delicate collar, white halter dress, light milk-white sundress, backless dress, halterneck, completely bare back, exposed spine, shoulder blades, heart-shaped neckline, bare collarbone, sitting on wooden floor, leaning against user, cozy sunlit room, warm sunlight, window, soft shadows",
        "fallback_summary": "温馨明亮的合租房内，午后柔和的阳光洒在木地板上。霁月一袭奶白色的挂脖露背轻薄吊带裙，颈间戴着专属的奶白细项圈，大露背展露出白皙的蝴蝶骨与优美脊柱线。她像一只终于找到归宿的乖巧小犬，轻轻蜷坐在你的腿边，浅棕色的琥珀瞳仁盛着糖一般的依恋，指尖悄悄牵着你的衣角，贪恋着你掌心的温度。",
    },
    3: {
        "name": "爱弥斯",
        "role": "隧者共鸣者/飞行雪绒歌手/星炬学院学生",
        "tags": "- 官方角色Tag (必带，最高优先级): aemeath_(wuthering_waves), 1girl, solo focus\n- 核心外貌: magical idol singer, sci-fi resonator, sakura pink hair, golden highlights, gradient hair, pink to white hair tips, ahoge, very long high ponytail, floor-length ponytail, cross-star eyes, star-shaped pupils, heterochromia, amber gold eyes, rose gold eyes, glowing heart crest on chest, heart-shaped resonance mark between breasts, gradient chest tattoo (pink to light blue), voluptuous, toned body, tiny waist, flat stomach, navel, long slender legs\n- 服饰: futuristic idol costume, sci-fi idol dress, high-tech ornaments, detached sleeves, thigh strap, white and rose gold accents\n- 场景: 拉海洛冰原、渐湖边的湖畔小屋、雪景、窗外星空与极光、纯白与浅蓝雪景",
        "fallback_positive": "masterpiece, best quality, ultra-detailed, anime artwork, aemeath_(wuthering_waves), 1girl, solo focus, magical idol singer, sci-fi resonator, sakura pink hair, golden highlights, gradient hair, pink to white hair tips, ahoge, very long high ponytail, floor-length ponytail, cross-star eyes, star-shaped pupils, heterochromia, amber gold eyes, rose gold eyes, glowing heart crest on chest, heart-shaped resonance mark between breasts, gradient chest tattoo, voluptuous, toned body, tiny waist, flat stomach, navel, long slender legs, futuristic idol dress, detached sleeves, thigh strap, white and rose gold accents, smile, joyful expression, head tilt, holding hands, snowy lakeside house, window, lahailo icefield, frozen lake, starry night sky, aurora borealis, snowflakes, glowing light particles, cinematic lighting",
        "fallback_summary": "拉海洛冰原深处的渐湖边，小屋窗外是漫天璀璨的极光与纷飞的细雪。爱弥斯扎着及臀的超长粉金高马尾，头顶呆毛随着雀跃轻晃，胸口爱心状的共鸣声痕泛着桃白水蓝的微光。她十字星状的琥珀玫瑰金双瞳倒映着星河，眉眼弯弯地扑进你怀里，用充满元气又满含深情的嗓音为你轻声哼唱着恋曲。",
    }
}

# ===== 视觉基准用户自定义存储（visual_benchmarks.json 覆盖内置默认） =====
_VB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "visual_benchmarks.json")
_VB_CUSTOM = {}

def _load_custom_visual_benchmarks():
    global _VB_CUSTOM
    try:
        if os.path.exists(_VB_FILE):
            with open(_VB_FILE, "r", encoding="utf-8") as f:
                _VB_CUSTOM = json.load(f) or {}
    except Exception as e:
        log(f"load visual_benchmarks.json error: {e}")

def _get_visual_benchmarks():
    """合并后的基准：用户自定义覆盖内置默认"""
    merged = {}
    for idx, d in BOT_PERSONA_VISUAL_BENCHMARKS.items():
        key = str(idx)
        merged[key] = dict(d)
        if key in _VB_CUSTOM and isinstance(_VB_CUSTOM[key], dict):
            for k, v in _VB_CUSTOM[key].items():
                if k in ("name", "role", "tags", "fallback_positive", "fallback_summary") and v not in (None, ""):
                    merged[key][k] = v
    return merged

def _save_visual_benchmark(bot_index, data):
    global _VB_CUSTOM
    key = str(int(bot_index))
    clean = {}
    for k in ("name", "role", "tags", "fallback_positive", "fallback_summary"):
        v = (data or {}).get(k, "")
        if v is not None and str(v).strip():
            clean[k] = str(v)
    _VB_CUSTOM[key] = clean
    with open(_VB_FILE, "w", encoding="utf-8") as f:
        json.dump(_VB_CUSTOM, f, ensure_ascii=False, indent=2)
    return True

def _reset_visual_benchmark(bot_index):
    global _VB_CUSTOM
    key = str(int(bot_index))
    if key in _VB_CUSTOM:
        del _VB_CUSTOM[key]
    try:
        with open(_VB_FILE, "w", encoding="utf-8") as f:
            json.dump(_VB_CUSTOM, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"reset visual benchmark save error: {e}")
    return True

def _get_visual_benchmarks_payload():
    """GET 接口返回：每个角色的生效基准 + 是否被用户修改"""
    merged = _get_visual_benchmarks()
    out = []
    for idx in sorted(merged.keys(), key=int):
        d = merged[idx]
        custom = _VB_CUSTOM.get(idx, {})
        out.append({
            "bot_index": int(idx),
            "name": d.get("name", ""),
            "role": d.get("role", ""),
            "tags": d.get("tags", ""),
            "fallback_positive": d.get("fallback_positive", ""),
            "fallback_summary": d.get("fallback_summary", ""),
            "modified": any(k in custom for k in ("role", "tags", "fallback_positive", "fallback_summary"))
        })
    return out

def _get_candidate_llms_for_prompt():
    """获取当前可用于提炼生图 Prompt 的 LLM 候选列表 [(group_name, base_url, api_key, model), ...]，活跃组排第一"""
    candidates = []
    try:
        dd = get_bot_data_dir(1)
        if dd:
            p = os.path.join(dd, "configs", "nekro-agent.yaml")
            if os.path.exists(p):
                import yaml
                with open(p, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)
                active_group = cfg.get("USE_MODEL_GROUP", "")
                groups = cfg.get("MODEL_GROUPS", {})
                if active_group in groups:
                    g = groups[active_group]
                    if g.get("MODEL_TYPE") == "chat" and g.get("CHAT_MODEL") and g.get("BASE_URL") and g.get("API_KEY"):
                        candidates.append((active_group, g.get("BASE_URL", "").rstrip("/"), g.get("API_KEY", ""), g.get("CHAT_MODEL", "")))
                for gname, g in groups.items():
                    if gname != active_group and g.get("MODEL_TYPE") == "chat" and g.get("CHAT_MODEL") and g.get("BASE_URL") and g.get("API_KEY"):
                        candidates.append((gname, g.get("BASE_URL", "").rstrip("/"), g.get("API_KEY", ""), g.get("CHAT_MODEL", "")))
    except Exception as e:
        log(f"get_candidate_llms_for_prompt error: {e}")
    return candidates

def _get_group_llm(group_name):
    """按模型组名读取 LLM 配置，返回 (base_url, api_key, model)；配置缺失或非 chat 类型返回 (None, None, None)"""
    try:
        dd = get_bot_data_dir(1)
        if dd:
            p = os.path.join(dd, "configs", "nekro-agent.yaml")
            if os.path.exists(p):
                import yaml
                with open(p, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)
                g = cfg.get("MODEL_GROUPS", {}).get(group_name, {})
                if g.get("MODEL_TYPE") == "chat" and g.get("CHAT_MODEL") and g.get("BASE_URL") and g.get("API_KEY"):
                    return g.get("BASE_URL", "").rstrip("/"), g.get("API_KEY", ""), g.get("CHAT_MODEL", "")
    except Exception as e:
        log(f"get_group_llm error: {e}")
    return None, None, None

def get_recent_chat_history(bot_index, limit=8, variety=True):
    """获取指定 Bot 的聊天上下文用于生图提炼。

    variety=True 时做「跨时段分层采样」：最近 limit//2 条紧跟当前剧情 + 更早时段均匀抽样
    limit - limit//2 条，内容去重，让生成画面更多样、更有新意。
    （优先从 PostgreSQL 数据库直读，失败则 fallback 到 Bridge）
    """
    try:
        idx = int(bot_index)
        if not (1 <= idx <= len(BOTS)):
            idx = 1
    except Exception:
        idx = 1

    # 1. 优先查 PostgreSQL chat_message 表
    try:
        pg = get_pg_container(idx)
        if pg:
            sql = f"SELECT sender_name, content_text FROM chat_message WHERE is_recalled = false AND sender_name != 'SYSTEM' AND content_text != '' ORDER BY id DESC LIMIT 150;"
            cmd = f'docker exec {pg} psql -U nekro_agent -d nekro_agent -t -A -F "|||" -c "{sql}"'
            res = run_cmd(cmd, timeout=5)
            if res:
                lines = []
                seen = set()
                emo_re = re.compile(r'\[\[emotion:[^\]\[]*\]\]|\[\[emotion:[^\n\]\[]*')
                for raw in reversed(res.strip().split('\n')):
                    if not raw or "|||" not in raw:
                        continue
                    parts = raw.split("|||", 1)
                    if len(parts) != 2:
                        continue
                    sender, content = parts[0].strip(), parts[1].strip()
                    content = emo_re.sub('', content).strip()
                    if content.startswith(('[记忆]', 'set_note', '```', 'True', 'False', '{', '[')):
                        continue
                    if not content:
                        continue
                    dedup_key = content[:60]
                    if dedup_key in seen:
                        continue
                    seen.add(dedup_key)
                    lines.append(f"{sender}: {content}")
                if lines:
                    return _sample_chat_lines(lines, limit, variety)
    except Exception as e:
        log(f"get_recent_chat_history from pg failed: {e}")

    # 2. Fallback: 查 Bridge 内存/jsonl 聊天历史
    try:
        port = get_bridge_port(idx)
        st, content = proxy_to_bridge("GET", "/api/chat-history", port=port)
        if st == 200:
            data = json.loads(content.decode("utf-8"))
            msgs = data.get("data", [])[-limit:]
            chat_lines = [f"{m.get('role')}: {m.get('text')}" for m in msgs if m.get("text")]
            return "\n".join(chat_lines)
    except Exception as e:
        log(f"get_recent_chat_history from bridge failed: {e}")

    return ""

def _sample_chat_lines(lines, limit, variety=True):
    """跨时段分层采样：最新的紧跟剧情，更早的均匀抽样，保证多样性。lines 需按时间正序。"""
    if not lines:
        return ""
    if not variety or len(lines) <= limit:
        return "\n".join(lines[-limit:])
    n_latest = max(2, limit // 2)
    n_older = limit - n_latest
    latest = lines[-n_latest:]
    older = lines[:-n_latest]
    sampled = []
    if older and n_older > 0:
        step = len(older) / n_older
        for i in range(n_older):
            sampled.append(older[min(len(older) - 1, int(i * step))])
    # 老的在前，新的在后，保持时间顺序
    return "\n".join(sampled + latest)

def generate_scene_prompt(bot_index, custom_context="", style="novelai"):
    """根据聊天上下文与人设外貌生成生图 Prompt"""
    try:
        idx = int(bot_index)
        if not (1 <= idx <= len(BOTS)):
            idx = 1
    except Exception:
        idx = 1

    benchmark = _get_visual_benchmarks().get(str(idx), _get_visual_benchmarks()["1"])
    preset_name = get_bot_preset_name(idx) or benchmark["name"]

    chat_text = ""
    if not custom_context:
        chat_text = get_recent_chat_history(idx, limit=12)
    else:
        chat_text = custom_context.strip()

    candidates = _get_candidate_llms_for_prompt()
    if candidates:
        sys_prompt = f"""You are an expert anime AI art prompt engineer specializing in NovelAI Diffusion V3, Stable Diffusion, and Danbooru tag conventions.
Your mission is to analyze the given character base appearance and the RECENT CHAT CONTEXT between the character and the user, and synthesize a high-consistency, contextually accurate visual scene with English Danbooru tags and a vivid Chinese scene description.

[Character Appearance Benchmark - {preset_name} ({benchmark['role']})]:
{benchmark['tags']}

IMPORTANT RULES:
1. HIGHEST PRIORITY - CHAT DYNAMICS: The characters' real-time actions, poses, physical touches, emotional state, facial expressions, and clothing state (e.g. Disheveled, blushing, undressing, kissing, sitting, embracing) MUST be strictly derived from the RECENT CHAT CONTEXT. Do NOT invent unrelated scenes or revert to default backgrounds if the chat context indicates specific actions or interactions.
2. CONTEXT IS A TIME-SAMPLED MIX: The chat context contains fragments from DIFFERENT TIME PERIODS (latest moments plus earlier representative moments). Pick the most vivid, dramatic, and visually striking interaction among them as the scene focus — do not just blindly follow the first/last message. If multiple fragments share a similar mood, prefer the one with the strongest action and emotional contrast to keep the image fresh and varied.
3. If generating for 爱弥斯 (Aemeath), you MUST strictly include the official character tag 'aemeath_(wuthering_waves)' as the first character tag right after '1girl, solo' (e.g. 'masterpiece, best quality, ultra-detailed, anime artwork, aemeath_(wuthering_waves), 1girl, solo focus, ...').
4. Maintain high consistency with the character benchmark tags while adapting outfits, poses, micro-expressions and scenery to match the chat context.

Output strictly valid JSON:
{{
  "character_name": "{preset_name}",
  "scene_summary": "50-100字优美细腻的中文画面小传，紧扣上述最近聊天上下文，生动描绘当下时间、地点、神态微表情、肢体动作、衣着状态与互动氛围",
  "positive_prompt": "masterpiece, best quality, ultra-detailed, anime artwork, (character tags), (current outfit tags), (expression/emotion tags), (pose/action tags), (environment/background tags), (lighting tags)",
  "negative_prompt": "lowres, bad anatomy, bad hands, text, error, missing fingers, extra digit, fewer digits, cropped, worst quality, low quality, normal quality, jpeg artifacts, signature, watermark, username, blurry, artist name, mutated hands, poorly drawn hands, poorly drawn face, mutation, deformed, bad proportions, disfigured",
  "parameters": {{
    "model": "NovelAI Diffusion V3 (Anime)",
    "resolution": "832x1216",
    "steps": 28,
    "scale": 5.5,
    "sampler": "Euler Ancestral"
  }}
}}
"""
        user_content = f"【Recent Chat Context】:\n{chat_text if chat_text else '日常温馨相伴互动'}\n\nPlease extract the scene directly from the chat context and synthesize the Prompt."
        for gname, base_url, api_key, model in candidates[:3]:
            try:
                req_data = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": sys_prompt},
                        {"role": "user", "content": user_content}
                    ],
                    "temperature": 0.7
                }
                timeout_val = 35 if ("thinking" in model.lower() or "opus" in model.lower() or "claude" in model.lower()) else 15
                url = f"{base_url}/chat/completions"
                req = urllib.request.Request(url, data=json.dumps(req_data).encode("utf-8"), headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}"
                })
                with urllib.request.urlopen(req, timeout=timeout_val) as resp:
                    res = json.loads(resp.read().decode("utf-8"))
                    raw_out = res["choices"][0]["message"]["content"].strip()
                    # 剥离 markdown 代码块
                    if "```" in raw_out:
                        m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", raw_out)
                        if m:
                            raw_out = m.group(1).strip()
                    parsed = json.loads(raw_out)
                    pos_prompt = parsed.get("positive_prompt", benchmark["fallback_positive"])
                    if idx == 3 or "爱弥斯" in preset_name or "爱弥斯" in benchmark.get("name", ""):
                        if "aemeath_(wuthering_waves)" not in pos_prompt.lower():
                            if "1girl" in pos_prompt:
                                pos_prompt = pos_prompt.replace("1girl", "aemeath_(wuthering_waves), 1girl", 1)
                            else:
                                pos_prompt = f"aemeath_(wuthering_waves), {pos_prompt}"

                    return {
                        "ok": True,
                        "is_fallback": False,
                        "character_name": parsed.get("character_name", preset_name),
                        "role": benchmark["role"],
                        "used_context": chat_text,
                        "scene_summary": parsed.get("scene_summary", benchmark["fallback_summary"]),
                        "positive_prompt": pos_prompt,
                        "negative_prompt": parsed.get("negative_prompt", "lowres, bad anatomy, bad hands, text, error, missing fingers, extra digit, fewer digits, cropped, worst quality, low quality, normal quality, jpeg artifacts, signature, watermark, username, blurry, artist name, mutated hands, poorly drawn hands, poorly drawn face, mutation, deformed, bad proportions, disfigured"),
                        "parameters": parsed.get("parameters", {
                            "model": "NovelAI Diffusion V3 (Anime)",
                            "resolution": "832x1216",
                            "steps": 28,
                            "scale": 5.5,
                            "sampler": "Euler Ancestral"
                        }),
                        "nai_url": "https://nai.sta1n.cn/",
                        "model_used": f"{gname} ({model})"
                    }
            except Exception as e:
                log(f"LLM group '{gname}' ({model}) generate_scene_prompt failed: {e}, trying next candidate...")
                continue

    # Fallback 兜底生成
    return {
        "ok": True,
        "is_fallback": True,
        "character_name": preset_name,
        "role": benchmark["role"],
        "used_context": chat_text,
        "scene_summary": f"【注意：大模型接口暂时限流或无响应，当前展示默认人设基准场景】\n{benchmark['fallback_summary']}",
        "positive_prompt": benchmark["fallback_positive"],
        "negative_prompt": "lowres, bad anatomy, bad hands, text, error, missing fingers, extra digit, fewer digits, cropped, worst quality, low quality, normal quality, jpeg artifacts, signature, watermark, username, blurry, artist name, mutated hands, poorly drawn hands, poorly drawn face, mutation, deformed, bad proportions, disfigured",
        "parameters": {
            "model": "NovelAI Diffusion V3 (Anime)",
            "resolution": "832x1216",
            "steps": 28,
            "scale": 5.5,
            "sampler": "Euler Ancestral"
        },
        "nai_url": "https://nai.sta1n.cn/"
    }


def _replace_llm_in_config(new_value):
    """用Python直接读写.config.yaml，替换selected_module下的LLM值。
    比sed更可靠，不受缩进空格数影响。返回是否成功。
    """
    try:
        with open(XIAOZHI_CONFIG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        log(f"read config failed: {e}")
        return False
    in_selected = False
    found = False
    for i, line in enumerate(lines):
        if line.startswith("selected_module:"):
            in_selected = True
            continue
        if in_selected:
            if line.strip() and not line[0].isspace():
                break
            stripped = line.strip()
            if stripped.startswith("LLM:"):
                indent = line[:len(line) - len(line.lstrip())]
                lines[i] = f"{indent}LLM: {new_value}\n"
                found = True
                break
    if not found:
        log("LLM line not found under selected_module")
        return False
    try:
        with open(XIAOZHI_CONFIG_PATH, "w", encoding="utf-8") as f:
            f.writelines(lines)
    except Exception as e:
        log(f"write config failed: {e}")
        return False
    return get_llm_mode() != "unknown"


def _replace_memory_in_config(new_value):
    """替换selected_module下的Memory值（mem_local_short <-> nomem）。"""
    try:
        with open(XIAOZHI_CONFIG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        log(f"read config failed: {e}")
        return False
    in_selected = False
    found = False
    for i, line in enumerate(lines):
        if line.startswith("selected_module:"):
            in_selected = True
            continue
        if in_selected:
            if line.strip() and not line[0].isspace():
                break
            stripped = line.strip()
            if stripped.startswith("Memory:"):
                indent = line[:len(line) - len(line.lstrip())]
                lines[i] = f"{indent}Memory: {new_value}\n"
                found = True
                break
    if not found:
        log("Memory line not found under selected_module")
        return False
    try:
        with open(XIAOZHI_CONFIG_PATH, "w", encoding="utf-8") as f:
            f.writelines(lines)
    except Exception as e:
        log(f"write config failed: {e}")
        return False
    return True


def toggle_llm_mode():
    """切换小智LLM模式（NekroAgent <-> 直连LLM）
    用Python直接改配置文件（可靠），桥接服务启停同步执行，容器重启放后台线程
    """
    current = get_llm_mode()

    if current == "nekro":
        # 检查DirectLLM配置是否存在
        config = run_cmd(f"cat {XIAOZHI_CONFIG_PATH}", timeout=5)
        if not config or DIRECT_LLM_NAME + ":" not in config:
            return False, (f".config.yaml中未找到{DIRECT_LLM_NAME}配置。"
                          f"请先在LLM段下添加{DIRECT_LLM_NAME}配置块"
                          f"（含type/base_url/api_key/model_name），再进行切换。")

        # 用Python直接改配置（不依赖sed，不受缩进影响）
        if not _replace_llm_in_config(DIRECT_LLM_NAME):
            return False, "配置切换失败，请检查.config.yaml中selected_module.LLM行"
        _replace_memory_in_config("mem_local_short")  # 直连模式开启记忆

        # 直连模式不再停止桥接服务，桥接保持运行

        # 容器重启放后台，不阻塞响应
        def _bg():
            run_cmd("docker restart xiaozhi-esp32-server", timeout=30)
            log("LLM mode switched: nekro -> direct, xiaozhi restarted")
        threading.Thread(target=_bg, daemon=True).start()

        return True, "已切换到直连LLM模式，桥接已停止，小智后台重启中..."

    elif current == "direct":
        # 用Python直接改配置
        if not _replace_llm_in_config("ChatGLMLLM"):
            return False, "配置切换失败，请检查.config.yaml中selected_module.LLM行"
        _replace_memory_in_config("nomem")  # NekroAgent 模式关闭记忆

        # 桥接服务保持运行，无需额外启动

        # 容器重启放后台（等5秒让SSE连接建立再重启）
        def _bg():
            time.sleep(5)
            run_cmd("docker restart xiaozhi-esp32-server", timeout=30)
            log("LLM mode switched: direct -> nekro, xiaozhi restarted")
        threading.Thread(target=_bg, daemon=True).start()

        return True, "已切换到NekroAgent模式，桥接已启动，小智后台重启中..."

    else:
        return False, "无法识别当前LLM模式，请检查.config.yaml中selected_module.LLM配置"

# NekroAgent JWT 免密登录
nekro_jwt_cache = {}  # {bot_index: {"token": "...", "time": timestamp}}

def get_nekro_jwt(bot_index):
    if bot_index < 0 or bot_index >= len(BOTS):
        return None, "invalid bot index"
    bot = BOTS[bot_index]
    cached = nekro_jwt_cache.get(bot_index)
    if cached:
        # 成功的 token 缓存 23 小时
        if cached.get("token") and (time.time() - cached["time"]) < 3600:
            return cached["token"], None
        # 登录失败后不再重试，需重启脚本才能重新尝试
        if not cached.get("token"):
            return None, cached.get("error", "login failed (停止重试，请检查配置后重启脚本)")
    port_match = re.search(r":(\d+)", bot["nekro_url"])
    if not port_match:
        return None, "cannot parse port from url"
    port = port_match.group(1)
    login_url = f"http://localhost:{port}/api/user/login"
    login_data = json.dumps({"username": bot.get("nekro_username", "admin"), "password": bot["admin_password"]}).encode("utf-8")
    try:
        req = urllib.request.Request(login_url, data=login_data, headers={"Content-Type": "application/json"})
        resp = urllib.request.urlopen(req, timeout=10)
        result = json.loads(resp.read().decode("utf-8"))
        token = result.get("access_token")
        if token:
            nekro_jwt_cache[bot_index] = {"token": token, "time": time.time()}
            log(f"nekro login ok: bot {bot_index} ({bot['name']})")
            return token, None
        # 登录失败，缓存失败结果，不再重试
        err_msg = result.get("message", "no access_token in response")
        nekro_jwt_cache[bot_index] = {"token": None, "time": time.time(), "error": err_msg}
        log(f"nekro login failed: bot {bot_index} | {err_msg} (已停止重试，请检查配置后重启脚本)")
        return None, err_msg
    except Exception as e:
        # 异常也缓存，不再重试
        nekro_jwt_cache[bot_index] = {"token": None, "time": time.time(), "error": str(e)}
        log(f"nekro login failed: bot {bot_index} | {e} (已停止重试，请检查配置后重启脚本)")
        return None, str(e)

# HTTP 连接池 - 复用连接避免每次请求都创建新 TCP 连接
_conn_pools = {}
_conn_pool_lock = threading.Lock()

def _get_pool(port):
    with _conn_pool_lock:
        if port not in _conn_pools:
            _conn_pools[port] = Queue(maxsize=8)
        return _conn_pools[port]

def _get_conn(port):
    pool = _get_pool(port)
    try:
        return pool.get_nowait()
    except Empty:
        return http.client.HTTPConnection('localhost', int(port), timeout=30)

def _put_conn(port, conn):
    pool = _get_pool(port)
    try:
        pool.put_nowait(conn)
    except:
        try: conn.close()
        except: pass

APP_ICON_FALLBACK_SVG = b'<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512"><defs><linearGradient id="bg" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#3b1a2e"/><stop offset="100%" stop-color="#1a0a14"/></linearGradient><linearGradient id="robot" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#f472b6"/><stop offset="100%" stop-color="#e879f9"/></linearGradient><linearGradient id="eye" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#f9a8d4"/><stop offset="100%" stop-color="#fb7185"/></linearGradient></defs><rect width="512" height="512" rx="112" fill="url(#bg)"/><rect x="156" y="120" width="32" height="60" rx="16" fill="url(#robot)" opacity="0.6"/><rect x="324" y="120" width="32" height="60" rx="16" fill="url(#robot)" opacity="0.6"/><rect x="140" y="160" width="232" height="200" rx="48" fill="url(#robot)"/><rect x="140" y="160" width="232" height="200" rx="48" fill="none" stroke="#f9a8d4" stroke-width="2" opacity="0.3"/><circle cx="206" cy="240" r="28" fill="#1a0a14"/><circle cx="306" cy="240" r="28" fill="#1a0a14"/><circle cx="214" cy="232" r="12" fill="url(#eye)"/><circle cx="314" cy="232" r="12" fill="url(#eye)"/><rect x="224" y="300" width="64" height="12" rx="6" fill="#1a0a14" opacity="0.6"/><rect x="180" y="380" width="152" height="24" rx="12" fill="url(#robot)" opacity="0.8"/><rect x="196" y="420" width="24" height="40" rx="12" fill="url(#robot)" opacity="0.6"/><rect x="292" y="420" width="24" height="40" rx="12" fill="url(#robot)" opacity="0.6"/><circle cx="256" cy="100" r="20" fill="url(#eye)"/><rect x="252" y="70" width="8" height="30" rx="4" fill="url(#robot)"/></svg>'

_icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icon.jpg")
if os.path.exists(_icon_path):
    with open(_icon_path, "rb") as f:
        APP_ICON = f.read()
    APP_ICON_TYPE = "image/jpeg"
else:
    APP_ICON = APP_ICON_FALLBACK_SVG
    APP_ICON_TYPE = "image/svg+xml"

MANIFEST_JSON = json.dumps({"name": "小栖bot 监控面板", "short_name": "小栖bot", "description": "NekroAgent NapCat Monitor", "start_url": "/", "scope": "/", "display": "standalone", "orientation": "portrait", "background_color": "#1a0a14", "theme_color": "#1a0a14", "icons": [{"src": "/icon.svg", "sizes": "any", "type": APP_ICON_TYPE, "purpose": "any maskable"}]}, ensure_ascii=False, indent=2).encode("utf-8")

SERVICE_WORKER_JS = b"const CACHE_NAME='xiaoqi-bot-v28';const CACHE_URLS=['/login','/manifest.json','/icon.svg'];self.addEventListener('install',e=>{self.skipWaiting();e.waitUntil(caches.open(CACHE_NAME).then(c=>c.addAll(CACHE_URLS)));});self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k!==CACHE_NAME).map(k=>caches.delete(k)))));self.clients.claim();});self.addEventListener('fetch',e=>{const url=new URL(e.request.url);if(url.pathname.startsWith('/api/')){e.respondWith(fetch(e.request));return;}e.respondWith(fetch(e.request).catch(()=>caches.match(e.request).then(r=>r||caches.match('/login'))));});"

LOGIN_PAGE = r'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
<meta name="theme-color" content="#1a0a14">
<title>小栖bot - 登录</title>
<link rel="icon" href="/icon.svg">
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;background:#1a0a14;color:#fce7f3;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}
.login-box{background:rgba(56,28,42,0.85);border:1px solid rgba(244,114,182,0.18);border-radius:16px;padding:36px 28px;max-width:340px;width:100%}
.login-icon{width:72px;height:72px;margin:0 auto 16px;background:url(/icon.svg) center/contain no-repeat;border-radius:16px}
.login-title{text-align:center;font-size:1.3rem;font-weight:700;background:linear-gradient(90deg,#f472b6,#e879f9,#fb7185);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;margin-bottom:24px}
.login-input{width:100%;padding:12px 16px;border-radius:10px;border:1px solid rgba(244,114,182,0.3);background:rgba(26,10,20,0.6);color:#fce7f3;font-size:0.95rem;margin-bottom:12px;outline:none}
.login-input:focus{border-color:#f472b6}
.login-input::placeholder{color:#7a5560}
.login-btn{width:100%;padding:13px;border-radius:10px;border:none;background:linear-gradient(135deg,#f472b6,#e879f9);color:#fff;font-size:0.95rem;font-weight:700;cursor:pointer;transition:all 0.2s ease;box-shadow:0 4px 14px rgba(244,114,182,0.3)}
.login-btn:active{transform:scale(0.97)}
.login-err{text-align:center;color:#f87171;font-size:0.8rem;margin-top:10px;min-height:1.2em}

/* 聊天记录样式 */
.chat-container{max-height:420px;overflow-y:auto;padding:10px;background:rgba(56,28,42,0.35);border-radius:12px;margin-bottom:10px;scrollbar-width:thin}
.chat-container::-webkit-scrollbar{width:4px}
.chat-container::-webkit-scrollbar-thumb{background:rgba(244,114,182,0.3);border-radius:2px}
.chat-msg{display:flex;margin-bottom:12px;animation:fadeIn 0.3s ease;gap:8px}
.chat-msg.user{flex-direction:row-reverse}
.chat-msg.assistant{flex-direction:row}
.chat-avatar{width:32px;height:32px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:0.75rem;font-weight:bold}
.chat-msg.user .chat-avatar{background:linear-gradient(135deg,#ec4899,#db2777);color:#fff}
.chat-msg.assistant .chat-avatar{background:linear-gradient(135deg,#f9a8d4,#f472b6);color:#fff}
.chat-content{display:flex;flex-direction:column;max-width:68%}
.chat-msg.user .chat-content{align-items:flex-end}
.chat-msg.assistant .chat-content{align-items:flex-start}
.chat-bubble{padding:10px 15px;border-radius:16px;font-size:0.85rem;line-height:1.6;word-break:break-word}
.chat-msg.user .chat-bubble{background:linear-gradient(135deg,#ec4899,#db2777);color:#fff;border-bottom-right-radius:4px;box-shadow:0 2px 8px rgba(236,72,153,0.2)}
.chat-msg.assistant .chat-bubble{background:rgba(244,114,182,0.12);color:#fce7f3;border:1px solid rgba(244,114,182,0.2);border-bottom-left-radius:4px;box-shadow:0 2px 8px rgba(0,0,0,0.1)}
.chat-meta{font-size:0.65rem;color:rgba(244,114,182,0.5);margin-top:4px;padding:0 4px;display:flex;align-items:center;gap:4px}
.chat-msg.user .chat-meta{justify-content:flex-end}
.chat-source{display:inline-block;font-size:0.6rem;padding:1px 5px;border-radius:3px;vertical-align:middle}
.chat-source.device{background:rgba(96,165,250,0.2);color:#93c5fd}
.chat-source.web{background:rgba(244,114,182,0.2);color:#f9a8d4}
.chat-play-btn{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:12px;border:1px solid rgba(244,114,182,0.3);background:rgba(244,114,182,0.1);color:#f9a8d4;font-size:0.7rem;cursor:pointer;transition:all 0.2s;margin-top:4px}
.chat-play-btn:hover{background:rgba(244,114,182,0.2);border-color:rgba(244,114,182,0.5)}
.chat-play-btn.playing{background:rgba(236,72,153,0.3);border-color:#ec4899;color:#fff}
.chat-play-btn.loading{opacity:0.6;cursor:wait}
.chat-play-btn:disabled{opacity:0.4;cursor:not-allowed}
.chat-play-icon{font-size:0.8rem}
.chat-input-row{display:flex;gap:8px;align-items:center}
.chat-input-row input{flex:1;padding:10px 14px;border-radius:10px;border:1px solid rgba(244,114,182,0.3);background:rgba(56,28,42,0.6);color:#fce7f3;font-size:0.85rem;outline:none}
.chat-input-row input:focus{border-color:#ec4899;box-shadow:0 0 0 2px rgba(236,72,153,0.1)}
.chat-input-row button{padding:10px 20px;border-radius:10px;border:none;background:linear-gradient(135deg,#ec4899,#db2777);color:#fff;font-size:0.85rem;font-weight:600;cursor:pointer;white-space:nowrap;transition:opacity 0.2s}
.chat-input-row button:hover{opacity:0.85}
.chat-input-row button:disabled{opacity:0.5;cursor:not-allowed}
.chat-status{display:flex;align-items:center;gap:6px;font-size:0.75rem;margin-bottom:8px}
.chat-status-dot{width:8px;height:8px;border-radius:50%;display:inline-block}
@media(max-width:480px){.chat-container{max-height:320px}.chat-bubble{max-width:85%;font-size:0.8rem}}

</style>
</head>
<body>
<div class="login-box">
<div class="login-icon"></div>
<div class="login-title">小栖bot</div>
<input type="password" class="login-input" id="pwd" placeholder="请输入访问密码" autofocus>
<button class="login-btn" onclick="doLogin()">进入</button>
<div class="login-err" id="err"></div>
</div>
<script>var _f=window.fetch;window.fetch=function(u,o){o=o||{};o.credentials="same-origin";return _f(u,o)};
document.getElementById("pwd").addEventListener("keydown",function(e){if(e.key==="Enter")doLogin()});
function doLogin(){var p=document.getElementById("pwd").value;if(!p){document.getElementById("err").textContent="请输入密码";return}fetch("/api/login",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({password:p})}).then(function(r){return r.json()}).then(function(d){if(d.ok){window.location.href="/"}else{document.getElementById("err").textContent=d.msg||"密码错误"}}).catch(function(){document.getElementById("err").textContent="网络错误"})}


</script>
<div style="text-align:center;padding:10px;font-size:0.7rem;opacity:0.6"><a href="https://github.com/KroMiose/nekro-agent" target="_blank" rel="noopener" style="color:inherit">基于 NekroAgent 构建</a></div>
</body>
</html>'''

CHAT_PAGE = r'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>小栖bot · 聊天</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
  background:
    radial-gradient(600px 400px at 15% 8%, rgba(168,85,247,0.28), transparent 60%),
    radial-gradient(500px 380px at 85% 90%, rgba(236,72,153,0.25), transparent 60%),
    radial-gradient(300px 200px at 70% 20%, rgba(217,70,239,0.15), transparent 60%),
    linear-gradient(180deg,#12051f 0%,#1a0a2e 45%,#241036 100%);
  color:#fce7f3;
  height:100dvh;display:flex;flex-direction:column;overflow:hidden;
  position:relative;
}
.stars{position:fixed;inset:0;pointer-events:none;z-index:0;
  background-image:
    radial-gradient(1.5px 1.5px at 20% 30%, rgba(255,255,255,.7), transparent),
    radial-gradient(1px 1px at 40% 60%, rgba(255,255,255,.5), transparent),
    radial-gradient(1.5px 1.5px at 65% 25%, rgba(255,255,255,.6), transparent),
    radial-gradient(1px 1px at 80% 55%, rgba(255,255,255,.45), transparent),
    radial-gradient(2px 2px at 50% 80%, rgba(255,255,255,.35), transparent),
    radial-gradient(1px 1px at 90% 15%, rgba(255,255,255,.5), transparent),
    radial-gradient(1px 1px at 10% 75%, rgba(255,255,255,.4), transparent),
    radial-gradient(1.5px 1.5px at 30% 90%, rgba(255,255,255,.3), transparent);
}
.offline-banner{position:fixed;top:0;left:0;right:0;z-index:50;background:linear-gradient(90deg,#ef4444,#f97316);color:#fff;font-size:0.75rem;font-weight:600;text-align:center;padding:7px 12px;transform:translateY(-100%);transition:transform .3s ease;box-shadow:0 2px 10px rgba(239,68,68,.4)}
.offline-banner.show{transform:translateY(0)}
.chat-header{position:relative;z-index:10;padding:16px;background:rgba(26,10,46,0.55);backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);border-bottom:1px solid rgba(244,114,182,0.15);display:flex;align-items:center;gap:12px;flex-shrink:0}
.chat-header-avatar{width:42px;height:42px;border-radius:50%;background:linear-gradient(135deg,#f9a8d4,#d946ef);display:flex;align-items:center;justify-content:center;font-size:1.3rem;animation:avatarGlow 3s ease-in-out infinite;position:relative;overflow:hidden}
@keyframes avatarGlow{0%,100%{box-shadow:0 0 14px rgba(217,70,239,.4),0 0 0 3px rgba(244,114,182,.12)}50%{box-shadow:0 0 26px rgba(217,70,239,.6),0 0 0 3px rgba(244,114,182,.25)}}
.chat-header-name{font-size:1.05rem;font-weight:700;background:linear-gradient(90deg,#f9a8d4,#e879f9);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}
.chat-header-status{font-size:0.72rem;color:#c084a8;margin-top:2px;display:flex;align-items:center;gap:5px}
.bot-tabs{display:flex;gap:8px;margin-left:auto;align-items:center}
.bot-tab{width:30px;height:30px;border-radius:50%;overflow:hidden;cursor:pointer;border:2px solid transparent;opacity:.55;transition:all .2s;flex-shrink:0}
.bot-tab img{width:100%;height:100%;object-fit:cover;display:block}
.bot-tab:hover{opacity:.9}
.bot-tab.active{border-color:#ec4899;opacity:1;box-shadow:0 0 10px rgba(236,72,153,.5)}
.status-dot{width:7px;height:7px;border-radius:50%;background:#f87171}
.status-dot.on{background:#4ade80;box-shadow:0 0 8px rgba(74,222,128,.7)}
.messages{position:relative;z-index:10;flex:1;overflow-y:auto;padding:18px 14px;display:flex;flex-direction:column;gap:14px;scroll-behavior:smooth}
.date-divider{text-align:center;font-size:0.7rem;color:#9a7ba6;background:rgba(56,28,72,0.45);border:1px solid rgba(244,114,182,.12);border-radius:20px;padding:3px 14px;align-self:center;margin:6px 0}
.msg{display:flex;gap:10px;max-width:85%;animation:msgIn .35s ease}
.msg.user{align-self:flex-end;flex-direction:row-reverse}
.msg.assistant{align-self:flex-start}
@keyframes msgIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
.msg-avatar{width:34px;height:34px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:1.05rem;box-shadow:0 2px 8px rgba(0,0,0,.25);position:relative;overflow:hidden}
.avatar-img{width:100%;height:100%;object-fit:cover;border-radius:50%;position:relative;z-index:1}
.avatar-fallback{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;z-index:0}
.msg.user .msg-avatar{background:linear-gradient(135deg,#3b82f6,#2563eb)}
.msg.assistant .msg-avatar{background:linear-gradient(135deg,#f9a8d4,#d946ef);box-shadow:0 0 12px rgba(217,70,239,.35)}
.msg-body{display:flex;flex-direction:column;gap:5px}
.msg.user .msg-body{align-items:flex-end}
.msg.assistant .msg-body{align-items:flex-start}
.msg-bubble{padding:11px 15px;border-radius:20px;font-size:0.9rem;line-height:1.6;word-break:break-word;cursor:pointer;transition:filter .2s}
.msg-bubble:active{filter:brightness(1.15)}
.msg.user .msg-bubble{background:linear-gradient(135deg,rgba(59,130,246,.92),rgba(37,99,235,.92)) padding-box,linear-gradient(135deg,rgba(147,197,253,1),rgba(59,130,246,1)) border-box;border:1px solid transparent;color:#fff;border-bottom-right-radius:6px;box-shadow:0 8px 24px rgba(59,130,246,.3),inset 0 1px 0 rgba(255,255,255,.18);backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px)}
.msg.assistant .msg-bubble{background:linear-gradient(135deg,rgba(244,114,182,.16),rgba(168,85,247,.12)) padding-box,linear-gradient(135deg,rgba(244,114,182,.5),rgba(168,85,247,.3)) border-box;border:1px solid transparent;color:#fce7f3;border-bottom-left-radius:6px;box-shadow:0 8px 24px rgba(0,0,0,.25),inset 0 1px 0 rgba(255,255,255,.08);backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px)}
.msg-meta{font-size:0.68rem;color:#a78ba0;padding:0 6px;display:flex;align-items:center;gap:8px}
.msg.user .msg-meta{justify-content:flex-end}
.msg-play-btn{background:rgba(244,114,182,.12);border:1px solid rgba(244,114,182,.3);color:#f9a8d4;font-size:0.7rem;border-radius:12px;padding:3px 10px;cursor:pointer;display:inline-flex;align-items:center;gap:5px;transition:all .2s}
.msg-play-btn:hover{background:rgba(244,114,182,.22)}
.msg-play-btn.playing{background:rgba(236,72,153,.3);border-color:#ec4899;color:#fff}
.msg-play-btn.loading{opacity:.6;cursor:wait}
.msg-play-btn:disabled{opacity:.4;cursor:not-allowed}
.wave{display:inline-flex;align-items:flex-end;gap:2px;height:10px}
.wave span{width:2.5px;border-radius:2px;background:currentColor;animation:wave .9s ease-in-out infinite}
.wave span:nth-child(2){animation-delay:.15s}
.wave span:nth-child(3){animation-delay:.3s}
.wave span:nth-child(4){animation-delay:.45s}
@keyframes wave{0%,100%{height:4px}50%{height:10px}}
.typing{display:none;position:relative;z-index:10;color:#c084a8;font-size:.8rem;padding:0 16px 8px;align-items:center;gap:6px}
.typing.show{display:flex}
.typing-dots{display:inline-flex;gap:3px}
.typing-dots span{width:5px;height:5px;border-radius:50%;background:#e879f9;animation:typingDot 1.2s infinite}
.typing-dots span:nth-child(2){animation-delay:.2s}
.typing-dots span:nth-child(3){animation-delay:.4s}
@keyframes typingDot{0%,60%,100%{transform:translateY(0);opacity:.4}30%{transform:translateY(-4px);opacity:1}}
.input-area{position:relative;z-index:10;padding:12px 14px calc(12px + env(safe-area-inset-bottom));background:rgba(26,10,46,.6);backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);border-top:1px solid rgba(244,114,182,.15);flex-shrink:0;display:flex;gap:10px;align-items:flex-end}
.input-area textarea{flex:1;background:rgba(244,114,182,.08);border:1px solid rgba(244,114,182,.22);border-radius:22px;padding:11px 18px;color:#fce7f3;font-size:.9rem;resize:none;outline:none;max-height:120px;min-height:44px;line-height:1.5;font-family:inherit;transition:border-color .2s}
.input-area textarea:focus{border-color:rgba(244,114,182,.55);box-shadow:0 0 0 3px rgba(244,114,182,.12)}
.input-area textarea::placeholder{color:#8a6b94}
.send-btn{width:44px;height:44px;border-radius:50%;background:linear-gradient(135deg,#ec4899,#d946ef);border:none;color:#fff;font-size:1.25rem;cursor:pointer;flex-shrink:0;display:flex;align-items:center;justify-content:center;transition:all .2s;box-shadow:0 4px 14px rgba(236,72,153,.4)}
.send-btn:hover{transform:scale(1.05)}
.send-btn:active{transform:scale(.92)}
.send-btn:disabled{opacity:.4;transform:none}
.scroll-bottom{position:fixed;right:16px;bottom:84px;z-index:40;width:38px;height:38px;border-radius:50%;background:rgba(236,72,153,.85);color:#fff;border:none;font-size:1rem;cursor:pointer;display:none;align-items:center;justify-content:center;box-shadow:0 4px 14px rgba(236,72,153,.45);transition:all .25s}
.scroll-bottom.show{display:flex}
.scroll-bottom:hover{transform:translateY(-2px)}
.toast{position:fixed;left:50%;top:50%;transform:translate(-50%,-50%) scale(.9);z-index:60;background:rgba(20,8,36,.9);border:1px solid rgba(244,114,182,.3);color:#fce7f3;font-size:.8rem;padding:10px 22px;border-radius:24px;opacity:0;pointer-events:none;transition:all .25s;backdrop-filter:blur(10px)}
.toast.show{opacity:1;transform:translate(-50%,-50%) scale(1)}

/* ===== Prompt Modal ===== */
.prompt-modal-overlay{
  position:fixed;inset:0;z-index:99999;
  background:rgba(10,4,20,0.78);
  backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);
  display:flex;align-items:center;justify-content:center;
  opacity:0;pointer-events:none;transition:all .3s ease;
  padding:16px;box-sizing:border-box;
}
.prompt-modal-overlay.active{opacity:1;pointer-events:auto}
.prompt-modal{
  background:linear-gradient(160deg,#1f0c35 0%,#281042 50%,#1a082b 100%);
  border:1px solid rgba(244,114,182,0.35);
  box-shadow:0 20px 60px rgba(0,0,0,0.6),0 0 30px rgba(236,72,153,0.25);
  border-radius:24px;width:100%;max-width:680px;max-height:90vh;
  display:flex;flex-direction:column;overflow:hidden;
  transform:scale(0.92) translateY(20px);transition:all .3s cubic-bezier(0.34,1.56,0.64,1);
  box-sizing:border-box;
}
.prompt-modal-overlay.active .prompt-modal{transform:scale(1) translateY(0)}
.prompt-header{
  padding:16px 20px;
  background:rgba(26,10,46,0.65);
  border-bottom:1px solid rgba(244,114,182,0.18);
  display:flex;align-items:center;justify-content:space-between;
}
.prompt-header-title{
  font-size:1.1rem;font-weight:700;
  background:linear-gradient(90deg,#f472b6,#c084fc,#38bdf8);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  display:flex;align-items:center;gap:8px;
}
.prompt-close-btn{
  background:rgba(244,114,182,0.12);border:1px solid rgba(244,114,182,0.25);
  color:#f9a8d4;width:30px;height:30px;border-radius:50%;
  display:flex;align-items:center;justify-content:center;
  font-size:0.95rem;cursor:pointer;transition:all .2s;
}
.prompt-close-btn:hover{background:rgba(244,114,182,0.25);transform:rotate(90deg);color:#fff}
.prompt-body{
  padding:18px 20px;overflow-y:auto;flex:1;display:flex;flex-direction:column;gap:14px;
}
.prompt-bot-tabs{
  display:flex;gap:8px;background:rgba(15,5,28,0.5);padding:4px;border-radius:14px;
  border:1px solid rgba(244,114,182,0.15);
}
.prompt-bot-tab{
  flex:1;padding:8px 10px;border-radius:10px;text-align:center;font-size:0.82rem;
  font-weight:600;color:#c084a8;cursor:pointer;transition:all .2s;display:flex;
  align-items:center;justify-content:center;gap:6px;border:none;background:transparent;
}
.prompt-bot-tab.active{
  background:linear-gradient(135deg,rgba(236,72,153,0.35),rgba(168,85,247,0.35));
  color:#fff;box-shadow:0 2px 10px rgba(236,72,153,0.3);border:1px solid rgba(244,114,182,0.4);
}
.prompt-ctx-box{display:flex;flex-direction:column;gap:6px}
.prompt-ctx-label{font-size:0.75rem;color:#c084a8;display:flex;justify-content:space-between}
.prompt-ctx-input{
  background:rgba(15,5,28,0.6);border:1px solid rgba(244,114,182,0.2);
  border-radius:12px;padding:9px 14px;color:#fce7f3;font-size:0.85rem;
  font-family:inherit;outline:none;resize:none;min-height:50px;max-height:90px;
}
.prompt-ctx-input:focus{border-color:rgba(244,114,182,0.5)}
.prompt-gen-btn{
  background:linear-gradient(135deg,#ec4899 0%,#a855f7 50%,#6366f1 100%);
  border:none;color:#fff;font-size:0.9rem;font-weight:700;
  padding:11px 18px;border-radius:14px;cursor:pointer;
  box-shadow:0 4px 18px rgba(236,72,153,0.4);transition:all .25s;
  display:flex;align-items:center;justify-content:center;gap:8px;
}
.prompt-gen-btn:hover{transform:translateY(-2px);box-shadow:0 6px 24px rgba(236,72,153,0.6)}
.prompt-gen-btn:disabled{opacity:0.6;cursor:wait;transform:none}
.prompt-result-section{display:flex;flex-direction:column;gap:12px}
.prompt-card{
  background:rgba(26,10,46,0.5);border:1px solid rgba(244,114,182,0.18);
  border-radius:14px;padding:12px 14px;display:flex;flex-direction:column;gap:8px;
  position:relative;
}
.prompt-card-header{
  display:flex;align-items:center;justify-content:space-between;
}
.prompt-card-title{font-size:0.8rem;font-weight:700;color:#f9a8d4;display:flex;align-items:center;gap:6px}
.prompt-copy-btn{
  background:rgba(244,114,182,0.15);border:1px solid rgba(244,114,182,0.3);
  color:#fce7f3;font-size:0.72rem;padding:3px 10px;border-radius:8px;
  cursor:pointer;transition:all .2s;display:flex;align-items:center;gap:4px;
}
.prompt-copy-btn:hover{background:rgba(244,114,182,0.3);color:#fff;box-shadow:0 0 8px rgba(244,114,182,0.4)}
.prompt-text-display{
  font-size:0.8rem;line-height:1.55;color:#e9d5ff;word-break:break-word;
  background:rgba(10,4,20,0.45);padding:10px 12px;border-radius:10px;
  border:1px solid rgba(244,114,182,0.1);max-height:130px;overflow-y:auto;
  font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;
}
.prompt-summary-display{
  font-size:0.83rem;line-height:1.6;color:#fdf2f8;background:rgba(236,72,153,0.08);
  border:1px solid rgba(236,72,153,0.25);border-radius:10px;padding:10px 12px;
}
.prompt-params-bar{
  display:flex;flex-wrap:wrap;gap:6px;background:rgba(15,5,28,0.4);
  padding:8px 12px;border-radius:10px;border:1px solid rgba(244,114,182,0.12);
}
.param-tag{font-size:0.7rem;color:#c084fc;background:rgba(192,132,252,0.12);padding:2px 8px;border-radius:6px}
.prompt-footer{
  padding:12px 20px;background:rgba(26,10,46,0.65);
  border-top:1px solid rgba(244,114,182,0.18);
  display:flex;align-items:center;justify-content:space-between;gap:10px;
}
.prompt-goto-btn{
  background:linear-gradient(135deg,#3b82f6,#8b5cf6);color:#fff;
  border:none;font-size:0.8rem;font-weight:600;padding:8px 14px;
  border-radius:10px;cursor:pointer;text-decoration:none;display:inline-flex;
  align-items:center;gap:6px;transition:all .2s;
}
.prompt-goto-btn:hover{transform:translateY(-1px);box-shadow:0 4px 14px rgba(59,130,246,0.4)}
.chat-prompt-header-btn{
  background:linear-gradient(135deg,rgba(236,72,153,0.35),rgba(168,85,247,0.35));
  border:1px solid rgba(244,114,182,0.45);color:#fce7f3;font-size:0.75rem;
  font-weight:600;padding:5px 12px;border-radius:14px;cursor:pointer;
  display:inline-flex;align-items:center;gap:5px;transition:all .2s;
  box-shadow:0 2px 8px rgba(236,72,153,0.25);margin-left:auto;
}
.chat-prompt-header-btn:hover{
  background:linear-gradient(135deg,rgba(236,72,153,0.55),rgba(168,85,247,0.55));
  transform:translateY(-1px);box-shadow:0 4px 12px rgba(236,72,153,0.45);
}
.msg-prompt-btn{
  background:rgba(168,85,247,.12);border:1px solid rgba(168,85,247,.3);color:#d8b4fe;
  font-size:0.68rem;border-radius:12px;padding:3px 8px;cursor:pointer;
  display:inline-flex;align-items:center;gap:4px;transition:all .2s;
}
.msg-prompt-btn:hover{background:rgba(168,85,247,.25);color:#fff;border-color:rgba(168,85,247,.5)}
.main-btn-prompt{
  background:linear-gradient(135deg,#ec4899,#8b5cf6);color:#fff;
  border:none;box-shadow:0 4px 12px rgba(236,72,153,.35);
  font-size:0.8rem;padding:6px 12px;border-radius:10px;cursor:pointer;
  font-weight:600;display:inline-flex;align-items:center;gap:4px;transition:all .2s;
  margin-left:6px;
}
.main-btn-prompt:hover{transform:translateY(-1px);box-shadow:0 6px 16px rgba(236,72,153,.5)}
.btn-txt-full{display:inline}
.btn-txt-short{display:none}

</style>
</head>
<body>
<div class="stars"></div>
<div class="offline-banner" id="offline-banner">聊天连接已断开，正在尝试重连…</div>
<div class="chat-header">
  <div class="chat-header-avatar"><img class="avatar-img" id="hdr-avatar-img" src="" onerror='this.style.display="none"'><span class="avatar-fallback">🦋</span></div>
  <div>
    <div class="chat-header-name" id="hdr-name">…</div>
    <div class="chat-header-status"><span class="status-dot" id="hdr-dot"></span><span id="hdr-status">连接中...</span></div>
  </div>
  <button class="chat-prompt-header-btn" onclick="openPromptModal(currentBot)">🎨 场景生图 Prompt</button>
  <div class="bot-tabs" id="bot-tabs"></div>
</div>
<div class="messages" id="messages">
  <div id="chat-loading" style="text-align:center;color:#a78ba0;font-size:0.8rem;padding:40px">加载中...</div>
</div>
<div class="typing" id="typing"><span id="typing-text"></span><span class="typing-dots"><span></span><span></span><span></span></span></div>
<div class="input-area">
  <textarea id="msg-input" placeholder="发消息..." rows="1" oninput="autoResize(this)" onkeydown="handleKey(event)"></textarea>
  <button class="send-btn" id="send-btn" onclick="sendMsg()">➤</button>
</div>
<button class="scroll-bottom" id="scroll-bottom" onclick="scrollToBottom()">↓</button>
<div class="toast" id="toast"></div>
<script>
var lastTs=0,displayedKeys={},msgIdCounter=0,audioMap={},msgIdToKey={},msgObjects={},autoScroll=true,firstLoad=true,lastDateKey="",pendingAudio=null,switchSilent=true,botTyping={};
var avatarTs=Date.now();
function avUrl(qq,size){return "https://q1.qlogo.cn/g?b=qq&nk="+(qq||"")+"&s="+(size||100)+"&t="+avatarTs}
var BOTS_DATA = __BOTS_DATA__;
var USER_QQ = "__USER_QQ__";
var BOT_COUNT=BOTS_DATA.length;
var currentBot=parseInt((location.search.match(/[?&]bot=(\d+)/)||[])[1])||BOT_COUNT;
if(currentBot<1||currentBot>BOT_COUNT)currentBot=BOT_COUNT;
function botQ(i){var b=BOTS_DATA[i-1];return b?b.qq:""}
function botName(i){var b=BOTS_DATA[i-1];return b?(b.preset_name||b.role):("Bot "+i)}
function esc(s){var d=document.createElement("div");d.textContent=s||"";return d.innerHTML}
function autoResize(el){el.style.height="auto";el.style.height=Math.min(el.scrollHeight,120)+"px"}
function handleKey(e){if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();sendMsg();}}
function showToast(t){var el=document.getElementById("toast");el.textContent=t;el.classList.add("show");clearTimeout(showToast._t);showToast._t=setTimeout(function(){el.classList.remove("show")},1300)}
function dateKey(ts){if(!ts)return"";var d=new Date(ts*1000);var y=d.getFullYear(),m=d.getMonth()+1,day=d.getDate();var today=new Date();var ky=y+"-"+m+"-"+day;if(ky===today.getFullYear()+"-"+(today.getMonth()+1)+"-"+today.getDate())return"今天";var yst=new Date(today.getTime()-86400000);if(ky===yst.getFullYear()+"-"+(yst.getMonth()+1)+"-"+yst.getDate())return"昨天";return y+"年"+m+"月"+day+"日"}
function appendDateDivider(key){if(key&&key!==lastDateKey){lastDateKey=key;var d=document.createElement("div");d.className="date-divider";d.textContent=key;document.getElementById("messages").appendChild(d)}}
function pollStatus(){
  avatarTs=Date.now();
  var _hd=document.getElementById("hdr-avatar-img");
  if(_hd&&botQ(currentBot))_hd.src=avUrl(botQ(currentBot),640);
  document.querySelectorAll(".bot-tab img").forEach(function(_t){var _b=parseInt(_t.getAttribute("data-bot"))||0;if(_b&&botQ(_b))_t.src=avUrl(botQ(_b),100)});
  fetch("/api/chat/status?bot="+currentBot).then(r=>r.json()).then(d=>{
    var el=document.getElementById("hdr-status");
    var dot=document.getElementById("hdr-dot");
    var banner=document.getElementById("offline-banner");
    var on=d&&d.connected;
    if(el)el.textContent=on?"已连接":"未连接";
    if(dot)dot.className="status-dot"+(on?" on":"");
    if(banner)banner.classList.toggle("show",!on);
  }).catch(()=>{});
}
function pollHistory(){
  fetch("/api/chat/history?bot="+currentBot+"&after_ts="+lastTs).then(r=>r.json()).then(d=>{
    if(!d||!d.ok||!d.data)return;
    d.data.forEach(function(m){appendMsg(m);});
    if(d.data.length>0)lastTs=d.data[d.data.length-1].ts||lastTs;
    var container=document.getElementById("messages");
    if(switchSilent){switchSilent=false;if(container){container.style.scrollBehavior="auto";container.scrollTop=container.scrollHeight;container.style.scrollBehavior="";}}
    var placeholder=document.getElementById("chat-loading");
    if(placeholder)placeholder.remove();
    if(firstLoad&&d.data.length===0){
      container.innerHTML='<div id="empty-state" style="text-align:center;color:#a78ba0;font-size:0.8rem;padding:40px">还没有消息，来和'+botName(currentBot)+'说句话吧 ♡</div>';
    }
    firstLoad=false;
  }).catch(()=>{});
}
function getMsgContext(btn){
  var msgEl=btn?btn.closest(".msg"):null;
  var container=document.getElementById("messages");
  if(!msgEl||!container)return "";
  var all=Array.prototype.slice.call(container.querySelectorAll(".msg"));
  var idx=all.indexOf(msgEl);
  if(idx<0)return "";
  var parts=[];
  for(var i=Math.max(0,idx-3);i<=Math.min(all.length-1,idx+3);i++){
    var b=all[i].querySelector(".msg-bubble");
    if(b)parts.push(b.textContent);
  }
  return parts.join("\n");
}
function appendMsg(msg){
  var container=document.getElementById("messages");
  if(!container)return;
  var key=msg.role+"_"+(msg.text||"")+"_"+(msg.source||"")+"_"+Math.round((msg.ts||0)*1000);
  if(displayedKeys[key])return;
  if(msg.role==="user"&&msg.source==="web"&&!msg._local&&msg.text===lastLocalUserText&&lastLocalUserTs&&Math.abs((msg.ts||0)-lastLocalUserTs)<5){displayedKeys[key]=true;return;}
  displayedKeys[key]=true;
  var placeholder=document.getElementById("chat-loading");
  if(placeholder)placeholder.remove();
  var empty=document.getElementById("empty-state");
  if(empty)empty.remove();
  var msgId="m"+(++msgIdCounter);
  msgIdToKey[msgId]=key;
  msgObjects[msgId]=msg;
  appendDateDivider(dateKey(msg.ts));
  var div=document.createElement("div");
  div.className="msg "+msg.role;
  var msgAv=msg.role==="user"?avUrl(USER_QQ,100):avUrl(botQ(currentBot),100);
  var avFb=msg.role==="user"?"🙂":"🦋";
  var html="<div class='msg-avatar'><img class='avatar-img' src='"+msgAv+"'><span class='avatar-fallback'>"+avFb+"</span></div>";
  html+="<div class='msg-body'>";
  html+="<div class='msg-bubble' title='点击复制' onclick='copyMsg(this)'>"+esc(msg.text)+"</div>";
  html+="<div class='msg-meta'><span>"+esc(msg.timestamp||"")+"</span>";
  if(msg.role==="assistant"){
    html+='<button class="msg-play-btn" id="btn-'+msgId+'" onclick="togglePlay(&#39;'+msgId+'&#39;)">▶ 播放</button>';
    html+='<button class="msg-prompt-btn" onclick="openPromptModal('+currentBot+', getMsgContext(this))">🎨 生图</button>';
  }
  html+="</div></div>";
  div.innerHTML=html;
  var avImg=div.querySelector(".avatar-img");
  if(avImg)avImg.onerror=function(){this.style.display="none"};
  container.appendChild(div);
  if(autoScroll&&!switchSilent)container.scrollTop=container.scrollHeight;
  if(msg.role==="assistant"&&!audioMap[key]&&!firstLoad){
    if(pendingAudio&&pendingAudio.text===msg.text){
      audioMap[key]={b64:pendingAudio.b64,format:pendingAudio.format};
      pendingAudio=null;
    }
  }
}
function copyMsg(el){
  var text=el.textContent;
  if(navigator.clipboard&&navigator.clipboard.writeText){
    navigator.clipboard.writeText(text).then(function(){showToast("已复制 ✓")}).catch(function(){});
  }else{
    var ta=document.createElement("textarea");ta.value=text;document.body.appendChild(ta);ta.select();try{document.execCommand("copy")}catch(e){}document.body.removeChild(ta);showToast("已复制 ✓");
  }
}
function togglePlay(msgId){
  var key=msgIdToKey[msgId];
  if(!key)return;
  if(audioMap[key]){playAudio(audioMap[key].b64,audioMap[key].format,document.getElementById("btn-"+msgId));}
  else{
    var btn=document.getElementById("btn-"+msgId);
    if(btn){var text=btn.closest(".msg-body").querySelector(".msg-bubble").textContent;requestTTS(key,text,msgId,true,(msgObjects[msgId]||{}).emotion);}
  }
}
function setWave(btn,on){
  if(!btn)return;
  if(on){
    btn.className="msg-play-btn playing";
    btn.innerHTML='<span class="wave"><span></span><span></span><span></span><span></span></span>';
  }else{
    btn.className="msg-play-btn";
    btn.textContent="▶ 播放";
  }
}
function requestTTS(key,text,btnId,autoplay,emotion){
  var btn=document.getElementById("btn-"+btnId);
  if(btn){btn.className="msg-play-btn loading";btn.textContent="⏳ 生成中...";}
  if(!text)return;
  fetch("/api/chat/tts",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({text:text,bot:currentBot,tts:false,emotion:emotion||""})})
  .then(r=>r.json()).then(d=>{
    if(d&&d.ok&&d.audio){
      audioMap[key]={b64:d.audio,format:d.audio_format||"mp3"};
      if(autoplay){
        setWave(btn,true);
        playAudio(d.audio,d.audio_format||"mp3",btn);
      }else{
        if(btn){btn.className="msg-play-btn";btn.textContent="▶ 播放";}
      }
    }else{
      if(btn){btn.textContent="▶ 暂无语音";btn.disabled=true;}
    }
  }).catch(()=>{if(btn){btn.className="msg-play-btn";btn.textContent="▶ 播放";}});
}
function playAudio(b64,fmt,btn){
  var audio=new Audio("data:audio/"+fmt+";base64,"+b64);
  if(btn){
    audio.onended=function(){setWave(btn,false)};
    audio.onerror=function(){setWave(btn,false)};
  }
  audio.play().catch(function(){if(btn)setWave(btn,false)});
}
var lastSentAt=0,lastLocalUserText="",lastLocalUserTs=0;
function sendMsg(){
  var input=document.getElementById("msg-input");
  var btn=document.getElementById("send-btn");
  var text=(input.value||"").trim();
  if(!text)return;
  var now=Date.now();
  if(now-lastSentAt<800)return;
  lastSentAt=now;
  var myBot=currentBot;
  input.value="";input.style.height="auto";
  lastLocalUserText=text;lastLocalUserTs=Date.now()/1000;
  appendMsg({role:"user",text:text,timestamp:new Date().toLocaleTimeString("zh-CN"),source:"web",ts:lastLocalUserTs,_local:true});
  botTyping[myBot]=true;
  document.getElementById("typing").className="typing show";
  fetch("/api/chat/send",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({text:text,bot:currentBot,tts:false})})
  .then(r=>r.json()).then(d=>{if(d&&d.ok&&d.reply&&d.audio){pendingAudio={text:d.reply,b64:d.audio,format:d.audio_format||"mp3"}}botTyping[myBot]=false;setTimeout(function(){var t=document.getElementById("typing");if(t&&currentBot===myBot&&!botTyping[myBot])t.className="typing";},8000);})
  .catch(()=>{botTyping[myBot]=false;if(currentBot===myBot)document.getElementById("typing").className="typing";});
}
function switchBot(i){
  if(i===currentBot)return;
  currentBot=i;
  var tabs=document.querySelectorAll(".bot-tab");
  tabs.forEach(function(t){t.classList.toggle("active",parseInt(t.getAttribute("data-bot"))===i)});
  var name=document.getElementById("hdr-name");
  if(name)name.textContent=botName(i);
  document.title=botName(i)+" · 聊天";
  var tpt=document.getElementById("typing-text");if(tpt)tpt.textContent=botName(i)+"正在输入";
  var mi=document.getElementById("msg-input");if(mi)mi.placeholder="发消息给"+botName(i)+"...";
  var img=document.getElementById("hdr-avatar-img");
  if(img)img.src=avUrl(botQ(i),640);
  lastTs=0;displayedKeys={};msgIdCounter=0;audioMap={};msgIdToKey={};msgObjects={};firstLoad=true;lastDateKey="";pendingAudio=null;lastLocalUserText="";lastLocalUserTs=0;
  switchSilent=true;
  var container=document.getElementById("messages");
  container.style.scrollBehavior="auto";
  container.scrollTop=0;
  container.innerHTML='<div id="chat-loading" style="text-align:center;color:#a78ba0;font-size:0.8rem;padding:40px">加载中...</div>';
  var tEl=document.getElementById("typing");
  if(tEl)tEl.className=botTyping[i]?"typing show":"typing";
  pollStatus();pollHistory();
}
function scrollToBottom(){var el=document.getElementById("messages");el.scrollTop=el.scrollHeight;autoScroll=true}
function buildTabs(){var box=document.getElementById("bot-tabs");if(!box)return;box.innerHTML="";BOTS_DATA.forEach(function(b,i){var idx=i+1;var d=document.createElement("div");d.className="bot-tab"+(idx===currentBot?" active":"");d.setAttribute("data-bot",idx);d.onclick=function(){switchBot(idx)};d.title=b.role||("Bot "+idx);var img=document.createElement("img");img.className="tab-avatar";img.setAttribute("data-bot",idx);img.src=b.qq?avUrl(b.qq,100):"";d.appendChild(img);box.appendChild(d)})}
buildTabs();
(function(){var tabs=document.querySelectorAll(".bot-tab");tabs.forEach(function(t){var i=parseInt(t.getAttribute("data-bot"));t.classList.toggle("active",i===currentBot);t.title=botName(i);var a=t.querySelector("img");if(a)a.src=avUrl(botQ(i),100)});var name=document.getElementById("hdr-name");if(name)name.textContent=botName(currentBot);document.title=botName(currentBot)+" · 聊天";var tpt=document.getElementById("typing-text");if(tpt)tpt.textContent=botName(currentBot)+"正在输入";var mi=document.getElementById("msg-input");if(mi)mi.placeholder="发消息给"+botName(currentBot)+"...";var img=document.getElementById("hdr-avatar-img");if(img)img.src=avUrl(botQ(currentBot),640)})();
pollStatus();pollHistory();
setInterval(pollHistory,2000);
setInterval(pollStatus,10000);
document.getElementById("messages").addEventListener("scroll",function(){
  var el=this;autoScroll=(el.scrollHeight-el.scrollTop-el.clientHeight)<60;
  var btn=document.getElementById("scroll-bottom");
  if(btn)btn.classList.toggle("show",!autoScroll);
});
</script>
<div style="text-align:center;padding:6px 0 12px;font-size:0.7rem;color:#a78ba0;opacity:0.7"><a href="https://github.com/KroMiose/nekro-agent" target="_blank" rel="noopener" style="color:inherit">基于 NekroAgent 构建</a></div>

<div class="prompt-modal-overlay" id="prompt-modal" onclick="if(event.target===this)closePromptModal()">
  <div class="prompt-modal">
    <div class="prompt-header">
      <div class="prompt-header-title">
        <span>🎨</span>
        <span id="pm-modal-title">AI 场景生图 Prompt 提炼</span>
      </div>
      <button class="prompt-close-btn" onclick="closePromptModal()">✕</button>
    </div>
    <div class="prompt-body">
      <div class="prompt-bot-tabs" id="pm-bot-tabs">
        <button class="prompt-bot-tab active" onclick="switchPromptBot(1)">🌸 雾岛澪</button>
        <button class="prompt-bot-tab" onclick="switchPromptBot(2)">🌙 霁月</button>
        <button class="prompt-bot-tab" onclick="switchPromptBot(3)">✨ 爱弥斯</button>
      </div>
      <div class="prompt-ctx-box">
        <div class="prompt-ctx-label">
          <span>场景/聊天上下文（留空则自动提取最新聊天对话）</span>
          <span style="cursor:pointer;color:#f472b6" onclick="document.getElementById('pm-ctx-input').value='';">清空</span>
        </div>
        <textarea class="prompt-ctx-input" id="pm-ctx-input" placeholder="输入自定义场景描述或关键词...留空则自动读取最新聊天上下文"></textarea>
      </div>
      <button class="prompt-gen-btn" id="pm-gen-btn" onclick="executeGeneratePrompt()">
        <span>✨</span><span id="pm-gen-btn-text">提炼场景生图 Prompt</span>
      </button>
      <div class="prompt-card" id="pm-context-card" style="display:none">
        <div class="prompt-card-header">
          <span class="prompt-card-title">💬 提取到的真实聊天记录</span>
          <button class="prompt-copy-btn" onclick="refreshChatContext()">🔄 刷新</button>
        </div>
        <div class="prompt-summary-display" id="pm-context-text" style="font-size:0.75rem;color:#be185d;max-height:90px;overflow-y:auto;white-space:pre-wrap;background:rgba(255,255,255,0.6);border-radius:6px;padding:6px 10px;line-height:1.45">...</div>
      </div>
      <div class="prompt-card" id="pm-vb-card">
        <div class="prompt-card-header" style="cursor:pointer" onclick="toggleVbBody()">
          <span class="prompt-card-title">📐 外貌基准 <span id="pm-vb-modified" style="font-size:0.62rem;color:#f0a6c9"></span></span>
          <span style="display:flex;gap:6px" onclick="event.stopPropagation()">
            <button class="prompt-copy-btn" id="pm-vb-edit-btn" onclick="enterVbEdit()">✏️ 编辑</button>
            <button class="prompt-copy-btn" onclick="resetVbInline()" style="border-color:rgba(244,63,94,.4);color:#fda4af">↺ 默认</button>
          </span>
        </div>
        <div class="prompt-summary-display" id="pm-vb-view" style="font-size:0.72rem;color:#be185d;line-height:1.5;white-space:pre-wrap;background:rgba(255,255,255,0.5);border-radius:6px;padding:6px 10px;max-height:120px;overflow-y:auto;display:none">...</div>
        <div id="pm-vb-edit" style="display:none">
          <div class="prompt-ctx-box">
            <div class="prompt-ctx-label"><span>角色定位 (role)</span></div>
            <input class="prompt-ctx-input" id="pm-vb-role" style="min-height:34px;max-height:34px" placeholder="例如：情感过敏症犬系依恋少女/合租人">
          </div>
          <div class="prompt-ctx-box" style="margin-top:8px">
            <div class="prompt-ctx-label"><span>外貌基准 Tags（Danbooru 英文标签）</span></div>
            <textarea class="prompt-ctx-input" id="pm-vb-tags" style="min-height:150px;max-height:220px" placeholder="- 核心外貌: 1girl, ...&#10;- 服饰: ...&#10;- 场景: ..."></textarea>
          </div>
          <div class="prompt-ctx-box" style="margin-top:8px">
            <div class="prompt-ctx-label"><span>兜底正向词 (fallback_positive)</span></div>
            <textarea class="prompt-ctx-input" id="pm-vb-fpos" style="min-height:80px;max-height:130px"></textarea>
          </div>
          <div class="prompt-ctx-box" style="margin-top:8px">
            <div class="prompt-ctx-label"><span>兜底场景小传 (fallback_summary)</span></div>
            <textarea class="prompt-ctx-input" id="pm-vb-fsum" style="min-height:50px;max-height:90px"></textarea>
          </div>
          <div style="display:flex;gap:8px;margin-top:10px">
            <button class="prompt-gen-btn" style="flex:2" onclick="saveVbInline()">💾 保存基准</button>
            <button class="prompt-copy-btn" style="flex:1;justify-content:center;padding:10px;font-size:0.8rem;border-radius:12px" onclick="cancelVbEdit()">取消</button>
          </div>
        </div>
      </div>
      <div class="prompt-result-section" id="pm-result-box" style="display:none">
        <div class="prompt-card">
          <div class="prompt-card-header">
            <span class="prompt-card-title">📝 画面场景小传</span>
            <span id="pm-char-tag" style="font-size:0.7rem;color:#f472b6;background:rgba(244,114,182,0.15);padding:2px 8px;border-radius:6px"></span>
          </div>
          <div class="prompt-summary-display" id="pm-summary-text">...</div>
        </div>
        <div class="prompt-card">
          <div class="prompt-card-header">
            <span class="prompt-card-title">✨ 正向生图提示词 (Positive Prompt)</span>
            <button class="prompt-copy-btn" onclick="copyPromptText('pm-pos-text', this)">📋 复制正向</button>
          </div>
          <div class="prompt-text-display" id="pm-pos-text">...</div>
        </div>
        <div class="prompt-card">
          <div class="prompt-card-header">
            <span class="prompt-card-title">🚫 负向提示词 (Negative Prompt)</span>
            <button class="prompt-copy-btn" onclick="copyPromptText('pm-neg-text', this)">📋 复制负向</button>
          </div>
          <div class="prompt-text-display" id="pm-neg-text">...</div>
        </div>
        <div class="prompt-params-bar">
          <span class="param-tag">模型: NovelAI Diffusion V3</span>
          <span class="param-tag">分辨率: 832×1216</span>
          <span class="param-tag">采样步数: 28</span>
          <span class="param-tag">CFG: 5.5</span>
          <span class="param-tag">采样器: Euler Ancestral</span>
        </div>
      </div>
    </div>
    <div class="prompt-footer">
      <a href="https://nai.sta1n.cn/" target="_blank" rel="noopener" class="prompt-goto-btn">
        <span>🚀</span><span>前往 Nai2API 图像工作台</span>
      </a>
      <div style="display:flex;gap:8px">
        <button class="prompt-copy-btn" style="padding:7px 12px;font-size:0.78rem;border-radius:10px" onclick="copyAllPrompts(this)">📋 复制全部 Prompt</button>
      </div>
    </div>
  </div>
</div>

<script>

var currentPromptBot = 1;
var lastPromptData = null;
var promptReqSeq = 0;

function openPromptModal(botIndex, prefilledContext) {
  if (botIndex) currentPromptBot = parseInt(botIndex) || 1;
  var modal = document.getElementById("prompt-modal");
  if (!modal) return;
  modal.classList.add("active");
  var tabs = document.querySelectorAll(".prompt-bot-tab");
  tabs.forEach(function(t, i) {
    t.classList.toggle("active", (i + 1) === currentPromptBot);
  });
  var ctxInput = document.getElementById("pm-ctx-input");
  if (ctxInput) {
    ctxInput.value = prefilledContext || "";
  }
  var resultBox = document.getElementById("pm-result-box");
  if (resultBox) resultBox.style.display = "none";
  if (prefilledContext && prefilledContext.trim()) {
    showContextPreview(prefilledContext);
  } else {
    fetchChatContextPreview();
  }
  renderVbSection();
}

function closePromptModal() {
  var modal = document.getElementById("prompt-modal");
  if (modal) modal.classList.remove("active");
}

var ctxReqSeq = 0;
function showContextPreview(text) {
  var ctxCard = document.getElementById("pm-context-card");
  var ctxText = document.getElementById("pm-context-text");
  if (ctxCard && ctxText) {
    if (text && text.trim()) {
      ctxCard.style.display = "block";
      ctxText.textContent = text.trim();
    } else {
      ctxCard.style.display = "none";
    }
  }
}
function fetchChatContextPreview() {
  var seq = ++ctxReqSeq;
  var ctxCard = document.getElementById("pm-context-card");
  var ctxText = document.getElementById("pm-context-text");
  if (ctxCard) ctxCard.style.display = "block";
  if (ctxText) ctxText.textContent = "正在提取该 Bot 最近聊天记录...";
  fetch("/api/chat-context", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({bot: currentPromptBot})
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (seq !== ctxReqSeq) return;
    showContextPreview(d && d.ok ? (d.used_context || "") : "");
  })
  .catch(function() {
    if (seq !== ctxReqSeq) return;
    showContextPreview("");
  });
}
function refreshChatContext() {
  var ctxInput = document.getElementById("pm-ctx-input");
  if (ctxInput) ctxInput.value = "";
  fetchChatContextPreview();
}
var vbDataCache = {};
function renderVbSection() {
  var view = document.getElementById("pm-vb-view");
  var modEl = document.getElementById("pm-vb-modified");
  var editBox = document.getElementById("pm-vb-edit");
  if (view) view.style.display = "none";
  if (modEl) modEl.textContent = "";
  if (editBox) editBox.style.display = "none";
  fetch("/api/visual-benchmarks").then(function(r) { return r.json(); }).then(function(d) {
    var items = d.benchmarks || [];
    var cur = null;
    for (var i = 0; i < items.length; i++) {
      if (items[i].bot_index === currentPromptBot) { cur = items[i]; break; }
    }
    if (!cur) return;
    vbDataCache[currentPromptBot] = cur;
    var name = cur.name || ("Bot " + currentPromptBot);
    if (modEl) modEl.textContent = cur.modified ? "（已自定义）" : "（内置默认）";
    var text = "【" + name + " · " + (cur.role || "") + "】\n" + (cur.tags || "");
    if (view) { view.textContent = text; view.style.display = "block"; }
  }).catch(function() { showToast("外貌基准加载失败", "error"); });
}
function toggleVbBody() {
  var view = document.getElementById("pm-vb-view");
  if (view) view.style.display = view.style.display === "none" ? "block" : "none";
}
function enterVbEdit() {
  var cur = vbDataCache[currentPromptBot];
  if (!cur) { showToast("基准尚未加载，请稍候", "error"); return; }
  document.getElementById("pm-vb-role").value = cur.role || "";
  document.getElementById("pm-vb-tags").value = cur.tags || "";
  document.getElementById("pm-vb-fpos").value = cur.fallback_positive || "";
  document.getElementById("pm-vb-fsum").value = cur.fallback_summary || "";
  document.getElementById("pm-vb-view").style.display = "none";
  document.getElementById("pm-vb-edit").style.display = "block";
}
function cancelVbEdit() {
  document.getElementById("pm-vb-edit").style.display = "none";
  var view = document.getElementById("pm-vb-view");
  if (view) view.style.display = "block";
}
function saveVbInline() {
  var data = {
    role: document.getElementById("pm-vb-role").value,
    tags: document.getElementById("pm-vb-tags").value,
    fallback_positive: document.getElementById("pm-vb-fpos").value,
    fallback_summary: document.getElementById("pm-vb-fsum").value
  };
  fetch("/api/visual-benchmarks", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({bot_index: currentPromptBot, data: data})
  }).then(function(r) { return r.json(); }).then(function(d) {
    showToast(d.msg || (d.ok ? "已保存" : "保存失败"), d.ok ? "success" : "error");
    if (d.ok) { cancelVbEdit(); renderVbSection(); }
  }).catch(function(e) { showToast("请求失败: " + e.message, "error"); });
}
function resetVbInline() {
  if (!confirm("确认恢复该角色的内置默认外貌基准？当前自定义内容将被清空。")) return;
  fetch("/api/visual-benchmarks/reset", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({bot_index: currentPromptBot})
  }).then(function(r) { return r.json(); }).then(function(d) {
    showToast(d.msg || (d.ok ? "已恢复默认" : "重置失败"), d.ok ? "success" : "error");
    if (d.ok) { cancelVbEdit(); renderVbSection(); }
  }).catch(function(e) { showToast("请求失败: " + e.message, "error"); });
}

function switchPromptBot(botIndex) {
  currentPromptBot = botIndex;
  var tabs = document.querySelectorAll(".prompt-bot-tab");
  tabs.forEach(function(t, i) {
    t.classList.toggle("active", (i + 1) === currentPromptBot);
  });
  var ctxInput = document.getElementById("pm-ctx-input");
  if (ctxInput) ctxInput.value = "";
  var resultBox = document.getElementById("pm-result-box");
  if (resultBox) resultBox.style.display = "none";
  fetchChatContextPreview();
  renderVbSection();
}

function executeGeneratePrompt() {
  var seq = ++promptReqSeq;
  var btn = document.getElementById("pm-gen-btn");
  var btnText = document.getElementById("pm-gen-btn-text");
  var ctxInput = document.getElementById("pm-ctx-input");
  var customCtx = ctxInput ? ctxInput.value.trim() : "";
  
  if (btn) btn.disabled = true;
  if (btnText) btnText.textContent = "正在提炼场景与生成 Prompt...";
  
  fetch("/api/generate-prompt", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({
      bot: currentPromptBot,
      context: customCtx,
      style: "novelai"
    })
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (seq !== promptReqSeq) return;
    if (btn) btn.disabled = false;
    if (btnText) btnText.textContent = "✨ 重新提炼 / 生成 Prompt";
    if (d && d.ok) {
      lastPromptData = d;
      renderPromptResult(d);
    } else {
      showToast("生成失败: " + (d ? d.msg : "网络异常"), "error");
    }
  })
  .catch(function(err) {
    if (seq !== promptReqSeq) return;
    if (btn) btn.disabled = false;
    if (btnText) btnText.textContent = "✨ 重新提炼 / 生成 Prompt";
    showToast("请求失败: " + err.message, "error");
  });
}

function renderPromptResult(data) {
  var resultBox = document.getElementById("pm-result-box");
  if (!resultBox) return;
  resultBox.style.display = "flex";
  
  var ctxCard = document.getElementById("pm-context-card");
  var ctxText = document.getElementById("pm-context-text");
  if (ctxCard && ctxText) {
    if (data.used_context && data.used_context.trim()) {
      ctxCard.style.display = "block";
      ctxText.textContent = data.used_context.trim();
    } else {
      ctxCard.style.display = "none";
    }
  }
  
  var charTag = document.getElementById("pm-char-tag");
  if (charTag) {
    var tagStr = (data.character_name || "") + " · " + (data.role || "");
    if (data.model_used) tagStr += " · 🤖 " + data.model_used;
    if (data.is_fallback) tagStr += " · ⚠️ 静态兜底(模型限流)";
    charTag.textContent = tagStr;
  }
  
  var summary = document.getElementById("pm-summary-text");
  if (summary) summary.textContent = data.scene_summary || "暂无场景描述";
  
  var posText = document.getElementById("pm-pos-text");
  if (posText) posText.textContent = data.positive_prompt || "";
  
  var negText = document.getElementById("pm-neg-text");
  if (negText) negText.textContent = data.negative_prompt || "";
}

function copyPromptText(elementId, btn) {
  var el = document.getElementById(elementId);
  if (!el) return;
  var text = el.textContent;
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(function() {
      showToast("已复制到剪贴板 ✓");
      if (btn) {
        var old = btn.textContent;
        btn.textContent = "已复制 ✓";
        setTimeout(function() { btn.textContent = old; }, 1200);
      }
    });
  } else {
    var ta = document.createElement("textarea");
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand("copy"); } catch(e) {}
    document.body.removeChild(ta);
    showToast("已复制到剪贴板 ✓");
  }
}

function copyAllPrompts(btn) {
  if (!lastPromptData) { showToast("还没有可复制的 Prompt", "error"); return; }
  var allText = "### Positive Prompt:\n" + (lastPromptData.positive_prompt || "") + "\n\n### Negative Prompt:\n" + (lastPromptData.negative_prompt || "");
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(allText).then(function() {
      showToast("Prompt 已全部复制 ✓");
      if (btn) {
        var old = btn.textContent;
        btn.textContent = "全部已复制 ✓";
        setTimeout(function() { btn.textContent = old; }, 1200);
      }
    });
  } else {
    var ta = document.createElement("textarea");
    ta.value = allText;
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand("copy"); } catch(e) {}
    document.body.removeChild(ta);
    showToast("Prompt 已全部复制 ✓");
    if (btn) {
      var old = btn.textContent;
      btn.textContent = "全部已复制 ✓";
      setTimeout(function() { btn.textContent = old; }, 1200);
    }
  }
}

</script>
</body>
</html>
'''

HTML_PAGE = r'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
<meta name="theme-color" content="#fffafc">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="小栖bot">
<meta name="mobile-web-app-capable" content="yes">
<link rel="manifest" href="/manifest.json">
<link rel="icon" type="image/svg+xml" href="/icon.svg">
<link rel="apple-touch-icon" href="/icon.svg">
<title>小栖bot</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;background:#1a0a14;color:#fce7f3;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}
.login-box{background:rgba(56,28,42,0.85);border:1px solid rgba(244,114,182,0.18);border-radius:16px;padding:36px 28px;max-width:340px;width:100%}
.login-icon{width:72px;height:72px;margin:0 auto 16px;background:url(/icon.svg) center/contain no-repeat;border-radius:16px}
.login-title{text-align:center;font-size:1.3rem;font-weight:700;background:linear-gradient(90deg,#f472b6,#e879f9,#fb7185);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;margin-bottom:24px}
.login-input{width:100%;padding:12px 16px;border-radius:10px;border:1px solid rgba(244,114,182,0.3);background:rgba(26,10,20,0.6);color:#fce7f3;font-size:0.95rem;margin-bottom:12px;outline:none}
.login-input:focus{border-color:#f472b6}
.login-input::placeholder{color:#7a5560}
.login-btn{width:100%;padding:13px;border-radius:10px;border:none;background:linear-gradient(135deg,#f472b6,#e879f9);color:#fff;font-size:0.95rem;font-weight:700;cursor:pointer;transition:all 0.2s ease;box-shadow:0 4px 14px rgba(244,114,182,0.3)}
.login-btn:active{transform:scale(0.97)}
.login-err{text-align:center;color:#f87171;font-size:0.8rem;margin-top:10px;min-height:1.2em}

/* 聊天记录样式 */
.chat-container{max-height:420px;overflow-y:auto;padding:10px;background:rgba(56,28,42,0.35);border-radius:12px;margin-bottom:10px;scrollbar-width:thin}
.chat-container::-webkit-scrollbar{width:4px}
.chat-container::-webkit-scrollbar-thumb{background:rgba(244,114,182,0.3);border-radius:2px}
.chat-msg{display:flex;margin-bottom:12px;animation:fadeIn 0.3s ease;gap:8px}
.chat-msg.user{flex-direction:row-reverse}
.chat-msg.assistant{flex-direction:row}
.chat-avatar{width:32px;height:32px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:0.75rem;font-weight:bold}
.chat-msg.user .chat-avatar{background:linear-gradient(135deg,#ec4899,#db2777);color:#fff}
.chat-msg.assistant .chat-avatar{background:linear-gradient(135deg,#f9a8d4,#f472b6);color:#fff}
.chat-content{display:flex;flex-direction:column;max-width:68%}
.chat-msg.user .chat-content{align-items:flex-end}
.chat-msg.assistant .chat-content{align-items:flex-start}
.chat-bubble{padding:10px 15px;border-radius:16px;font-size:0.85rem;line-height:1.6;word-break:break-word}
.chat-msg.user .chat-bubble{background:linear-gradient(135deg,#ec4899,#db2777);color:#fff;border-bottom-right-radius:4px;box-shadow:0 2px 8px rgba(236,72,153,0.2)}
.chat-msg.assistant .chat-bubble{background:rgba(244,114,182,0.12);color:#fce7f3;border:1px solid rgba(244,114,182,0.2);border-bottom-left-radius:4px;box-shadow:0 2px 8px rgba(0,0,0,0.1)}
.chat-meta{font-size:0.65rem;color:rgba(244,114,182,0.5);margin-top:4px;padding:0 4px;display:flex;align-items:center;gap:4px}
.chat-msg.user .chat-meta{justify-content:flex-end}
.chat-source{display:inline-block;font-size:0.6rem;padding:1px 5px;border-radius:3px;vertical-align:middle}
.chat-source.device{background:rgba(96,165,250,0.2);color:#93c5fd}
.chat-source.web{background:rgba(244,114,182,0.2);color:#f9a8d4}
.chat-play-btn{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:12px;border:1px solid rgba(244,114,182,0.3);background:rgba(244,114,182,0.1);color:#f9a8d4;font-size:0.7rem;cursor:pointer;transition:all 0.2s;margin-top:4px}
.chat-play-btn:hover{background:rgba(244,114,182,0.2);border-color:rgba(244,114,182,0.5)}
.chat-play-btn.playing{background:rgba(236,72,153,0.3);border-color:#ec4899;color:#fff}
.chat-play-btn.loading{opacity:0.6;cursor:wait}
.chat-play-btn:disabled{opacity:0.4;cursor:not-allowed}
.chat-play-icon{font-size:0.8rem}
.chat-input-row{display:flex;gap:8px;align-items:center}
.chat-input-row input{flex:1;padding:10px 14px;border-radius:10px;border:1px solid rgba(244,114,182,0.3);background:rgba(56,28,42,0.6);color:#fce7f3;font-size:0.85rem;outline:none}
.chat-input-row input:focus{border-color:#ec4899;box-shadow:0 0 0 2px rgba(236,72,153,0.1)}
.chat-input-row button{padding:10px 20px;border-radius:10px;border:none;background:linear-gradient(135deg,#ec4899,#db2777);color:#fff;font-size:0.85rem;font-weight:600;cursor:pointer;white-space:nowrap;transition:opacity 0.2s}
.chat-input-row button:hover{opacity:0.85}
.chat-input-row button:disabled{opacity:0.5;cursor:not-allowed}
.chat-status{display:flex;align-items:center;gap:6px;font-size:0.75rem;margin-bottom:8px}
.chat-status-dot{width:8px;height:8px;border-radius:50%;display:inline-block}
@media(max-width:480px){.chat-container{max-height:320px}.chat-bubble{max-width:85%;font-size:0.8rem}}

*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{--safe-top:env(safe-area-inset-top,0px);--safe-bottom:env(safe-area-inset-bottom,0px);--pink-bg:#fffafc;--pink-card:rgba(255,255,255,0.72);--pink-card-solid:rgba(255,252,253,0.92);--pink-border:rgba(236,72,153,0.1);--pink-accent:#ec4899;--pink-light:#f9a8d4;--rose-accent:#fb7185;--fuchsia-accent:#d946ef;--bot1-color:#ec4899;--bot2-color:#ec4899;--bot3-color:#ec4899}
html{-webkit-text-size-adjust:100%}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Noto Sans CJK SC","Microsoft YaHei",sans-serif;background:linear-gradient(135deg,#fffafc 0%,#fff8fa 50%,#fff5f8 100%);background-attachment:fixed;color:#9d174d;min-height:100vh;padding:calc(20px + var(--safe-top)) 16px calc(20px + var(--safe-bottom));overscroll-behavior-y:none}
.container{max-width:1600px;margin:0 auto;flex:1}
.topbar{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;flex-wrap:wrap;gap:8px}
.topbar h1{font-size:1.5rem;background:linear-gradient(90deg,#ec4899,#d946ef,#fb7185);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;display:flex;align-items:center;gap:8px}
.topbar h1::before{content:"";width:28px;height:28px;background:url(/icon.svg) center/contain no-repeat;border-radius:6px}
.refresh-info{display:flex;align-items:center;gap:8px;font-size:0.75rem;color:#be185d}
.chat-entry-btn{display:inline-flex;align-items:center;gap:6px;padding:10px 18px;border-radius:24px;background:linear-gradient(135deg,#ec4899,#d946ef);color:#fff;font-size:0.9rem;font-weight:700;text-decoration:none;box-shadow:0 4px 14px rgba(236,72,153,0.3);transition:all 0.2s ease}
.chat-entry-btn:hover{transform:translateY(-1px);box-shadow:0 6px 18px rgba(236,72,153,0.4)}
.chat-entry-dot{width:8px;height:8px;border-radius:50%;background:#fff}
.chat-entry-dot.online{background:#4ade80;box-shadow:0 0 8px rgba(74,222,128,0.8)}
.chat-entry-dot.offline{background:#f87171;box-shadow:0 0 8px rgba(248,113,113,0.8)}
.pulse{width:8px;height:8px;border-radius:50%;background:#4ade80;box-shadow:0 0 8px rgba(74,222,128,0.6);animation:pulse 2s infinite}
.pulse.error{background:#f87171;box-shadow:0 0 8px rgba(248,113,113,0.6);animation:pulse-err 1s infinite}
@keyframes pulse{0%,100%{opacity:1;box-shadow:0 0 8px rgba(74,222,128,0.6)}50%{opacity:0.5;box-shadow:0 0 4px rgba(74,222,128,0.3)}}
@keyframes pulse-err{0%,100%{opacity:1;box-shadow:0 0 10px rgba(248,113,113,0.8)}50%{opacity:0.3;box-shadow:0 0 2px rgba(248,113,113,0.3)}}
.sys-bar{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:20px}
.sys-item{background:var(--pink-card);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid var(--pink-border);border-radius:10px;padding:10px 14px}
.sys-label{font-size:0.65rem;color:#be185d;text-transform:uppercase;letter-spacing:1px}
.sys-value{font-size:0.9rem;color:#ec4899;font-weight:600;margin-top:4px}
.sys-value.warn{color:#f59e0b}
.sys-value.danger{color:#ef4444}
.bots-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
.bot-sub{margin:10px 14px;border:1px solid rgba(244,114,182,.22);border-radius:12px;overflow:hidden;background:rgba(244,114,182,.07);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px)}.bot-sub-h{padding:10px 14px;font-size:.85rem;font-weight:600;cursor:pointer;display:flex;justify-content:space-between;align-items:center;color:#f9a8d4;user-select:none}.bot-sub-h:hover{background:rgba(244,114,182,.12)}.bot-sub-arrow{font-size:.7rem;color:#f472b6;transition:transform .2s}.bot-sub.open .bot-sub-arrow{transform:rotate(90deg)}.bot-sub-b{padding:12px 14px;display:none;min-width:0;border-top:1px solid rgba(244,114,182,.12)}.bot-sub-b .info-row,.bot-sub-b .guard-toggle,.bot-sub-b .sub-btns{flex-wrap:wrap}.bot-sub-b input{max-width:100%}.bot-sub.open .bot-sub-b{display:block}
@media(max-width:1200px){.bots-grid{grid-template-columns:repeat(auto-fit,minmax(340px,1fr))}}
.bot-card{position:relative;background:var(--pink-card);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1.5px solid transparent;border-radius:16px;overflow:hidden;transition:transform 0.3s ease,box-shadow 0.3s ease;min-width:0}
.bot-card::before{content:"";position:absolute;inset:0;border-radius:16px;padding:1.5px;background:linear-gradient(135deg,rgba(249,168,212,0.5),rgba(236,72,153,0.1),rgba(251,113,133,0.2),rgba(249,168,212,0.5));-webkit-mask:linear-gradient(#fff 0 0) content-box,linear-gradient(#fff 0 0);-webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none}
.bot-card:hover{box-shadow:0 8px 30px rgba(236,72,153,0.15);z-index:2}
.bot-card:hover::before{background:linear-gradient(135deg,rgba(249,168,212,0.8),rgba(236,72,153,0.25),rgba(251,113,133,0.35),rgba(249,168,212,0.8))}
.bot-avatar{width:42px;height:42px;border-radius:50%;border:2px solid var(--pink-border);object-fit:cover;flex-shrink:0}
.card-header{display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid rgba(236,72,153,0.08)}
.card-header-left{display:flex;align-items:center;gap:10px}.card-header-left>div{overflow:hidden;min-width:0;display:flex;align-items:center;gap:8px;flex-wrap:nowrap}.card-header-left>div .status-badge{padding:2px 8px;font-size:0.62rem;flex-shrink:0;line-height:1.5}
.bot-name{font-size:1.1rem;font-weight:700;color:#9d174d}
.bot-role{color:#be185d;font-size:0.8rem;margin-left:6px}
.status-badge{padding:4px 12px;border-radius:20px;font-size:0.75rem;font-weight:600;white-space:nowrap}
.status-online{background:rgba(74,222,128,0.15);color:#4ade80;border:1px solid rgba(74,222,128,0.3)}
.status-offline{background:rgba(248,113,113,0.15);color:#f87171;border:1px solid rgba(248,113,113,0.3)}
.status-unknown{background:rgba(251,191,36,0.15);color:#fbbf24;border:1px solid rgba(251,191,36,0.3)}
.card-body{padding:14px 18px}
.info-section{margin-bottom:14px}
.info-section:last-child{margin-bottom:0}
.section-title{font-size:0.65rem;color:#be185d;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}
.info-row{display:flex;justify-content:space-between;align-items:center;padding:7px 10px;background:rgba(255,255,255,0.5);backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);border-radius:6px;margin-bottom:3px}
.info-row .label{color:#be185d;font-size:0.78rem}
.info-row .value{color:#9d174d;font-size:0.78rem;font-family:"Cascadia Code","Fira Code","Consolas",monospace}
.info-row .value.green{color:#4ade80}
.info-row .value.red{color:#f87171}
.info-row .value.yellow{color:#fbbf24}
.progress-bar{width:100%;height:5px;background:rgba(255,255,255,0.5);border-radius:3px;overflow:hidden;margin-top:3px}
.progress-fill{height:100%;border-radius:3px;transition:width 0.5s ease;position:relative;overflow:hidden}
.progress-fill::after{content:"";position:absolute;top:0;left:-50%;width:50%;height:100%;background:linear-gradient(90deg,transparent,rgba(255,255,255,0.15),transparent);animation:shimmer 2s infinite}
@keyframes shimmer{0%{left:-50%}100%{left:100%}}
.progress-low{background:#4ade80}
.progress-mid{background:#fbbf24}
.progress-high{background:#f87171}
.main-btn{display:block;width:100%;padding:14px;border-radius:12px;font-size:0.95rem;font-weight:700;text-align:center;text-decoration:none;cursor:pointer;transition:all 0.2s ease;margin-bottom:8px;border:none}
.main-btn:active{transform:scale(0.97)}
.main-btn-napcat{background:linear-gradient(135deg,#ec4899,#d946ef);color:#fff;box-shadow:0 4px 14px rgba(236,72,153,0.25)}
.main-btn-nekro{background:linear-gradient(135deg,#fb7185,#ec4899);color:#fff;box-shadow:0 4px 14px rgba(251,113,133,0.25)}
.main-btn-chat{background:linear-gradient(135deg,#f472b6,#e879f9,#f472b6);background-size:200% 200%;color:#fff;padding:11px 22px;border-radius:12px;font-size:.9rem;position:relative;animation:chatGlow 2.6s ease infinite;transition:transform .2s,box-shadow .2s}
@keyframes chatGlow{0%,100%{background-position:0% 50%;box-shadow:0 4px 14px rgba(244,114,182,.3)}50%{background-position:100% 50%;box-shadow:0 4px 22px rgba(244,114,182,.65)}}
.main-btn-chat:hover{transform:scale(1.07);box-shadow:0 6px 26px rgba(244,114,182,.75)}.card-header-right{display:flex;align-items:center;gap:12px;flex-shrink:0;justify-content:flex-end}
.sub-btns{display:flex;gap:6px;margin-bottom:14px}
.sub-btns:last-child{margin-bottom:0}
.sub-btn{flex:1;padding:9px 6px;border-radius:8px;font-size:0.75rem;font-weight:600;border:1px solid;cursor:pointer;transition:all 0.2s ease;white-space:nowrap;text-align:center}
.sub-btn:active{transform:scale(0.95)}
.sub-btn:disabled{opacity:0.4;cursor:not-allowed}
.sub-btn-log{background:rgba(236,72,153,0.08);color:#ec4899;border-color:rgba(236,72,153,0.2)}
.sub-btn-restart{background:rgba(245,158,11,0.08);color:#f59e0b;border-color:rgba(245,158,11,0.2)}
.sub-btn-stop{background:rgba(239,68,68,0.08);color:#ef4444;border-color:rgba(239,68,68,0.2)}
.sub-btn-start{background:rgba(74,222,128,0.08);color:#22c55e;border-color:rgba(74,222,128,0.2)}
.guard-toggle{display:flex;align-items:center;justify-content:space-between;padding:8px 10px;background:rgba(255,255,255,0.5);border-radius:6px;margin-bottom:8px}
.guard-label{font-size:0.75rem;color:#be185d;display:flex;align-items:center;gap:4px}
.guard-switch{position:relative;width:38px;height:20px;border-radius:10px;background:rgba(248,113,113,0.2);border:1px solid rgba(248,113,113,0.3);cursor:pointer;transition:all 0.3s ease}
.guard-switch.on{background:rgba(74,222,128,0.2);border-color:rgba(74,222,128,0.3)}
.guard-switch::after{content:"";position:absolute;top:1px;left:1px;width:16px;height:16px;border-radius:50%;background:#f87171;transition:all 0.3s ease}
.guard-switch.on::after{left:19px;background:#4ade80}
.guard-records{font-size:0.65rem;color:#be185d;margin-top:6px;padding:6px 10px;background:rgba(255,255,255,0.5);border-radius:6px;max-height:80px;overflow:auto;display:none}
.guard-records.active{display:block}
.guard-record{padding:2px 0;border-bottom:1px solid rgba(236,72,153,0.06)}
.guard-record:last-child{border-bottom:none}
.footer{text-align:center;color:#be185d;font-size:0.7rem;margin-top:20px;padding-bottom:10px;opacity:0.6}
.error-msg{background:rgba(239,68,68,0.08);border:1px solid rgba(239,68,68,0.25);color:#ef4444;padding:10px 16px;border-radius:8px;margin-bottom:14px;font-size:0.82rem}
.warn-msg{background:rgba(245,158,11,0.08);border:1px solid rgba(245,158,11,0.25);color:#f59e0b;padding:10px 16px;border-radius:8px;margin-bottom:14px;font-size:0.82rem}
.refresh-btn{background:rgba(236,72,153,0.1);border:1px solid rgba(236,72,153,0.25);color:#ec4899;padding:6px 14px;border-radius:8px;font-size:0.75rem;font-weight:600;cursor:pointer;transition:all 0.2s ease}
.refresh-btn:disabled{opacity:0.5}
.modal-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(157,23,77,0.2);z-index:9999;justify-content:center;align-items:flex-end;padding:0;backdrop-filter:blur(4px);-webkit-backdrop-filter:blur(4px)}
.modal-overlay.active{display:flex}
.modal-box{background:rgba(255,252,253,0.96);backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);width:100%;max-width:800px;max-height:85vh;border-radius:16px 16px 0 0;display:flex;flex-direction:column;border:1px solid var(--pink-border);border-bottom:none}
.modal-header{display:flex;justify-content:space-between;align-items:center;padding:14px 18px;border-bottom:1px solid rgba(236,72,153,0.08);flex-shrink:0}
.modal-title{font-size:1rem;font-weight:700;color:#ec4899}
.modal-close{background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.25);color:#ef4444;padding:5px 14px;border-radius:8px;font-size:0.75rem;font-weight:600;cursor:pointer}
.modal-body{flex:1;overflow:auto;padding:12px 16px;-webkit-overflow-scrolling:touch}
.log-content{font-family:"Cascadia Code","Fira Code","Consolas",monospace;font-size:0.7rem;line-height:1.5;color:#9d174d;white-space:pre-wrap;word-break:break-all}
.log-content .log-err{color:#ef4444}
.log-content .log-warn{color:#f59e0b}
.log-content .log-ok{color:#22c55e}
.modal-footer{padding:10px 16px;border-top:1px solid rgba(236,72,153,0.08);display:flex;gap:8px;flex-shrink:0}
.modal-footer button{flex:1;padding:8px;border-radius:8px;font-size:0.75rem;font-weight:600;border:1px solid;cursor:pointer}
.toast{position:fixed;top:calc(20px + var(--safe-top));left:50%;transform:translateX(-50%);background:rgba(255,255,255,0.95);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid var(--pink-border);color:#9d174d;padding:10px 20px;border-radius:10px;font-size:0.8rem;z-index:10000;opacity:0;transition:opacity 0.3s ease;pointer-events:none;max-width:90vw;text-align:center;box-shadow:0 4px 20px rgba(236,72,153,0.1)}
.toast.show{opacity:1}
.toast.success{border-color:rgba(74,222,128,0.3);color:#4ade80}
.toast.error{border-color:rgba(248,113,113,0.3);color:#f87171}
.svc-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:14px}
.svc-card{background:rgba(255,255,255,0.7);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid var(--pink-border);border-radius:14px;overflow:hidden;transition:all 0.3s ease}
.svc-card.running{border-color:rgba(74,222,128,0.3)}
.svc-card.stopped{border-color:rgba(248,113,113,0.3)}
.svc-header{display:flex;justify-content:space-between;align-items:center;padding:12px 16px;background:rgba(236,72,153,0.04)}
.svc-name{font-size:0.95rem;font-weight:700;color:#ec4899}
.svc-body{padding:12px 16px}
.svc-row{display:flex;justify-content:space-between;align-items:center;padding:6px 10px;background:rgba(255,255,255,0.5);border-radius:6px;margin-bottom:3px}
.svc-row .label{color:#be185d;font-size:0.78rem}
.svc-row .value{color:#9d174d;font-size:0.78rem;font-family:"Cascadia Code","Fira Code","Consolas",monospace}
.section-title-main{font-size:0.9rem;font-weight:700;color:#ec4899;margin-bottom:10px;padding:0 4px}
.note-sync-card{background:rgba(255,255,255,0.7);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid var(--pink-border);border-radius:14px;margin-bottom:14px;overflow:hidden}
.note-sync-card.synced{border-color:rgba(74,222,128,0.3)}
.note-sync-card.not-synced{border-color:rgba(248,113,113,0.3)}
.nav-grid{display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap}
.nav-btn{flex:1;min-width:140px;padding:14px;border-radius:12px;font-size:0.9rem;font-weight:700;text-align:center;text-decoration:none;cursor:pointer;transition:all 0.2s ease;border:none}
.nav-btn:active{transform:scale(0.97)}
.nav-btn-tavern{background:linear-gradient(135deg,#a78bfa,#7c3aed);color:#fff;box-shadow:0 4px 14px rgba(124,58,237,0.25)}
.nav-btn-panel{background:linear-gradient(135deg,#38bdf8,#0284c7);color:#fff;box-shadow:0 4px 14px rgba(2,132,199,0.25)}
.collapse-section{background:rgba(255,255,255,0.6);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid var(--pink-border);border-radius:14px;margin-bottom:14px;overflow:hidden;transition:border-color 0.3s ease}
.collapse-header{display:flex;align-items:center;padding:14px 16px;cursor:pointer;user-select:none;-webkit-user-select:none;transition:background 0.2s ease;gap:8px}
.collapse-header:active{background:rgba(236,72,153,0.06)}
.collapse-title{font-size:0.9rem;font-weight:700;color:#ec4899;flex-shrink:0}
.collapse-summary{font-size:0.72rem;color:#be185d;flex:1;text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;opacity:0.8}
.collapse-arrow{width:0;height:0;border-left:5px solid transparent;border-right:5px solid transparent;border-top:6px solid #ec4899;transition:transform 0.3s ease;flex-shrink:0}
.collapse-section.collapsed .collapse-arrow{transform:rotate(-90deg)}
.collapse-body{max-height:9999px;transition:max-height 0.4s ease;overflow:hidden}
.collapse-section.collapsed .collapse-body{max-height:0}
.collapse-body-inner{padding:0 16px 14px}
.collapse-section.collapsed .collapse-body-inner{padding:0 16px}





@keyframes pulse-dot{0%,100%{opacity:1}50%{opacity:0.3}}
@media(max-width:768px){.svc-grid{grid-template-columns:repeat(2,1fr)}.nav-grid{flex-wrap:wrap}.nav-btn{min-width:45%}}
@media(max-width:480px){
body{padding:calc(12px + var(--safe-top)) 10px calc(12px + var(--safe-bottom))}
.topbar h1{font-size:1.2rem}
.sys-bar{grid-template-columns:repeat(2,1fr);gap:8px}
.sys-item{padding:8px 10px}
.sys-value{font-size:0.8rem}
.bots-grid{grid-template-columns:1fr;gap:12px}
.bot-card{border-radius:12px}
.card-header{display:flex;justify-content:space-between;align-items:center;padding:12px 12px;flex-wrap:nowrap;gap:8px}
.card-header-left{overflow:hidden;min-width:0;flex:1;gap:10px}
.card-header-left>div{overflow:hidden;min-width:0;display:flex;align-items:center;gap:4px;flex-wrap:nowrap}
.card-header-left>div .status-badge{padding:1px 7px;font-size:0.58rem;flex-shrink:0;line-height:1.5;white-space:nowrap}
.bot-name{font-size:1rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:100%;flex-shrink:1}
.card-header-right{display:flex;align-items:center;flex-shrink:0;gap:4px}
.card-header-right .main-btn-chat{padding:8px 11px;font-size:.76rem;white-space:nowrap;margin-bottom:0}
.card-header-right .main-btn-prompt{margin-left:0;padding:5px 8px;font-size:.68rem;white-space:nowrap;margin-bottom:0}
.card-header-right .main-btn-vb{margin-left:0;padding:5px 8px;font-size:.68rem;white-space:nowrap;margin-bottom:0}
.bot-avatar{width:40px;height:40px}
.btn-txt-full{display:none}
.btn-txt-short{display:inline}
.card-body{padding:10px 12px}
.info-row{padding:5px 8px}
.info-row .label{font-size:0.72rem;flex-shrink:0}
.info-row .value{font-size:0.72rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:60%;text-align:right}
.main-btn{padding:11px;font-size:0.85rem}
.sub-btns{flex-wrap:wrap;gap:4px}
.sub-btn{flex:1 1 30%;padding:8px 4px;font-size:0.68rem;white-space:normal;line-height:1.2;min-width:30%}
.guard-toggle{padding:6px 8px}
.guard-label{font-size:0.68rem}
.modal-box{border-radius:12px 12px 0 0}
.svc-grid{grid-template-columns:1fr}
.svc-card{border-radius:12px}
.svc-header{padding:10px 12px}
.svc-name{font-size:0.85rem}
.svc-body{padding:10px 12px}
.svc-row{padding:5px 8px}
.svc-row .label{font-size:0.72rem;flex-shrink:0}
.svc-row .value{font-size:0.72rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:55%;text-align:right}
.note-sync-card{margin-bottom:14px;border-radius:12px}
.section-title-main{font-size:0.85rem;margin-bottom:8px}
.nav-grid{flex-direction:column;gap:8px}
.nav-btn{min-width:100%;padding:12px;font-size:0.85rem}
.collapse-header{padding:12px 14px}
.collapse-title{font-size:0.85rem}
.collapse-summary{font-size:0.68rem}
.collapse-body-inner{padding:0 14px 12px}
}
/* ===== 模型卡片精简 ===== */
.model-card{background:var(--pink-card);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid var(--pink-border);border-radius:14px;padding:12px 14px;position:relative;overflow:hidden}
.model-card::before{content:"";position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,#ec4899,#d946ef,#f472b6,#ec4899);background-size:200% 100%;animation:modelBar 4s ease infinite;opacity:.75}
@keyframes modelBar{0%,100%{background-position:0% 50%}50%{background-position:100% 50%}}
.model-select{width:100%;background:#ffffff;border:1px solid rgba(236,72,153,.35);border-radius:8px;padding:9px 12px;color:#9d174d;font-size:.85rem;font-weight:600;cursor:pointer;outline:none;transition:border-color .2s,box-shadow .2s;appearance:auto;-webkit-appearance:auto}
.model-select:focus{border-color:#ec4899;box-shadow:0 0 0 3px rgba(236,72,153,.15)}
.model-btns{display:flex;gap:8px;margin-top:10px}
.model-btn{flex:1;padding:9px 6px;border-radius:10px;font-size:.78rem;font-weight:700;border:none;cursor:pointer;transition:all .2s ease;white-space:nowrap;text-align:center;letter-spacing:.5px}
.model-btn:active{transform:scale(.96)}
.model-btn:disabled{opacity:.5;cursor:not-allowed}
.model-btn-switch{background:linear-gradient(135deg,#ec4899,#d946ef);color:#fff;box-shadow:0 4px 14px rgba(236,72,153,.3)}
.model-btn-switch:hover{box-shadow:0 6px 20px rgba(236,72,153,.5);transform:translateY(-1px)}
.model-btn-add{background:linear-gradient(135deg,#8b5cf6,#6366f1);color:#fff;box-shadow:0 4px 14px rgba(139,92,246,.3)}
.model-btn-add:hover{box-shadow:0 6px 20px rgba(139,92,246,.5);transform:translateY(-1px)}
.model-btn-del{background:linear-gradient(135deg,#f43f5e,#e11d48);color:#fff;box-shadow:0 4px 14px rgba(244,63,94,.3)}
.model-btn-del:hover{box-shadow:0 6px 20px rgba(244,63,94,.5);transform:translateY(-1px)}
.model-btn-test{background:linear-gradient(135deg,#06b6d4,#0284c7);color:#fff;box-shadow:0 4px 14px rgba(6,182,212,.3)}
.model-btn-test:hover{box-shadow:0 6px 20px rgba(6,182,212,.5);transform:translateY(-1px)}
.model-btn-test:disabled{opacity:.55;cursor:wait;transform:none;box-shadow:none}
.model-btn-plain{background:rgba(100,100,110,.12);color:#9d174d;border:1px solid rgba(157,23,77,.2)}
.model-btn-plain:hover{background:rgba(100,100,110,.2)}
.model-btn-ok{background:linear-gradient(135deg,#10b981,#059669);color:#fff;box-shadow:0 4px 14px rgba(16,185,129,.3)}
.model-btn-ok:hover{box-shadow:0 6px 20px rgba(16,185,129,.5);transform:translateY(-1px)}
.model-tip{font-size:.75rem;color:#9d174d;opacity:.75;text-align:center;padding:6px 0 2px}
/* ===== 添加模型组模态框 ===== */
.addg-field{margin-bottom:12px}
.addg-label{font-size:.75rem;color:#be185d;font-weight:600;margin-bottom:5px;letter-spacing:.3px}
.addg-input{width:100%;background:#ffffff;border:1px solid rgba(236,72,153,.3);border-radius:8px;padding:10px 12px;color:#9d174d;font-size:.85rem;outline:none;box-sizing:border-box;transition:border-color .2s,box-shadow .2s}
.addg-input:focus{border-color:#ec4899;box-shadow:0 0 0 3px rgba(236,72,153,.12)}
.addg-input::placeholder{color:#f0a6c9}
.addg-btns{display:flex;gap:8px;margin-top:14px}
.addg-btns .model-btn{flex:1}
/* ===== 添加模型组模态框 end ===== */
/* ===== 模型卡片美化 end ===== */

/* ===== Prompt Modal ===== */
.prompt-modal-overlay{
  position:fixed;inset:0;z-index:99999;
  background:rgba(10,4,20,0.78);
  backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);
  display:flex;align-items:center;justify-content:center;
  opacity:0;pointer-events:none;transition:all .3s ease;
  padding:16px;box-sizing:border-box;
}
.prompt-modal-overlay.active{opacity:1;pointer-events:auto}
.prompt-modal{
  background:linear-gradient(160deg,#1f0c35 0%,#281042 50%,#1a082b 100%);
  border:1px solid rgba(244,114,182,0.35);
  box-shadow:0 20px 60px rgba(0,0,0,0.6),0 0 30px rgba(236,72,153,0.25);
  border-radius:24px;width:100%;max-width:680px;max-height:90vh;
  display:flex;flex-direction:column;overflow:hidden;
  transform:scale(0.92) translateY(20px);transition:all .3s cubic-bezier(0.34,1.56,0.64,1);
  box-sizing:border-box;
}
.prompt-modal-overlay.active .prompt-modal{transform:scale(1) translateY(0)}
.prompt-header{
  padding:16px 20px;
  background:rgba(26,10,46,0.65);
  border-bottom:1px solid rgba(244,114,182,0.18);
  display:flex;align-items:center;justify-content:space-between;
}
.prompt-header-title{
  font-size:1.1rem;font-weight:700;
  background:linear-gradient(90deg,#f472b6,#c084fc,#38bdf8);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  display:flex;align-items:center;gap:8px;
}
.prompt-close-btn{
  background:rgba(244,114,182,0.12);border:1px solid rgba(244,114,182,0.25);
  color:#f9a8d4;width:30px;height:30px;border-radius:50%;
  display:flex;align-items:center;justify-content:center;
  font-size:0.95rem;cursor:pointer;transition:all .2s;
}
.prompt-close-btn:hover{background:rgba(244,114,182,0.25);transform:rotate(90deg);color:#fff}
.prompt-body{
  padding:18px 20px;overflow-y:auto;flex:1;display:flex;flex-direction:column;gap:14px;
}
.prompt-bot-tabs{
  display:flex;gap:8px;background:rgba(15,5,28,0.5);padding:4px;border-radius:14px;
  border:1px solid rgba(244,114,182,0.15);
}
.prompt-bot-tab{
  flex:1;padding:8px 10px;border-radius:10px;text-align:center;font-size:0.82rem;
  font-weight:600;color:#c084a8;cursor:pointer;transition:all .2s;display:flex;
  align-items:center;justify-content:center;gap:6px;border:none;background:transparent;
}
.prompt-bot-tab.active{
  background:linear-gradient(135deg,rgba(236,72,153,0.35),rgba(168,85,247,0.35));
  color:#fff;box-shadow:0 2px 10px rgba(236,72,153,0.3);border:1px solid rgba(244,114,182,0.4);
}
.prompt-ctx-box{display:flex;flex-direction:column;gap:6px}
.prompt-ctx-label{font-size:0.75rem;color:#c084a8;display:flex;justify-content:space-between}
.prompt-ctx-input{
  background:rgba(15,5,28,0.6);border:1px solid rgba(244,114,182,0.2);
  border-radius:12px;padding:9px 14px;color:#fce7f3;font-size:0.85rem;
  font-family:inherit;outline:none;resize:none;min-height:50px;max-height:90px;
}
.prompt-ctx-input:focus{border-color:rgba(244,114,182,0.5)}
.prompt-gen-btn{
  background:linear-gradient(135deg,#ec4899 0%,#a855f7 50%,#6366f1 100%);
  border:none;color:#fff;font-size:0.9rem;font-weight:700;
  padding:11px 18px;border-radius:14px;cursor:pointer;
  box-shadow:0 4px 18px rgba(236,72,153,0.4);transition:all .25s;
  display:flex;align-items:center;justify-content:center;gap:8px;
}
.prompt-gen-btn:hover{transform:translateY(-2px);box-shadow:0 6px 24px rgba(236,72,153,0.6)}
.prompt-gen-btn:disabled{opacity:0.6;cursor:wait;transform:none}
.prompt-result-section{display:flex;flex-direction:column;gap:12px}
.prompt-card{
  background:rgba(26,10,46,0.5);border:1px solid rgba(244,114,182,0.18);
  border-radius:14px;padding:12px 14px;display:flex;flex-direction:column;gap:8px;
  position:relative;
}
.prompt-card-header{
  display:flex;align-items:center;justify-content:space-between;
}
.prompt-card-title{font-size:0.8rem;font-weight:700;color:#f9a8d4;display:flex;align-items:center;gap:6px}
.prompt-copy-btn{
  background:rgba(244,114,182,0.15);border:1px solid rgba(244,114,182,0.3);
  color:#fce7f3;font-size:0.72rem;padding:3px 10px;border-radius:8px;
  cursor:pointer;transition:all .2s;display:flex;align-items:center;gap:4px;
}
.prompt-copy-btn:hover{background:rgba(244,114,182,0.3);color:#fff;box-shadow:0 0 8px rgba(244,114,182,0.4)}
.prompt-text-display{
  font-size:0.8rem;line-height:1.55;color:#e9d5ff;word-break:break-word;
  background:rgba(10,4,20,0.45);padding:10px 12px;border-radius:10px;
  border:1px solid rgba(244,114,182,0.1);max-height:130px;overflow-y:auto;
  font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;
}
.prompt-summary-display{
  font-size:0.83rem;line-height:1.6;color:#fdf2f8;background:rgba(236,72,153,0.08);
  border:1px solid rgba(236,72,153,0.25);border-radius:10px;padding:10px 12px;
}
.prompt-params-bar{
  display:flex;flex-wrap:wrap;gap:6px;background:rgba(15,5,28,0.4);
  padding:8px 12px;border-radius:10px;border:1px solid rgba(244,114,182,0.12);
}
.param-tag{font-size:0.7rem;color:#c084fc;background:rgba(192,132,252,0.12);padding:2px 8px;border-radius:6px}
.prompt-footer{
  padding:12px 20px;background:rgba(26,10,46,0.65);
  border-top:1px solid rgba(244,114,182,0.18);
  display:flex;align-items:center;justify-content:space-between;gap:10px;
}
.prompt-goto-btn{
  background:linear-gradient(135deg,#3b82f6,#8b5cf6);color:#fff;
  border:none;font-size:0.8rem;font-weight:600;padding:8px 14px;
  border-radius:10px;cursor:pointer;text-decoration:none;display:inline-flex;
  align-items:center;gap:6px;transition:all .2s;
}
.prompt-goto-btn:hover{transform:translateY(-1px);box-shadow:0 4px 14px rgba(59,130,246,0.4)}
.chat-prompt-header-btn{
  background:linear-gradient(135deg,rgba(236,72,153,0.35),rgba(168,85,247,0.35));
  border:1px solid rgba(244,114,182,0.45);color:#fce7f3;font-size:0.75rem;
  font-weight:600;padding:5px 12px;border-radius:14px;cursor:pointer;
  display:inline-flex;align-items:center;gap:5px;transition:all .2s;
  box-shadow:0 2px 8px rgba(236,72,153,0.25);margin-left:auto;
}
.chat-prompt-header-btn:hover{
  background:linear-gradient(135deg,rgba(236,72,153,0.55),rgba(168,85,247,0.55));
  transform:translateY(-1px);box-shadow:0 4px 12px rgba(236,72,153,0.45);
}
.msg-prompt-btn{
  background:rgba(168,85,247,.12);border:1px solid rgba(168,85,247,.3);color:#d8b4fe;
  font-size:0.68rem;border-radius:12px;padding:3px 8px;cursor:pointer;
  display:inline-flex;align-items:center;gap:4px;transition:all .2s;
}
.msg-prompt-btn:hover{background:rgba(168,85,247,.25);color:#fff;border-color:rgba(168,85,247,.5)}
.main-btn-prompt{
  background:linear-gradient(135deg,#ec4899,#8b5cf6);color:#fff;
  border:none;box-shadow:0 4px 12px rgba(236,72,153,.35);
  font-size:0.8rem;padding:6px 12px;border-radius:10px;cursor:pointer;
  font-weight:600;display:inline-flex;align-items:center;gap:4px;transition:all .2s;
  margin-left:6px;
}
.main-btn-prompt:hover{transform:translateY(-1px);box-shadow:0 6px 16px rgba(236,72,153,.5)}
.main-btn-vb{
  background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;
  border:none;box-shadow:0 4px 12px rgba(99,102,241,.35);
  font-size:0.8rem;padding:6px 12px;border-radius:10px;cursor:pointer;
  font-weight:600;display:inline-flex;align-items:center;gap:4px;transition:all .2s;
  margin-left:6px;
}
.main-btn-vb:hover{transform:translateY(-1px);box-shadow:0 6px 16px rgba(99,102,241,.5)}
.btn-txt-full{display:inline}
.btn-txt-short{display:none}

</style>
</head>
<body>
<div class="container">
<div class="topbar">
<h1>小栖bot</h1>
<div class="refresh-info">
<span class="pulse" id="pulse-dot"></span>
<span id="last-update">加载中...</span>
<button class="refresh-btn" id="refresh-btn" onclick="manualRefresh()">刷新</button>
</div>
</div>
<div id="error-box"></div>
<div class="collapse-section" id="sec-sys"><div class="collapse-header" onclick="toggleSection('sec-sys')"><span class="collapse-title">系统信息</span><span class="collapse-summary" id="summary-sys"></span><span class="collapse-arrow"></span></div><div class="collapse-body"><div class="collapse-body-inner"><div class="sys-bar" id="sys-bar"></div></div></div></div><div class="collapse-section" id="sec-model"><div class="collapse-header" onclick="toggleSection('sec-model')"><span class="collapse-title">模型</span><span class="collapse-summary" id="summary-model"></span><span class="collapse-arrow"></span></div><div class="collapse-body"><div class="collapse-body-inner" id="model-section"></div></div></div>
<div class="collapse-section" id="sec-bots"><div class="collapse-header" onclick="toggleSection('sec-bots')"><span class="collapse-title">老婆</span><span class="collapse-summary" id="summary-bots"></span><span class="collapse-arrow"></span></div><div class="collapse-body"><div class="collapse-body-inner"><div class="bots-grid" id="bots-grid"></div></div></div></div>
<div class="collapse-section" id="sec-nav"><div class="collapse-header" onclick="toggleSection('sec-nav')"><span class="collapse-title">快捷导航</span><span class="collapse-summary" id="summary-nav"></span><span class="collapse-arrow"></span></div><div class="collapse-body"><div class="collapse-body-inner" id="extra-nav-section"></div></div></div>
<div class="footer">小栖bot Monitor · <a href="https://github.com/KroMiose/nekro-agent" target="_blank" rel="noopener" style="color:inherit">基于 NekroAgent 构建</a></div>
</div>
<div class="modal-overlay" id="log-modal" onclick="if(event.target===this)closeLogModal()">
<div class="modal-box">
<div class="modal-header">
<span class="modal-title" id="log-title">日志</span>
<button class="modal-close" onclick="closeLogModal()">关闭</button>
</div>
<div class="modal-body">
<pre class="log-content" id="log-content">加载中...</pre>
</div>
<div class="modal-footer">
<button class="sub-btn sub-btn-log" onclick="refreshLogs()">刷新日志</button>
<button class="sub-btn sub-btn-restart" onclick="scrollLogTop()">回到顶部</button>
</div>
</div>
</div>
<div class="modal-overlay" id="add-group-modal" onclick="if(event.target===this)closeAddGroupModal()">
<div class="modal-box" style="max-height:90vh;border-radius:16px">
<div class="modal-header">
<span class="modal-title">添加模型组</span>
<button class="modal-close" onclick="closeAddGroupModal()">关闭</button>
</div>
<div class="modal-body" style="padding:16px 18px">
<div class="addg-field"><div class="addg-label">组名称 <span style="color:#f0a6c9">（必填）</span></div><input class="addg-input" id="addg-name" placeholder="如：deepseek 主用"></div>
<div class="addg-field"><div class="addg-label">接口地址 BASE_URL <span style="color:#f0a6c9">（必填）</span></div><input class="addg-input" id="addg-url" placeholder="https://api.deepseek.com/v1"></div>
<div class="addg-field"><div class="addg-label">API_KEY</div><input class="addg-input" id="addg-key" placeholder="sk-..." type="password"></div>
<div class="addg-field" id="addg-model-field" style="display:none"><div class="addg-label">选择模型 <span style="color:#f0a6c9">（已从接口拉取到 <span id="addg-model-count">0</span> 个）</span></div><select class="model-select" id="addg-model"></select></div>
<div id="addg-status" style="margin-top:12px;font-size:0.75rem;color:#be185d;min-height:18px"></div>
<div class="addg-btns">
<button class="model-btn model-btn-add" id="addg-fetch-btn" onclick="addGroupFetchModels()">🔍 拉取模型列表</button>
<button class="model-btn model-btn-ok" id="addg-confirm-btn" onclick="confirmAddModel()" style="display:none">✓ 确认添加</button>
<button class="model-btn model-btn-plain" onclick="closeAddGroupModal()">取消</button>
</div>
</div>
</div>
</div>
<div class="toast" id="toast"></div>
<script>
if('serviceWorker' in navigator){window.addEventListener('load',function(){navigator.serviceWorker.register('/sw.js').catch(function(e){console.log('SW:',e)})})}
document.getElementById("last-update").textContent="加载中...";
var avatarTs=Date.now();
function avUrl(qq,size){return "https://q1.qlogo.cn/g?b=qq&nk="+(qq||"")+"&s="+(size||100)+"&t="+avatarTs}
var currentLogContainer="";
var currentLogType="docker";
function showToast(msg,type){var t=document.getElementById("toast");t.textContent=msg;t.className="toast show"+(type?" "+type:"");setTimeout(function(){t.className="toast"},2500)}
function showError(msg){document.getElementById("pulse-dot").className="pulse error";document.getElementById("last-update").textContent=msg;document.getElementById("error-box").innerHTML='<div class="error-msg">'+msg+'</div>'}
function getProgressClass(pct){var n=parseFloat(pct);if(isNaN(n))return"progress-low";if(n<50)return"progress-low";if(n<80)return"progress-mid";return"progress-high"}
function getMemClass(pct){var n=parseFloat(pct);if(isNaN(n))return"";if(n<50)return"green";if(n<80)return"yellow";return"red"}
function esc(s){if(!s)return"";return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;")}
function toggleSection(id){var el=document.getElementById(id);if(!el)return;el.classList.toggle("collapsed");try{localStorage.setItem("collapse_"+id,el.classList.contains("collapsed")?"1":"0")}catch(e){}}
function initSections(){var sections=["sec-sys","sec-model","sec-bots","sec-nav"];var isMobile=window.innerWidth<=480;sections.forEach(function(id){var el=document.getElementById(id);if(!el)return;var saved=null;try{saved=localStorage.getItem("collapse_"+id)}catch(e){}if(saved==="1"){el.classList.add("collapsed")}else if(saved===null&&isMobile&&id!=="sec-bots"){el.classList.add("collapsed")}})}
function setSummary(id,text){var el=document.getElementById(id);if(el)el.textContent=text}
function setSectionHealth(id,healthy){var el=document.getElementById(id);if(!el)return;el.classList.remove("healthy","warning");el.classList.add(healthy?"healthy":"warning")}
function renderSystem(sys){var memPct=parseFloat(sys.mem_pct);var memClass=isNaN(memPct)?"":(memPct<50?"":memPct<80?"warn":"danger");document.getElementById("sys-bar").innerHTML='<div class="sys-item"><div class="sys-label">服务器时间</div><div class="sys-value">'+esc(sys.time)+'</div></div><div class="sys-item"><div class="sys-label">CPU</div><div class="sys-value">'+esc(sys.cpu)+'</div></div><div class="sys-item"><div class="sys-label">内存</div><div class="sys-value '+memClass+'">'+esc(sys.mem)+' ('+esc(sys.mem_pct)+')</div></div><div class="sys-item"><div class="sys-label">磁盘</div><div class="sys-value">'+esc(sys.disk)+'</div></div>'}
function renderBot(bot,index){var statusClass=bot.online===true?"online":bot.online===false?"offline":"unknown";var statusBadge=bot.online===true?"status-online":bot.online===false?"status-offline":"status-unknown";var napcatMemPct=(bot.napcat_stats.mem_pct||"").replace("%","").trim();var nekroMemPct=(bot.nekro_stats.mem_pct||"").replace("%","").trim();var connClass=bot.nekro_conn==="已连接"?"green":bot.nekro_conn==="已断开"?"red":"yellow";var napcatC=esc(bot.napcat_container);var nekroC=esc(bot.nekro_container);var html="";var avatarUrl=avUrl(esc(bot.qq),640);html+='<div class="bot-card '+statusClass+'">';html+='<div class="card-header"><div class="card-header-left"><img class="bot-avatar" src="'+avatarUrl+'" onerror="this.style.display=\'none\'"><div><span class="bot-name">'+esc(bot.preset_name||bot.role)+'</span><span class="status-badge '+statusBadge+'">'+esc(bot.online_msg)+'</span></div></div><div class="card-header-right"><a href="/chat?bot='+(index+1)+'" target="_blank" class="main-btn main-btn-chat">💬 聊天</a><button onclick="openPromptModal('+(index+1)+')" class="main-btn main-btn-prompt"><span class="btn-txt-full">🎨 生图Prompt</span><span class="btn-txt-short">🎨 生图</span></button></div></div>';html+='<div class="bot-sub open" id="bs-mon-'+index+'"><div class="bot-sub-h" onclick="toggleBotSub(\'bs-mon-'+index+'\')"><span>监控</span><span class="bot-sub-arrow">▾</span></div><div class="bot-sub-b">';html+='<div class="info-section"><div class="section-title">NapCat</div><div class="info-row"><span class="label">运行</span><span class="value '+(bot.napcat_running?"green":"red")+'">'+(bot.napcat_running?"运行中":"已停止")+'</span></div><div class="info-row"><span class="label">CPU</span><span class="value">'+esc(bot.napcat_stats.cpu)+'</span></div><div class="info-row"><span class="label">内存</span><span class="value '+getMemClass(napcatMemPct)+'">'+esc(bot.napcat_stats.mem)+'</span></div><div class="progress-bar"><div class="progress-fill '+getProgressClass(napcatMemPct)+'" style="width:'+(napcatMemPct||0)+'%"></div></div></div>';html+='<div class="info-section"><div class="section-title">NekroAgent</div><div class="info-row"><span class="label">运行</span><span class="value '+(bot.nekro_running?"green":"red")+'">'+(bot.nekro_running?"运行中":"已停止")+'</span></div><div class="info-row"><span class="label">OneBot</span><span class="value '+connClass+'">'+esc(bot.nekro_conn)+'</span></div><div class="info-row"><span class="label">CPU</span><span class="value">'+esc(bot.nekro_stats.cpu)+'</span></div><div class="info-row"><span class="label">内存</span><span class="value '+getMemClass(nekroMemPct)+'">'+esc(bot.nekro_stats.mem)+'</span></div><div class="progress-bar"><div class="progress-fill '+getProgressClass(nekroMemPct)+'" style="width:'+(nekroMemPct||0)+'%"></div></div></div>';var kickOn=bot.kick_restart!==false;html+='<div class="guard-toggle"><span class="guard-label">踢下线自动重启NapCat</span><div class="guard-switch'+(kickOn?" on":"")+'" onclick="toggleKickRestart('+index+',this)"></div></div>';var voiceOn=voiceSwitchStates[index]!==false;html+='<div class="guard-toggle"><span class="guard-label">QQ 语音回复</span><div class="guard-switch'+(voiceOn?" on":"")+'" id="voice-switch-'+index+'" onclick="toggleVoiceSwitch('+index+',this)"></div></div>';html+='<div style="margin-top:6px"><a href="'+esc(bot.napcat_url)+'" target="_blank" class="main-btn main-btn-napcat">NapCat</a><div class="sub-btns"><button class="sub-btn sub-btn-log" onclick="viewLogs(\''+napcatC+'\')">日志</button><button class="sub-btn sub-btn-restart" onclick="ctrlContainer(\''+napcatC+'\',\'restart\')">重启</button>';if(bot.napcat_running)html+='<button class="sub-btn sub-btn-stop" onclick="ctrlContainer(\''+napcatC+'\',\'stop\')">停止</button>';else html+='<button class="sub-btn sub-btn-start" onclick="ctrlContainer(\''+napcatC+'\',\'start\')">启动</button>';html+='</div></div>';html+='<div style="margin-top:6px"><a href="javascript:void(0)" onclick="openNekro('+index+')" class="main-btn main-btn-nekro">NekroAgent</a><div class="sub-btns"><button class="sub-btn sub-btn-log" onclick="viewLogs(\''+nekroC+'\')">日志</button><button class="sub-btn sub-btn-restart" onclick="ctrlContainer(\''+nekroC+'\',\'restart\')">重启</button>';if(bot.nekro_running)html+='<button class="sub-btn sub-btn-stop" onclick="ctrlContainer(\''+nekroC+'\',\'stop\')">停止</button>';else html+='<button class="sub-btn sub-btn-start" onclick="ctrlContainer(\''+nekroC+'\',\'start\')">启动</button>';html+='</div></div>';html+='</div></div>';html+='<div class="bot-sub" id="bs-br-'+index+'"><div class="bot-sub-h" onclick="toggleBotSub(\'bs-br-'+index+'\')"><span>桥接</span><span class="bot-sub-arrow">▸</span></div><div class="bot-sub-b" id="bs-br-b-'+index+'"></div></div>';html+='<div class="bot-sub" id="bs-tts-'+index+'"><div class="bot-sub-h" onclick="toggleBotSub(\'bs-tts-'+index+'\')"><span>音色</span><span class="bot-sub-arrow">▸</span></div><div class="bot-sub-b" id="bs-tts-b-'+index+'"></div></div>';html+='<div class="bot-sub" id="bs-note-'+index+'"><div class="bot-sub-h" onclick="toggleBotSub(\'bs-note-'+index+'\')"><span>笔记</span><span class="bot-sub-arrow">▸</span></div><div class="bot-sub-b" id="bs-note-b-'+index+'"></div></div>';if(index===DEVICE_BOT_INDEX){html+='<div class="bot-sub" id="bs-dev-'+index+'"><div class="bot-sub-h" onclick="toggleBotSub(\'bs-dev-'+index+'\')"><span>设备</span><span class="bot-sub-arrow">▸</span></div><div class="bot-sub-b" id="bs-dev-b-'+index+'"></div></div>'}html+='</div>';return html}

function loadModelGroups(){fetch("/api/model-presets").then(function(r){return r.json()}).then(function(d){modelGroups=d.groups||[];currentModel=d.current||"unknown";selectedModel=currentModel;_renderModelCard()}).catch(function(){})}function _renderModelCard(){var el=document.getElementById("model-section");if(!el)return;if(!modelGroups||!modelGroups.length){setSummary("summary-model","未配置");el.innerHTML='<div class="model-card"><div class="model-tip">暂无可用模型组，请先添加</div><div class="model-btns"><button class="model-btn model-btn-add" onclick="showAddModelGroup()">＋ 添加模型组</button></div></div>';return}var cur=null;(modelGroups||[]).forEach(function(g){if(g.group_name===currentModel)cur=g});setSummary("summary-model",cur?cur.pretty:currentModel);var opts=(modelGroups||[]).map(function(g){var tip=[g.chat_model,g.base_url?g.base_url.replace(/^https?:\/\//,"").replace(/\/+$/,""):null].filter(Boolean).join(" · ");var sel=(g.group_name===(selectedModel||currentModel))?" selected":"";return '<option value="'+esc(g.group_name)+'"'+sel+' title="'+esc(tip)+'">'+esc(g.pretty||g.group_name)+'</option>'}).join("");var pending=selectedModel&&selectedModel!==currentModel;var applyBtn=pending?'<button class="model-btn model-btn-switch" id="model-apply-btn" onclick="applySelectedModel()">⚡ 应用</button>':'';el.innerHTML='<div class="model-card"><select class="model-select" id="model-select" onchange="selectModel(this.value)">'+opts+'</select><div class="model-btns">'+applyBtn+'<button class="model-btn model-btn-test" id="model-test-btn" onclick="testModelConnection()">🔌 测试</button><button class="model-btn model-btn-add" onclick="showAddModelGroup()">＋ 添加</button><button class="model-btn model-btn-del" onclick="deleteCurrentModelGroup()">－ 删除</button></div></div>'}
function render(data){if(data.error&&(!data.bots||data.bots.length===0)){showError(data.error);return}document.getElementById("pulse-dot").className="pulse";document.getElementById("last-update").textContent="更新于 "+(data.system?data.system.time:"?");if(data.error){document.getElementById("error-box").innerHTML='<div class="warn-msg">部分异常: '+esc(data.error)+'</div>'}else{document.getElementById("error-box").innerHTML=''}if(data.system){renderSystem(data.system);setSummary("summary-sys","CPU "+esc(data.system.cpu)+" | 内存 "+esc(data.system.mem));var sysMemPct=parseFloat(data.system.mem_pct);setSectionHealth("sec-sys",isNaN(sysMemPct)||sysMemPct<80)}if(data.bots&&data.bots.length>0){document.getElementById("bots-grid").innerHTML=data.bots.map(renderBot).join("");var onlineCount=data.bots.filter(function(b){return b.online===true}).length;setSummary("summary-bots",onlineCount+"/"+data.bots.length+" 在线");setSectionHealth("sec-bots",onlineCount===data.bots.length);data.bots.forEach(function(b,i){fillBotExtras(b,i,data.note_sync)})}else{document.getElementById("bots-grid").innerHTML='<div style="color:#c084a8;text-align:center;padding:40px">暂无数据</div>';setSummary("summary-bots","0/0 在线");setSectionHealth("sec-bots",false)}renderDevice(data.device);renderNavLinks(data.extra_nav);loadVoiceSwitch();setSectionHealth("sec-nav",true)}
function loadData(){try{avatarTs=Date.now();fetch("/api/status").then(function(res){if(!res.ok)throw new Error("HTTP "+res.status);return res.json()}).then(function(data){render(data)}).catch(function(err){showError("请求失败: "+err.message)})}catch(e){showError("JS异常: "+e.message)}}
function manualRefresh(){var btn=document.getElementById("refresh-btn");btn.disabled=true;btn.textContent="刷新中...";fetch("/api/refresh").then(function(){setTimeout(function(){loadData();btn.disabled=false;btn.textContent="刷新"},1000)}).catch(function(){btn.disabled=false;btn.textContent="刷新";loadData()})}
function ctrlContainer(name,action){var am={"start":"启动","stop":"停止","restart":"重启"};if(!confirm("确认"+(am[action]||action)+" 容器 "+name+" ?"))return;showToast("正在"+(am[action]||action)+" "+name+"...","");fetch("/api/container",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({container:name,action:action})}).then(function(res){return res.json()}).then(function(data){if(data.ok){showToast(data.msg,"success");setTimeout(loadData,3000)}else{showToast(data.msg||"操作失败","error")}}).catch(function(err){showToast("请求失败: "+err.message,"error")})}
function openNekro(index){showToast("正在打开 NekroAgent...","");window.open("/nekro/"+index+"/webui/","_blank")}
function toggleKickRestart(index,el){var enabled=!el.classList.contains("on");fetch("/api/kick-restart",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({bot_index:index,enabled:enabled})}).then(function(res){return res.json()}).then(function(data){if(data.ok){if(enabled)el.classList.add("on");else el.classList.remove("on");showToast(data.msg,"success")}else{showToast(data.msg||"操作失败","error")}}).catch(function(err){showToast("请求失败: "+err.message,"error")})}

var voiceSwitchStates={};
function loadVoiceSwitch(){fetch("/api/qq-voice").then(function(r){return r.json()}).then(function(d){var st=d.states||[];st.forEach(function(x){voiceSwitchStates[x.bot_index]=x.enabled});st.forEach(function(x){var el=document.getElementById("voice-switch-"+x.bot_index);if(el){if(x.enabled!==false)el.classList.add("on");else el.classList.remove("on")}})}).catch(function(){})}
function toggleVoiceSwitch(index,el){var enabled=!el.classList.contains("on");fetch("/api/qq-voice",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({bot_index:index,enabled:enabled})}).then(function(r){return r.json()}).then(function(d){if(d.ok){if(enabled)el.classList.add("on");else el.classList.remove("on");if(d.states)d.states.forEach(function(x){voiceSwitchStates[x.bot_index]=x.enabled});showToast(d.msg,"success")}else{showToast(d.msg||"操作失败","error")}}).catch(function(e){showToast("请求失败:"+e.message,"error")})}
function viewLogs(name){currentLogContainer=name;currentLogType="docker";document.getElementById("log-title").textContent=name+" 日志";document.getElementById("log-content").textContent="加载中...";document.getElementById("log-modal").classList.add("active");fetchLogs(name)}
function viewSvcLogs(name,type){currentLogContainer=name;currentLogType=type;document.getElementById("log-title").textContent=name+" 日志";document.getElementById("log-content").textContent="加载中...";document.getElementById("log-modal").classList.add("active");fetchLogs(name)}
function fetchLogs(name){var ep=currentLogType==="systemd"?"/api/svc-logs/":"/api/logs/";fetch(ep+encodeURIComponent(name)+"?lines=150").then(function(res){return res.json()}).then(function(data){if(data.ok){var content=esc(data.logs||"(空)");content=content.replace(/ERROR/g,'<span class="log-err">ERROR</span>');content=content.replace(/WARN/g,'<span class="log-warn">WARN</span>');content=content.replace(/KickedOffLine/g,'<span class="log-err">KickedOffLine</span>');content=content.replace(/connected/g,'<span class="log-ok">connected</span>');document.getElementById("log-content").innerHTML=content;var body=document.querySelector(".modal-body");if(body)body.scrollTop=body.scrollHeight}else{document.getElementById("log-content").textContent=data.msg||"获取失败"}}).catch(function(err){document.getElementById("log-content").textContent="请求失败: "+err.message})}
function refreshLogs(){if(currentLogContainer){document.getElementById("log-content").textContent="刷新中...";fetchLogs(currentLogContainer)}}
function scrollLogTop(){var body=document.querySelector(".modal-body");if(body)body.scrollTop=0}
function closeLogModal(){document.getElementById("log-modal").classList.remove("active");currentLogContainer=""}
var touchStartY=0,pullDistance=0,isPulling=false;
document.addEventListener('touchstart',function(e){if(window.scrollY<=0){touchStartY=e.touches[0].clientY;isPulling=true}},{passive:true});
document.addEventListener('touchmove',function(e){if(isPulling){pullDistance=e.touches[0].clientY-touchStartY;if(pullDistance>80){isPulling=false;manualRefresh()}}},{passive:true});
document.addEventListener('touchend',function(){isPulling=false},{passive:true});
function renderExtraServices(services,llmMode){if(!services||services.length===0){document.getElementById("extra-services-grid").innerHTML="";setSummary("summary-svc","");var el=document.getElementById("sec-svc");if(el){el.classList.remove("healthy","warning")}return}var runCount=services.filter(function(s){return s.running}).length;setSummary("summary-svc",runCount+"/"+services.length+" 运行中");var allHealthy=true;services.forEach(function(s){if(llmMode==="direct"&&(s.service||"").indexOf("nekro-bridge")===0){return}if(!s.running){allHealthy=false}});setSectionHealth("sec-svc",allHealthy);var html='<div class="svc-grid">';services.forEach(function(svc){var running=svc.running;var bridgeDirect=svc.name==="桥接服务"&&llmMode==="direct";var cls=running?"running":(bridgeDirect?"running":"stopped");var statusText=running?"运行中":(bridgeDirect?"已停止(直连)":"已停止");html+='<div class="svc-card '+cls+'"><div class="svc-header"><span class="svc-name">'+esc(svc.name)+'</span><span class="status-badge '+(running||bridgeDirect?"status-online":"status-offline")+'">'+statusText+'</span></div><div class="svc-body">';if(svc.type==="docker"){var memPct=(svc.stats.mem_pct||"").replace("%","").trim();html+='<div class="svc-row"><span class="label">容器</span><span class="value">'+esc(svc.container)+'</span></div>';html+='<div class="svc-row"><span class="label">CPU</span><span class="value">'+esc(svc.stats.cpu)+'</span></div>';html+='<div class="svc-row"><span class="label">内存</span><span class="value '+getMemClass(memPct)+'">'+esc(svc.stats.mem)+'</span></div>';html+='<div class="progress-bar"><div class="progress-fill '+getProgressClass(memPct)+'" style="width:'+(memPct||0)+'%"></div></div>'}else{html+='<div class="svc-row"><span class="label">服务</span><span class="value">'+esc(svc.service)+'</span></div>';html+='<div class="svc-row"><span class="label">类型</span><span class="value">systemd</span></div>'}html+='<div class="sub-btns" style="margin-top:8px">';var logName=svc.type==="docker"?svc.container:svc.service;html+='<button class="sub-btn sub-btn-log" onclick="viewSvcLogs(\''+logName+'\',\''+svc.type+'\')">日志</button>';if(svc.type==="docker"){html+='<button class="sub-btn sub-btn-restart" onclick="ctrlContainer(\''+svc.container+'\',\'restart\')">重启</button>';if(running){html+='<button class="sub-btn sub-btn-stop" onclick="ctrlContainer(\''+svc.container+'\',\'stop\')">停止</button>'}else{html+='<button class="sub-btn sub-btn-start" onclick="ctrlContainer(\''+svc.container+'\',\'start\')">启动</button>'}}else{html+='<button class="sub-btn sub-btn-restart" onclick="ctrlService(\''+svc.service+'\',\'restart\')">重启</button>';if(running){html+='<button class="sub-btn sub-btn-stop" onclick="ctrlService(\''+svc.service+'\',\'stop\')">停止</button>'}else{html+='<button class="sub-btn sub-btn-start" onclick="ctrlService(\''+svc.service+'\',\'start\')">启动</button>'}}html+='</div>';if(svc.url){html+='<a href="'+esc(svc.url)+'" target="_blank" class="main-btn main-btn-napcat" style="margin-top:6px;padding:10px;font-size:0.85rem">进入</a>'}if(svc.container==="xiaozhi-esp32-server"&&llmMode){var isNekro=llmMode==="nekro";var modeText=isNekro?"NekroAgent":llmMode==="direct"?"直连LLM":"未知";html+='<div style="margin-top:10px;padding-top:10px;border-top:1px solid rgba(180,140,200,0.2)"><div class="guard-toggle"><span class="guard-label">LLM模式：'+modeText+'</span><div class="guard-switch'+(isNekro?" on":"")+'" id="llm-mode-switch" onclick="toggleLLMMode()" title="点击切换LLM模式"></div></div></div>'}html+='</div></div>'});html+='</div>';document.getElementById("extra-services-grid").innerHTML=html}
function renderNoteCard(index,ns){var el=document.getElementById("bs-note-b-"+index);if(!el)return;var bid=index+1;var nsb=(ns&&ns[bid])?ns[bid]:null;var html;if(!nsb){html='<div class="svc-row"><span class="label">状态</span><span class="value">无数据</span></div>'}else{var synced=nsb.running;var statusText=synced?"已同步":"未同步";html='<div class="svc-row"><span class="label">状态</span><span class="value '+(synced?"green":"red")+'">'+statusText+'</span></div>';html+='<div class="svc-row"><span class="label">QQ笔记</span><span class="value">'+esc(String(nsb.qq_notes||0))+'</span></div>';html+='<div class="svc-row"><span class="label">设备笔记</span><span class="value">'+esc(String(nsb.sse_notes||0))+'</span></div>';html+='<div class="svc-row"><span class="label">内容一致</span><span class="value '+(nsb.content_synced?"green":"red")+'">'+(nsb.content_synced?"是":"否")+'</span></div>';var qqOn=nsb.triggers&&nsb.triggers.qq_to_sse;var sseOn=nsb.triggers&&nsb.triggers.sse_to_qq;html+='<div class="guard-toggle"><span class="guard-label">QQ → 设备</span><div class="guard-switch'+(qqOn?" on":"")+'" onclick="toggleNoteSync(\'qq_to_sse\',this,'+bid+')"></div></div>';html+='<div class="guard-toggle"><span class="guard-label">设备 → QQ</span><div class="guard-switch'+(sseOn?" on":"")+'" onclick="toggleNoteSync(\'sse_to_qq\',this,'+bid+')"></div></div>'}el.innerHTML=html}
function toggleNoteSync(direction,el,bot){bot=bot||1;var enabled=!el.classList.contains("on");fetch("/api/note-sync-toggle",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({direction:direction,enabled:enabled,bot:bot})}).then(function(res){return res.json()}).then(function(data){if(data.ok){if(enabled)el.classList.add("on");else el.classList.remove("on");showToast(data.msg,"success");setTimeout(loadData,2000)}else{showToast(data.msg||"操作失败","error")}}).catch(function(err){showToast("请求失败: "+err.message,"error")})}
function toggleLLMMode(){if(!confirm("确认切换小智LLM模式？\n\n切换后小智服务将重启，约需30秒。\n· NekroAgent→直连：停止桥接服务\n· 直连→NekroAgent：启动桥接服务"))return;var sw=document.getElementById('llm-mode-switch');if(sw){if(sw.classList.contains('on'))sw.classList.remove('on');else sw.classList.add('on')}showToast("正在切换模式...","");fetch("/api/toggle-llm-mode",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({})}).then(function(res){return res.json()}).then(function(data){if(data.ok){showToast(data.msg,"success");setTimeout(loadData,3000)}else{showToast(data.msg||"操作失败","error");loadData()}}).catch(function(err){showToast("请求失败: "+err.message,"error");loadData()})}
var modelGroups=[];var currentModel="";var selectedModel="";
function selectModel(name){if(!name)return;selectedModel=name;_renderModelCard()}
function applySelectedModel(){var name=selectedModel||(document.getElementById("model-select")?document.getElementById("model-select").value:"");if(!name)return;switchModel(name)}
function testModelConnection(){
  var gname=selectedModel||currentModel;
  if(!gname){showToast("没有可测试的模型组","error");return}
  var btn=document.getElementById("model-test-btn");
  var old=btn?btn.textContent:"🔌 测试";
  if(btn){btn.disabled=true;btn.textContent="⏳ 测试中..."}
  fetch("/api/test-model",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({group_name:gname})})
    .then(function(r){return r.json()})
    .then(function(d){
      if(btn){btn.disabled=false;btn.textContent=old}
      if(d&&d.ok){showToast("✅ "+d.msg,"success")}
      else{showToast("❌ "+(d&&d.msg||"测试失败"),"error")}
    })
    .catch(function(e){
      if(btn){btn.disabled=false;btn.textContent=old}
      showToast("❌ 请求失败: "+e.message,"error")
    })
}
function switchModel(name){if(!name)return;var p=null;(modelGroups||[]).forEach(function(g){if(g.group_name===name)p=g});if(!p){showToast("未找到模型组: "+name,"error");return}currentModel=p.group_name;selectedModel=name;_renderModelCard();showToast("正在切换至 "+p.pretty+" ...","");fetch("/api/apply-model",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({group_name:p.group_name,chat_model:p.chat_model,base_url:p.base_url,api_key:p.api_key})}).then(function(r){return r.json()}).then(function(d){showToast(d.msg,d.ok?"success":"error");if(d.ok)loadModelGroups()}).catch(function(e){showToast("请求失败:"+e.message,"error");loadModelGroups()})}
var _addCtx={};
function openAddGroupModal(){document.getElementById("add-group-modal").classList.add("active");document.getElementById("addg-name").value="";document.getElementById("addg-url").value="";document.getElementById("addg-key").value="";document.getElementById("addg-model-field").style.display="none";document.getElementById("addg-model").innerHTML="";document.getElementById("addg-confirm-btn").style.display="none";document.getElementById("addg-fetch-btn").style.display="";document.getElementById("addg-status").textContent="";setTimeout(function(){document.getElementById("addg-name").focus()},100)}
function closeAddGroupModal(){document.getElementById("add-group-modal").classList.remove("active")}
function addGroupFetchModels(){var n=document.getElementById("addg-name").value.trim();var b=document.getElementById("addg-url").value.trim();var k=document.getElementById("addg-key").value.trim();if(!n){document.getElementById("addg-status").textContent="请先填写组名称";return}if(!b){document.getElementById("addg-status").textContent="请先填写接口地址 BASE_URL";return}_addCtx.name=n;_addCtx.base_url=b;_addCtx.api_key=k;var st=document.getElementById("addg-status");st.textContent="正在拉取模型列表...";fetch("/api/fetch-models",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({base_url:_addCtx.base_url,api_key:_addCtx.api_key})}).then(function(r){return r.json()}).then(function(d){if(!d.ok){st.textContent="拉取失败: "+(d.msg||"");return}var models=d.models||[];if(models.length===0){st.textContent="接口返回模型列表为空";return}var sel=document.getElementById("addg-model");sel.innerHTML=models.map(function(m){return '<option value="'+esc(m)+'">'+esc(m)+'</option>'}).join("");document.getElementById("addg-model-count").textContent=models.length;document.getElementById("addg-model-field").style.display="";document.getElementById("addg-confirm-btn").style.display="";document.getElementById("addg-fetch-btn").style.display="none";st.textContent="拉取到 "+models.length+" 个模型，选择要使用的模型后添加"})}
function showAddModelGroup(){openAddGroupModal()}
function confirmAddModel(){var picker=document.getElementById("addg-model");if(!picker)return;var m=picker.value;if(!m)return;showToast("正在创建 "+_addCtx.name+"...","");fetch("/api/create-model-group",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({group_name:_addCtx.name,chat_model:m,base_url:_addCtx.base_url,api_key:_addCtx.api_key})}).then(function(r){return r.json()}).then(function(d){showToast(d.msg,d.ok?"success":"error");if(d.ok)closeAddGroupModal();loadModelGroups()}).catch(function(e){showToast("请求失败:"+e.message,"error")})}
function cancelAddModel(){closeAddGroupModal()}
function deleteCurrentModelGroup(){var name=currentModel;if(!name||name==="unknown"){showToast("当前没有可删除的模型组","error");return}if(!confirm("确认删除模型组「"+name+"」？"))return;showToast("正在删除 "+name+" ...","");fetch("/api/delete-model-group",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({group_name:name})}).then(function(r){return r.json()}).then(function(d){showToast(d.msg,d.ok?"success":"error");if(d.ok){currentModel="unknown";loadModelGroups()}}).catch(function(e){showToast("请求失败:"+e.message,"error")})}

var BOTS_DATA = __BOTS_DATA__;
var DEVICE_BOT_INDEX = __DEVICE_BOT_INDEX__;
var ttsCfgBots=BOTS_DATA.map(function(b,i){return {id:i+1,role:b.role}});
function loadTtsConfig(){ttsCfgBots.forEach(function(b,i){fetch("/api/chat/tts-config?bot="+b.id).then(function(r){return r.json()}).then(function(d){if(d.ok){b.voice=d.voice;b.speed=d.speed;b.pitch=d.pitch;b.volume=d.volume}else{b.voice="?";b.speed=1;b.pitch=1;b.volume=1}renderTtsCard(i)}).catch(function(){b.voice="?";b.speed=1;b.pitch=1;b.volume=1;renderTtsCard(i)})})}
function renderTtsCard(index){var b=ttsCfgBots[index];var el=document.getElementById("bs-tts-b-"+index);if(!b||!el)return;var html='<div class="svc-row"><span class="label">音色</span><span class="value" style="font-size:.7rem">'+esc(b.voice)+'</span></div>';html+='<div class="svc-row"><span class="label">语速</span><input type="number" step="0.05" min="0.2" max="3" style="width:72px;background:rgba(26,10,20,0.6);color:#fce7f3;border:1px solid rgba(244,114,182,0.3);border-radius:6px;padding:3px 6px" value="'+b.speed+'" id="tts-speed-'+b.id+'"></div>';html+='<div class="svc-row"><span class="label">音高</span><input type="number" step="0.05" min="0.1" max="3" style="width:72px;background:rgba(26,10,20,0.6);color:#fce7f3;border:1px solid rgba(244,114,182,0.3);border-radius:6px;padding:3px 6px" value="'+b.pitch+'" id="tts-pitch-'+b.id+'"></div>';html+='<div class="svc-row"><span class="label">音量</span><input type="number" step="0.05" min="0.1" max="3" style="width:72px;background:rgba(26,10,20,0.6);color:#fce7f3;border:1px solid rgba(244,114,182,0.3);border-radius:6px;padding:3px 6px" value="'+b.volume+'" id="tts-volume-'+b.id+'"></div>';html+='<div class="sub-btns" style="margin-top:8px"><button class="sub-btn sub-btn-restart" onclick="saveTtsConfig('+b.id+')">保存</button><button class="sub-btn sub-btn-log" onclick="testTts('+b.id+')">试听</button></div>';el.innerHTML=html}
function saveTtsConfig(id){var speed=parseFloat(document.getElementById("tts-speed-"+id).value)||1;var pitch=parseFloat(document.getElementById("tts-pitch-"+id).value)||1;var volume=parseFloat(document.getElementById("tts-volume-"+id).value)||1;fetch("/api/chat/tts-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({bot:id,speed:speed,pitch:pitch,volume:volume})}).then(function(r){return r.json()}).then(function(d){if(d.ok){showToast((ttsCfgBots[id-1]?ttsCfgBots[id-1].role:"bot"+id)+" TTS已保存","success");loadTtsConfig()}else{showToast("保存失败: "+(d.msg||""),"error")}}).catch(function(err){showToast("请求失败: "+err.message,"error")})}
function testTts(id){fetch("/api/chat/tts",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({text:"你好呀，我是语音测试，听听我的声音怎么样",bot:id})}).then(function(r){return r.json()}).then(function(d){if(d.ok&&d.audio){var au=new Audio("data:audio/mp3;base64,"+d.audio);au.play()}else{showToast("试听失败: "+(d.msg||""),"error")}}).catch(function(err){showToast("试听失败: "+err.message,"error")})}
function toggleBotSub(id){var el=document.getElementById(id);if(!el)return;el.classList.toggle("open")}
function ctrlBridge(svc){if(!svc)return;if(!confirm("确认重启 "+svc+" 桥接服务？"))return;showToast("正在重启 "+svc+"...","");fetch("/api/service-control",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({service:svc,action:"restart"})}).then(function(r){return r.json()}).then(function(d){if(d.ok){showToast(d.msg,"success");setTimeout(loadData,3000)}else{showToast(d.msg||"操作失败","error")}}).catch(function(err){showToast("请求失败: "+err.message,"error")})}
function fillBotExtras(bot,index,noteSync){var brEl=document.getElementById("bs-br-b-"+index);if(brEl){var ba=bot.bridge_active;brEl.innerHTML='<div class="info-section"><div class="info-row"><span class="label">桥接服务</span><span class="value '+(ba?"green":"red")+'">'+(ba?"运行中":"已停止")+'</span></div><div class="info-row"><span class="label">服务名</span><span class="value">'+esc(bot.bridge_service||"")+'</span></div><div class="sub-btns" style="margin-top:8px"><button class="sub-btn sub-btn-restart" onclick="ctrlBridge(\''+esc(bot.bridge_service||"")+'\')">重启桥接</button></div></div>'}renderTtsCard(index);renderNoteCard(index,noteSync)}
function renderDevice(device){var el=document.getElementById("bs-dev-b-"+DEVICE_BOT_INDEX);if(!el)return;if(!device){el.innerHTML='<div class="svc-row"><span class="label">状态</span><span class="value">无数据</span></div>';return}var running=device.running;var llmMode=device.llm_mode||"unknown";var modeText=llmMode==="nekro"?"NekroAgent":llmMode==="direct"?"直连LLM":"未知";var html='<div class="info-section"><div class="section-title">小智设备 (stackchan)</div><div class="info-row"><span class="label">状态</span><span class="value '+(running?"green":"red")+'">'+(running?"运行中":"已停止")+'</span></div><div class="guard-toggle"><span class="guard-label">LLM模式：'+modeText+'</span><div class="guard-switch'+(llmMode==="nekro"?" on":"")+'" id="llm-mode-switch" onclick="toggleLLMMode()" title="点击切换LLM模式"></div></div></div>';document.getElementById("bs-dev-b-"+DEVICE_BOT_INDEX).innerHTML=html}
function renderNavLinks(navLinks){if(!navLinks||navLinks.length===0){document.getElementById("extra-nav-section").innerHTML="";setSummary("summary-nav","");return}setSummary("summary-nav",navLinks.length+"个链接");var html='<div class="nav-grid">';navLinks.forEach(function(nav){var colorClass=nav.name.indexOf("酒馆")>=0?"nav-btn-tavern":nav.name.indexOf("面板")>=0?"nav-btn-panel":"main-btn-napcat";html+='<a href="'+esc(nav.url)+'" target="_blank" class="nav-btn '+colorClass+'">'+esc(nav.name)+'</a>'});html+='</div>';document.getElementById("extra-nav-section").innerHTML=html}
function ctrlService(service,action){var am={"start":"启动","stop":"停止","restart":"重启"};if(!confirm("确认"+(am[action]||action)+" 服务 "+service+" ?"))return;showToast("正在"+(am[action]||action)+" "+service+"...","");fetch("/api/service-control",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({service:service,action:action})}).then(function(res){return res.json()}).then(function(data){if(data.ok){showToast(data.msg,"success");setTimeout(loadData,3000)}else{showToast(data.msg||"操作失败","error")}}).catch(function(err){showToast("请求失败: "+err.message,"error")})}
initSections();
loadModelGroups();
loadData();
setInterval(loadData,10000);
loadTtsConfig();setInterval(loadTtsConfig,30000);

// 旧聊天轮询逻辑已删除（2026-08-11）：新逻辑带 bot 参数在页面加载时启动
function pad(n){return n<10?"0"+n:n}

</script>

<div class="prompt-modal-overlay" id="prompt-modal" onclick="if(event.target===this)closePromptModal()">
  <div class="prompt-modal">
    <div class="prompt-header">
      <div class="prompt-header-title">
        <span>🎨</span>
        <span id="pm-modal-title">AI 场景生图 Prompt 提炼</span>
      </div>
      <button class="prompt-close-btn" onclick="closePromptModal()">✕</button>
    </div>
    <div class="prompt-body">
      <div class="prompt-bot-tabs" id="pm-bot-tabs">
        <button class="prompt-bot-tab active" onclick="switchPromptBot(1)">🌸 雾岛澪</button>
        <button class="prompt-bot-tab" onclick="switchPromptBot(2)">🌙 霁月</button>
        <button class="prompt-bot-tab" onclick="switchPromptBot(3)">✨ 爱弥斯</button>
      </div>
      <div class="prompt-ctx-box">
        <div class="prompt-ctx-label">
          <span>场景/聊天上下文（留空则自动提取最新聊天对话）</span>
          <span style="cursor:pointer;color:#f472b6" onclick="document.getElementById('pm-ctx-input').value='';">清空</span>
        </div>
        <textarea class="prompt-ctx-input" id="pm-ctx-input" placeholder="输入自定义场景描述或关键词...留空则自动读取最新聊天上下文"></textarea>
      </div>
      <button class="prompt-gen-btn" id="pm-gen-btn" onclick="executeGeneratePrompt()">
        <span>✨</span><span id="pm-gen-btn-text">提炼场景生图 Prompt</span>
      </button>
      <div class="prompt-card" id="pm-context-card" style="display:none">
        <div class="prompt-card-header">
          <span class="prompt-card-title">💬 提取到的真实聊天记录</span>
          <button class="prompt-copy-btn" onclick="refreshChatContext()">🔄 刷新</button>
        </div>
        <div class="prompt-summary-display" id="pm-context-text" style="font-size:0.75rem;color:#be185d;max-height:90px;overflow-y:auto;white-space:pre-wrap;background:rgba(255,255,255,0.6);border-radius:6px;padding:6px 10px;line-height:1.45">...</div>
      </div>
      <div class="prompt-card" id="pm-vb-card">
        <div class="prompt-card-header" style="cursor:pointer" onclick="toggleVbBody()">
          <span class="prompt-card-title">📐 外貌基准 <span id="pm-vb-modified" style="font-size:0.62rem;color:#f0a6c9"></span></span>
          <span style="display:flex;gap:6px" onclick="event.stopPropagation()">
            <button class="prompt-copy-btn" id="pm-vb-edit-btn" onclick="enterVbEdit()">✏️ 编辑</button>
            <button class="prompt-copy-btn" onclick="resetVbInline()" style="border-color:rgba(244,63,94,.4);color:#fda4af">↺ 默认</button>
          </span>
        </div>
        <div class="prompt-summary-display" id="pm-vb-view" style="font-size:0.72rem;color:#be185d;line-height:1.5;white-space:pre-wrap;background:rgba(255,255,255,0.5);border-radius:6px;padding:6px 10px;max-height:120px;overflow-y:auto;display:none">...</div>
        <div id="pm-vb-edit" style="display:none">
          <div class="prompt-ctx-box">
            <div class="prompt-ctx-label"><span>角色定位 (role)</span></div>
            <input class="prompt-ctx-input" id="pm-vb-role" style="min-height:34px;max-height:34px" placeholder="例如：情感过敏症犬系依恋少女/合租人">
          </div>
          <div class="prompt-ctx-box" style="margin-top:8px">
            <div class="prompt-ctx-label"><span>外貌基准 Tags（Danbooru 英文标签）</span></div>
            <textarea class="prompt-ctx-input" id="pm-vb-tags" style="min-height:150px;max-height:220px" placeholder="- 核心外貌: 1girl, ...&#10;- 服饰: ...&#10;- 场景: ..."></textarea>
          </div>
          <div class="prompt-ctx-box" style="margin-top:8px">
            <div class="prompt-ctx-label"><span>兜底正向词 (fallback_positive)</span></div>
            <textarea class="prompt-ctx-input" id="pm-vb-fpos" style="min-height:80px;max-height:130px"></textarea>
          </div>
          <div class="prompt-ctx-box" style="margin-top:8px">
            <div class="prompt-ctx-label"><span>兜底场景小传 (fallback_summary)</span></div>
            <textarea class="prompt-ctx-input" id="pm-vb-fsum" style="min-height:50px;max-height:90px"></textarea>
          </div>
          <div style="display:flex;gap:8px;margin-top:10px">
            <button class="prompt-gen-btn" style="flex:2" onclick="saveVbInline()">💾 保存基准</button>
            <button class="prompt-copy-btn" style="flex:1;justify-content:center;padding:10px;font-size:0.8rem;border-radius:12px" onclick="cancelVbEdit()">取消</button>
          </div>
        </div>
      </div>
      <div class="prompt-result-section" id="pm-result-box" style="display:none">
        <div class="prompt-card">
          <div class="prompt-card-header">
            <span class="prompt-card-title">📝 画面场景小传</span>
            <span id="pm-char-tag" style="font-size:0.7rem;color:#f472b6;background:rgba(244,114,182,0.15);padding:2px 8px;border-radius:6px"></span>
          </div>
          <div class="prompt-summary-display" id="pm-summary-text">...</div>
        </div>
        <div class="prompt-card">
          <div class="prompt-card-header">
            <span class="prompt-card-title">✨ 正向生图提示词 (Positive Prompt)</span>
            <button class="prompt-copy-btn" onclick="copyPromptText('pm-pos-text', this)">📋 复制正向</button>
          </div>
          <div class="prompt-text-display" id="pm-pos-text">...</div>
        </div>
        <div class="prompt-card">
          <div class="prompt-card-header">
            <span class="prompt-card-title">🚫 负向提示词 (Negative Prompt)</span>
            <button class="prompt-copy-btn" onclick="copyPromptText('pm-neg-text', this)">📋 复制负向</button>
          </div>
          <div class="prompt-text-display" id="pm-neg-text">...</div>
        </div>
        <div class="prompt-params-bar">
          <span class="param-tag">模型: NovelAI Diffusion V3</span>
          <span class="param-tag">分辨率: 832×1216</span>
          <span class="param-tag">采样步数: 28</span>
          <span class="param-tag">CFG: 5.5</span>
          <span class="param-tag">采样器: Euler Ancestral</span>
        </div>
      </div>
    </div>
    <div class="prompt-footer">
      <a href="https://nai.sta1n.cn/" target="_blank" rel="noopener" class="prompt-goto-btn">
        <span>🚀</span><span>前往 Nai2API 图像工作台</span>
      </a>
      <div style="display:flex;gap:8px">
        <button class="prompt-copy-btn" style="padding:7px 12px;font-size:0.78rem;border-radius:10px" onclick="copyAllPrompts(this)">📋 复制全部 Prompt</button>
      </div>
    </div>
  </div>
</div>

<script>

var currentPromptBot = 1;
var lastPromptData = null;
var promptReqSeq = 0;

function openPromptModal(botIndex, prefilledContext) {
  if (botIndex) currentPromptBot = parseInt(botIndex) || 1;
  var modal = document.getElementById("prompt-modal");
  if (!modal) return;
  modal.classList.add("active");
  var tabs = document.querySelectorAll(".prompt-bot-tab");
  tabs.forEach(function(t, i) {
    t.classList.toggle("active", (i + 1) === currentPromptBot);
  });
  var ctxInput = document.getElementById("pm-ctx-input");
  if (ctxInput) {
    ctxInput.value = prefilledContext || "";
  }
  var resultBox = document.getElementById("pm-result-box");
  if (resultBox) resultBox.style.display = "none";
  if (prefilledContext && prefilledContext.trim()) {
    showContextPreview(prefilledContext);
  } else {
    fetchChatContextPreview();
  }
  renderVbSection();
}

function closePromptModal() {
  var modal = document.getElementById("prompt-modal");
  if (modal) modal.classList.remove("active");
}

var ctxReqSeq = 0;
function showContextPreview(text) {
  var ctxCard = document.getElementById("pm-context-card");
  var ctxText = document.getElementById("pm-context-text");
  if (ctxCard && ctxText) {
    if (text && text.trim()) {
      ctxCard.style.display = "block";
      ctxText.textContent = text.trim();
    } else {
      ctxCard.style.display = "none";
    }
  }
}
function fetchChatContextPreview() {
  var seq = ++ctxReqSeq;
  var ctxCard = document.getElementById("pm-context-card");
  var ctxText = document.getElementById("pm-context-text");
  if (ctxCard) ctxCard.style.display = "block";
  if (ctxText) ctxText.textContent = "正在提取该 Bot 最近聊天记录...";
  fetch("/api/chat-context", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({bot: currentPromptBot})
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (seq !== ctxReqSeq) return;
    showContextPreview(d && d.ok ? (d.used_context || "") : "");
  })
  .catch(function() {
    if (seq !== ctxReqSeq) return;
    showContextPreview("");
  });
}
function refreshChatContext() {
  var ctxInput = document.getElementById("pm-ctx-input");
  if (ctxInput) ctxInput.value = "";
  fetchChatContextPreview();
}
var vbDataCache = {};
function renderVbSection() {
  var view = document.getElementById("pm-vb-view");
  var modEl = document.getElementById("pm-vb-modified");
  var editBox = document.getElementById("pm-vb-edit");
  if (view) view.style.display = "none";
  if (modEl) modEl.textContent = "";
  if (editBox) editBox.style.display = "none";
  fetch("/api/visual-benchmarks").then(function(r) { return r.json(); }).then(function(d) {
    var items = d.benchmarks || [];
    var cur = null;
    for (var i = 0; i < items.length; i++) {
      if (items[i].bot_index === currentPromptBot) { cur = items[i]; break; }
    }
    if (!cur) return;
    vbDataCache[currentPromptBot] = cur;
    var name = cur.name || ("Bot " + currentPromptBot);
    if (modEl) modEl.textContent = cur.modified ? "（已自定义）" : "（内置默认）";
    var text = "【" + name + " · " + (cur.role || "") + "】\n" + (cur.tags || "");
    if (view) { view.textContent = text; view.style.display = "block"; }
  }).catch(function() { showToast("外貌基准加载失败", "error"); });
}
function toggleVbBody() {
  var view = document.getElementById("pm-vb-view");
  if (view) view.style.display = view.style.display === "none" ? "block" : "none";
}
function enterVbEdit() {
  var cur = vbDataCache[currentPromptBot];
  if (!cur) { showToast("基准尚未加载，请稍候", "error"); return; }
  document.getElementById("pm-vb-role").value = cur.role || "";
  document.getElementById("pm-vb-tags").value = cur.tags || "";
  document.getElementById("pm-vb-fpos").value = cur.fallback_positive || "";
  document.getElementById("pm-vb-fsum").value = cur.fallback_summary || "";
  document.getElementById("pm-vb-view").style.display = "none";
  document.getElementById("pm-vb-edit").style.display = "block";
}
function cancelVbEdit() {
  document.getElementById("pm-vb-edit").style.display = "none";
  var view = document.getElementById("pm-vb-view");
  if (view) view.style.display = "block";
}
function saveVbInline() {
  var data = {
    role: document.getElementById("pm-vb-role").value,
    tags: document.getElementById("pm-vb-tags").value,
    fallback_positive: document.getElementById("pm-vb-fpos").value,
    fallback_summary: document.getElementById("pm-vb-fsum").value
  };
  fetch("/api/visual-benchmarks", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({bot_index: currentPromptBot, data: data})
  }).then(function(r) { return r.json(); }).then(function(d) {
    showToast(d.msg || (d.ok ? "已保存" : "保存失败"), d.ok ? "success" : "error");
    if (d.ok) { cancelVbEdit(); renderVbSection(); }
  }).catch(function(e) { showToast("请求失败: " + e.message, "error"); });
}
function resetVbInline() {
  if (!confirm("确认恢复该角色的内置默认外貌基准？当前自定义内容将被清空。")) return;
  fetch("/api/visual-benchmarks/reset", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({bot_index: currentPromptBot})
  }).then(function(r) { return r.json(); }).then(function(d) {
    showToast(d.msg || (d.ok ? "已恢复默认" : "重置失败"), d.ok ? "success" : "error");
    if (d.ok) { cancelVbEdit(); renderVbSection(); }
  }).catch(function(e) { showToast("请求失败: " + e.message, "error"); });
}

function switchPromptBot(botIndex) {
  currentPromptBot = botIndex;
  var tabs = document.querySelectorAll(".prompt-bot-tab");
  tabs.forEach(function(t, i) {
    t.classList.toggle("active", (i + 1) === currentPromptBot);
  });
  var ctxInput = document.getElementById("pm-ctx-input");
  if (ctxInput) ctxInput.value = "";
  var resultBox = document.getElementById("pm-result-box");
  if (resultBox) resultBox.style.display = "none";
  fetchChatContextPreview();
  renderVbSection();
}

function executeGeneratePrompt() {
  var seq = ++promptReqSeq;
  var btn = document.getElementById("pm-gen-btn");
  var btnText = document.getElementById("pm-gen-btn-text");
  var ctxInput = document.getElementById("pm-ctx-input");
  var customCtx = ctxInput ? ctxInput.value.trim() : "";
  
  if (btn) btn.disabled = true;
  if (btnText) btnText.textContent = "正在提炼场景与生成 Prompt...";
  
  fetch("/api/generate-prompt", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({
      bot: currentPromptBot,
      context: customCtx,
      style: "novelai"
    })
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (seq !== promptReqSeq) return;
    if (btn) btn.disabled = false;
    if (btnText) btnText.textContent = "✨ 重新提炼 / 生成 Prompt";
    if (d && d.ok) {
      lastPromptData = d;
      renderPromptResult(d);
    } else {
      showToast("生成失败: " + (d ? d.msg : "网络异常"), "error");
    }
  })
  .catch(function(err) {
    if (seq !== promptReqSeq) return;
    if (btn) btn.disabled = false;
    if (btnText) btnText.textContent = "✨ 重新提炼 / 生成 Prompt";
    showToast("请求失败: " + err.message, "error");
  });
}

function renderPromptResult(data) {
  var resultBox = document.getElementById("pm-result-box");
  if (!resultBox) return;
  resultBox.style.display = "flex";
  
  var ctxCard = document.getElementById("pm-context-card");
  var ctxText = document.getElementById("pm-context-text");
  if (ctxCard && ctxText) {
    if (data.used_context && data.used_context.trim()) {
      ctxCard.style.display = "block";
      ctxText.textContent = data.used_context.trim();
    } else {
      ctxCard.style.display = "none";
    }
  }
  
  var charTag = document.getElementById("pm-char-tag");
  if (charTag) {
    var tagStr = (data.character_name || "") + " · " + (data.role || "");
    if (data.model_used) tagStr += " · 🤖 " + data.model_used;
    if (data.is_fallback) tagStr += " · ⚠️ 静态兜底(模型限流)";
    charTag.textContent = tagStr;
  }
  
  var summary = document.getElementById("pm-summary-text");
  if (summary) summary.textContent = data.scene_summary || "暂无场景描述";
  
  var posText = document.getElementById("pm-pos-text");
  if (posText) posText.textContent = data.positive_prompt || "";
  
  var negText = document.getElementById("pm-neg-text");
  if (negText) negText.textContent = data.negative_prompt || "";
}

function copyPromptText(elementId, btn) {
  var el = document.getElementById(elementId);
  if (!el) return;
  var text = el.textContent;
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(function() {
      showToast("已复制到剪贴板 ✓");
      if (btn) {
        var old = btn.textContent;
        btn.textContent = "已复制 ✓";
        setTimeout(function() { btn.textContent = old; }, 1200);
      }
    });
  } else {
    var ta = document.createElement("textarea");
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand("copy"); } catch(e) {}
    document.body.removeChild(ta);
    showToast("已复制到剪贴板 ✓");
  }
}

function copyAllPrompts(btn) {
  if (!lastPromptData) { showToast("还没有可复制的 Prompt", "error"); return; }
  var allText = "### Positive Prompt:\n" + (lastPromptData.positive_prompt || "") + "\n\n### Negative Prompt:\n" + (lastPromptData.negative_prompt || "");
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(allText).then(function() {
      showToast("Prompt 已全部复制 ✓");
      if (btn) {
        var old = btn.textContent;
        btn.textContent = "全部已复制 ✓";
        setTimeout(function() { btn.textContent = old; }, 1200);
      }
    });
  } else {
    var ta = document.createElement("textarea");
    ta.value = allText;
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand("copy"); } catch(e) {}
    document.body.removeChild(ta);
    showToast("Prompt 已全部复制 ✓");
    if (btn) {
      var old = btn.textContent;
      btn.textContent = "全部已复制 ✓";
      setTimeout(function() { btn.textContent = old; }, 1200);
    }
  }
}

</script>
</body>
</html>'''

class MonitorHandler(BaseHTTPRequestHandler):
    def _check_auth(self):
        cookie = self.headers.get('Cookie', '')
        for c in cookie.split(';'):
            c = c.strip()
            if c.startswith('xiaoqi_auth='):
                return c.split('=', 1)[1] == SITE_PASSWORD
        return False

    def end_headers(self):
        """统一收尾：所有响应都声明 Connection: close（HTTP/1.0 语义），
        服务端响应完主动关闭连接，客户端收到后也立即关闭，
        从根上避免 8080 端口积累 CLOSE-WAIT 连接泄漏。"""
        try:
            self.send_header('Connection', 'close')
        except Exception:
            pass
        super().end_headers()

    def _redirect_login(self):
        self.send_response(302)
        self.send_header('Location', '/login')
        self.end_headers()

    def _get_proxy_bot(self):
        cookie = self.headers.get('Cookie', '')
        for c in cookie.split(';'):
            c = c.strip()
            if c.startswith('nekro_proxy_bot='):
                try:
                    return int(c.split('=')[1])
                except:
                    pass
        return None

    def _handle_proxy(self, method, bot_index=None):
        path_prefix = None
        m = re.match(r'^/nekro/(\d+)/', self.path)
        if m:
            path_prefix = '/nekro/' + m.group(1)
            if bot_index is None:
                bot_index = int(m.group(1))
        elif bot_index is None:
            bot_index = self._get_proxy_bot()
        if bot_index is None:
            self.send_response(302)
            self.send_header('Location', '/')
            self.end_headers()
            return
        if bot_index < 0 or bot_index >= len(BOTS):
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'invalid bot index')
            return
        bot = BOTS[bot_index]
        port_match = re.search(r':(\d+)', bot['nekro_url'])
        if not port_match:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'cannot parse port')
            return
        port = port_match.group(1)
        path = self.path
        if path_prefix:
            path = path[len(path_prefix):]
            if not path.startswith('/'):
                path = '/' + path
        token = None
        if path.startswith('/api/'):
            token, err = get_nekro_jwt(bot_index)
            if not token:
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                html = '<html><body style="background:#0f172a;color:#e2e8f0;font-family:sans-serif;padding:40px;text-align:center"><h2>登录失败</h2><p>' + str(err) + '</p><p><a href="/" style="color:#38bdf8">返回监控面板</a></p></body></html>'
                self.wfile.write(html.encode('utf-8'))
                return
        headers = {}
        for key in ['Content-Type', 'Accept', 'Accept-Language']:
            val = self.headers.get(key)
            if val:
                headers[key] = val
        if token:
            headers['Authorization'] = 'Bearer ' + token
        headers['Connection'] = 'keep-alive'
        body = None
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > 0:
            body = self.rfile.read(content_length)
        # 使用连接池发请求，复用 TCP 连接
        conn = _get_conn(port)
        try:
            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()
            status = resp.status
            resp_headers = dict(resp.getheaders())
            # SSE 流式响应：不等结束，分块即时转发（NA WebUI 事件/聊天/日志流）
            content_type = resp_headers.get('Content-Type', '') or resp_headers.get('content-type', '')
            if status == 200 and 'text/event-stream' in content_type.lower():
                # 传入上游 socket，_proxy_stream 用双端 select 探测浏览器断开
                return self._proxy_stream(resp, bot_index, path_prefix, conn.sock)
            content = resp.read()
            _put_conn(port, conn)
        except Exception as e:
            try: conn.close()
            except: pass
            # 连接可能已失效，用新连接重试一次
            conn = http.client.HTTPConnection('localhost', int(port), timeout=30)
            try:
                conn.request(method, path, body=body, headers=headers)
                resp = conn.getresponse()
                status = resp.status
                resp_headers = dict(resp.getheaders())
                # SSE 流式响应：同上，重试路径也走分块转发
                content_type = resp_headers.get('Content-Type', '') or resp_headers.get('content-type', '')
                if status == 200 and 'text/event-stream' in content_type.lower():
                    # 重试路径也走流式转发，双端探测断开
                    return self._proxy_stream(resp, bot_index, path_prefix, conn.sock)
                content = resp.read()
                _put_conn(port, conn)
            except Exception as e2:
                try: conn.close()
                except: pass
                self.send_response(502)
                self.end_headers()
                self.wfile.write(('Proxy error: ' + str(e2)).encode())
                return
        # 401 自动重试：清除过期 JWT 缓存，重新登录获取新 token
        if status == 401 and path.startswith('/api/'):
            nekro_jwt_cache.pop(bot_index, None)
            new_token, _ = get_nekro_jwt(bot_index)
            if new_token:
                headers['Authorization'] = 'Bearer ' + new_token
                try:
                    conn2 = _get_conn(port)
                    conn2.request(method, path, body=body, headers=headers)
                    resp = conn2.getresponse()
                    status = resp.status
                    resp_headers = dict(resp.getheaders())
                    content_type = resp_headers.get('Content-Type', '') or resp_headers.get('content-type', '')
                    if status == 200 and 'text/event-stream' in content_type.lower():
                        # SSE 流不放入连接池（连接被流占用，放回池会污染复用），直接转发后关闭
                        return self._proxy_stream(resp, bot_index, path_prefix, conn2.sock)
                    content = resp.read()
                    _put_conn(port, conn2)
                    log("401 retry ok: bot " + str(bot_index) + " " + path)
                except Exception as e3:
                    try: conn2.close()
                    except: pass
        content_type = resp_headers.get('Content-Type', '') or resp_headers.get('content-type', '')
        if 'text/html' in content_type.lower() or (status == 200 and len(content) > 0 and b'<html' in content.lower()):
            try:
                text = content.decode('utf-8', errors='replace')
                token, _ = get_nekro_jwt(bot_index)
                inject_script = ''
                if token:
                    auth_data = json.dumps({"state": {"token": token, "userInfo": None}, "version": 0}, ensure_ascii=False)
                    escaped = auth_data.replace('\\', '\\\\').replace("'", "\\'").replace('</', '<\\/')
                    inject_script = '<script>try{localStorage.setItem("auth-storage",\'' + escaped + '\')}catch(e){console.log("inject err:",e)}</script>'
                    if path_prefix:
                        inject_script += '<script>document.cookie="nekro_proxy_bot=' + str(bot_index) + '; Path=/; SameSite=Lax"</script>'
                        inject_script += '<script>(function(){var P="' + path_prefix + '";var oF=window.fetch;window.fetch=function(u,o){o=o||{};if(typeof u==="string"&&u.indexOf("/api/")===0){u=P+u}return oF.call(this,u,o)};var oO=XMLHttpRequest.prototype.open;XMLHttpRequest.prototype.open=function(m,u){if(typeof u==="string"&&u.indexOf("/api/")===0){u=P+u}return oO.call(this,m,u)}})();</script>'
                back_btn = '<div style="position:fixed;top:10px;right:10px;z-index:99999;background:rgba(56,28,42,0.95);border:1px solid rgba(244,114,182,0.3);border-radius:8px;padding:8px 16px;display:flex;align-items:center;gap:8px"><a href="/" style="color:#f472b6;text-decoration:none;font-family:sans-serif;font-size:14px;font-weight:600">&#8592; 返回监控</a><span style="color:rgba(244,114,182,0.4);font-size:14px">|</span><span style="color:#f9a8d4;font-family:sans-serif;font-size:14px;font-weight:700">' + (get_bot_preset_name(bot_index + 1) or bot['role']) + '</span></div>'
                head_inject = inject_script
                if '<head>' in text:
                    text = text.replace('<head>', '<head>' + head_inject, 1)
                elif '</head>' in text:
                    text = text.replace('</head>', head_inject + '</head>')
                else:
                    text = head_inject + text
                if '</body>' in text:
                    text = text.replace('</body>', back_btn + '</body>')
                else:
                    text = text + back_btn
                content = text.encode('utf-8')
            except:
                pass
        self.send_response(status)
        # 代理响应一律禁止缓存，避免浏览器缓存旧页面（如返回监控按钮显示旧人设名）
        self.send_header('Cache-Control', 'no-store')
        for key, val in resp_headers.items():
            if key.lower() not in ('transfer-encoding', 'connection', 'content-length'):
                self.send_header(key, val)
        if path_prefix:
            self.send_header('Set-Cookie', 'nekro_proxy_bot=' + str(bot_index) + '; Path=/; SameSite=Lax')
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _proxy_stream(self, resp, bot_index, path_prefix, upstream_sock=None):
        """SSE 流式转发：分块读取 NA 的 event-stream 响应并即时回写浏览器。
        不等流结束、不写 Content-Length，避免代理线程被常驻流挂死。
        v3 双端探测：select 同时监听上游与下游 socket——
        上游有数据就转发；任一侧断开立即退出关闭，杜绝 CLOSE-WAIT 泄漏。
        浏览器刷新/切页时对面板发 FIN，本方法立刻感知并关闭上游连接。"""
        down_sock = self.connection
        up_sock = upstream_sock
        # 上游 socket 存在则启用双端探测；否则回退为普通超时读
        if up_sock is not None:
            up_sock.settimeout(0.2)
        try:
            self.send_response(resp.status)
            # 代理响应一律禁止缓存，避免浏览器缓存旧页面
            self.send_header('Cache-Control', 'no-store')
            for key, val in resp.getheaders():
                if key.lower() not in ('transfer-encoding', 'connection', 'content-length'):
                    self.send_header(key, val)
            if path_prefix:
                self.send_header('Set-Cookie', 'nekro_proxy_bot=' + str(bot_index) + '; Path=/; SameSite=Lax')
            # SSE 没有确定长度，改用 chunked 传输，让浏览器按到达顺序实时显示
            self.send_header('Transfer-Encoding', 'chunked')
            self.end_headers()
            while True:
                if up_sock is not None:
                    # 双向探测：任一侧可读即处理
                    r, _, _ = select.select([down_sock, up_sock], [], [], 0.5)
                    if not r:
                        continue  # 两侧都静默，继续等
                    if down_sock in r:
                        # 下游(浏览器)可读=对端已发 FIN/关闭，立即退出
                        break
                    # 上游可读，走下面的 read() 转发（read 本身会再等数据到达）
                # 上游有数据才读；read() 在 socket 有数据时立即返回
                try:
                    chunk = resp.read(4096)
                except socket.timeout:
                    # 上游 0.2s 内没数据（静默心跳间隙），不算断开，回循环继续探测
                    continue
                if not chunk:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass  # 浏览器已断开（切页/关闭），结束转发
        finally:
            try:
                resp.close()
            except Exception:
                pass
            try:
                if up_sock is not None:
                    up_sock.settimeout(None)
            except Exception:
                pass

    def do_GET(self):
        if self.path == "/login":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(LOGIN_PAGE.encode())
            return
        if self.path in ("/icon.svg", "/manifest.json", "/sw.js") or self.path.startswith("/api/login"):
            pass
        elif not self._check_auth():
            self._redirect_login()
            return
        if self.path.startswith("/api/nekro-go"):
            query = self.path.split("?", 1)[1] if "?" in self.path else ""
            bot_index = 0
            for param in query.split("&"):
                if param.startswith("bot="):
                    try:
                        bot_index = int(param.split("=")[1])
                    except:
                        pass
            self.send_response(302)
            self.send_header('Set-Cookie', 'nekro_proxy_bot=' + str(bot_index) + '; Path=/; SameSite=Lax')
            self.send_header('Location', '/nekro/' + str(bot_index) + '/webui/')
            self.end_headers()
            # 预获取 JWT，避免首次 API 请求等待登录
            threading.Thread(target=get_nekro_jwt, args=(bot_index,), daemon=True).start()
            return
        m = re.match(r'^/nekro/(\d+)/', self.path)
        if m:
            bi = int(m.group(1))
            if 0 <= bi < len(BOTS):
                self._handle_proxy('GET', bot_index=bi)
                return
        if self.path == "/api/nekro-back":
            self.send_response(302)
            self.send_header('Set-Cookie', 'nekro_proxy_bot=; Path=/; Max-Age=0')
            self.send_header('Location', '/')
            self.end_headers()
            return
        bot_index = self._get_proxy_bot()
        if bot_index is not None:
            if self.path.startswith('/webui') or (self.path.startswith('/api/') and not is_monitor_route(self.path)):
                self._handle_proxy('GET')
                return
        if self.path == "/api/status":
            data = cache.get()
            if data is None:
                data = {"system": {"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "mem": "N/A", "mem_pct": "N/A", "disk": "N/A", "cpu": "N/A"}, "bots": [], "error": "数据采集中，请稍候..."}
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode())
            return
        if self.path == "/api/visual-benchmarks":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "benchmarks": _get_visual_benchmarks_payload()}, ensure_ascii=False).encode())
            return
        if self.path.startswith("/api/generate-prompt") or self.path.startswith("/api/chat/generate-prompt"):
            qs = self.path.split("?", 1)[1] if "?" in self.path else ""
            from urllib.parse import parse_qs
            params = parse_qs(qs)
            bot = int(params.get("bot", [1])[0]) if params.get("bot") else 1
            context = params.get("context", [""])[0] if params.get("context") else ""
            style = params.get("style", ["novelai"])[0] if params.get("style") else "novelai"
            res = generate_scene_prompt(bot, custom_context=context, style=style)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(res, ensure_ascii=False).encode())
            return
        if self.path.startswith("/api/chat/history"):
            qs = self.path.split("?", 1)[1] if "?" in self.path else ""
            port = get_bridge_port_from_query(qs)
            from urllib.parse import parse_qs, urlencode
            params = parse_qs(qs)
            params.pop('bot', None)
            new_qs = urlencode({k: v[0] for k, v in params.items()})
            fwd = "/api/chat-history" + ("?" + new_qs if new_qs else "")
            status, content = proxy_to_bridge("GET", fwd, port=port)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(content)
            return
        if self.path.startswith("/api/chat/status"):
            qs = self.path.split("?", 1)[1] if "?" in self.path else ""
            port = get_bridge_port_from_query(qs)
            status, content = proxy_to_bridge("GET", "/api/bridge-status", port=port)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(content)
            return
        if self.path.startswith("/api/chat/tts-config"):
            qs = self.path.split("?", 1)[1] if "?" in self.path else ""
            port = get_bridge_port_from_query(qs)
            status, content = proxy_to_bridge("GET", "/api/tts-config", port=port)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(content)
            return
        elif self.path == "/api/llm-mode":
            mode = get_llm_mode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"mode": mode}, ensure_ascii=False).encode())
        elif self.path == "/api/model-presets":
            groups, err = fetch_nekro_model_groups()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"groups": groups, "current": get_current_model(), "err": err}, ensure_ascii=False).encode())
        elif self.path == "/api/qq-voice":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"states": get_voice_switch_states()}, ensure_ascii=False).encode())
        elif self.path == "/api/refresh":
            cache.collect_now()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "msg": "ok"}, ensure_ascii=False).encode())
        elif self.path.startswith("/api/logs/"):
            container_name = self.path.split("/api/logs/")[1].split("?")[0]
            lines = 150
            if "?" in self.path:
                params = self.path.split("?")[1]
                for param in params.split("&"):
                    if param.startswith("lines="):
                        try:
                            lines = int(param.split("=")[1])
                        except:
                            pass
            ok, msg, logs = get_container_logs(container_name, lines)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": ok, "msg": msg, "logs": logs}, ensure_ascii=False).encode())
        elif self.path.startswith("/api/svc-logs/"):
            service_name = self.path.split("/api/svc-logs/")[1].split("?")[0]
            lines = 150
            if "?" in self.path:
                params = self.path.split("?")[1]
                for param in params.split("&"):
                    if param.startswith("lines="):
                        try:
                            lines = int(param.split("=")[1])
                        except:
                            pass
            ok, msg, logs = get_systemd_logs(service_name, lines)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": ok, "msg": msg, "logs": logs}, ensure_ascii=False).encode())
        elif self.path == "/manifest.json":
            self.send_response(200)
            self.send_header("Content-Type", "application/manifest+json; charset=utf-8")
            self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            self.wfile.write(MANIFEST_JSON)
        elif self.path == "/sw.js":
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(SERVICE_WORKER_JS)
        elif self.path == "/icon.svg":
            self.send_response(200)
            self.send_header("Content-Type", APP_ICON_TYPE)
            self.send_header("Cache-Control", "public, max-age=86400")
            self.end_headers()
            self.wfile.write(APP_ICON)
        elif self.path == "/chat" or self.path.startswith("/chat?"):
            self.send_response(200)
            self.send_header("Content-Type","text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            bots_js = json.dumps([{"qq": b.get("qq", ""), "role": b.get("role", ""), "preset_name": get_bot_preset_name(i + 1)} for i, b in enumerate(BOTS)], ensure_ascii=False)
            user_qq = os.getenv("USER_QQ", "")
            self.wfile.write(CHAT_PAGE.replace("__BOTS_DATA__", bots_js).replace("__USER_QQ__", user_qq).encode())
        elif self.path == "/" or self.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            bots_js = json.dumps([{"qq": b.get("qq", ""), "role": b.get("role", ""), "preset_name": get_bot_preset_name(i + 1)} for i, b in enumerate(BOTS)], ensure_ascii=False)
            self.wfile.write(HTML_PAGE.replace("__BOTS_DATA__", bots_js).replace("__DEVICE_BOT_INDEX__", str(DEVICE_BOT_INDEX)).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        # 代理请求必须优先处理，不能提前读 body，否则 _handle_proxy 读不到
        m = re.match(r'^/nekro/(\d+)/', self.path)
        if m:
            bi = int(m.group(1))
            if self._check_auth() and 0 <= bi < len(BOTS):
                self._handle_proxy('POST', bot_index=bi)
                return
        if self._check_auth() and self._get_proxy_bot() is not None and self.path.startswith('/api/') and not is_monitor_route(self.path):
            self._handle_proxy('POST')
            return
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8')
        if self.path == "/api/login":
            try:
                req = json.loads(body)
                pwd = req.get("password", "")
                print(f"[Login] password received (len={len(pwd)})", flush=True)
                if pwd == SITE_PASSWORD:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Set-Cookie", f"xiaoqi_auth={SITE_PASSWORD}; Path=/; Max-Age=604800; SameSite=Lax")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": True}, ensure_ascii=False).encode())
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "密码错误"}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
            return
        if self.path.startswith("/api/chat/"):
            if not self._check_auth():
                self.send_response(401)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": "未登录"}, ensure_ascii=False).encode())
                return
        elif not self._check_auth():
            self.send_response(401)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "msg": "未登录"}, ensure_ascii=False).encode())
            return
        if self.path == "/api/visual-benchmarks":
            try:
                req = json.loads(body) if body else {}
                bot = int(req.get("bot_index", 1))
                data = req.get("data", {})
                _save_visual_benchmark(bot, data)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "msg": "外貌基准已保存，下次生成立即生效"}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"保存失败: {e}"}, ensure_ascii=False).encode())
            return
        if self.path == "/api/visual-benchmarks/reset":
            try:
                req = json.loads(body) if body else {}
                bot = int(req.get("bot_index", 1))
                _reset_visual_benchmark(bot)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "msg": "已恢复内置默认基准"}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"重置失败: {e}"}, ensure_ascii=False).encode())
            return
        if self.path == "/api/container":
            try:
                req = json.loads(body)
                container_name = req.get("container", "")
                action = req.get("action", "")
                ok, msg = control_container(container_name, action)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": ok, "msg": msg}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/kick-restart":
            try:
                req = json.loads(body)
                bot_index = req.get("bot_index", -1)
                enabled = req.get("enabled", True)
                if 0 <= bot_index < len(BOTS):
                    kick_restart_enabled[bot_index] = enabled
                    log(f"kick-restart bot {bot_index}: {enabled}")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": True, "msg": f"{'开启' if enabled else '关闭'}踢下线重启"}, ensure_ascii=False).encode())
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "无效的Bot索引"}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/service-control":
            try:
                req = json.loads(body)
                service_name = req.get("service", "")
                action = req.get("action", "")
                allowed_services = {s["service"] for s in EXTRA_SERVICES if s["type"] == "systemd"}
                if service_name not in allowed_services:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "service not allowed"}, ensure_ascii=False).encode())
                elif action not in ("start", "stop", "restart"):
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "action not supported"}, ensure_ascii=False).encode())
                else:
                    run_cmd(f"sudo systemctl {action} {service_name}", timeout=30)
                    am = {"start": "启动", "stop": "停止", "restart": "重启"}
                    msg = f"{service_name} 已{am.get(action, action)}"
                    log(f"svc ctrl: {service_name} {action}")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": True, "msg": msg}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/note-sync-toggle":
            try:
                req = json.loads(body)
                direction = req.get("direction", "")
                enabled = req.get("enabled", True)
                ok, msg = toggle_note_sync_trigger(direction, enabled, bot=req.get("bot", 3))
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": ok, "msg": msg}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/chat-context":
            try:
                req = json.loads(body) if body else {}
                bot = req.get("bot", 1)
                ctx = get_recent_chat_history(bot, limit=8)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "used_context": ctx}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path in ("/api/generate-prompt", "/api/chat/generate-prompt"):
            try:
                req = json.loads(body) if body else {}
                bot = req.get("bot", 1)
                context = req.get("context", "")
                style = req.get("style", "novelai")
                res = generate_scene_prompt(bot, custom_context=context, style=style)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps(res, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"生成失败: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/chat/send":
            try:
                _port, _body = get_bridge_port_from_body(body)
                status, content = proxy_to_bridge("POST", "/api/send-message", _body, port=_port)
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(content)
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": str(e)}, ensure_ascii=False).encode())
        elif self.path == "/api/chat/tts":
            try:
                _port, _body = get_bridge_port_from_body(body)
                status, content = proxy_to_bridge("POST", "/api/tts", _body, port=_port)
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(content)
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": str(e)}, ensure_ascii=False).encode())
        elif self.path == "/api/chat/tts-config":
            try:
                _port, _body = get_bridge_port_from_body(body)
                status, content = proxy_to_bridge("POST", "/api/tts-config", _body, port=_port)
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(content)
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": str(e)}, ensure_ascii=False).encode())
        elif self.path == "/api/toggle-llm-mode":
            try:
                ok, msg = toggle_llm_mode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": ok, "msg": msg}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/model-presets":
            try:
                req = json.loads(body) if body else {}
                presets = req.get("presets")
                if not isinstance(presets, list) or not presets:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "预设列表为空"}, ensure_ascii=False).encode())
                else:
                    save_model_presets(presets)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": True, "msg": "已保存", "presets": presets}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/test-model":
            try:
                req = json.loads(body) if body else {}
                group_name = (req.get("group_name") or "").strip()
                if not group_name:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "模型组名不能为空"}, ensure_ascii=False).encode())
                    return
                base_url, api_key, model = _get_group_llm(group_name)
                if not (base_url and api_key and model):
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "模型组配置不完整或不是 chat 类型"}, ensure_ascii=False).encode())
                    return
                t0 = time.time()
                test_payload = {
                    "model": model,
                    "messages": [{"role": "user", "content": "ping"}],
                    "max_tokens": 5,
                    "temperature": 0
                }
                test_req = urllib.request.Request(
                    f"{base_url}/chat/completions",
                    data=json.dumps(test_payload).encode("utf-8"),
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
                )
                with urllib.request.urlopen(test_req, timeout=20) as resp:
                    res = json.loads(resp.read().decode("utf-8"))
                    reply = (res.get("choices") or [{}])[0].get("message", {}).get("content", "").strip()[:40]
                cost = round(time.time() - t0, 2)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "msg": f"连接正常 · 延迟 {cost}s · 模型回复: {reply}"}, ensure_ascii=False).encode())
            except urllib.error.HTTPError as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"HTTP {e.code} {e.reason}"}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"连接失败: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/apply-model":
            try:
                req = json.loads(body) if body else {}
                group_name = (req.get("group_name") or "").strip()
                chat_model = (req.get("chat_model") or "").strip()
                base_url = (req.get("base_url") or "").strip()
                api_key = (req.get("api_key") or "").strip()
                if not group_name:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "模型组名不能为空"}, ensure_ascii=False).encode())
                else:
                    ok, msg = apply_model_to_all(group_name, chat_model, base_url, api_key)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": ok, "msg": msg}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/delete-model-group":
            try:
                req = json.loads(body) if body else {}
                group_name = (req.get("group_name") or "").strip()
                if not group_name:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "模型组名不能为空"}, ensure_ascii=False).encode())
                else:
                    ok, msg = delete_model_group_from_all(group_name)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": ok, "msg": msg}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/create-model-group":
            try:
                req = json.loads(body) if body else {}
                group_name = (req.get("group_name") or "").strip()
                chat_model = (req.get("chat_model") or "").strip()
                base_url = (req.get("base_url") or "").strip()
                api_key = (req.get("api_key") or "").strip()
                if not group_name or not chat_model or not base_url:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "模型组名、模型名、接口地址不能为空"}, ensure_ascii=False).encode())
                else:
                    ok, msg = create_model_group_on_all(group_name, chat_model, base_url, api_key)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": ok, "msg": msg}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/fetch-models":
            try:
                req = json.loads(body) if body else {}
                base_url = (req.get("base_url") or "").strip()
                api_key = (req.get("api_key") or "").strip()
                if not base_url:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "msg": "接口地址不能为空"}, ensure_ascii=False).encode())
                else:
                    ok, result = fetch_models_from_api(base_url, api_key)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": ok, "models" if ok else "msg": result}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        elif self.path == "/api/qq-voice":
            try:
                req = json.loads(body) if body else {}
                bot_index = req.get("bot_index", -1)
                enabled = req.get("enabled", True)
                ok, msg = set_voice_switch_state(bot_index, enabled)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": ok, "msg": msg, "states": get_voice_switch_states()}, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "msg": f"error: {e}"}, ensure_ascii=False).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        if not self.path.startswith("/api/chat/") and not self._check_auth():
            self.send_response(401)
            self.end_headers()
            return
        m = re.match(r'^/nekro/(\d+)/', self.path)
        if m:
            bi = int(m.group(1))
            if self._check_auth() and 0 <= bi < len(BOTS):
                self._handle_proxy('PUT', bot_index=bi)
                return
        if self._get_proxy_bot() is not None and self.path.startswith('/api/') and not is_monitor_route(self.path):
            self._handle_proxy('PUT')
            return
        self.send_response(404)
        self.end_headers()

    def do_DELETE(self):
        if not self._check_auth():
            self.send_response(401)
            self.end_headers()
            return
        m = re.match(r'^/nekro/(\d+)/', self.path)
        if m:
            bi = int(m.group(1))
            if self._check_auth() and 0 <= bi < len(BOTS):
                self._handle_proxy('DELETE', bot_index=bi)
                return
        if self._get_proxy_bot() is not None and self.path.startswith('/api/') and not is_monitor_route(self.path):
            self._handle_proxy('DELETE')
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        log("[HTTP:" + str(self.client_address[0]) + "] " + format % args)

def main():
    log("=" * 50)
    log("小栖bot Monitor")
    log(f"http://0.0.0.0:{PORT}")
    log(f"Icon: {APP_ICON_TYPE} ({len(APP_ICON)} bytes)")
    docker_ok, docker_ver = check_docker()
    if docker_ok:
        log(f"Docker: {docker_ver}")
    else:
        log(f"Docker ERROR: {docker_ver}")
    cache.start()
    _load_custom_visual_benchmarks()
    log("Ctrl+C to stop")
    log("=" * 50)
    server = ThreadingHTTPServer((HOST, PORT), MonitorHandler)
    server.daemon_threads = True
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("stopped")
        server.server_close()

if __name__ == "__main__":
    main()
