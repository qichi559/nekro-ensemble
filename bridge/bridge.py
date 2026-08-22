#!/usr/bin/env python3
"""
NekroAgent <-> xiaozhi-esp32-server 桥接代理
原版 + 回执确认 + 多消息收集 + ChannelInfo修复 + 聊天记录API + TTS API
"""
import os
import re
import json
import time
import uuid
import base64
import threading
import requests
from flask import Flask, request, jsonify, Response
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # 允许监控页面跨域调用

NEKRO_BASE_URL = os.getenv("NEKRO_BASE_URL", "http://127.0.0.1:8021")
ACCESS_KEY = os.getenv("ACCESS_KEY", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "private_stackchan")
CLIENT_NAME = os.getenv("CLIENT_NAME", "stackchan")
PLATFORM = os.getenv("PLATFORM", "device")
LISTEN_PORT = int(os.getenv("LISTEN_PORT", "8090"))
OWNER_QQ = os.getenv("OWNER_QQ", "")
OWNER_NAME = os.getenv("OWNER_NAME", "")
BRIDGE_DATA_DIR = os.getenv("BRIDGE_DATA_DIR", "/opt/nekro-bridge")

# ===== 豆包 TTS 配置 =====
TTS_API_URL = "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
TTS_API_KEY = os.getenv("TTS_API_KEY", "")
TTS_RESOURCE_ID = os.getenv("TTS_RESOURCE_ID", "")
TTS_VOICE = os.getenv("TTS_VOICE", "")
TTS_SPEED = float(os.getenv("TTS_SPEED", "1.0"))
# 豆包单次请求文本上限（字），超长自动分段逐段合成
TTS_SEG_MAX = int(os.getenv("TTS_SEG_MAX", "300"))
TTS_PITCH = float(os.getenv("TTS_PITCH", "1.0"))
TTS_VOLUME = float(os.getenv("TTS_VOLUME", "1.0"))
TTS_CONFIG_FILE = f"{BRIDGE_DATA_DIR}/tts_config_{LISTEN_PORT}.json"


def load_tts_config():
    """读取 TTS 动态配置（文件优先于环境变量）"""
    try:
        with open(TTS_CONFIG_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def get_tts_params():
    """获取生效的 TTS 参数：配置文件 > 环境变量 > 默认"""
    cfg = load_tts_config() or {}
    return {
        "voice": TTS_VOICE,  # 音色由环境变量管理，配置文件只控制 speed/pitch/volume
        "speed": float(cfg.get("speed", TTS_SPEED)),
        "pitch": float(cfg.get("pitch", TTS_PITCH)),
        "volume": float(cfg.get("volume", TTS_VOLUME)),
    }

sse_connected = False
web_sending = False  # 标记当前回复是否由网页发送触发
web_last_ts = 0.0  # 最近一次网页发送的时间戳
client_id = None
response_queue = []
queue_lock = threading.Lock()
connected_event = threading.Event()

# ===== 聊天记录存储（内存循环缓冲，最多200条）=====
chat_history = []
chat_history_lock = threading.Lock()
MAX_HISTORY = 2000
CHAT_HISTORY_FILE = os.getenv("CHAT_HISTORY_FILE", f"{BRIDGE_DATA_DIR}/chat_history.jsonl")
MAX_FILE_HISTORY = int(os.getenv("MAX_FILE_HISTORY", "20000"))
MAX_FILE_SIZE = 20 * 1024 * 1024  # history file > 20MB triggers compaction


EMOTION_TAG_RE = re.compile(r"\[\[emotion:([^\]\[]*)\]\]")
# 残缺/未闭合标签（如 [[emotion:调皮 后接正文）：删到换行或括号前
DANGLING_EMOTION_RE = re.compile(r"\[\[emotion:[^\n\]\[]*")

def parse_emotion_tag(text):
    """提取 [[emotion:xx]] 标签，返回 (干净文本, 情感或None)
    兼容 LLM 偶尔输出的混合标签（如 [[emotion:lovey-dovey撒娇]] / [[emotion:调皮]]）：
    标签一律剥离，情感值取英文枚举前缀，取不到则交给 detect_emotion 兜底。"""
    if not text:
        return text or "", None
    m = EMOTION_TAG_RE.search(text)
    raw = m.group(1) if m else ""
    m2 = re.match(r"[a-zA-Z0-9_\-]+", raw)
    emotion = m2.group(0).lower() if m2 else None
    clean = EMOTION_TAG_RE.sub("", text)
    clean = DANGLING_EMOTION_RE.sub("", clean).strip()
    return clean, emotion


def add_chat_record(role, text, source="device", emotion=None):
    """添加聊天记录"""
    text = EMOTION_TAG_RE.sub("", text or "")
    text = DANGLING_EMOTION_RE.sub("", text).strip()
    record = {
        "id": str(uuid.uuid4())[:8],
        "role": role,  # "user" 或 "assistant"
        "text": text,
        "source": source,  # "device"=设备语音, "web"=监控页面
        "emotion": emotion or "",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "ts": time.time(),
    }
    with chat_history_lock:
        chat_history.append(record)
        if len(chat_history) > MAX_HISTORY:
            chat_history.pop(0)
    _append_history_file(record)
    print(f"[Chat] {role}: {text[:80]} [{source}]", flush=True)


def _load_chat_history():
    """Load chat history from JSONL file into memory at startup"""
    global chat_history
    loaded = []
    if os.path.exists(CHAT_HISTORY_FILE):
        try:
            with open(CHAT_HISTORY_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        loaded.append(json.loads(line))
                    except Exception:
                        continue
        except Exception as e:
            print(f"[Chat] load history failed: {e}", flush=True)
    with chat_history_lock:
        chat_history = loaded[-MAX_HISTORY:]
    print(f"[Chat] loaded {len(chat_history)} history records", flush=True)


def _append_history_file(record):
    """Append one record to the JSONL history file"""
    try:
        with open(CHAT_HISTORY_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[Chat] write history file failed: {e}", flush=True)
        return
    try:
        if os.path.getsize(CHAT_HISTORY_FILE) > MAX_FILE_SIZE:
            _compact_history_file()
    except Exception:
        pass


def _compact_history_file():
    """Keep only the latest MAX_FILE_HISTORY lines when file is too large"""
    try:
        with open(CHAT_HISTORY_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > MAX_FILE_HISTORY:
            tmp = CHAT_HISTORY_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.writelines(lines[-MAX_FILE_HISTORY:])
            os.replace(tmp, CHAT_HISTORY_FILE)
            print(f"[Chat] history file compacted to latest {MAX_FILE_HISTORY} lines", flush=True)
    except Exception as e:
        print(f"[Chat] compact history file failed: {e}", flush=True)


def filter_tts_text(text):
    """去除括号内的动作神态描写"""
    text = re.sub(r'\uff08[^\uff09]*\uff09', '', text)
    text = re.sub(r'\([^)]*\)', '', text)
    text = re.sub(r'\uff08[^\uff09]*$', '', text)
    text = re.sub(r'\([^)]*$', '', text)
    text = re.sub(r'^[\uff09\)]+', '', text)
    text = re.sub(r'[\uff08\(]+$', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def detect_emotion(text):
    """根据文本内容自动检测情感类型"""
    angry_kw = ["生气", "哼！", "讨厌", "烦", "怒", "可恶", "大笨蛋", "受够了", "不准", "不可以", "不许", "烦死", "气死"]
    for kw in angry_kw:
        if kw in text:
            return "angry"
    surprised_kw = ["什么？", "不是吧", "怎么会", "诶？！", "真的假的", "不会吧", "啊？！", "天哪"]
    for kw in surprised_kw:
        if kw in text:
            return "surprise"
    fear_kw = ["怕", "害怕", "担心", "不安", "紧张", "心慌", "吓到"]
    for kw in fear_kw:
        if kw in text:
            return "scare"
    lovey_kw = ["赖着", "粘着", "黏着", "抱抱", "抱一会儿", "不许走", "不许跑", "陪你", "一辈子", "缠上", "不放手", "要抱", "再抱", "撒娇", "哼哼~", "嘻嘻~", "嘿嘿~", "人家", "嘛~", "啦~"]
    for kw in lovey_kw:
        if kw in text:
            return "lovey-dovey"
    shy_kw = ["脸红", "害羞", "羞", "烫烫", "脸颊烫", "脸热", "不敢看", "别看我", "丢人", "不好意思", "遮住脸", "埋进", "脸埋"]
    for kw in shy_kw:
        if kw in text:
            return "lovey-dovey"
    comfort_kw = ["没关系", "别难", "会好", "别哭", "陪在你身边", "陪着你", "不要怕", "有我在", "别担心", "没事的", "一切都会"]
    for kw in comfort_kw:
        if kw in text:
            return "comfort"
    excited_kw = ["太棒了", "好耶", "兴奋", "激动", "终于", "耶！", "太好了！", "拉钩", "真的吗？！", "颤", "不能自已", "忍不住"]
    for kw in excited_kw:
        if kw in text:
            return "energetic"
    tender_kw = ["嗯。", "别动", "就这样", "呼呼", "安心", "轻轻", "柔柔", "睡吧", "休息", "放轻松", "慢慢"]
    for kw in tender_kw:
        if kw in text:
            return "comfort"
    happy_kw = ["哈哈", "嘻嘻", "哼哼", "嘿嘿", "开心", "高兴", "耶", "好开心", "厉害", "我想你", "喜欢你", "不错", "棒", "好哒", "好呀", "当然", "没问题"]
    for kw in happy_kw:
        if kw in text:
            return "happy"
    sad_kw = ["呜", "难过", "伤心", "哭", "心疼", "不舍", "失落", "叹", "哎~", "唉", "孤独", "想念", "舍不得", "不要走", "别走", "对不起", "抱歉"]
    for kw in sad_kw:
        if kw in text:
            return "sad"
    conniving_kw = ["绿茶", "茶艺", "假惺惺", "装无辜", "心机"]
    for kw in conniving_kw:
        if kw in text:
            return "conniving"
    storytelling_kw = ["很久很久以前", "从前有个", "传说中", "讲个故事", "故事是"]
    for kw in storytelling_kw:
        if kw in text:
            return "storytelling"
    novel_kw = ["平平淡淡", "若无其事", "淡然地说", "平静地说"]
    for kw in novel_kw:
        if kw in text:
            return "novel_dialog"
    wave_count = text.count("~") + text.count("～")
    excl_count = text.count("！") + text.count("!")
    dot_count = text.count("…") + text.count("...")
    if wave_count >= 2:
        return "lovey-dovey"
    if excl_count >= 3:
        return "energetic"
    if dot_count >= 2:
        return "sad"
    return ""


def _tts_cache_dir():
    """TTS 缓存目录（按监听端口隔离，各 bot 音色参数不同）"""
    import os
    d = f"{BRIDGE_DATA_DIR}/tts_cache_{LISTEN_PORT}"
    os.makedirs(d, exist_ok=True)
    return d


def _tts_cache_get(key):
    """读缓存，命中返回 mp3 bytes，未命中返回 None"""
    import os
    p = os.path.join(_tts_cache_dir(), key + ".mp3")
    if os.path.exists(p):
        try:
            with open(p, "rb") as f:
                return f.read()
        except Exception:
            return None
    return None


def _tts_cache_put(key, data):
    """写缓存 + 控制目录大小（超 200 个文件删最旧）"""
    import os
    d = _tts_cache_dir()
    p = os.path.join(d, key + ".mp3")
    try:
        with open(p, "wb") as f:
            f.write(data)
        files = sorted([os.path.join(d, x) for x in os.listdir(d) if x.endswith(".mp3")],
                       key=os.path.getmtime)
        while len(files) > 200:
            os.remove(files.pop(0))
    except Exception as e:
        print(f"[TTS] 缓存写入失败: {e}", flush=True)


def _tts_cache_key(text, forced_emotion=None):
    """计算缓存 key：过滤文本 + 当前音色 + 情感 + 语速/音高/音量 的 hash"""
    import hashlib
    filtered = filter_tts_text(text) or ""
    p = get_tts_params()
    emotion = forced_emotion or detect_emotion(filtered)
    sig = "|".join([
        str(p.get("voice", "")),
        filtered,
        str(emotion or ""),
        str(p.get("speed", 1.0)),
        str(p.get("pitch", 1.0)),
        str(p.get("volume", 1.0)),
    ])
    return hashlib.md5(sig.encode("utf-8")).hexdigest()


def generate_tts(text, forced_emotion=None):
    """调用豆包 TTS API 生成语音，返回 mp3 bytes（带文件缓存）。
    超长文本自动按句子分段逐段合成后拼接（豆包单次有长度上限）。"""
    import os
    key = _tts_cache_key(text, forced_emotion)
    cached = _tts_cache_get(key)
    if cached:
        print(f"[TTS] 命中缓存: {len(cached)} bytes (key={key[:8]})", flush=True)
        return cached
    os.makedirs(_tts_cache_dir(), exist_ok=True)
    filtered = filter_tts_text(text)
    if not filtered:
        return None

    # 超长文本分段：豆包单次请求建议 <= TTS_SEG_MAX 字
    segs = _split_tts_text(filtered)
    if len(segs) > 1:
        print(f"[TTS] 文本 {len(filtered)} 字，分段 {len(segs)} 段逐段合成...", flush=True)
        audio_total = b""
        for i, seg in enumerate(segs, 1):
            part = _generate_tts_once(seg, forced_emotion)
            if part:
                audio_total += part
                print(f"[TTS] 段 {i}/{len(segs)} ok ({len(part)} bytes)", flush=True)
        if audio_total:
            _tts_cache_put(key, audio_total)
            return audio_total
        print(f"[TTS] 分段合成失败", flush=True)
        return None

    return _generate_tts_once(filtered, forced_emotion)


def _split_tts_text(text, max_len=TTS_SEG_MAX):
    """把长文本按句号等标点切分成 <= max_len 的段列表"""
    if len(text) <= max_len:
        return [text]
    import re
    # 优先在句子边界切（中文句号、感叹号、问号、分号、换行）
    pieces = re.split(r'(?<=[。！？；!?;\n])', text)
    segs, cur = [], ""
    for p in pieces:
        if not p:
            continue
        if len(cur) + len(p) <= max_len:
            cur += p
        else:
            if cur:
                segs.append(cur)
            # 单段超长则硬切
            while len(p) > max_len:
                segs.append(p[:max_len])
                p = p[max_len:]
            cur = p
    if cur:
        segs.append(cur)
    return segs


def _generate_tts_once(filtered, forced_emotion=None):
    """单段文本调豆包 TTS，返回 mp3 bytes（无缓存逻辑，供 generate_tts 复用）"""
    p = get_tts_params()
    audio_params = {
        "format": "mp3",
        "sample_rate": 24000,
    }
    if 0.2 <= p["speed"] <= 3.0:
        audio_params["speed_ratio"] = p["speed"]
    if 0.1 <= p["pitch"] <= 3.0:
        audio_params["pitch_ratio"] = p["pitch"]
    if 0.1 <= p["volume"] <= 3.0:
        audio_params["volume_ratio"] = p["volume"]
    emotion = forced_emotion or detect_emotion(filtered)
    if emotion:
        audio_params["emotion"] = emotion
        audio_params["emotion_scale"] = 5

    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": TTS_API_KEY,
        "X-Api-Resource-Id": TTS_RESOURCE_ID,
    }
    request_json = {
        "user": {"uid": "1"},
        "req_params": {
            "text": filtered,
            "speaker": p["voice"],
            "audio_params": audio_params,
        },
    }
    try:
        resp = requests.post(TTS_API_URL, json.dumps(request_json), headers=headers, timeout=60)
        audio_bytes = b""
        for line in resp.text.strip().split("\n"):
            if not line:
                continue
            chunk = json.loads(line)
            if chunk.get("data"):
                audio_bytes += base64.b64decode(chunk["data"])
        if audio_bytes:
            print(f"[TTS] 生成成功: {len(audio_bytes)} bytes, emotion={emotion}", flush=True)
            return audio_bytes
        else:
            print(f"[TTS] 生成失败: {resp.text[:200]}", flush=True)
            return None
    except Exception as e:
        print(f"[TTS] 异常: {e}", flush=True)
        return None


def sse_listener():
    global sse_connected, client_id
    while True:
        try:
            url = f"{NEKRO_BASE_URL}/api/adapters/sse/connect"
            params = {
                "client_name": CLIENT_NAME,
                "platform": PLATFORM,
                "access_key": ACCESS_KEY,
            }
            print(f"[Bridge] 连接 SSE: {url}", flush=True)
            resp = requests.get(url, params=params, stream=True, timeout=(10, None))

            current_event = None
            for raw_line in resp.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue
                line = raw_line.replace("\r\n", "\n").replace("\r", "\n").strip()
                if not line:
                    current_event = None
                    continue
                if line.startswith("event:"):
                    current_event = line[6:].strip()
                elif line.startswith("data:"):
                    data_str = line[5:].strip()
                    try:
                        data = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    if current_event == "connected":
                        client_id = data.get("client_id")
                        sse_connected = True
                        connected_event.set()
                        print(f"[Bridge] SSE 已连接, client_id={client_id}", flush=True)
                        subscribe_channels()
                    elif current_event == "send_message":
                        req_id = data.get("request_id")
                        if req_id:
                            ack(req_id, {
                                "message_id": f"msg_{int(time.time() * 1000)}",
                                "success": True,
                            })
                        msg_data = data.get("data", {})
                        segments = msg_data.get("segments", [])
                        text = "".join(
                            s.get("content", "")
                            for s in segments
                            if s.get("type") == "text"
                        )
                        if text:
                            with queue_lock:
                                response_queue.append(text)
                            if not web_sending and time.time() - web_last_ts > 3:
                                add_chat_record("assistant", text, "device")
                    elif current_event == "get_channel_info":
                        req_id = data.get("request_id")
                        if req_id:
                            ack(req_id, {
                                "channel_id": CHANNEL_ID,
                                "channel_name": OWNER_NAME,
                                "channel_avatar": None,
                                "member_count": 2,
                                "owner_id": "device_bot",
                                "is_admin": True,
                            })
                    elif current_event == "get_self_info":
                        req_id = data.get("request_id")
                        if req_id:
                            ack(req_id, {
                                "user_id": "device_bot",
                                "user_name": OWNER_NAME,
                                "user_avatar": None,
                                "platform_name": PLATFORM,
                            })
                    elif current_event == "get_user_info":
                        req_id = data.get("request_id")
                        if req_id:
                            ack(req_id, {
                                "user_id": OWNER_QQ,
                                "user_name": OWNER_NAME,
                                "user_avatar": None,
                                "platform_name": PLATFORM,
                            })
                    elif current_event and current_event != "heartbeat":
                        req_id = data.get("request_id")
                        if req_id:
                            ack(req_id)
        except Exception as e:
            print(f"[Bridge] SSE 连接异常: {e}", flush=True)
        sse_connected = False
        connected_event.clear()
        time.sleep(3)


def ack(request_id, resp_data=None):
    if not client_id:
        return
    url = f"{NEKRO_BASE_URL}/api/adapters/sse/connect"
    headers = {"X-Client-ID": client_id, "X-Access-Key": ACCESS_KEY}
    payload = {
        "cmd": "response",
        "request_id": request_id,
        "success": True,
        "data": resp_data or {},
    }
    try:
        requests.post(url, json=payload, headers=headers, timeout=10)
    except:
        pass


def subscribe_channels():
    if not client_id:
        return
    try:
        url = f"{NEKRO_BASE_URL}/api/adapters/sse/connect"
        headers = {"X-Client-ID": client_id, "X-Access-Key": ACCESS_KEY}
        data = {"cmd": "subscribe", "channel_ids": [CHANNEL_ID]}
        requests.post(url, json=data, headers=headers, timeout=10)
        print(f"[Bridge] 订阅频道 {CHANNEL_ID}", flush=True)
    except Exception as e:
        print(f"[Bridge] 订阅失败: {e}", flush=True)


def send_to_nekro(text):
    if not client_id:
        print("[Bridge] SSE 未连接，无法发送", flush=True)
        return False
    url = f"{NEKRO_BASE_URL}/api/adapters/sse/connect"
    headers = {"X-Client-ID": client_id, "X-Access-Key": ACCESS_KEY}
    data = {
        "cmd": "message",
        "channel_id": CHANNEL_ID,
        "message": {
            "from_id": OWNER_QQ,
            "from_name": OWNER_NAME,
            "channel_id": CHANNEL_ID,
            "channel_name": OWNER_NAME,
            "platform_name": PLATFORM,
            "segments": [{"type": "text", "content": text}],
            "timestamp": int(time.time()),
            "is_to_me": True,
        },
    }
    try:
        requests.post(url, json=data, headers=headers, timeout=120)
        print(f"[Bridge] 已发送: {text[:80]}", flush=True)
        return True
    except Exception as e:
        print(f"[Bridge] 发送失败: {e}", flush=True)
        return False


# ===== 原有 OpenAI 兼容接口（供 xiaozhi-server 调用）=====

@app.route("/v1/chat/completions", methods=["POST"])
def chat_completions():
    data = request.json
    messages = data.get("messages", [])
    is_stream = data.get("stream", False)

    user_text = ""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            user_text = msg.get("content", "")
            break

    if not user_text:
        return jsonify({"error": "No user message"}), 400

    add_chat_record("user", user_text, "device")

    if not connected_event.wait(timeout=10):
        return jsonify({"error": "Bridge not connected"}), 503

    with queue_lock:
        response_queue.clear()

    if not send_to_nekro(user_text):
        return jsonify({"error": "Send failed"}), 502

    if is_stream:
        def generate():
            init_chunk = {
                "id": "chatcmpl-bridge",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": "nekro-agent",
                "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": None}],
            }
            yield f"data: {json.dumps(init_chunk)}\n\n"

            deadline = time.time() + 120
            while time.time() < deadline:
                with queue_lock:
                    if response_queue:
                        break
                keepalive = {
                    "id": "chatcmpl-bridge",
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": "nekro-agent",
                    "choices": [{"index": 0, "delta": {}, "finish_reason": None}],
                }
                yield f"data: {json.dumps(keepalive)}\n\n"
                time.sleep(3)

            time.sleep(2)

            collected = []
            with queue_lock:
                while response_queue:
                    collected.append(response_queue.pop(0))

            if collected:
                full_reply = "。".join(collected)
                clean_reply, _ = parse_emotion_tag(full_reply)
                print(f"[Bridge] 发送回复({len(collected)}条): {clean_reply[:100]}", flush=True)
                chunk = {
                    "id": "chatcmpl-bridge",
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": "nekro-agent",
                    "choices": [{"index": 0, "delta": {"content": clean_reply}, "finish_reason": None}],
                }
                yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                chunk["choices"][0] = {"index": 0, "delta": {}, "finish_reason": "stop"}
                yield f"data: {json.dumps(chunk)}\n\n"
            yield "data: [DONE]\n\n"

        return Response(generate(), mimetype="text/event-stream")
    else:
        deadline = time.time() + 120
        while time.time() < deadline:
            with queue_lock:
                if response_queue:
                    break
            time.sleep(0.1)
        time.sleep(2)
        collected = []
        with queue_lock:
            while response_queue:
                collected.append(response_queue.pop(0))
        reply = "。".join(collected) if collected else "抱歉，回复超时了。"
        return jsonify({
            "id": "chatcmpl-bridge",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": "nekro-agent",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": reply},
                "finish_reason": "stop",
            }],
        })


@app.route("/v1/models", methods=["GET"])
def list_models():
    return jsonify({"data": [{"id": "nekro-agent", "object": "model"}]})


# ===== 新增：监控页面聊天 API =====

@app.route("/api/chat-history", methods=["GET"])
def api_chat_history():
    """返回聊天记录"""
    after_ts = request.args.get("after_ts", 0, type=float)
    with chat_history_lock:
        if after_ts > 0:
            records = [r for r in chat_history if r["ts"] > after_ts]
        else:
            records = list(chat_history)
    return jsonify({"ok": True, "data": records, "count": len(records)})


@app.route("/api/send-message", methods=["POST"])
def api_send_message():
    """从监控页面发送消息给 NekroAgent，返回 AI 回复"""
    data = request.json
    text = data.get("text", "").strip()
    need_tts = data.get("tts", True)  # 是否需要返回 TTS 音频

    if not text:
        return jsonify({"ok": False, "msg": "消息不能为空"}), 400

    if not connected_event.wait(timeout=10):
        return jsonify({"ok": False, "msg": "Bridge 未连接"}), 503

    # 不记录用户消息（前端已显示，避免重复）
    add_chat_record("user", text, "web")

    # 清空队列，发送给 NekroAgent
    with queue_lock:
        response_queue.clear()

    global web_sending, web_last_ts
    web_sending = True
    web_last_ts = time.time()
    if not send_to_nekro(text):
        web_sending = False
        return jsonify({"ok": False, "msg": "发送失败"}), 502

    # 等待回复
    deadline = time.time() + 120
    while time.time() < deadline:
        with queue_lock:
            if response_queue:
                break
        time.sleep(0.1)

    time.sleep(2)  # 收集多段回复

    collected = []
    with queue_lock:
        while response_queue:
            collected.append(response_queue.pop(0))

    reply = "。".join(collected) if collected else "抱歉，回复超时了。"
    clean_reply, forced_emotion = parse_emotion_tag(reply)
    web_sending = False
    add_chat_record("assistant", clean_reply, "web", emotion=forced_emotion)

    result = {"ok": True, "reply": clean_reply, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}

    # 生成 TTS 音频
    if need_tts and clean_reply:
        audio_bytes = generate_tts(clean_reply, forced_emotion)
        if audio_bytes:
            import base64 as b64
            result["audio"] = b64.b64encode(audio_bytes).decode("utf-8")
            result["audio_format"] = "mp3"

    return jsonify(result)


@app.route("/api/record-chat", methods=["POST"])
def api_record_chat():
    """接收外部（如 QQ 适配器、多端同步）上报的聊天记录"""
    data = request.get_json(silent=True) or {}
    role = data.get("role", "user")
    text = (data.get("text") or "").strip()
    source = data.get("source", "qq")
    emotion = (data.get("emotion") or "").strip() or None
    if not text:
        return jsonify({"ok": False, "msg": "text is empty"}), 400

    clean_text, parsed_emotion = parse_emotion_tag(text)
    final_emotion = emotion or parsed_emotion or None
    add_chat_record(role, clean_text, source=source, emotion=final_emotion)
    return jsonify({"ok": True})


@app.route("/api/tts", methods=["POST"])
def api_tts():
    """单独生成 TTS 音频"""
    data = request.json
    text = data.get("text", "").strip()
    emotion = (data.get("emotion") or "").strip() or None
    if not text:
        return jsonify({"ok": False, "msg": "文本不能为空"}), 400

    audio_bytes = generate_tts(text, emotion)
    if audio_bytes:
        import base64 as b64
        return jsonify({
            "ok": True,
            "audio": b64.b64encode(audio_bytes).decode("utf-8"),
            "audio_format": "mp3",
        })
    else:
        return jsonify({"ok": False, "msg": "文本为空或TTS生成失败"}), 200


@app.route("/api/tts-config", methods=["GET", "POST"])
def api_tts_config():
    """获取/更新 TTS 参数（speed/pitch/volume/voice），保存到配置文件即时生效"""
    if request.method == "GET":
        p = get_tts_params()
        return jsonify({"ok": True, "voice": p["voice"], "speed": p["speed"], "pitch": p["pitch"], "volume": p["volume"]})
    try:
        data = request.get_json(silent=True) or {}
        p = get_tts_params()
        voice = TTS_VOICE
        speed = float(data.get("speed", p["speed"]))
        pitch = float(data.get("pitch", p["pitch"]))
        volume = float(data.get("volume", p["volume"]))
        cfg = {"voice": voice, "speed": speed, "pitch": pitch, "volume": volume}
        with open(TTS_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        return jsonify({"ok": True, **cfg})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})


@app.route("/api/bridge-status", methods=["GET"])
def api_bridge_status():
    """返回 bridge 连接状态"""
    return jsonify({
        "ok": True,
        "connected": sse_connected,
        "client_id": client_id,
    })


if __name__ == "__main__":
    _load_chat_history()
    sse_thread = threading.Thread(target=sse_listener, daemon=True)
    sse_thread.start()
    print(f"[Bridge] 启动，端口 {LISTEN_PORT}", flush=True)
    print(f"[Bridge] NekroAgent: {NEKRO_BASE_URL}", flush=True)
    print(f"[Bridge] 频道: {CHANNEL_ID}", flush=True)
    print(f"[Bridge] TTS语音: {TTS_VOICE}", flush=True)
    app.run(host="0.0.0.0", port=LISTEN_PORT, threaded=True)
