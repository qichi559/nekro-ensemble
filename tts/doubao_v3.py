# ============================================================
# 本文件基于 xiaozhi-server（小智）的 TTS provider 机制编写
#   项目地址：https://github.com/xinnan-tech/xiaozhi-esp32-server
#   许可证：MIT
# 用途：豆包（火山引擎）语音合成 provider，需放入 xiaozhi-server 使用
# ============================================================
import os
import uuid
import json
import base64
import requests
from typing import Optional
from config.logger import setup_logging
from core.providers.tts.base import TTSProviderBase

TAG = __name__
logger = setup_logging()


class TTSProvider(TTSProviderBase):
    def __init__(self, config, delete_audio_file: bool):
        super().__init__(config, delete_audio_file)
        self.api_url = "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
        self.voice = config.get("voice", "")

        self.api_key = config.get("api_key")
        self.appid = config.get("appid")
        self.access_token = config.get("access_token")
        self.resource_id = config.get("resource_id", "seed-tts-1.0")

        if self.api_key:
            self.header = {
                "Content-Type": "application/json",
                "X-Api-Key": self.api_key,
                "X-Api-Resource-Id": self.resource_id,
            }
        else:
            self.header = {
                "Content-Type": "application/json",
                "X-Api-App-Id": self.appid,
                "X-Api-Access-Key": self.access_token,
                "X-Api-Resource-Id": self.resource_id,
            }

        self.audio_format = config.get("format", "mp3")
        self.audio_file_type = self.audio_format
        self.sample_rate = int(config.get("sample_rate", 24000))

    async def text_to_speak(self, text, output_file):
        request_json = {
            "user": {
                "uid": str(uuid.uuid4())
            },
            "req_params": {
                "text": text,
                "speaker": self.voice,
                "audio_params": {
                    "format": self.audio_format,
                    "sample_rate": self.sample_rate,
                },
            },
        }

        try:
            resp = requests.post(
                self.api_url,
                json=request_json,
                headers=self.header,
                stream=True,
                timeout=60,
            )

            if resp.status_code != 200:
                error_text = resp.text
                logger.bind(tag=TAG).error(
                    f"DoubaoTTS HTTP {resp.status_code}: {error_text}"
                )
                raise Exception(
                    f"{__name__} status_code: {resp.status_code} response: {error_text}"
                )

            audio_chunks = []
            for line in resp.iter_lines():
                if not line:
                    continue
                try:
                    result = json.loads(line)
                    code = result.get("code")
                    if code == 0:
                        data = result.get("data")
                        if data:
                            audio_chunks.append(base64.b64decode(data))
                    elif code == 20000000:
                        break
                    else:
                        msg = result.get("message", "")
                        logger.bind(tag=TAG).error(
                            f"DoubaoTTS error: code={code}, message={msg}"
                        )
                        raise Exception(
                            f"{__name__} error: code={code}, message={msg}"
                        )
                except json.JSONDecodeError:
                    continue

            if not audio_chunks:
                raise Exception(f"{__name__} error: No audio data received")

            audio_data = b"".join(audio_chunks)

            if output_file:
                with open(output_file, "wb") as f:
                    f.write(audio_data)
            else:
                return audio_data

        except Exception as e:
            logger.bind(tag=TAG).error(f"DoubaoTTS error: {e}")
            raise Exception(f"{__name__} error: {e}")
