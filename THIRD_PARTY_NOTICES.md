# 第三方代码与依赖声明

本项目包含或引用了以下第三方开源项目。感谢各项目的贡献。

## NekroAgent

- **项目**：https://github.com/KroMiose/nekro-agent
- **作者**：KroMiose
- **许可证**：Nekro Agent 开源协议 V1.1（基于 Apache License 2.0 修改），完整协议见本仓库根目录 [`NEKRO_LICENSE`](./NEKRO_LICENSE)

以下文件是基于 NekroAgent 源码修改而来的派生作品：

| 文件 | 原始文件 | 修改内容 |
|---|---|---|
| `patches/adapter.py` | `nekro_agent/adapters/onebot_v11/adapter.py` | 增加 QQ 语音回复、LLM 情感标注、语音开关 |
| `patches/commands.py` | `nekro_agent/adapters/sse/commands.py` | 修复 SSE 适配器 `channel_type` 被硬编码为 group 的问题 |

`deploy/docker-compose.yml` 亦参考了 NekroAgent 官方部署编排。

本项目整体遵循 Nekro Agent 开源协议 V1.1 的「派生作品规范」，保留「基于 NekroAgent 构建」标识。

## NapCat

- **项目**：https://github.com/NapNeko/NapCat-Docker
- **作者**：NapNeko
- **用途**：本项目通过 Docker 镜像 `mlikiowa/napcat-docker` 引用 NapCat 作为 QQ 协议端，未修改其源码。

## xiaozhi-server（小智）

- **项目**：https://github.com/xinnan-tech/xiaozhi-esp32-server
- **许可证**：MIT
- **用途**：`tts/doubao_v3.py` 是基于 xiaozhi-server 的 TTS provider 机制（`core/providers/tts/base.py`）编写的豆包（火山引擎）语音合成 provider，需放入 xiaozhi-server 使用。

## 其他

- 豆包语音合成 API 由火山引擎（Volcano Engine）提供，为外部服务。
- QQ 头像通过腾讯 qlogo 公开接口实时获取。
