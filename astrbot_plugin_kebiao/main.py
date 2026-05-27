import asyncio
import datetime
import json
import os
import re
import socket
import ssl

import aiohttp
from curl_cffi import requests as curl_requests

import astrbot.api.message_components as Comp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.abspath(
    os.path.join(PLUGIN_DIR, "..", "..", "config", "plugin_config.json")
)
PUSH_ORIGINS_PATH = os.path.abspath(
    os.path.join(PLUGIN_DIR, "..", "astrbot_plugin_schedule_push", "user_origins.json")
)
VERSION = "1.0.48"


def _load_config():
    """热加载配置文件，每次调用 Coze API 前刷新"""
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"【课表插件】读取配置文件失败: {e}")
        return {}


def _resolve_coze_base(config: dict) -> tuple:
    """(base_url, host_header) 根据配置决定是否 IP 直连"""
    coze_cfg = config.get("coze", {})
    if coze_cfg.get("ip_direct", True):
        ip = coze_cfg.get("ip_address", "113.57.56.233")
        host = coze_cfg.get("domain", "api.coze.cn")
        return f"https://{ip}", host
    domain = coze_cfg.get("domain", "api.coze.cn")
    return f"https://{domain}", None


@register("astrbot_plugin_kebiao", "YourName", "课表图片生成与入库插件", VERSION)
class KebiaoPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.processing_users = set()
        self._validate_config()
        logger.info(f"【课表插件】v{VERSION} 已加载，配置文件路径: {CONFIG_PATH}")

    def _validate_config(self):
        cfg = _load_config()
        checks = [
            ("coze.api_key", "Coze API Key"),
            ("kebiao_plugin.workflow_id", "课表图片工作流 ID"),
            ("kebiao_plugin.upload_workflow_id", "入库工作流 ID"),
            ("kebiao_plugin.workflow_id_get_next_class", "下节课工作流 ID"),
            ("schedule_push_plugin.workflow_id_get_next_class", "推送下节课工作流 ID"),
        ]
        for dotted, name in checks:
            val = cfg
            for k in dotted.split("."):
                val = val.get(k, None) if isinstance(val, dict) else None
                if val is None:
                    break
            if not val:
                logger.warning(f"【课表插件】配置缺失: {name} ({dotted})")

    def _register_user_for_push(self, event):
        """自动注册用户到推送插件"""
        uid = str(event.get_sender_id())
        origin = getattr(event, "unified_msg_origin", None)
        if not uid or not origin:
            return
        try:
            origins = {}
            if os.path.exists(PUSH_ORIGINS_PATH):
                with open(PUSH_ORIGINS_PATH, encoding="utf-8") as f:
                    origins = json.load(f)
            if uid not in origins:
                origins[uid] = origin
                with open(PUSH_ORIGINS_PATH, "w", encoding="utf-8") as f:
                    json.dump(origins, f, ensure_ascii=False, indent=2)
                logger.info(f"【课表插件】已自动注册用户 {uid} 到推送")
        except Exception as e:
            logger.error(f"【课表插件】注册推送失败: {e}")

    # ---- 配置读取 ----

    def _cfg(self, key: str, default=None):
        return _load_config().get(key, default)

    def _coze_cfg(self, key: str, default=None):
        return _load_config().get("coze", {}).get(key, default)

    def _plugin_cfg(self, key: str, default=None):
        return _load_config().get("kebiao_plugin", {}).get(key, default)

    @property
    def _semester_start(self):
        s = self._cfg("semester_start", "2026-03-02")
        return datetime.date.fromisoformat(s)

    # ---- SSL / 网络 ----

    @staticmethod
    def _ssl_ctx():
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _session(self):
        cfg = _load_config()
        base_url, host = _resolve_coze_base(cfg)
        connector = aiohttp.TCPConnector(
            ssl=self._ssl_ctx(),
            force_close=True,
            ttl_dns_cache=300,
            family=socket.AF_INET,
        )
        headers = {"Host": host} if host else {}
        return base_url, aiohttp.ClientSession(connector=connector, headers=headers)

    # ---- 工具 ----

    def _get_current_week(self):
        delta = datetime.date.today() - self._semester_start
        return max(1, delta.days // 7 + 1)

    def _find_current_period(self) -> int:
        """根据当前时间找到刚结束或正在进行的节次编号"""
        now = datetime.datetime.now()
        now_minutes = now.hour * 60 + now.minute
        period_map = self._plugin_cfg("period_to_time", {})
        current_period = 0
        for period_str, time_range in sorted(period_map.items(), key=lambda x: int(x[0])):
            parts = time_range.split("-")
            if len(parts) != 2:
                continue
            try:
                end_h, end_m = map(int, parts[1].split(":"))
                end = end_h * 60 + end_m
                if now_minutes > end:
                    current_period = int(period_str)
            except ValueError:
                continue
        return current_period

    async def _reply_next_class(self, user_id) -> list:
        weekday_names = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
        today = datetime.date.today()
        params = {
            "user_id": str(user_id),
            "weekday": weekday_names[today.weekday()],
            "weekday_num": str(today.weekday() + 1),
            "current_week": self._get_current_week(),
            "current_period": self._find_current_period(),
        }
        course, err = await self._call_next_class_workflow(params)
        if err:
            return [Comp.Plain(f"❌ 查询失败：{err}")]
        if not course:
            return [Comp.Plain("今天后面没有课啦，好好休息一下吧～")]
        start_p = int(course.get("start_period", 0))
        period_map = self._plugin_cfg("period_to_time", {})
        time_range = period_map.get(str(start_p)) or period_map.get(start_p, "时间未知")
        msg = (
            f"下一节课：{course.get('course_name', '未知课程')}\n"
            f"地点：{course.get('location', '未知')}\n"
            f"教师：{course.get('teachers', '未知')}\n"
            f"时间：{time_range}"
        )
        return [Comp.Plain(msg)]

    async def _call_next_class_workflow(self, parameters: dict) -> tuple:
        """返回 (course_dict | None, error_str | None)
        course=None, error=None  → 真的没课
        course=None, error="..." → 查询出错
        """
        cfg = _load_config()
        api_key = cfg.get("coze", {}).get("api_key", "")
        workflow_id = cfg.get("kebiao_plugin", {}).get("workflow_id_get_next_class", "")
        base_url, session = self._session()
        url = f"{base_url}/v1/workflow/run"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {"workflow_id": workflow_id, "parameters": parameters}
        try:
            async with session as s:
                async with s.post(url, headers=headers, json=payload, timeout=30) as resp:
                    data = await resp.json()
                    logger.info(f"[Coze 下节课响应] {data}")
            return self._parse_coze_course(data)
        except asyncio.TimeoutError:
            logger.error("[Coze 下节课] 请求超时")
            return None, "工作流请求超时，请稍后重试"
        except aiohttp.ClientError as e:
            logger.error(f"[Coze 下节课] 网络错误: {e}")
            return None, f"网络错误：{e}"
        except Exception as e:
            logger.error(f"[Coze 下节课异常] {e}")
            return None, f"系统异常：{e}"

    @staticmethod
    def _parse_coze_course(data: dict) -> tuple:
        """返回 (course_dict | None, error_str | None)"""
        if data.get("code") != 0:
            return None, f"Coze API 错误: {data.get('msg', '未知错误')}"
        raw = data.get("data")
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return None, "工作流返回数据格式异常"
        else:
            parsed = raw
        if not isinstance(parsed, dict):
            return None, "工作流返回数据格式异常"
        course = parsed.get("course")
        if course is None and parsed.get("output") is None:
            return None, None  # 真的没课
        if isinstance(course, dict):
            return course, None
        output = parsed.get("output")
        if isinstance(output, dict):
            return output, None
        if isinstance(output, str):
            try:
                nested = json.loads(output)
                if isinstance(nested, dict):
                    return nested, None
            except (json.JSONDecodeError, TypeError):
                pass
        return None, None  # 无课程数据视为没课

    @staticmethod
    def _parse_coze_response(data: dict) -> str | None:
        """从 Coze workflow run 响应中提取 output 字符串"""
        if data.get("code") != 0:
            return None
        raw = data.get("data")
        if isinstance(raw, str):
            try:
                inner = json.loads(raw)
            except json.JSONDecodeError:
                return None
        else:
            inner = raw
        if isinstance(inner, dict):
            return inner.get("output") or inner.get("data")
        return str(inner) if inner else None

    # ---- 全局：过滤 Coze 错误消息 ----

    @filter.on_decorating_result()
    async def _suppress_coze_errors(self, event: AstrMessageEvent):
        """只拦截本插件产生的 Coze 错误消息，不影响其他插件"""
        result = event.get_result()
        if result and result.chain:
            for comp in result.chain:
                if isinstance(comp, Comp.Plain) and "Coze 请求失败" in comp.text:
                    logger.info("【课表插件】已拦截 Coze 错误消息")
                    result.chain.clear()
                    event.stop_event()
                    return

    # ---- 自然语言课表请求拦截 ----

    @filter.platform_adapter_type(filter.PlatformAdapterType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        text = event.get_message_str().strip()
        if not text:
            return

        user_id = event.get_sender_id()
        self._register_user_for_push(event)

        # —— 下节课查询（"下节"二字覆盖所有变体） ——
        if "下节" in text:
            event.stop_event()
            yield event.chain_result(
                await self._reply_next_class(user_id)
            )
            return

        # —— 周次课表（自然语言 → 图片） ——
        week_number = None
        if re.search(r"本周|这周|这星期", text):
            week_number = self._get_current_week()
        elif re.search(r"下周|下星期", text):
            week_number = self._get_current_week() + 1
        elif re.search(r"上周|上星期", text):
            week_number = max(1, self._get_current_week() - 1)
        else:
            m = re.search(r"第\s*(\d+)\s*周", text)
            if m:
                week_number = int(m.group(1))

        if week_number is None:
            return

        event.stop_event()
        logger.info(f"【课表插件】自然语言: {text} → 第{week_number}周")

        try:
            image_url = await self._call_coze_workflow(str(week_number), user_id)
            if not image_url:
                yield event.plain_result("❌ 课表生成失败，请稍后重试。")
                return

            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: curl_requests.get(
                    image_url, impersonate="chrome120", timeout=15
                ),
            )
            if resp.status_code != 200:
                yield event.plain_result(f"❌ 图片下载失败，HTTP {resp.status_code}")
                return

            yield event.chain_result([
                Comp.Plain(f"📸 第{week_number}周课表"),
                Comp.Image.fromBytes(resp.content),
            ])
        except Exception as e:
            logger.error(f"【课表插件】自然语言发送图片失败: {e}")
            yield event.plain_result(f"❌ 发生错误：{str(e)}")

    # ---- /kebiao 指令 ----

    @filter.command("kebiao")
    async def get_kebiao(self, event: AstrMessageEvent, week: str = None):
        if not week:
            yield event.plain_result("请指定周次，例如 /kebiao 9")
            return

        user_id = event.get_sender_id()
        self._register_user_for_push(event)
        try:
            image_url = await self._call_coze_workflow(week, user_id)
            if not image_url:
                yield event.plain_result("❌ 课表生成失败：工作流未返回图片链接")
                return

            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: curl_requests.get(
                    image_url, impersonate="chrome120", timeout=15
                ),
            )
            if resp.status_code != 200:
                yield event.plain_result(f"❌ 图片下载失败，HTTP {resp.status_code}")
                return

            yield event.chain_result([
                Comp.Plain(f"📸 第{week}周课表"),
                Comp.Image.fromBytes(resp.content),
            ])
        except Exception as e:
            logger.error(f"【课表插件】图片下载失败: {e}")
            yield event.plain_result(f"❌ 图片生成失败：{str(e)}")

    async def _call_coze_workflow(self, week: str, user_id: str) -> str | None:
        cfg = _load_config()
        api_key = cfg.get("coze", {}).get("api_key", "")
        workflow_id = cfg.get("kebiao_plugin", {}).get("workflow_id", "")
        base_url, session = self._session()

        url = f"{base_url}/v1/workflow/run"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "workflow_id": workflow_id,
            "parameters": {"target_week": week, "user_id": user_id},
        }
        try:
            async with session as s:
                async with s.post(url, headers=headers, json=payload, timeout=30) as resp:
                    data = await resp.json()
                    logger.info(f"[Coze 图片工作流响应] {data}")
            output = self._parse_coze_response(data)
            if not output:
                raise Exception(f"Coze 工作流失败: {data.get('msg', '未知错误')}")
            return output
        except Exception as e:
            logger.error(f"[Coze 图片工作流异常] {e}")
            raise

    # ---- 文件监听与入库 ----

    @filter.command("", message_type="file")
    async def on_file_received(self, event: AstrMessageEvent):
        msg = event.message_obj
        if not hasattr(msg, "message"):
            return

        file_info = self._extract_file_info(msg)
        if not file_info:
            return

        user_id = event.get_sender_id()
        if user_id in self.processing_users:
            yield event.plain_result("⏳ 正在处理上一个文件，请稍候...")
            return

        self.processing_users.add(user_id)
        filename = file_info.get("file") or file_info.get("file_name") or "schedule.xlsx"

        yield event.plain_result("📎 文件已收到，正在解析并录入课表，请稍候...")
        try:
            result = await self._process_file(file_info, filename, user_id)
            yield event.plain_result(result)
        except Exception as e:
            logger.error(f"【课表插件】文件处理异常: {e}")
            yield event.plain_result(f"❌ 文件处理失败：{str(e)}")
        finally:
            self.processing_users.discard(user_id)

    @staticmethod
    def _extract_file_info(msg) -> dict | None:
        """从消息中提取文件信息，兼容多种消息格式"""
        if not hasattr(msg, "message"):
            return None
        for comp in msg.message:
            # 标准 file 组件
            if hasattr(comp, "type") and comp.type == "file":
                return getattr(comp, "data", {})
            # dict 格式含 file_id
            if hasattr(comp, "data") and isinstance(comp.data, dict) and "file_id" in comp.data:
                return comp.data
            # 通过类名判断
            if "File" in type(comp).__name__:
                data = getattr(comp, "data", None)
                if data:
                    return data
                url = getattr(comp, "url", None)
                if url:
                    return {"url": url, "file": getattr(comp, "name", "unknown")}
        return None

    async def _process_file(self, file_info: dict, filename: str, user_id: str) -> str:
        file_bytes = await self._download_file(file_info.get("url"))
        if not file_bytes:
            return "❌ 文件下载失败，请检查网络后重试。"

        if not self._is_valid_excel(file_bytes, filename):
            return "❌ 文件不是有效的 Excel 格式，请上传 .xlsx 或 .xls 文件。"

        file_id = await self._upload_to_coze(filename, file_bytes)
        if not file_id:
            return "❌ 文件上传到 Coze 失败，请稍后重试。"

        success, message = await self._call_upload_workflow(file_id, user_id)
        if success:
            count = "多"
            m = re.search(r"解析到\s*(\d+)\s*门", message or "")
            if m:
                count = m.group(1)
            return f"✅ 课表录入成功！共解析到 {count} 门课程。可使用 /kebiao 9 查看课表。"
        return f"❌ 课表录入失败：{message or '未知错误'}"

    @staticmethod
    def _is_valid_excel(data: bytes, filename: str) -> bool:
        name_lower = filename.lower()
        if name_lower.endswith(".xlsx"):
            return data.startswith(b"PK\x03\x04")
        if name_lower.endswith(".xls"):
            return data.startswith(b"\xd0\xcf\x11\xe0")
        # 未知扩展名，靠 Coze 服务端校验
        return True

    @staticmethod
    async def _download_file(url: str) -> bytes | None:
        if not url:
            return None
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        logger.info(f"【课表插件】文件下载成功 {len(data)} bytes")
                        return data
                    logger.error(f"【课表插件】下载失败 HTTP {resp.status}")
                    return None
        except Exception as e:
            logger.error(f"【课表插件】下载异常: {e}")
            return None

    async def _upload_to_coze(self, filename: str, file_bytes: bytes) -> str | None:
        cfg = _load_config()
        api_key = cfg.get("coze", {}).get("api_key", "")
        base_url, session = self._session()
        url = f"{base_url}/v1/files/upload"
        headers = {"Authorization": f"Bearer {api_key}"}
        form = aiohttp.FormData()
        form.add_field(
            "file",
            file_bytes,
            filename=filename,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        try:
            async with session as s:
                async with s.post(url, headers=headers, data=form, timeout=60) as resp:
                    data = await resp.json()
                    logger.info(f"[Coze 文件上传] {data}")
                    if data.get("code") == 0:
                        return data.get("data", {}).get("id")
                    logger.error(f"[Coze 文件上传失败] {data.get('msg')}")
                    return None
        except Exception as e:
            logger.error(f"[Coze 文件上传异常] {e}")
            return None

    async def _call_upload_workflow(self, file_id: str, user_id: str) -> tuple:
        cfg = _load_config()
        api_key = cfg.get("coze", {}).get("api_key", "")
        workflow_id = cfg.get("kebiao_plugin", {}).get("upload_workflow_id", "")
        base_url, session = self._session()
        url = f"{base_url}/v1/workflow/run"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "workflow_id": workflow_id,
            "parameters": {"input": {"file_id": file_id}, "user_id": user_id},
        }
        try:
            async with session as s:
                async with s.post(url, headers=headers, json=payload, timeout=120) as resp:
                    data = await resp.json()
                    logger.info(f"[Coze 入库响应] {data}")
            output = self._parse_coze_response(data)
            return (True, output or "录入完成") if output is not None else (False, data.get("msg"))
        except asyncio.TimeoutError:
            return False, "请求超时，请稍后重试"
        except aiohttp.ClientError as e:
            return False, f"网络错误：{e}"
        except Exception as e:
            return False, f"未知异常：{e}"

    # ---- 辅助指令 ----

    @filter.command("upload_kebiao")
    async def upload_kebiao(self, event: AstrMessageEvent):
        yield event.plain_result("直接发送课表 Excel 文件给我即可，无需使用此指令。")
