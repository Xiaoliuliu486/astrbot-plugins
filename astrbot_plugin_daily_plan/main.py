import asyncio
import datetime
import json
import os
import ssl
import traceback

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import astrbot.api.message_components as Comp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.abspath(
    os.path.join(PLUGIN_DIR, "..", "..", "config", "plugin_config.json")
)
ORIGINS_FILE = os.path.abspath(
    os.path.join(
        PLUGIN_DIR, "..", "astrbot_plugin_schedule_push", "user_origins.json"
    )
)
VERSION = "1.0.0"


def _load_config():
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"【日常计划插件】读取配置文件失败: {e}")
        return {}


def _resolve_coze_base(config: dict) -> tuple:
    coze_cfg = config.get("coze", {})
    if coze_cfg.get("ip_direct", True):
        ip = coze_cfg.get("ip_address", "113.57.56.233")
        host = coze_cfg.get("domain", "api.coze.cn")
        return f"https://{ip}", host
    domain = coze_cfg.get("domain", "api.coze.cn")
    return f"https://{domain}", None


@register(
    "astrbot_plugin_daily_plan",
    "YourName",
    "日常计划推送插件（08:00今日计划/20:00明日提醒）",
    VERSION,
)
class DailyPlanPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self._dedup_minute = None
        self.scheduler = AsyncIOScheduler()

        cfg = _load_config()
        push_cfg = cfg.get("schedule_push_plugin", {})
        daily_push = push_cfg.get("daily_push", {})

        for time_str, push_type in daily_push.items():
            hour, minute = map(int, time_str.split(":"))
            self.scheduler.add_job(
                self._handle_daily_push,
                CronTrigger(hour=hour, minute=minute),
                args=[push_type],
                id=f"daily_{push_type}",
                replace_existing=True,
                misfire_grace_time=60,
            )

        self.scheduler.start()

        wf_today = push_cfg.get("workflow_id_get_today_plan", "")
        wf_reminder = push_cfg.get("workflow_id_get_reminders", "")
        wf_status = []
        if wf_today:
            wf_status.append("今日计划")
        if wf_reminder:
            wf_status.append("明日提醒")
        logger.info(
            f"【日常计划插件】v{VERSION} 已启动，{len(daily_push)} 个定时推送，"
            f"已配置: {', '.join(wf_status) if wf_status else '无工作流配置'}"
        )

    async def terminate(self):
        if self.scheduler:
            self.scheduler.shutdown(wait=False)
            logger.info("【日常计划插件】调度器已关闭")

    # ---- SSL ----

    @staticmethod
    def _ssl_connector():
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return aiohttp.TCPConnector(ssl=ctx)

    # ---- 用户列表 ----

    def _load_user_origins(self) -> dict:
        try:
            if os.path.exists(ORIGINS_FILE):
                with open(ORIGINS_FILE, encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"【日常计划插件】加载用户文件失败: {e}")
        return {}

    # ---- Coze API ----

    async def _call_workflow(self, workflow_id: str, parameters: dict) -> str | None:
        if not workflow_id:
            return None
        cfg = _load_config()
        api_key = cfg.get("coze", {}).get("push_api_key", "")
        base_url, host = _resolve_coze_base(cfg)
        url = f"{base_url}/v1/workflow/run"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        if host:
            headers["Host"] = host
        payload = {"workflow_id": workflow_id, "parameters": parameters}

        try:
            async with aiohttp.ClientSession(
                connector=self._ssl_connector()
            ) as session:
                async with session.post(
                    url, headers=headers, json=payload, timeout=30
                ) as resp:
                    raw_text = await resp.text()
                    data = json.loads(raw_text)
                    logger.info(f"【日常计划插件】Coze 响应: {data}")

            if data.get("code") != 0:
                logger.error(f"【日常计划插件】Coze 错误: {data.get('msg')}")
                return None

            raw = data.get("data")
            if isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    return raw if raw else None
            else:
                parsed = raw
            if not isinstance(parsed, dict):
                return str(parsed) if parsed else None

            return parsed.get("formatted_text") or parsed.get("output") or None
        except asyncio.TimeoutError:
            logger.error("【日常计划插件】Coze 请求超时")
            return None
        except Exception as e:
            logger.error(
                f"【日常计划插件】Coze 异常: {e}\n{traceback.format_exc()}"
            )
            return None

    @property
    def _semester_start(self):
        s = _load_config().get("semester_start", "2026-03-02")
        return datetime.date.fromisoformat(s)

    # ---- 定时推送 ----

    async def _handle_daily_push(self, push_type: str):
        now = datetime.datetime.now()
        minute_key = now.strftime("%Y%m%d%H%M")
        if self._dedup_minute == minute_key:
            return
        self._dedup_minute = minute_key

        cfg = _load_config()
        push_cfg = cfg.get("schedule_push_plugin", {})

        key_map = {
            "today_plan": "workflow_id_get_today_plan",
            "reminder": "workflow_id_get_reminders",
        }
        config_key = key_map.get(push_type, "")
        workflow_id = push_cfg.get(config_key, "")

        if not workflow_id:
            logger.warning(f"【日常计划插件】{push_type} 工作流未配置（{config_key}），跳过")
            return

        user_origins = self._load_user_origins()
        if not user_origins:
            logger.info("【日常计划插件】无已注册用户，跳过推送")
            return

        today = datetime.date.today()
        weekday_names = [
            "星期一",
            "星期二",
            "星期三",
            "星期四",
            "星期五",
            "星期六",
            "星期日",
        ]
        weekday_name = weekday_names[today.weekday()]
        weekday_num = str(today.weekday() + 1)
        current_week = max(1, (today - self._semester_start).days // 7 + 1)

        for uid, origin in list(user_origins.items()):
            try:
                params = {
                    "user_id": str(uid),
                    "weekday_name": weekday_name,
                    "weekday_num": weekday_num,
                    "current_week": current_week,
                }
                formatted_text = await self._call_workflow(workflow_id, params)
                if formatted_text:
                    await self._send(origin, formatted_text)
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"【日常计划插件】推送失败 {uid}: {e}")

    async def _send(self, origin: str, text: str):
        try:
            from astrbot.api.event import MessageChain

            await self.context.send_message(
                origin, MessageChain(chain=[Comp.Plain(text)])
            )
        except ImportError:
            await self.context.send_message(origin, Comp.Plain(text))
        except Exception:
            try:
                await self.context.send_message(origin, Comp.Plain(text))
            except Exception as e:
                logger.error(f"【日常计划插件】发送失败: {e}")

    # ---- 指令 ----

    @filter.command("test_plan")
    async def test_plan(self, event: AstrMessageEvent):
        """手动测试今日计划推送"""
        yield event.plain_result("📋 正在测试今日计划推送...")
        uid = str(event.get_sender_id())

        cfg = _load_config()
        push_cfg = cfg.get("schedule_push_plugin", {})
        workflow_id = push_cfg.get("workflow_id_get_today_plan", "")
        if not workflow_id:
            yield event.plain_result("❌ 请先在 plugin_config.json 中配置 workflow_id_get_today_plan")
            return

        try:
            today = datetime.date.today()
            weekday_names = [
                "星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日",
            ]
            weekday_name = weekday_names[today.weekday()]
            weekday_num = str(today.weekday() + 1)
            current_week = max(1, (today - self._semester_start).days // 7 + 1)
            params = {
                "user_id": uid,
                "weekday_name": weekday_name,
                "weekday_num": weekday_num,
                "current_week": current_week,
            }
            text = await self._call_workflow(workflow_id, params)
            if text:
                yield event.chain_result([Comp.Plain(f"📅 今日计划：\n\n{text}")])
            else:
                yield event.plain_result("❌ 工作流返回为空，请检查 Coze 工作流配置")
        except Exception as e:
            yield event.plain_result(f"❌ 测试失败：{str(e)}")

    @filter.command("test_reminder")
    async def test_reminder(self, event: AstrMessageEvent):
        """手动测试明日提醒推送"""
        yield event.plain_result("⏰ 正在测试明日提醒推送...")
        uid = str(event.get_sender_id())

        cfg = _load_config()
        push_cfg = cfg.get("schedule_push_plugin", {})
        workflow_id = push_cfg.get("workflow_id_get_reminders", "")
        if not workflow_id:
            yield event.plain_result("❌ 请先在 plugin_config.json 中配置 workflow_id_get_reminders")
            return

        try:
            params = {"user_id": uid}
            text = await self._call_workflow(workflow_id, params)
            if text:
                yield event.chain_result([Comp.Plain(f"⏰ 明日提醒：\n\n{text}")])
            else:
                yield event.plain_result("❌ 工作流返回为空，请检查 Coze 工作流配置")
        except Exception as e:
            yield event.plain_result(f"❌ 测试失败：{str(e)}")

    @filter.command("reg_plan")
    async def reg_plan(self, event: AstrMessageEvent):
        """注册日常计划推送"""
        uid = str(event.get_sender_id())
        origin = getattr(event, "unified_msg_origin", None)
        if not origin:
            yield event.plain_result("❌ 无法获取消息来源，注册失败")
            return

        try:
            origins = {}
            if os.path.exists(ORIGINS_FILE):
                with open(ORIGINS_FILE, encoding="utf-8") as f:
                    origins = json.load(f)
            if uid not in origins:
                origins[uid] = origin
                with open(ORIGINS_FILE, "w", encoding="utf-8") as f:
                    json.dump(origins, f, ensure_ascii=False, indent=2)
                yield event.plain_result(
                    "✅ 已注册日常计划推送（08:00 今日计划 + 20:00 明日提醒）"
                )
            else:
                yield event.plain_result("ℹ️ 你已注册推送服务，无需重复注册")
        except Exception as e:
            yield event.plain_result(f"❌ 注册失败：{str(e)}")
