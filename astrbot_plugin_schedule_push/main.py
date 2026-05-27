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

try:
    from astrbot.api.event import MessageChain
except ImportError:
    MessageChain = None  # 兼容旧版本 AstrBot

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.abspath(
    os.path.join(PLUGIN_DIR, "..", "..", "config", "plugin_config.json")
)
ORIGINS_FILE = os.path.join(PLUGIN_DIR, "user_origins.json")
VERSION = "1.1.0"


def _load_config():
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"【推送插件】读取配置文件失败: {e}")
        return {}


def _resolve_coze_base(config: dict) -> tuple:
    coze_cfg = config.get("coze", {})
    if coze_cfg.get("ip_direct", True):
        ip = coze_cfg.get("ip_address", "113.57.56.233")
        host = coze_cfg.get("domain", "api.coze.cn")
        return f"https://{ip}", host
    domain = coze_cfg.get("domain", "api.coze.cn")
    return f"https://{domain}", None


@register("astrbot_plugin_schedule_push", "YourName", "课表定时推送插件", VERSION)
class SchedulePushPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self._push_lock = asyncio.Lock()
        self._origins_lock = asyncio.Lock()
        self._last_push_minute = None
        self.scheduler = AsyncIOScheduler()

        cfg = _load_config()
        push_cfg = cfg.get("schedule_push_plugin", {})
        push_times = push_cfg.get("push_times", {})

        for time_str in push_times:
            hour, minute = map(int, time_str.split(":"))
            self.scheduler.add_job(
                self._push_next_class,
                CronTrigger(hour=hour, minute=minute),
                id=f"push_{time_str}",
                replace_existing=True,
                misfire_grace_time=30,  # 允许 30s 延迟
            )

        self.scheduler.start()
        logger.info(
            f"【推送插件】v{VERSION} 已启动，{len(push_times)} 个定时任务，配置: {CONFIG_PATH}"
        )

        # 异步加载用户 origins
        asyncio.create_task(self._load_origins())

    async def terminate(self):
        if self.scheduler:
            self.scheduler.shutdown(wait=False)
            logger.info("【推送插件】调度器已关闭")

    # ---- SSL ----

    @staticmethod
    def _ssl_connector():
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return aiohttp.TCPConnector(ssl=ctx)

    # ---- 用户 origins 持久化 ----

    async def _load_origins(self):
        try:
            if os.path.exists(ORIGINS_FILE):
                async with self._origins_lock:
                    with open(ORIGINS_FILE, encoding="utf-8") as f:
                        data = json.load(f)
                    # 写入到实例变量需要迁移
                    self._user_origins = data
                    logger.info(f"【推送插件】加载 {len(data)} 个用户 origins")
                    return
        except Exception as e:
            logger.error(f"【推送插件】加载用户文件失败: {e}")
        self._user_origins = {}

    async def _save_origins(self):
        try:
            async with self._origins_lock:
                with open(ORIGINS_FILE, "w", encoding="utf-8") as f:
                    json.dump(self._user_origins, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"【推送插件】保存用户文件失败: {e}")

    # ---- Coze API ----

    async def call_workflow(self, workflow_id: str, parameters: dict) -> dict | None:
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
                async with session.post(url, headers=headers, json=payload, timeout=30) as resp:
                    raw_text = await resp.text()
                    data = json.loads(raw_text)
                    logger.info(f"【推送插件】Coze 响应: {data}")
            return self._extract_course(data)
        except asyncio.TimeoutError:
            logger.error("【推送插件】Coze 请求超时")
            return None
        except Exception as e:
            logger.error(f"【推送插件】Coze 异常: {e}\n{traceback.format_exc()}")
            return None

    @staticmethod
    def _extract_course(data: dict) -> dict | None:
        """从 Coze 工作流响应中提取课程对象，兼容多种返回格式"""
        if data.get("code") != 0:
            return None
        raw = data.get("data")
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return None
        else:
            parsed = raw
        if not isinstance(parsed, dict):
            return None

        # 查找课程对象：course / output(对象) / output(JSON字符串)
        for key in ("course",):
            val = parsed.get(key)
            if isinstance(val, dict):
                return val

        output = parsed.get("output")
        if not output:
            return None
        if isinstance(output, dict):
            return output
        if isinstance(output, str):
            try:
                nested = json.loads(output)
                return nested if isinstance(nested, dict) else None
            except (json.JSONDecodeError, TypeError):
                return None
        return None

    # ---- 定时推送 ----

    async def _push_next_class(self):
        """定时推送：查询每个用户下一节课并发送"""
        now = datetime.datetime.now()
        minute_key = now.strftime("%Y%m%d%H%M")

        if self._last_push_minute == minute_key:
            return
        self._last_push_minute = minute_key

        async with self._push_lock:
            cfg = _load_config()
            push_cfg = cfg.get("schedule_push_plugin", {})
            push_times = push_cfg.get("push_times", {})
            period_map = push_cfg.get("period_to_time", {})
            weekday_names = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]

            # 匹配当前时间对应的节次
            current_period = None
            now_minutes = now.hour * 60 + now.minute
            for time_str, period_num in push_times.items():
                h, m = map(int, time_str.split(":"))
                if abs(now_minutes - (h * 60 + m)) <= 1:
                    current_period = period_num
                    break
            if current_period is None:
                return

            today = datetime.date.today()
            weekday_name = weekday_names[today.weekday()]
            weekday_num = str(today.weekday() + 1)
            current_week = max(1, (today - self._semester_start).days // 7 + 1)

            sem_start = self._semester_start

            user_origins = getattr(self, "_user_origins", {})
            if not user_origins:
                return

            for uid, origin in list(user_origins.items()):
                try:
                    params = {
                        "user_id": str(uid),
                        "weekday": weekday_name,
                        "weekday_num": weekday_num,
                        "current_week": current_week,
                        "current_period": current_period,
                    }
                    result = await self.call_workflow(
                        push_cfg.get("workflow_id_get_next_class", ""), params
                    )
                    if result:
                        start_p = int(result.get("start_period", 0))
                        time_range = period_map.get(str(start_p)) or period_map.get(start_p, "时间未知")
                        msg = (
                            f"下一节课是 {result.get('course_name', '未知课程')}\n"
                            f"地点：{result.get('location', '未知')}\n"
                            f"教师：{result.get('teachers', '未知')}\n"
                            f"时间：{time_range}\n"
                            f"记得准时哦～"
                        )
                    else:
                        msg = "今天后面没有课啦，好好休息一下吧～"

                    await self._send(origin, msg)
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"【推送插件】推送失败 {uid}: {e}")

    @property
    def _semester_start(self):
        s = _load_config().get("semester_start", "2026-03-02")
        return datetime.date.fromisoformat(s)

    async def _send(self, origin: str, text: str):
        """发送消息，兼容有无 MessageChain"""
        if MessageChain is not None:
            await self.context.send_message(origin, MessageChain(chain=[Comp.Plain(text)]))
        else:
            await self.context.send_message(origin, Comp.Plain(text))

    # ---- 指令 ----

    @filter.command("test_push")
    async def test_push(self, event: AstrMessageEvent):
        uid = str(event.get_sender_id())
        if not hasattr(self, "_user_origins"):
            self._user_origins = {}
        self._user_origins[uid] = event.unified_msg_origin
        await self._save_origins()
        yield event.plain_result(f"已注册推送(v{VERSION})，开始测试...")
        await self._push_next_class()
        yield event.plain_result("测试完成，请检查消息")

    @filter.command("astr_schedule_test")
    async def test_schedule(self, event: AstrMessageEvent):
        uid = str(event.get_sender_id())
        if not hasattr(self, "_user_origins"):
            self._user_origins = {}
        self._user_origins[uid] = event.unified_msg_origin
        await self._save_origins()

        test_date = datetime.date(2026, 4, 28)
        sem_start = self._semester_start
        week = max(1, (test_date - sem_start).days // 7 + 1)
        params = {
            "user_id": uid,
            "weekday": "星期二",
            "weekday_num": "2",
            "current_week": week,
            "current_period": 1,
        }
        push_cfg = _load_config().get("schedule_push_plugin", {})
        yield event.plain_result(f"测试 4月28日 周二第1节后(v{VERSION})...")
        result = await self.call_workflow(
            push_cfg.get("workflow_id_get_next_class", ""), params
        )
        if result:
            start_p = int(result.get("start_period", 0))
            period_map = push_cfg.get("period_to_time", {})
            time_range = period_map.get(str(start_p)) or period_map.get(start_p, "时间未知")
            msg = (
                f"4月28日周二 08:45 后的课程：\n"
                f"课程：{result.get('course_name', '未知')}\n"
                f"时间：{time_range}\n"
                f"地点：{result.get('location', '未知')}\n"
                f"教师：{result.get('teachers', '未知')}"
            )
            yield event.plain_result(msg)
        else:
            yield event.plain_result("4月28日周二第一节课后没有更多课程了")
        yield event.plain_result("测试完成")

    @filter.command("unreg_push")
    async def unreg_push(self, event: AstrMessageEvent):
        """取消推送注册"""
        uid = str(event.get_sender_id())
        if hasattr(self, "_user_origins") and uid in self._user_origins:
            del self._user_origins[uid]
            await self._save_origins()
            yield event.plain_result("已取消推送注册")
        else:
            yield event.plain_result("你尚未注册推送")
