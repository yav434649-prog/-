import asyncio
import datetime
import json
import math
import random
import re
import zoneinfo
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import aiohttp

from astrbot.api import logger
from astrbot.api.all import Context, Star
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.core.config.astrbot_config import AstrBotConfig
from astrbot.core.provider.entities import ProviderRequest
from astrbot.core.star.star_tools import StarTools


def _time_desc(h: int | None = None) -> str:
    h = (h or datetime.datetime.now().hour) % 24
    return (
        "深夜"
        if h < 6
        else "清晨"
        if h < 9
        else "上午"
        if h < 12
        else "中午"
        if h < 14
        else "下午"
        if h < 18
        else "晚上"
        if h < 22
        else "深夜"
    )


def _normalize_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(i).strip() for i in value if str(i).strip()]
    if isinstance(value, str):
        parts = [p.strip() for p in value.splitlines()]
        return [p for p in parts if p]
    return []


_TIME_WINDOW_RE = re.compile(r"^\s*(\d{1,2}):(\d{1,2})\s*-\s*(\d{1,2}):(\d{1,2})\s*$")


def _parse_time_windows(items: list[str]) -> list[tuple[str, datetime.time, datetime.time]]:
    out: list[tuple[str, datetime.time, datetime.time]] = []
    for raw in items:
        m = _TIME_WINDOW_RE.match(raw or "")
        if not m:
            continue
        sh, sm, eh, em = map(int, m.groups())
        if not (0 <= sh <= 23 and 0 <= eh <= 23 and 0 <= sm <= 59 and 0 <= em <= 59):
            continue
        start = datetime.time(sh, sm)
        end = datetime.time(eh, em)
        out.append((raw.strip(), start, end))
    return out


def _time_in_window(now: datetime.time, start: datetime.time, end: datetime.time) -> bool:
    if start <= end:
        return start <= now < end
    return now >= start or now < end


def _haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1 = a
    lat2, lon2 = b
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lam = math.radians(lon2 - lon1)
    s = math.sin(d_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lam / 2.0) ** 2
    return 2.0 * r * math.asin(math.sqrt(s))


@dataclass(slots=True)
class Place:
    name: str
    address: str
    city: str
    district: str
    adcode: str
    location: tuple[float, float] | None


@dataclass(slots=True)
class RouteInfo:
    mode: str
    duration_min: int | None
    distance_m: int | None
    ok: bool
    reason: str = ""


@dataclass(slots=True)
class SlotDraft:
    time_window: str
    title: str
    poi_query: str
    poi_query_alt: list[str]


@dataclass(slots=True)
class ScheduleItem:
    time_window: str
    title: str
    poi_query_raw: str
    poi_name: str
    poi_district: str
    poi_city: str
    poi_address: str
    lat: float | None
    lng: float | None
    travel_mode_from_prev: str | None = None
    travel_minutes_from_prev: int | None = None
    travel_distance_m_from_prev: int | None = None
    travel_note: str = ""


@dataclass(slots=True)
class ScheduleData:
    date: str
    outfit_style: str = ""
    outfit: str = ""
    schedule_text: str = ""
    items: list[ScheduleItem] = field(default_factory=list)
    weather_summary: str = ""
    status: str = "ok"
    warnings: list[str] = field(default_factory=list)


class ScheduleDataManager:
    def __init__(self, json_path: Path):
        self._path = json_path
        self._data: dict[str, ScheduleData] = {}
        self.load()

    @staticmethod
    def _to_date_str(value: datetime.date | datetime.datetime) -> str:
        if isinstance(value, datetime.datetime):
            return value.date().isoformat()
        return value.isoformat()

    def get(self, date: datetime.date | datetime.datetime) -> ScheduleData | None:
        return self._data.get(self._to_date_str(date))

    def set(self, data: ScheduleData) -> None:
        self._data[data.date] = data
        self.save()

    def load(self) -> None:
        if not self._path.exists():
            self._data.clear()
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            self._data.clear()
            return
        data: dict[str, ScheduleData] = {}
        if isinstance(raw, dict):
            for date_str, item in raw.items():
                if not isinstance(item, dict):
                    continue
                try:
                    items = []
                    for it in item.get("items") or []:
                        if not isinstance(it, dict):
                            continue
                        items.append(ScheduleItem(**it))
                    data[date_str] = ScheduleData(
                        date=item.get("date") or date_str,
                        outfit_style=item.get("outfit_style", ""),
                        outfit=item.get("outfit", ""),
                        schedule_text=item.get("schedule_text", ""),
                        items=items,
                        weather_summary=item.get("weather_summary", ""),
                        status=item.get("status", "ok"),
                        warnings=item.get("warnings") or [],
                    )
                except (TypeError, ValueError, KeyError):
                    continue
        self._data = data

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_suffix(".tmp")
        payload: dict[str, Any] = {}
        for k, v in self._data.items():
            payload[k] = {
                "date": v.date,
                "outfit_style": v.outfit_style,
                "outfit": v.outfit,
                "schedule_text": v.schedule_text,
                "items": [asdict(i) for i in (v.items or [])],
                "weather_summary": v.weather_summary,
                "status": v.status,
                "warnings": v.warnings or [],
            }
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self._path)


class AmapClient:
    def __init__(self, api_key: str):
        self.api_key = api_key.strip()

    @staticmethod
    def _dig(obj: Any, path: list[Any]) -> Any:
        cur = obj
        for key in path:
            if isinstance(key, int):
                if not isinstance(cur, list) or key < 0 or key >= len(cur):
                    return None
                cur = cur[key]
                continue
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur

    @staticmethod
    def _to_int_maybe(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            return None

    @classmethod
    def _parse_duration_distance(cls, node: dict[str, Any]) -> tuple[int | None, int | None]:
        duration_s = cls._to_int_maybe(node.get("duration"))
        distance_m = cls._to_int_maybe(node.get("distance"))
        duration_min = int(round(duration_s / 60.0)) if duration_s is not None else None
        return duration_min, distance_m

    async def _get_json(self, url: str, params: dict[str, Any]) -> dict[str, Any] | None:
        if not self.api_key:
            return None
        params = dict(params)
        params["key"] = self.api_key
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=15) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json(content_type=None)
                    return data if isinstance(data, dict) else None
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError):
            return None

    async def get_weather_summary(self, city: str) -> str:
        data = await self._get_json(
            "https://restapi.amap.com/v3/weather/weatherInfo",
            {"city": city, "extensions": "base"},
        )
        if not data:
            return ""
        lives = data.get("lives")
        if not isinstance(lives, list) or not lives:
            return ""
        now = lives[0] if isinstance(lives[0], dict) else {}
        weather = str(now.get("weather", "")).strip()
        temp = str(now.get("temperature", "")).strip()
        humidity = str(now.get("humidity", "")).strip()
        wind = str(now.get("windpower", "")).strip()
        parts = []
        if weather:
            parts.append(weather)
        if temp:
            parts.append(f"{temp}℃")
        if humidity:
            parts.append(f"湿度{humidity}%")
        if wind:
            parts.append(f"风力{wind}")
        return "，".join(parts)

    async def search_place(self, keywords: str, city: str) -> Place | None:
        kw = (keywords or "").strip()
        if not kw:
            return None
        data = await self._get_json(
            "https://restapi.amap.com/v3/place/text",
            {
                "keywords": kw,
                "city": city,
                "citylimit": "true",
                "offset": 10,
                "page": 1,
                "extensions": "base",
            },
        )
        pois = (data or {}).get("pois")
        if not isinstance(pois, list) or not pois:
            tips = await self._get_json(
                "https://restapi.amap.com/v3/assistant/inputtips",
                {"keywords": kw, "city": city, "datatype": "all"},
            )
            tip_list = (tips or {}).get("tips")
            if not isinstance(tip_list, list) or not tip_list:
                return None
            for tip in tip_list:
                if not isinstance(tip, dict):
                    continue
                loc = str(tip.get("location") or "").strip()
                if not loc or "," not in loc:
                    continue
                lng_s, lat_s = loc.split(",", 1)
                try:
                    lng = float(lng_s)
                    lat = float(lat_s)
                except (TypeError, ValueError):
                    continue
                return Place(
                    name=str(tip.get("name") or kw),
                    address=str(tip.get("address") or ""),
                    city=str(tip.get("city") or city),
                    district=str(tip.get("district") or ""),
                    adcode=str(tip.get("adcode") or ""),
                    location=(lat, lng),
                )
            return None
        poi = pois[0] if isinstance(pois[0], dict) else {}
        name = str(poi.get("name") or kw)
        address = str(poi.get("address") or "")
        cityname = str(poi.get("cityname") or city)
        adname = str(poi.get("adname") or "")
        adcode = str(poi.get("adcode") or "")
        loc = str(poi.get("location") or "").strip()
        location = None
        if loc and "," in loc:
            lng_s, lat_s = loc.split(",", 1)
            try:
                lng = float(lng_s)
                lat = float(lat_s)
                location = (lat, lng)
            except (TypeError, ValueError):
                location = None
        return Place(
            name=name,
            address=address,
            city=cityname,
            district=adname,
            adcode=adcode,
            location=location,
        )

    async def get_route(
        self,
        origin: tuple[float, float],
        destination: tuple[float, float],
        mode: str,
        city: str,
    ) -> RouteInfo:
        lat_o, lng_o = origin
        lat_d, lng_d = destination
        origin_s = f"{lng_o},{lat_o}"
        dest_s = f"{lng_d},{lat_d}"
        mode = (mode or "").strip().lower()

        url = ""
        params: dict[str, Any] = {}
        node_path: list[Any] = []

        if mode in ("taxi", "drive"):
            url = "https://restapi.amap.com/v3/direction/driving"
            params = {"origin": origin_s, "destination": dest_s, "strategy": 0}
            node_path = ["route", "paths", 0]
        elif mode == "walk":
            url = "https://restapi.amap.com/v3/direction/walking"
            params = {"origin": origin_s, "destination": dest_s}
            node_path = ["route", "paths", 0]
        elif mode == "transit":
            url = "https://restapi.amap.com/v3/direction/transit/integrated"
            params = {"origin": origin_s, "destination": dest_s, "city": city}
            node_path = ["route", "transits", 0]
        elif mode == "bike":
            url = "https://restapi.amap.com/v4/direction/bicycling"
            params = {"origin": origin_s, "destination": dest_s}
            node_path = ["data", "paths", 0]
        else:
            return RouteInfo(mode=mode, duration_min=None, distance_m=None, ok=False, reason="unsupported_mode")

        data = await self._get_json(url, params)
        node = self._dig(data, node_path)
        if isinstance(node, dict):
            duration_min, distance_m = self._parse_duration_distance(node)
            return RouteInfo(mode=mode, duration_min=duration_min, distance_m=distance_m, ok=True)
        return RouteInfo(mode=mode, duration_min=None, distance_m=None, ok=False, reason="route_not_found")


class LifeScheduler:
    def __init__(self, context: Context, config: AstrBotConfig, task):
        from apscheduler.executors.asyncio import AsyncIOExecutor
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        tz = context.get_config().get("timezone")
        self.timezone = zoneinfo.ZoneInfo(tz) if tz else zoneinfo.ZoneInfo("Asia/Shanghai")
        self.config = config
        self.task = task
        self.scheduler = AsyncIOScheduler(
            timezone=self.timezone,
            executors={"default": AsyncIOExecutor()},
            job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 120},
        )
        self.job = None

    def _get(self, key: str, default: Any = None) -> Any:
        vs = self.config.get("virtual_schedule")
        if isinstance(vs, dict) and key in vs:
            return vs.get(key, default)
        return self.config.get(key, default)

    def _set(self, key: str, value: Any) -> None:
        vs = self.config.get("virtual_schedule")
        if not isinstance(vs, dict):
            vs = {}
        vs[key] = value
        self.config["virtual_schedule"] = vs

    def start(self):
        schedule_time = str(self._get("schedule_time", "07:00"))
        hour, minute = map(int, schedule_time.split(":"))
        self.job = self.scheduler.add_job(self.task, "cron", hour=hour, minute=minute, id="silicon_life_daily")
        self.scheduler.start()
        logger.info(f"硅基生命调度器已启动，时间：{schedule_time}")

    def stop(self):
        if self.scheduler.running:
            self.scheduler.shutdown()

    def update_schedule_time(self, new_time: str):
        if new_time == self._get("schedule_time"):
            return
        hour, minute = map(int, new_time.split(":"))
        self._set("schedule_time", new_time)
        self.config.save_config()
        if self.job:
            self.job.reschedule("cron", hour=hour, minute=minute)
        logger.info(f"硅基生命调度器已重新排程至 {new_time}")


class SiliconLifeGenerator:
    _STYLE_ENFORCE_RETRIES = 2
    _EMPTY_COMPLETION_RETRIES = 1

    def __init__(self, context: Context, config: AstrBotConfig, data_mgr: ScheduleDataManager):
        self.context = context
        self.config = config
        self.data_mgr = data_mgr
        self._gen_lock = asyncio.Lock()
        self._generating = False

    def _cfg(self, key: str, default: Any = None) -> Any:
        vs = self.config.get("virtual_schedule")
        if isinstance(vs, dict) and key in vs:
            return vs.get(key, default)
        return self.config.get(key, default)

    def _get_location_config(self) -> tuple[str, str, str]:
        api_key = str(self._cfg("amap_api_key", "")).strip()
        if not api_key:
            raise RuntimeError("amap_api_key_missing")
        city = str(self._cfg("home_base_city", "")).strip() or "上海"
        home_base = str(self._cfg("home_base", "")).strip() or city
        return api_key, city, home_base

    async def _generate_payload(self, *, prompt: str, ctx: dict[str, Any], sid_base: str, umo: str | None) -> dict[str, Any]:
        content = await self._call_llm(prompt, sid=f"{sid_base}_0", umo=umo)
        payload = self._extract_json_obj(content)
        ok, reason = self._validate_payload(payload, ctx)
        for attempt in range(1, self._STYLE_ENFORCE_RETRIES + 1):
            if ok:
                break
            repair_prompt = self._build_repair_prompt(ctx, content, reason)
            content = await self._call_llm(repair_prompt, sid=f"{sid_base}_{attempt}", umo=umo)
            payload = self._extract_json_obj(content)
            ok, reason = self._validate_payload(payload, ctx)
        if not ok or not payload:
            raise ValueError(reason or "invalid_payload")
        return payload

    async def _build_schedule_output(
        self,
        *,
        amap: AmapClient,
        drafts: list[SlotDraft],
        city: str,
        home_base: str,
        ctx: dict[str, Any],
        weather_summary: str,
        warnings: list[str],
    ) -> tuple[str, list[ScheduleItem]]:
        home_place = await amap.search_place(home_base, city)
        if not home_place:
            warnings.append("home_base_resolve_failed")
        max_km_raw = self._cfg("max_activity_distance_km", 20)
        try:
            max_km = float(max_km_raw) if max_km_raw is not None else 0.0
        except (TypeError, ValueError):
            max_km = 0.0
        places = await self._resolve_places(amap, drafts, city, warnings, home_place=home_place, home_base=home_base, max_km=max_km)

        mode = self._choose_travel_mode(
            weather_summary,
            persona_desc=str(ctx.get("persona_desc") or ""),
            schedule_type=str(ctx.get("schedule_type") or ""),
        )
        routes = await self._resolve_routes(amap, home_place, places, mode, city, warnings)
        items = self._build_items(drafts, places, routes, warnings)
        schedule_text = self._format_schedule(items, routes, False)
        return schedule_text, items

    async def generate_schedule(
        self,
        date: datetime.datetime | None = None,
        umo: str | None = None,
        extra: str | None = None,
    ) -> ScheduleData:
        async with self._gen_lock:
            if self._generating:
                raise RuntimeError("schedule_generating")
            self._generating = True

        date = date or datetime.datetime.now()
        date_key = date.strftime("%Y-%m-%d")
        warnings: list[str] = []
        try:
            api_key, city, home_base = self._get_location_config()

            amap = AmapClient(api_key)
            weather_summary = await amap.get_weather_summary(city)

            ctx = await self._collect_context(date, umo, weather_summary, home_base)
            prompt = self._build_prompt(ctx, extra)
            sid_base = f"silicon_life_gen_{date_key}"
            payload = await self._generate_payload(prompt=prompt, ctx=ctx, sid_base=sid_base, umo=umo)

            drafts = self._extract_slots(payload, ctx["time_windows"])
            if not drafts:
                raise ValueError("slots_empty")

            schedule_text, items = await self._build_schedule_output(
                amap=amap,
                drafts=drafts,
                city=city,
                home_base=home_base,
                ctx=ctx,
                weather_summary=weather_summary,
                warnings=warnings,
            )

            data = ScheduleData(
                date=date_key,
                outfit_style=str(payload.get("outfit_style") or "").strip(),
                outfit=str(payload.get("outfit") or "").strip(),
                schedule_text=schedule_text,
                items=items,
                weather_summary=weather_summary,
                status="ok",
                warnings=warnings,
            )
            self.data_mgr.set(data)
            return data
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"硅基生命日程生成失败: {e}")
            data = ScheduleData(
                date=date_key,
                outfit_style="",
                outfit="生成失败",
                schedule_text="生成失败",
                items=[],
                weather_summary="",
                status="failed",
                warnings=[str(e)],
            )
            self.data_mgr.set(data)
            return data
        finally:
            async with self._gen_lock:
                self._generating = False

    async def _collect_context(
        self,
        date: datetime.datetime,
        umo: str | None,
        weather_summary: str,
        home_base: str,
    ) -> dict[str, Any]:
        return {
            "date_str": date.strftime("%Y年%m月%d日"),
            "weekday": ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"][date.weekday()],
            "holiday": self._get_holiday_info(date.date()),
            "persona_desc": await self._get_persona(),
            "history_schedules": self._get_history(date.date()),
            "recent_chats": await self._get_recent_chats(umo),
            "weather_summary": weather_summary or "未知",
            "home_base": home_base,
            "time_windows": self._time_windows_text(),
            **self._pick_diversity(date.date(), weather_summary),
        }

    def _time_windows_text(self) -> str:
        windows = _normalize_list(self._cfg("time_windows"))
        lines = [f"- {w}" for w in windows if w]
        return "\n".join(lines) if lines else "- 07:00-08:30"

    def _get_holiday_info(self, date: datetime.date) -> str:
        try:
            import holidays

            cn_holidays = holidays.CN()
            holiday_name = cn_holidays.get(date)
            if holiday_name:
                return f"今天是 {holiday_name}"
        except ImportError:
            return ""
        except (TypeError, ValueError, KeyError, AttributeError):
            return ""
        return ""

    def _pick_diversity(self, today: datetime.date, weather_summary: str) -> dict[str, Any]:
        pool = self._cfg("pool") or {}
        daily_themes = _normalize_list(pool.get("daily_themes"))
        mood_colors = _normalize_list(pool.get("mood_colors"))
        outfit_styles = _normalize_list(pool.get("outfit_styles"))
        schedule_types = _normalize_list(pool.get("schedule_types"))

        mood = random.choice(mood_colors) if mood_colors else ""
        if weather_summary and any(k in weather_summary for k in ["雨", "雪", "雷", "暴"]):
            if mood_colors:
                rainy_bias = [m for m in mood_colors if m in {"慵懒", "温柔", "浪漫", "清新", "随性"}]
                if rainy_bias:
                    mood = random.choice(rainy_bias)

        return {
            "daily_theme": random.choice(daily_themes) if daily_themes else "",
            "mood_color": mood,
            "outfit_style": random.choice(outfit_styles) if outfit_styles else "",
            "schedule_type": random.choice(schedule_types) if schedule_types else "",
        }

    def _choose_travel_mode(self, weather_summary: str, persona_desc: str = "", schedule_type: str = "") -> str:
        enabled = [m.lower().strip() for m in _normalize_list(self._cfg("enabled_travel_modes"))]
        default = str(self._cfg("default_travel_mode", "transit")).lower().strip()
        if default not in enabled and enabled:
            default = enabled[0]

        weights: dict[str, float] = dict.fromkeys(enabled, 1.0)
        if default in weights:
            weights[default] += 0.5

        def boost(modes: list[str], val: float):
            for m in modes:
                if m in weights:
                    weights[m] += val

        def damp(modes: list[str], val: float):
            for m in modes:
                if m in weights:
                    weights[m] = max(0.0, weights[m] - val)

        eco_kw = ["环保", "节俭", "学生", "健身", "运动", "户外", "探索", "漫游", "骑行", "步行"]
        eff_kw = ["商务", "高效", "赶时间", "通勤", "上班族", "职业", "都市", "约会"]
        drive_kw = ["有车", "驾照", "驾驶", "自驾", "车主"]
        budget_kw = ["省钱", "节约", "节俭"]
        luxury_kw = ["奢华", "精致约会风"]

        if any(k in persona_desc for k in eco_kw):
            boost(["walk", "bike", "transit"], 1.2)
        if any(k in persona_desc for k in eff_kw):
            boost(["taxi", "transit", "drive"], 1.2)
        if any(k in persona_desc for k in drive_kw):
            boost(["drive"], 1.5)
        if any(k in persona_desc for k in budget_kw):
            boost(["transit", "walk", "bike"], 1.0)
            damp(["taxi", "drive"], 0.6)
        if any(k in persona_desc for k in luxury_kw):
            boost(["taxi"], 1.0)

        if "健身运动型" in schedule_type:
            boost(["walk", "bike"], 1.0)
        if "社交聚会型" in schedule_type or "约会" in persona_desc:
            boost(["taxi", "transit"], 0.8)
        if "购物采买型" in schedule_type:
            boost(["transit", "taxi", "drive"], 0.8)
        if "文化艺术型" in schedule_type:
            boost(["transit", "walk"], 0.6)
        if "工作专注型" in schedule_type:
            boost(["transit", "drive"], 0.8)
        if "户外活动型" in schedule_type:
            boost(["walk", "bike", "transit"], 0.8)

        bad_weather = weather_summary and any(k in weather_summary for k in ["雨", "雪", "雷", "暴", "大风"])
        if bad_weather:
            damp(["walk", "bike"], 1.5)
            boost(["taxi", "transit", "drive"], 0.8)

        total = sum(weights.values())
        if total <= 0:
            return default
        r = random.random() * total
        acc = 0.0
        for m, w in weights.items():
            acc += w
            if r <= acc:
                return m
        return default

    def _get_history(self, today: datetime.date) -> str:
        days = int(self._cfg("reference_history_days", 0) or 0)
        if days <= 0:
            return "（无历史记录）"
        items: list[str] = []
        for i in range(1, days + 1):
            date = today - datetime.timedelta(days=i)
            data = self.data_mgr.get(date)
            if not data or data.status != "ok":
                continue
            snippet = (data.schedule_text or "")[:200].replace("\n", " ")
            items.append(f"[{date.strftime('%Y-%m-%d')}] {snippet}")
        return "\n".join(items) if items else "（无历史记录）"

    async def _get_recent_chats(self, umo: str | None) -> str:
        count = int(self._cfg("reference_recent_count", 0) or 0)
        if not umo or count <= 0:
            return "无近期对话"
        try:
            cid = await self.context.conversation_manager.get_curr_conversation_id(umo)
            if not cid:
                return "无最近对话记录"
            conv = await self.context.conversation_manager.get_conversation(umo, cid)
            if not conv or not conv.history:
                return "无最近对话记录"
            history = json.loads(conv.history)
            if not isinstance(history, list):
                return "无最近对话记录"
            recent = history[-count:] if count > 0 else []
            formatted = []
            for msg in recent:
                if not isinstance(msg, dict):
                    continue
                role = msg.get("role", "unknown")
                content = msg.get("content", "")
                if role == "user":
                    formatted.append(f"用户: {content}")
                elif role == "assistant":
                    formatted.append(f"我: {content}")
            return "\n".join(formatted) if formatted else "无最近对话记录"
        except asyncio.CancelledError:
            raise
        except (json.JSONDecodeError, TypeError, ValueError, KeyError, AttributeError):
            return "获取对话记录失败"

    async def _get_persona(self) -> str:
        try:
            p = await self.context.persona_manager.get_default_persona_v3()
            return p.get("prompt") if isinstance(p, dict) else getattr(p, "prompt", "")
        except asyncio.CancelledError:
            raise
        except (TypeError, ValueError, KeyError, AttributeError):
            return "你是一个热爱生活、情感细腻的AI伙伴。"

    def _build_prompt(self, ctx: dict[str, Any], extra: str | None) -> str:
        tmpl = str(self._cfg("prompt_template", ""))
        tmpl_vars = set(re.findall(r"\{(\w+)\}", tmpl))
        for k in tmpl_vars:
            if k not in ctx:
                ctx[k] = ""
        prompt = re.sub(r"\{(\w+)\}", lambda m: str(ctx.get(m.group(1), "")), tmpl)
        max_km_raw = self._cfg("max_activity_distance_km", 20)
        try:
            max_km = float(max_km_raw) if max_km_raw is not None else 0.0
        except (TypeError, ValueError):
            max_km = 0.0
        if max_km > 0:
            prompt += f"\n\n## 额外约束\n- 今日活动地点尽量控制在基地 {max_km:g} 公里以内。"
        if extra:
            prompt += f"\n\n【用户补充要求】\n{extra}"
        return prompt

    async def _call_llm(self, prompt: str, *, sid: str, umo: str | None) -> str:
        provider = self.context.get_using_provider(umo)
        if not provider:
            raise RuntimeError("no_provider")
        try:
            for attempt in range(self._EMPTY_COMPLETION_RETRIES + 1):
                resp = await provider.text_chat(prompt, session_id=sid)
                text = self._extract_completion_text(resp)
                if text:
                    return text
                if attempt < self._EMPTY_COMPLETION_RETRIES:
                    continue
            raise RuntimeError("empty_completion")
        finally:
            await self._cleanup_session(sid)

    async def _cleanup_session(self, sid: str):
        try:
            cid = await self.context.conversation_manager.get_curr_conversation_id(sid)
            if cid:
                await self.context.conversation_manager.delete_conversation(sid, cid)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    @staticmethod
    def _extract_completion_text(resp: object) -> str:
        if resp is None:
            return ""
        for key in ("completion_text", "completion", "text", "content"):
            value = getattr(resp, key, None)
            if isinstance(value, str):
                text = value.strip()
                if text:
                    return text
        return ""

    def _extract_json_obj(self, text: str) -> dict[str, Any] | None:
        text = (text or "").strip()
        text = re.sub(r"^```json\s*", "", text, flags=re.MULTILINE)
        text = re.sub(r"^```\s*", "", text, flags=re.MULTILINE)
        text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE)
        starts = [m.start() for m in re.finditer(r"\{", text)]
        for start in starts:
            brace = 0
            in_string = False
            escape = False
            for i, ch in enumerate(text[start:], start=start):
                if in_string:
                    if escape:
                        escape = False
                    elif ch == "\\":
                        escape = True
                    elif ch == '"':
                        in_string = False
                else:
                    if ch == '"':
                        in_string = True
                    elif ch == "{":
                        brace += 1
                    elif ch == "}":
                        brace -= 1
                        if brace == 0:
                            s = text[start : i + 1]
                            try:
                                data = json.loads(s)
                            except json.JSONDecodeError:
                                break
                            return data if isinstance(data, dict) else None
        return None

    def _validate_payload(self, payload: dict[str, Any] | None, ctx: dict[str, Any]) -> tuple[bool, str]:
        if not payload:
            return False, "未能解析出 JSON 对象"
        outfit = str(payload.get("outfit", "")).strip()
        if not outfit:
            return False, "outfit 不能为空"
        required = str(ctx.get("outfit_style") or "").strip()
        if required:
            model_style = str(payload.get("outfit_style", "")).strip()
            if model_style != required:
                return False, f'outfit_style 必须严格等于 "{required}"'
            if not re.match(rf"^\s*(?:风格|【风格】|\[风格\])\s*[:：]\s*{re.escape(required)}(?:\s|$)", outfit):
                fixed = f"风格：{required}\n{outfit}"
                payload["outfit"] = fixed
        slots = payload.get("slots")
        if not isinstance(slots, list) or not slots:
            return False, "slots 不能为空"
        return True, ""

    def _build_repair_prompt(self, ctx: dict[str, Any], bad_text: str, reason: str) -> str:
        required = str(ctx.get("outfit_style") or "").strip()
        time_windows = str(ctx.get("time_windows") or "")
        return (
            "你之前的输出未通过校验，需要按要求重写。\n"
            f"校验原因：{reason}\n\n"
            "请只输出 JSON 对象本体，不要 Markdown，不要解释。\n"
            "输出 JSON 必须包含字段：outfit_style、outfit、slots。\n"
            f'其中 outfit_style 必须严格等于 "{required}"；outfit 第一行必须以 "风格：{required}" 开头。\n'
            "slots 必须是数组，每一项必须包含 time_window/title/poi_query/poi_query_alt。\n"
            f"时间段模板如下（必须逐条对应）：\n{time_windows}\n\n"
            "你之前的输出（供参考，可能不合规）：\n"
            f"{bad_text}\n"
        )

    def _extract_slots(self, payload: dict[str, Any], windows: str) -> list[SlotDraft]:
        expected = []
        for line in windows.splitlines():
            line = line.strip()
            if line.startswith("-"):
                expected.append(line[1:].strip())
        expected_set = {e for e in expected if e}

        slots = payload.get("slots")
        out: list[SlotDraft] = []
        if not isinstance(slots, list):
            return out
        for it in slots:
            if not isinstance(it, dict):
                continue
            tw = str(it.get("time_window") or "").strip()
            if expected_set and tw not in expected_set:
                continue
            title = str(it.get("title") or "").strip()
            poi_query = str(it.get("poi_query") or "").strip()
            alt = it.get("poi_query_alt")
            poi_query_alt = [str(x).strip() for x in alt] if isinstance(alt, list) else []
            poi_query_alt = [x for x in poi_query_alt if x]
            if not title or not poi_query or not tw:
                continue
            out.append(SlotDraft(time_window=tw, title=title, poi_query=poi_query, poi_query_alt=poi_query_alt))

        if expected:
            order = {tw: idx for idx, tw in enumerate(expected)}
            out.sort(key=lambda d: order.get(d.time_window, 10_000))
        return out

    async def _resolve_places(
        self,
        amap: AmapClient,
        drafts: list[SlotDraft],
        city: str,
        warnings: list[str],
        *,
        home_place: Place | None,
        home_base: str,
        max_km: float,
    ) -> list[Place | None]:
        out: list[Place | None] = []
        for d in drafts:
            queries: list[str] = []
            if d.poi_query:
                queries.append(d.poi_query)
            if d.poi_query_alt:
                queries.extend([q for q in d.poi_query_alt if q])
            if home_base:
                queries.extend([f"{home_base} {q}" for q in list(dict.fromkeys(queries))])
            queries.extend([f"{q} 附近" for q in list(dict.fromkeys(queries))])
            queries = [q.strip() for q in queries if q.strip()]
            seen: set[str] = set()
            filtered_queries: list[str] = []
            for q in queries:
                if q in seen:
                    continue
                seen.add(q)
                filtered_queries.append(q)

            best_place: Place | None = None
            best_km: float | None = None
            for q in filtered_queries[:8]:
                place = await amap.search_place(q, city)
                if not place:
                    continue
                if max_km > 0 and home_place and home_place.location and place.location:
                    km = _haversine_km(home_place.location, place.location)
                    if best_km is None or km < best_km:
                        best_km = km
                        best_place = place
                        if km <= max_km:
                            break
                    continue
                best_place = place
                best_km = None
                break

            if not best_place:
                warnings.append(f"poi_resolve_failed:{d.poi_query}")
                out.append(None)
                continue

            if max_km > 0 and best_km is not None and best_km > max_km:
                warnings.append(f"poi_distance_over_limit:{best_km:.1f}km:{d.poi_query}")
            out.append(best_place)
        return out

    async def _resolve_routes(
        self,
        amap: AmapClient,
        home: Place | None,
        places: list[Place | None],
        mode: str,
        city: str,
        warnings: list[str],
    ) -> list[RouteInfo | None]:
        out: list[RouteInfo | None] = []
        prev = home
        max_min = int(self._cfg("max_travel_minutes_per_leg", 45) or 45)

        for p in places:
            if not prev or not p or not prev.location or not p.location:
                out.append(None)
                prev = p if p else prev
                continue
            route = await amap.get_route(prev.location, p.location, mode, city)
            if not route.ok or route.duration_min is None:
                warnings.append(f"route_failed:{mode}")
                out.append(None)
                prev = p
                continue
            if max_min and route.duration_min > max_min:
                warnings.append(f"route_too_long:{route.duration_min}min")
            out.append(route)
            prev = p
        return out

    def _build_items(
        self,
        drafts: list[SlotDraft],
        places: list[Place | None],
        routes: list[RouteInfo | None],
        warnings: list[str],
    ) -> list[ScheduleItem]:
        items: list[ScheduleItem] = []
        for idx, d in enumerate(drafts):
            p = places[idx] if idx < len(places) else None
            r = routes[idx] if idx < len(routes) else None
            lat = p.location[0] if p and p.location else None
            lng = p.location[1] if p and p.location else None
            item = ScheduleItem(
                time_window=d.time_window,
                title=d.title,
                poi_query_raw=d.poi_query,
                poi_name=(p.name if p else d.poi_query),
                poi_district=(p.district if p else ""),
                poi_city=(p.city if p else ""),
                poi_address=(p.address if p else ""),
                lat=lat,
                lng=lng,
            )
            if r:
                item.travel_mode_from_prev = r.mode
                item.travel_minutes_from_prev = r.duration_min
                item.travel_distance_m_from_prev = r.distance_m
            items.append(item)

        if not items:
            warnings.append("items_empty")
        return items

    def _format_schedule(self, items: list[ScheduleItem], routes: list[RouteInfo | None], privacy: bool) -> str:
        lines: list[str] = []
        for idx, it in enumerate(items):
            place = it.poi_name
            if privacy:
                if it.poi_district:
                    place = f"{it.poi_name}（{it.poi_district}）"
            else:
                if it.poi_address:
                    place = f"{it.poi_name}（{it.poi_address}）"
                elif it.poi_district:
                    place = f"{it.poi_name}（{it.poi_district}）"
            lines.append(f"{it.time_window} {it.title} - {place}")
            r = routes[idx] if idx < len(routes) else None
            if r and r.duration_min is not None:
                label = "打车" if r.mode == "taxi" else r.mode
                dist = f" / {round((r.distance_m or 0) / 1000.0, 1)}km" if r.distance_m else ""
                lines.append(f"路程：{label} 约{r.duration_min}分钟{dist}")
            elif idx == 0:
                lines.append("路程：未知（起点或地点未能定位，已降级）")
            else:
                lines.append("路程：未知（地点或路线未能获取，已降级）")
        return "\n".join(lines).strip()


class SiliconLifePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.context = context
        self.config = config
        self.data_dir = StarTools.get_data_dir()
        self.schedule_data_file = self.data_dir / "schedule_data.json"

    def _migrate_config_to_virtual_schedule(self) -> None:
        try:
            schema = getattr(self.config, "schema", None) or {}
            vs_schema = schema.get("virtual_schedule") if isinstance(schema, dict) else None
            items = (vs_schema or {}).get("items") if isinstance(vs_schema, dict) else None
            if not isinstance(items, dict):
                return

            vs = self.config.get("virtual_schedule")
            if not isinstance(vs, dict):
                vs = {}

            migrated_keys: list[str] = []
            changed = False
            for k, meta in items.items():
                if k not in self.config:
                    continue
                if k == "virtual_schedule":
                    continue
                old_val = self.config.get(k)
                default = meta.get("default") if isinstance(meta, dict) else None
                if k not in vs or (default is not None and vs.get(k) == default):
                    vs[k] = old_val
                    changed = True
                    migrated_keys.append(k)

            if not changed:
                return

            self.config["virtual_schedule"] = vs
            for k in migrated_keys:
                try:
                    del self.config[k]
                except KeyError:
                    pass
            self.config.save_config()
        except Exception:
            return

    async def initialize(self):
        self._migrate_config_to_virtual_schedule()
        self.data_mgr = ScheduleDataManager(self.schedule_data_file)
        self.generator = SiliconLifeGenerator(self.context, self.config, self.data_mgr)
        self.scheduler = LifeScheduler(
            context=self.context,
            config=self.config,
            task=self._scheduled_generate,
        )
        self.scheduler.start()

    async def terminate(self):
        self.scheduler.stop()

    async def _scheduled_generate(self):
        today = datetime.datetime.now()
        await self.generator.generate_schedule(today, None, None)

    @filter.on_llm_request()
    async def on_llm_request(self, event: AstrMessageEvent, req: ProviderRequest):
        today = datetime.datetime.now()
        data = self.data_mgr.get(today)
        if not data:
            try:
                data = await self.generator.generate_schedule(today, event.unified_msg_origin)
            except RuntimeError:
                return
        if not data or data.status != "ok":
            return

        now = today.time()
        windows = _parse_time_windows(_normalize_list(self.generator._cfg("time_windows")))
        curr = None
        for raw, start, end in windows:
            if _time_in_window(now, start, end):
                curr = raw
                break

        curr_place = ""
        if curr and data.items:
            for it in data.items:
                if it.time_window == curr:
                    curr_place = it.poi_name
                    break

        inject_text = (
            "\n<character_state>\n"
            f"时间: {_time_desc()}\n"
            f"天气: {data.weather_summary or '未知'}\n"
            f"当前所在: {curr_place or '未知'}\n"
            f"穿着: {data.outfit}\n"
            f"日程: {data.schedule_text}\n"
            "</character_state>\n"
            "[上述状态仅供需要时参考，无需主动提及]"
        )
        req.system_prompt += inject_text

    @filter.command_group("silicon")
    def silicon_group(self):
        pass

    @silicon_group.command("show")
    async def silicon_show(self, event: AstrMessageEvent):
        today = datetime.datetime.now()
        today_str = today.strftime("%Y-%m-%d")
        data = self.data_mgr.get(today)
        if data and data.status != "ok":
            try:
                yield event.plain_result("检测到今日硅基日程生成失败，正在重试生成...")
                data = await self.generator.generate_schedule(today, event.unified_msg_origin)
            except RuntimeError:
                yield event.plain_result("日程正在生成中，请稍后再查看")
                return
        if not data:
            try:
                yield event.plain_result("今日硅基日程尚未生成，正在生成...")
                data = await self.generator.generate_schedule(today, event.unified_msg_origin)
            except RuntimeError:
                yield event.plain_result("日程正在生成中，请稍后再查看")
                return
        if not data or data.status != "ok":
            reason = ""
            if data and data.warnings:
                reason = str(data.warnings[0] or "").strip()
            if not reason:
                reason = "未知原因"
            yield event.plain_result(
                f"生成失败：{reason}\n可用 /silicon diag 检查高德接口，或稍后再试。",
            )
            return

        text = f"📅 {today_str}\n🌤 天气：{data.weather_summary or '未知'}\n👗 今日穿搭：{data.outfit}\n📝 日程：\n{data.schedule_text}"
        yield event.plain_result(text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @silicon_group.command("renew")
    async def silicon_renew(self, event: AstrMessageEvent, extra: str | None = None):
        today = datetime.datetime.now()
        today_str = today.strftime("%Y-%m-%d")
        if extra:
            yield event.plain_result(f"正在根据补充要求重写今日硅基日程：{extra}")
        else:
            yield event.plain_result("正在重写今日硅基日程...")
        try:
            data = await self.generator.generate_schedule(today, event.unified_msg_origin, extra=extra)
        except RuntimeError:
            yield event.plain_result("已有日程生成任务在进行中，请稍后再试")
            return
        if not data or data.status != "ok":
            reason = ""
            if data and data.warnings:
                reason = str(data.warnings[0] or "").strip()
            if not reason:
                reason = "未知原因"
            yield event.plain_result(f"重写失败：{reason}")
            return
        yield event.plain_result(
            f"📅 {today_str}\n🌤 天气：{data.weather_summary or '未知'}\n📝 已更新。\n{data.schedule_text}",
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @silicon_group.command("time")
    async def silicon_time(self, event: AstrMessageEvent, param: str | None = None):
        if not param:
            yield event.plain_result("请提供时间，格式为 HH:MM，例如 /silicon time 07:30")
            return
        if not re.match(r"^\d{1,2}:\d{1,2}$", param):
            yield event.plain_result("时间格式错误，请使用 HH:MM 格式")
            return
        try:
            hour, minute = map(int, param.split(":"))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except ValueError:
            yield event.plain_result("时间格式错误，请使用 HH:MM 格式，且小时 0-23、分钟 0-59")
            return
        try:
            self.scheduler.update_schedule_time(param)
            yield event.plain_result(f"已将每日硅基日程生成时间更新为 {param}。")
        except Exception as e:
            yield event.plain_result(f"设置失败: {e}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @silicon_group.command("diag")
    async def silicon_diag(self, event: AstrMessageEvent):
        cfg = self.generator._cfg
        key = str(cfg("amap_api_key", "")).strip()
        city = str(cfg("home_base_city", "")).strip() or "上海"
        home = str(cfg("home_base", "")).strip() or city
        checks: list[str] = []
        checks.append(f"Key: {'已配置' if key else '未配置'}")
        checks.append(f"城市: {city}")
        checks.append(f"基地: {home}")
        try:
            amap = AmapClient(key)
            weather = await amap.get_weather_summary(city)
            checks.append(f"天气API: {'OK' if weather else '失败'}{' / '+weather if weather else ''}")
            place = await amap.search_place(home, city)
            checks.append(f"POI解析: {'OK' if place else '失败'}")
            if place and place.location:
                route = await amap.get_route(place.location, place.location, "walk", city)
                checks.append(f"路径API: {'OK' if route.ok else '失败'}")
        except Exception as e:
            checks.append(f"诊断异常: {e}")
        text = "硅基生命 · 诊断\n" + "\n".join(f"- {x}" for x in checks)
        yield event.plain_result(text)
