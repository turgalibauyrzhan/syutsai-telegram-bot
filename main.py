#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Syucai Telegram bot (Render-friendly, webhook-first)

Исправления:
- Уходим от getUpdates => webhook (никаких 409 Conflict).
- Парсер Google Service Account:
  * GOOGLE_SA_JSON (просто JSON строкой) ИЛИ
  * GOOGLE_SA_JSON_B64 (base64 одной строкой)
  * также ловим частую ошибку: base64 по ошибке положили в GOOGLE_SA_JSON.
- Trial 3 дня (как Premium). 1-й день — полный разбор; далее — коротко.
- Ежедневная рассылка в 09:00 Asia/Almaty (JobQueue PTB).
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, Optional, Tuple, List
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes,
    filters
)

# ---- Google Sheets deps ----
try:
    import gspread  # type: ignore
    from google.oauth2.service_account import Credentials  # type: ignore
except Exception:
    gspread = None
    Credentials = None


LOGGER = logging.getLogger("syucai")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - syucai - %(levelname)s - %(message)s",
)

TZ = ZoneInfo("Asia/Almaty")


# =========================
# Тексты (правь здесь)
# =========================

TEXTS: Dict[str, Any] = {
    "od": {  # Общий день (ОД)
        1: "Не желательно начинать новые проекты и события. Есть высокая вероятность обнуления всех результатов ваших действий. Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и т.д.",
        2: "День мягкой силы: переговоры, примирение, аккуратные решения. Не давите — договаривайтесь.",
        3: "День удачи, простых решений и быстрых результатов. Хорош для стартов, поездок, встреч, общения.",
        4: "День дисциплины и структуры. Лучше закрывать хвосты, наводить порядок, работать по плану.",
        5: "День перемен. Возможны резкие повороты, новости, смена планов. Будьте гибкими.",
        6: "День семьи, заботы, дома и гармонии. Хорошо решать бытовые вопросы и укреплять отношения.",
        7: "День анализа, тишины, фокуса и глубины. Подходит для обучения, размышлений, планирования.",
        8: "День ресурсов и денег. Хорошо решать финансовые вопросы, договариваться о выгоде.",
        9: "День завершений и подведения итогов. Хорошо закрывать дела и отпускать лишнее.",
    },
    "ld": {  # Личный день (ЛД)
        1: "День решения и лидерства. Делайте первый шаг, но без лишней агрессии.",
        2: "День чувств и контакта. Полезны переговоры, совместные дела, примирения.",
        3: "День общения и креатива. Подходит для знакомств, презентаций, контента.",
        4: "День мистических событий, как положительных, так и отрицательных. Человек может испытывать чувство неудовлетворенности. Важно быть на позитиве, чтобы были положительные мистические события. Иначе могут быть мистические потери. Посвятить день целям и мечтам. Визуализируйте цели, позвольте мечтать без ограничений.",
        5: "День мистических событий, как положительных, так и отрицательных. Человек может испытывать чувство неудовлетворенности. Важно быть на позитиве, чтобы были положительные мистические события. Иначе могут быть мистические потери. Посвятить день целям и мечтам. Визуализируйте цели, позвольте мечтать без ограничений.",
        6: "День ответственности и заботы. Дом, семья, здоровье, полезные привычки.",
        7: "ЛД=7 — анализ, тишина, фокус, глубина.",
        8: "День ресурсов и денег.",
        9: "День завершений: закрывайте долги, завершайте дела, фиксируйте результат.",
    },
    "lm_short": {  # Личный месяц - коротко
        1: "Месяц стартов.",
        2: "Месяц отношений и договоренностей.",
        3: "Месяц общения и роста.",
        4: "Месяц дисциплины и структуры.",
        5: "Месяц перемен.",
        6: "Месяц семьи и заботы.",
        7: "Месяц глубины и обучения.",
        8: "Месяц денег и ресурсов.",
        9: "Месяц завершений.",
    },
    "lg_short": {  # Личный год - коротко
        1: "Год стартов.",
        2: "Год отношений и партнерств.",
        3: "Год анализа и успеха.",
        4: "Год дисциплины и фундамента.",
        5: "Год перемен.",
        6: "Год семьи и ответственности.",
        7: "Год глубины.",
        8: "Год денег и силы.",
        9: "Год завершений.",
    },
    "lg_full": {  # Полное (на 1-й день full-доступа)
        3: "Год анализа и успеха. Главная задача года — учиться, систематизировать знания и превращать их в результат. Важно выбрать 1–2 ключевые цели и идти вглубь, а не распыляться. Возможны заметные достижения, если действовать по плану и не лениться.",
        7: "Год глубины. Период внутреннего роста: обучение, самоанализ, поиск смысла, перезагрузка целей. Важно не форсировать внешние события — лучше углубляться, укреплять компетенции и здоровье.",
    },
    "lm_full": {
        2: "Месяц отношений и договоренностей. Фокус на общении, семье, партнерстве. Хорошо выравнивать конфликты, укреплять связи, договариваться о правилах и совместных планах. Плохо — давить и спорить из принципа.",
        1: "Месяц стартов. Хорошо запускать новые привычки, начинать проекты, пробовать новое. Важно не распыляться и фиксировать прогресс.",
    },
    "special_dates": {
        10: "🔟 10 число — день удачи и быстрых возможностей. Хорошо начинать дела, запускать инициативы, выходить на людей.",
        20: "2️⃣0️⃣ 20 число — день партнерств и договоров. Хорошо обсуждать условия, мириться, укреплять связи.",
        30: "3️⃣0️⃣ 30 число — день творчества и коммуникаций. Хорошо выступать, писать, создавать контент и идеи.",
    },
    "ui": {
        "need_birth": "Чтобы считать ЛГ/ЛМ/ЛД, пришли дату рождения в формате ДД.ММ.ГГГГ (например 05.11.1992).",
        "saved_birth": "✅ Дата рождения сохранена: {birth}.",
        "trial_started": "🎁 Тебе активирован Trial на 3 дня. День 1 — полный разбор, далее — короткая версия.",
        "trial_expired": "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору.",
        "premium_active": "⭐️ Premium активен: полный прогноз доступен + ежедневка 09:00.",
        "help": (
            "Команды:\n"
            "/start — регистрация\n"
            "/status — статус доступа\n"
            "/setbirth ДД.ММ.ГГГГ — сохранить дату рождения\n"
            "/today — прогноз на сегодня\n"
        ),
    },
}


# =========================
# Numerology calc
# =========================

def digit_sum(n: int) -> int:
    s = 0
    while n > 0:
        s += n % 10
        n //= 10
    return s

def reduce_1_9(n: int) -> int:
    if n <= 0:
        return 0
    while n > 9:
        n = digit_sum(n)
    return n

def parse_birth(s: str) -> Optional[date]:
    m = re.fullmatch(r"\s*(\d{2})\.(\d{2})\.(\d{4})\s*", s)
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None

def calc_personal_year(birth: date, today: date) -> int:
    return reduce_1_9(reduce_1_9(birth.day) + reduce_1_9(birth.month) + reduce_1_9(today.year))

def calc_personal_month(personal_year: int, today: date) -> int:
    return reduce_1_9(personal_year + reduce_1_9(today.month))

def calc_personal_day(personal_month: int, today: date) -> int:
    return reduce_1_9(personal_month + reduce_1_9(today.day))

def calc_general_day(today: date) -> int:
    return reduce_1_9(reduce_1_9(today.day) + reduce_1_9(today.month) + reduce_1_9(today.year))


# =========================
# Access model (Sheets)
# =========================

@dataclass
class SubRow:
    telegram_user_id: int
    status: str
    plan: str
    trial_expires: str
    birth_date: str
    created_at: str
    last_seen_at: str
    username: str
    first_name: str
    last_name: str
    registered_on: str
    last_full_ym: str

    @staticmethod
    def headers() -> List[str]:
        return [
            "telegram_user_id", "status", "plan", "trial_expires", "birth_date",
            "created_at", "last_seen_at", "username", "first_name", "last_name",
            "registered_on", "last_full_ym",
        ]

def now_iso() -> str:
    return datetime.now(TZ).replace(microsecond=0).isoformat()

def today_iso() -> str:
    return date.today().isoformat()

def safe_int(s: Any, default: int = 0) -> int:
    try:
        return int(str(s).strip())
    except Exception:
        return default

def iso_to_date(s: str) -> Optional[date]:
    try:
        return date.fromisoformat(s.strip())
    except Exception:
        return None

def iso_to_dt(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s.strip())
    except Exception:
        return None

def compute_access(sub: SubRow, today: date) -> Tuple[bool, str]:
    status = (sub.status or "").strip().lower()
    plan = (sub.plan or "").strip().lower()

    if status == "premium" or plan == "premium":
        return True, "premium"

    if status == "trial" or plan == "trial":
        exp = iso_to_dt(sub.trial_expires)
        if exp and exp.date() >= today:
            return True, "trial"
        return False, "expired"

    return False, "restricted"


# =========================
# Google Sheets wrapper
# =========================

class SheetStore:
    def __init__(self) -> None:
        self.enabled = False
        self._client = None
        self._ws = None

    def _parse_sa_json(self) -> Dict[str, Any]:
        raw = (os.getenv("GOOGLE_SA_JSON") or "").strip()
        raw_b64 = (os.getenv("GOOGLE_SA_JSON_B64") or "").strip()

        if raw_b64:
            decoded = base64.b64decode(raw_b64).decode("utf-8")
            return json.loads(decoded)

        if not raw:
            raise RuntimeError("GOOGLE_SA_JSON is empty")

        # Частая ошибка: base64 положили в GOOGLE_SA_JSON
        if re.fullmatch(r"[A-Za-z0-9+/=\s]+", raw) and raw.startswith(("ewog", "eyJ")):
            try:
                decoded = base64.b64decode(raw).decode("utf-8")
                return json.loads(decoded)
            except Exception:
                pass

        return json.loads(raw)

    def init_sync(self) -> None:
        if gspread is None or Credentials is None:
            raise RuntimeError("gspread/google-auth not installed")

        sa = self._parse_sa_json()
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(sa, scopes=scopes)
        self._client = gspread.authorize(creds)

        sheet_id = (os.getenv("GSHEET_ID") or "").strip()
        if not sheet_id:
            raise RuntimeError("GSHEET_ID is empty")

        sheet_name = (os.getenv("SUBS_SHEET_NAME") or "subscriptions").strip()
        ws = self._client.open_by_key(sheet_id).worksheet(sheet_name)

        headers = ws.row_values(1)
        if not headers:
            ws.append_row(SubRow.headers(), value_input_option="RAW")

        self._ws = ws
        self.enabled = True

    async def init(self) -> None:
        await asyncio.to_thread(self.init_sync)

    def _require(self) -> None:
        if not self.enabled or self._ws is None:
            raise RuntimeError("Google Sheets not ready")

    async def find_row_idx(self, telegram_user_id: int) -> Optional[int]:
        self._require()
        def _find() -> Optional[int]:
            col = self._ws.col_values(1)
            target = str(telegram_user_id)
            for i, v in enumerate(col, start=1):
                if str(v).strip() == target:
                    return i
            return None
        return await asyncio.to_thread(_find)

    async def get_row(self, telegram_user_id: int) -> Optional[SubRow]:
        self._require()
        idx = await self.find_row_idx(telegram_user_id)
        if not idx:
            return None

        def _get() -> SubRow:
            headers = self._ws.row_values(1)
            values = self._ws.row_values(idx)
            data = {headers[i]: (values[i] if i < len(values) else "") for i in range(len(headers))}
            return SubRow(
                telegram_user_id=safe_int(data.get("telegram_user_id", telegram_user_id)),
                status=str(data.get("status", "") or ""),
                plan=str(data.get("plan", "") or ""),
                trial_expires=str(data.get("trial_expires", "") or ""),
                birth_date=str(data.get("birth_date", "") or ""),
                created_at=str(data.get("created_at", "") or ""),
                last_seen_at=str(data.get("last_seen_at", "") or ""),
                username=str(data.get("username", "") or ""),
                first_name=str(data.get("first_name", "") or ""),
                last_name=str(data.get("last_name", "") or ""),
                registered_on=str(data.get("registered_on", "") or ""),
                last_full_ym=str(data.get("last_full_ym", "") or ""),
            )
        return await asyncio.to_thread(_get)

    async def upsert_user(self, update: Update, status: str, plan: str, trial_expires: str) -> SubRow:
        self._require()
        user = update.effective_user
        assert user is not None
        uid = user.id
        now = now_iso()

        existing = await self.get_row(uid)
        if existing:
            await self.touch_seen(uid)
            return existing

        def _append() -> SubRow:
            headers = self._ws.row_values(1) or SubRow.headers()
            if self._ws.row_values(1) == []:
                self._ws.append_row(headers, value_input_option="RAW")

            row_dict = {
                "telegram_user_id": str(uid),
                "status": status,
                "plan": plan,
                "trial_expires": trial_expires,
                "birth_date": "",
                "created_at": now,
                "last_seen_at": now,
                "username": user.username or "",
                "first_name": user.first_name or "",
                "last_name": user.last_name or "",
                "registered_on": today_iso(),
                "last_full_ym": "",
            }
            row = [row_dict.get(h, "") for h in headers]
            self._ws.append_row(row, value_input_option="RAW")
            return SubRow(
                telegram_user_id=uid, status=status, plan=plan, trial_expires=trial_expires,
                birth_date="", created_at=now, last_seen_at=now,
                username=user.username or "", first_name=user.first_name or "", last_name=user.last_name or "",
                registered_on=today_iso(), last_full_ym="",
            )
        return await asyncio.to_thread(_append)

    async def set_birth(self, telegram_user_id: int, birth: date) -> None:
        self._require()
        idx = await self.find_row_idx(telegram_user_id)
        if not idx:
            return

        def _set() -> None:
            headers = self._ws.row_values(1)
            if "birth_date" in headers:
                col = headers.index("birth_date") + 1
                self._ws.update_cell(idx, col, birth.isoformat())
            if "last_seen_at" in headers:
                col2 = headers.index("last_seen_at") + 1
                self._ws.update_cell(idx, col2, now_iso())
        await asyncio.to_thread(_set)

    async def touch_seen(self, telegram_user_id: int) -> None:
        if not self.enabled:
            return
        try:
            idx = await self.find_row_idx(telegram_user_id)
            if not idx:
                return
            def _touch() -> None:
                headers = self._ws.row_values(1)
                if "last_seen_at" in headers:
                    col = headers.index("last_seen_at") + 1
                    self._ws.update_cell(idx, col, now_iso())
            await asyncio.to_thread(_touch)
        except Exception:
            return

    async def list_users(self) -> List[SubRow]:
        self._require()
        def _all() -> List[SubRow]:
            rows = self._ws.get_all_records()
            out: List[SubRow] = []
            for r in rows:
                out.append(SubRow(
                    telegram_user_id=safe_int(r.get("telegram_user_id", 0)),
                    status=str(r.get("status","") or ""),
                    plan=str(r.get("plan","") or ""),
                    trial_expires=str(r.get("trial_expires","") or ""),
                    birth_date=str(r.get("birth_date","") or ""),
                    created_at=str(r.get("created_at","") or ""),
                    last_seen_at=str(r.get("last_seen_at","") or ""),
                    username=str(r.get("username","") or ""),
                    first_name=str(r.get("first_name","") or ""),
                    last_name=str(r.get("last_name","") or ""),
                    registered_on=str(r.get("registered_on","") or ""),
                    last_full_ym=str(r.get("last_full_ym","") or ""),
                ))
            return out
        return await asyncio.to_thread(_all)

SHEETS = SheetStore()


# =========================
# Message builder
# =========================

def build_today_text(today: date, birth: Optional[date], full_access: bool, access_kind: str, first_full_day: bool) -> str:
    od = calc_general_day(today)
    special = TEXTS["special_dates"].get(today.day)

    ld = lg = lm = None
    if birth:
        lg = calc_personal_year(birth, today)
        lm = calc_personal_month(lg, today)
        ld = calc_personal_day(lm, today)

    lines: List[str] = []
    lines.append(f"📅 Дата: {today.strftime('%d.%m.%Y')}")
    lines.append("")
    lines.append(f"🌐 Общий день (ОД): {od}")
    lines.append(TEXTS["od"].get(od, ""))

    if special:
        lines.append("")
        lines.append(special)

    if not birth:
        lines.append("")
        lines.append(TEXTS["ui"]["need_birth"])
        lines.append("")
        lines.append(TEXTS["ui"]["premium_active"] if access_kind == "premium" else ("🎁 Trial активен." if access_kind == "trial" else TEXTS["ui"]["trial_expired"]))
        return "\n".join([l for l in lines if str(l).strip()])

    assert ld is not None and lg is not None and lm is not None

    lines.append("")
    if full_access and first_full_day:
        lines.append(f"🗓 Личный год (ЛГ): {lg}")
        lines.append(TEXTS["lg_full"].get(lg) or TEXTS["lg_short"].get(lg, ""))
        lines.append("")
        lines.append(f"🗓 Личный месяц (ЛМ): {lm}")
        lines.append(TEXTS["lm_full"].get(lm) or TEXTS["lm_short"].get(lm, ""))
        lines.append("")
        lines.append(f"🔢 Личный день (ЛД): {ld}")
        lines.append(TEXTS["ld"].get(ld, ""))
    else:
        lines.append(f"🗓 Личный год (ЛГ): {lg}. {TEXTS['lg_short'].get(lg, '').strip()}")
        lines.append(f"🗓 Личный месяц (ЛМ): {lm}. {TEXTS['lm_short'].get(lm, '').strip()}")
        lines.append("")
        lines.append(f"🔢 Личный день (ЛД): {ld}")
        lines.append(TEXTS["ld"].get(ld, ""))

    lines.append("")
    lines.append(TEXTS["ui"]["premium_active"] if access_kind == "premium" else ("🎁 Trial активен: полный прогноз доступен + ежедневка 09:00." if access_kind == "trial" else TEXTS["ui"]["trial_expired"]))
    return "\n".join([l for l in lines if str(l).strip()])


# =========================
# Handlers
# =========================

async def ensure_sheets_ready() -> None:
    if SHEETS.enabled:
        return
    try:
        await SHEETS.init()
        LOGGER.info("Google Sheets ready")
    except Exception as e:
        LOGGER.warning("Google Sheets not ready: %s", e)

async def get_or_register(update: Update) -> Tuple[Optional[SubRow], str, bool]:
    await ensure_sheets_ready()
    user = update.effective_user
    assert user is not None
    uid = user.id
    today = date.today()

    if not SHEETS.enabled:
        return None, "trial", True

    sub = await SHEETS.get_row(uid)
    if not sub:
        exp = (datetime.now(TZ) + timedelta(days=3)).replace(microsecond=0).isoformat()
        sub = await SHEETS.upsert_user(update, status="trial", plan="trial", trial_expires=exp)
        return sub, "trial", True

    await SHEETS.touch_seen(uid)
    full_access, kind = compute_access(sub, today)
    return sub, kind, full_access

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sub, kind, _ = await get_or_register(update)
    if sub and kind == "trial" and (sub.registered_on == today_iso()):
        await update.message.reply_text(TEXTS["ui"]["trial_started"])
    await update.message.reply_text(TEXTS["ui"]["help"])

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(TEXTS["ui"]["help"])

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sub, kind, _ = await get_or_register(update)
    if not sub:
        await update.message.reply_text("Статус: trial (Sheets не подключены).")
        return
    txt = f"Статус: {kind}\nplan={sub.plan}\ntrial_expires={sub.trial_expires or '-'}\nbirth_date={sub.birth_date or '-'}"
    await update.message.reply_text(txt)

async def cmd_setbirth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sub, _, _ = await get_or_register(update)
    if not sub:
        await update.message.reply_text("Sheets недоступны: дата рождения не будет сохранена.")
        return
    if not context.args:
        await update.message.reply_text(TEXTS["ui"]["need_birth"])
        return
    b = parse_birth(context.args[0])
    if not b:
        await update.message.reply_text("Неверный формат. Пример: 05.11.1992")
        return
    await SHEETS.set_birth(sub.telegram_user_id, b)
    await update.message.reply_text(TEXTS["ui"]["saved_birth"].format(birth=b.strftime("%d.%m.%Y")))

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sub, kind, full_access = await get_or_register(update)
    today = date.today()
    birth = None
    first_full_day = False

    if sub and sub.birth_date:
        birth = iso_to_date(sub.birth_date) or parse_birth(sub.birth_date)
    if sub:
        first_full_day = (sub.registered_on == today_iso()) and full_access

    text = build_today_text(today, birth, full_access=full_access, access_kind=kind, first_full_day=first_full_day)
    await update.message.reply_text(text)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = (update.message.text or "").strip()
    b = parse_birth(msg)
    if b:
        sub, _, _ = await get_or_register(update)
        if sub and SHEETS.enabled:
            await SHEETS.set_birth(sub.telegram_user_id, b)
            await update.message.reply_text(TEXTS["ui"]["saved_birth"].format(birth=b.strftime("%d.%m.%Y")))
            await cmd_today(update, context)
            return
    await update.message.reply_text("Напиши /today или пришли дату рождения ДД.ММ.ГГГГ")


# =========================
# Daily broadcast
# =========================

async def daily_broadcast(context: ContextTypes.DEFAULT_TYPE) -> None:
    await ensure_sheets_ready()
    if not SHEETS.enabled:
        return

    today = date.today()
    users = await SHEETS.list_users()
    bot = context.bot

    for sub in users:
        if not sub.telegram_user_id:
            continue
        birth = iso_to_date(sub.birth_date) if sub.birth_date else None
        full_access, kind = compute_access(sub, today)
        if not full_access:
            continue
        first_full_day = (sub.registered_on == today_iso())
        text = build_today_text(today, birth, full_access=True, access_kind=kind, first_full_day=first_full_day)
        try:
            await bot.send_message(chat_id=sub.telegram_user_id, text=text)
            await asyncio.sleep(0.05)
        except Exception:
            continue


# =========================
# Run
# =========================

def env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

def main() -> None:
    token = env("TELEGRAM_TOKEN") or env("BOT_TOKEN")
    if not token:
        LOGGER.error("TELEGRAM_TOKEN is empty")
        return

    port = int(env("PORT", "10000"))
    webhook_url = env("WEBHOOK_URL")  # https://<service>.onrender.com
    webhook_path = env("WEBHOOK_PATH", "/telegram/webhook/secret123")

    if not webhook_path.startswith("/"):
        webhook_path = "/" + webhook_path

    application = Application.builder().token(token).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("setbirth", cmd_setbirth))
    application.add_handler(CommandHandler("today", cmd_today))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    application.job_queue.run_daily(daily_broadcast, time=time(9, 0, tzinfo=TZ), name="daily_broadcast")
    LOGGER.info("Daily broadcast scheduled at 09:00 Asia/Almaty")

    if webhook_url:
        full_webhook_url = webhook_url.rstrip("/") + webhook_path
        LOGGER.info("Webhook server 0.0.0.0:%s path=%s => %s", port, webhook_path, full_webhook_url)
        application.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=webhook_path.lstrip("/"),
            webhook_url=full_webhook_url,
            drop_pending_updates=True,
        )
    else:
        LOGGER.warning("WEBHOOK_URL not set => polling (на Render будет 409).")
        application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
