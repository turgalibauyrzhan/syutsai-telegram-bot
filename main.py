#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Syucai Telegram Bot (Webhook + Google Sheets)

- Webhook mode for Render Web Service
- Reply-keyboard buttons for common commands
- After user sends birth date, bot immediately replies with today's forecast (no /today needed)
- Robust parsing (won't crash on unexpected updates)
- Google Sheets upsert in "old" column format:
  telegram_user_id, status, plan, trial_expires, birth_date, created_at, last_seen_at,
  username, first_name, last_name, registered_on, last_full_ym
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Optional, Dict

import pytz
import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, ReplyKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# -----------------------
# Logging
# -----------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - syucai - %(levelname)s - %(message)s",
)
logger = logging.getLogger("syucai")

TZ_NAME = os.getenv("TZ", "Asia/Almaty")
TZ = pytz.timezone(TZ_NAME)

# -----------------------
# Env helpers
# -----------------------
def get_env_first(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default

# Telegram token (support multiple naming conventions)
TELEGRAM_TOKEN = get_env_first("TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "TOKEN", default="")
if not TELEGRAM_TOKEN:
    raise ValueError(
        "Telegram token is not set. Set Render env var TELEGRAM_BOT_TOKEN (recommended) or TELEGRAM_TOKEN."
    )

# Google Sheets
GSHEET_ID = get_env_first("GSHEET_ID", "GOOGLE_SHEET_ID", default="")
SUBS_SHEET_NAME = get_env_first("SUBS_SHEET_NAME", default="subscriptions")
GOOGLE_SA_JSON = get_env_first("GOOGLE_SA_JSON", default="")
GOOGLE_SA_JSON_B64 = get_env_first("GOOGLE_SA_JSON_B64", default="")

# Webhook (Render Web Service)
WEBHOOK_BASE_URL = get_env_first("WEBHOOK_BASE_URL", "PUBLIC_BASE_URL", "RENDER_EXTERNAL_URL", default="")
WEBHOOK_SECRET = get_env_first("WEBHOOK_SECRET", default="")
PORT = int(get_env_first("PORT", default="10000"))

# -----------------------
# Google Sheets schema (OLD FORMAT)
# -----------------------
OLD_COLUMNS = [
    "telegram_user_id",
    "status",
    "plan",
    "trial_expires",
    "birth_date",
    "created_at",
    "last_seen_at",
    "username",
    "first_name",
    "last_name",
    "registered_on",
    "last_full_ym",
]

@dataclass
class UserRecord:
    telegram_user_id: int
    status: str = "active"
    plan: str = "trial"
    trial_expires: str = ""      # YYYY-MM-DD
    birth_date: str = ""         # DD.MM.YYYY
    created_at: str = ""         # ISO
    last_seen_at: str = ""       # ISO
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    registered_on: str = ""      # YYYY-MM-DD
    last_full_ym: str = ""       # YYYY-MM

    def to_row(self) -> list[str]:
        return [
            str(self.telegram_user_id),
            self.status,
            self.plan,
            self.trial_expires,
            self.birth_date,
            self.created_at,
            self.last_seen_at,
            self.username,
            self.first_name,
            self.last_name,
            self.registered_on,
            self.last_full_ym,
        ]

class SheetsStore:
    def __init__(self) -> None:
        self._enabled = bool(GSHEET_ID and (GOOGLE_SA_JSON or GOOGLE_SA_JSON_B64))
        self._gc: Optional[gspread.Client] = None
        self._ws: Optional[gspread.Worksheet] = None
        self._id_to_row: Dict[int, int] = {}
        self._cache_loaded_at: float = 0.0

    def enabled(self) -> bool:
        return self._enabled

    def _load_sa_info(self) -> dict:
        if GOOGLE_SA_JSON:
            raw = GOOGLE_SA_JSON.strip()
            try:
                return json.loads(raw)
            except Exception:
                try:
                    decoded = base64.b64decode(raw).decode("utf-8")
                    return json.loads(decoded)
                except Exception as e:
                    raise ValueError(f"GOOGLE_SA_JSON is not valid JSON/base64: {e}") from e

        if GOOGLE_SA_JSON_B64:
            try:
                decoded = base64.b64decode(GOOGLE_SA_JSON_B64.strip()).decode("utf-8")
                return json.loads(decoded)
            except Exception as e:
                raise ValueError(f"GOOGLE_SA_JSON_B64 is not valid base64 JSON: {e}") from e

        raise ValueError("Service account JSON not provided.")

    def connect(self) -> None:
        if not self._enabled:
            return
        if self._ws is not None:
            return

        sa_info = self._load_sa_info()
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        self._gc = gspread.authorize(creds)

        sh = self._gc.open_by_key(GSHEET_ID)
        self._ws = sh.worksheet(SUBS_SHEET_NAME)

        self._ensure_header()
        self._refresh_cache()
        logger.info("Google Sheets ready: worksheet=%s", SUBS_SHEET_NAME)

    def _ensure_header(self) -> None:
        assert self._ws is not None
        values = self._ws.get_all_values()
        if not values:
            self._ws.append_row(OLD_COLUMNS)
            return
        header = values[0]
        if header != OLD_COLUMNS:
            logger.warning("Sheet header differs from expected OLD_COLUMNS. Expected=%s Got=%s", OLD_COLUMNS, header)

    def _refresh_cache(self) -> None:
        assert self._ws is not None
        values = self._ws.get_all_values()
        self._id_to_row = {}
        for idx, row in enumerate(values[1:], start=2):
            if not row:
                continue
            try:
                tid = int(row[0])
                self._id_to_row[tid] = idx
            except Exception:
                continue

    def _maybe_refresh_cache(self) -> None:
        now = asyncio.get_event_loop().time()
        if now - self._cache_loaded_at > 60:
            self._refresh_cache()
            self._cache_loaded_at = now

    def upsert_user(self, rec: UserRecord) -> None:
        if not self._enabled:
            return
        self.connect()
        assert self._ws is not None

        self._maybe_refresh_cache()
        row_idx = self._id_to_row.get(rec.telegram_user_id)

        if row_idx is None:
            self._ws.append_row(rec.to_row())
            self._id_to_row[rec.telegram_user_id] = self._ws.row_count
            return

        self._ws.update(f"A{row_idx}:L{row_idx}", [rec.to_row()])

    def get_user(self, telegram_user_id: int) -> Optional[UserRecord]:
        if not self._enabled:
            return None
        self.connect()
        assert self._ws is not None
        self._maybe_refresh_cache()
        row_idx = self._id_to_row.get(telegram_user_id)
        if row_idx is None:
            return None

        row = self._ws.row_values(row_idx)
        row = (row + [""] * len(OLD_COLUMNS))[: len(OLD_COLUMNS)]
        try:
            return UserRecord(
                telegram_user_id=int(row[0]),
                status=row[1] or "active",
                plan=row[2] or "trial",
                trial_expires=row[3],
                birth_date=row[4],
                created_at=row[5],
                last_seen_at=row[6],
                username=row[7],
                first_name=row[8],
                last_name=row[9],
                registered_on=row[10],
                last_full_ym=row[11],
            )
        except Exception:
            return None

STORE = SheetsStore()

# -----------------------
# Numerology (simple, stable)
# -----------------------
def reduce_to_1_9(n: int) -> int:
    n = abs(int(n))
    while n > 9:
        n = sum(int(ch) for ch in str(n) if ch.isdigit())
    return 9 if n == 0 else n

def od_for_date(d: date) -> int:
    digits = f"{d.day:02d}{d.month:02d}{d.year:04d}"
    return reduce_to_1_9(sum(int(ch) for ch in digits))

def personal_year(birth: date, current: date) -> int:
    s = sum(int(ch) for ch in f"{birth.day:02d}{birth.month:02d}") + sum(int(ch) for ch in f"{current.year:04d}")
    return reduce_to_1_9(s)

def personal_month(lg: int, current: date) -> int:
    return reduce_to_1_9(lg + reduce_to_1_9(current.month))

def personal_day(lm: int, current: date) -> int:
    return reduce_to_1_9(lm + reduce_to_1_9(current.day))

OD_TEXT = {
    1: "День лидерства и новых начинаний. Хорошо брать инициативу и запускать дела.",
    2: "День дипломатии и партнерства. Подходит для переговоров и мягких решений.",
    3: "День удачи, простых решений и быстрых результатов. Хорош для стартов, поездок, встреч, общения.",
    4: "День порядка и дисциплины. Лучше закрывать хвосты, заниматься документами и рутиной.",
    5: "День движения и перемен. Хорош для поездок, обучения, экспериментов и гибкости.",
    6: "День семьи и ответственности. Подходит для заботы, дома, красоты, отношений.",
    7: "День анализа и уединения. Хорош для планирования, учебы, глубокой работы.",
    8: "День денег и результатов. Подходит для сделок, управления, KPI, финансов.",
    9: "День завершений и подведения итогов. Хорош закрывать циклы и отпускать лишнее.",
}

DATE_RE = re.compile(r"^\s*(\d{1,2})\.(\d{1,2})\.(\d{4})\s*$")

def parse_birth_date(text: str) -> Optional[date]:
    m = DATE_RE.match(text or "")
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    try:
        return date(yyyy, mm, dd)
    except Exception:
        return None

# -----------------------
# UI / keyboards
# -----------------------
BTN_TODAY = "📅 Сегодня"
BTN_SET_BDAY = "🧑‍🎂 Указать дату рождения"
BTN_HELP = "ℹ️ Помощь"
BTN_STATUS = "👤 Профиль"

MAIN_KB = ReplyKeyboardMarkup(
    [[BTN_TODAY, BTN_SET_BDAY], [BTN_STATUS, BTN_HELP]],
    resize_keyboard=True,
)

# -----------------------
# Helpers
# -----------------------
def now_iso() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")

def today_local() -> date:
    return datetime.now(TZ).date()

def ym_local() -> str:
    d = today_local()
    return f"{d.year:04d}-{d.month:02d}"

def ensure_user_record(update: Update) -> UserRecord:
    u = update.effective_user
    assert u is not None

    rec = STORE.get_user(u.id) or UserRecord(telegram_user_id=u.id)

    if not rec.created_at:
        rec.created_at = now_iso()
    if not rec.registered_on:
        rec.registered_on = today_local().isoformat()

    rec.last_seen_at = now_iso()
    rec.username = u.username or ""
    rec.first_name = u.first_name or ""
    rec.last_name = u.last_name or ""

    if not rec.trial_expires:
        rec.trial_expires = (today_local() + timedelta(days=7)).isoformat()
        rec.plan = rec.plan or "trial"
        rec.status = rec.status or "active"

    return rec

def is_trial_active(rec: UserRecord) -> bool:
    if rec.plan != "trial":
        return False
    try:
        exp = date.fromisoformat(rec.trial_expires)
        return today_local() <= exp
    except Exception:
        return True

def format_forecast(d: date, rec: Optional[UserRecord], bday: Optional[date]) -> str:
    od = od_for_date(d)
    lines = [
        f"📅 *Дата:* {d.strftime('%d.%m.%Y')}",
        f"🌐 *Общий день (ОД):* {od}",
        OD_TEXT.get(od, ""),
        "",
    ]

    if bday:
        lg = personal_year(bday, d)
        lm = personal_month(lg, d)
        ld = personal_day(lm, d)
        lines += [f"🧮 *ЛГ / ЛМ / ЛД:* {lg} / {lm} / {ld}"]
    else:
        lines += ["Чтобы считать *ЛГ/ЛМ/ЛД*, пришли дату рождения в формате *ДД.ММ.ГГГГ* (например 05.11.1992)."]

    if rec:
        if rec.plan == "trial" and is_trial_active(rec):
            lines += ["🎁 *Trial активен.*"]
        elif rec.plan == "trial":
            lines += ["⛔ *Trial закончился.*"]
        elif rec.plan:
            lines += [f"💳 *План:* {rec.plan}"]

    return "\n".join([x for x in lines if x is not None])

# -----------------------
# Handlers
# -----------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rec = ensure_user_record(update)
    try:
        STORE.upsert_user(rec)
    except Exception as e:
        logger.warning("Sheets upsert failed in /start: %s", e)

    text = (
        "Привет! Я *Сюцай_Бот*.\n\n"
        "• Нажми *📅 Сегодня* — прогноз на сегодня.\n"
        "• Отправь дату рождения *ДД.ММ.ГГГГ* — сохраню и сразу дам прогноз + ЛГ/ЛМ/ЛД.\n"
    )
    await update.effective_chat.send_message(text, parse_mode=ParseMode.MARKDOWN, reply_markup=MAIN_KB)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Команды:\n"
        "• /start — меню\n"
        "• /today — прогноз на сегодня\n"
        "• отправь дату рождения ДД.ММ.ГГГГ — сохраню и сразу дам прогноз\n"
    )
    await update.effective_chat.send_message(text, reply_markup=MAIN_KB)

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rec = ensure_user_record(update)
    bday = parse_birth_date(rec.birth_date) if rec.birth_date else None
    msg = format_forecast(today_local(), rec, bday)

    if bday:
        rec.last_full_ym = ym_local()

    try:
        STORE.upsert_user(rec)
    except Exception as e:
        logger.warning("Sheets upsert failed in /today: %s", e)

    await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=MAIN_KB)

async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rec = ensure_user_record(update)
    text = (
        f"👤 *Профиль*\n"
        f"• ID: `{rec.telegram_user_id}`\n"
        f"• Birth date: *{rec.birth_date or '—'}*\n"
        f"• Plan: *{rec.plan}*\n"
        f"• Trial expires: *{rec.trial_expires or '—'}*\n"
        f"• Status: *{rec.status}*\n"
    )
    try:
        STORE.upsert_user(rec)
    except Exception as e:
        logger.warning("Sheets upsert failed in profile: %s", e)

    await update.effective_chat.send_message(text, parse_mode=ParseMode.MARKDOWN, reply_markup=MAIN_KB)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message:
        return
    text = (update.effective_message.text or "").strip()

    # Button mapping
    if text == BTN_TODAY:
        await cmd_today(update, context)
        return
    if text == BTN_HELP:
        await cmd_help(update, context)
        return
    if text == BTN_STATUS:
        await cmd_profile(update, context)
        return
    if text == BTN_SET_BDAY:
        await update.effective_chat.send_message(
            "Отправь дату рождения в формате *ДД.ММ.ГГГГ* (например 05.11.1992).",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=MAIN_KB,
        )
        return

    # Birth date parsing -> save + immediate forecast
    bday = parse_birth_date(text)
    if bday:
        rec = ensure_user_record(update)
        rec.birth_date = bday.strftime("%d.%m.%Y")
        rec.last_full_ym = ym_local()
        msg = format_forecast(today_local(), rec, bday)

        try:
            STORE.upsert_user(rec)
        except Exception as e:
            logger.warning("Sheets upsert failed after bday: %s", e)

        await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=MAIN_KB)
        return

    await update.effective_chat.send_message(
        "Не понял. Нажми *📅 Сегодня* или отправь дату рождения *ДД.ММ.ГГГГ*.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=MAIN_KB,
    )

# -----------------------
# Daily broadcast (optional)
# -----------------------
async def daily_broadcast(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not STORE.enabled():
        return
    try:
        STORE.connect()
    except Exception as e:
        logger.warning("Daily broadcast skipped: sheets not ready: %s", e)
        return

    ws = STORE._ws
    if ws is None:
        return

    values = ws.get_all_values()
    if len(values) <= 1:
        return

    d = today_local()
    for row in values[1:]:
        try:
            tid = int(row[0])
            status = (row[1] or "").strip().lower()
            if status == "blocked":
                continue
            birth = row[4] if len(row) > 4 else ""
            bday = parse_birth_date(birth) if birth else None
            rec = STORE.get_user(tid)
            msg = format_forecast(d, rec, bday)
            await context.bot.send_message(chat_id=tid, text=msg, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            continue

def schedule_daily_job(app: Application) -> None:
    hour = int(get_env_first("DAILY_HOUR", default="9"))
    minute = int(get_env_first("DAILY_MINUTE", default="0"))
    app.job_queue.run_daily(
        daily_broadcast,
        time=datetime.now(TZ).replace(hour=hour, minute=minute, second=0, microsecond=0).timetz(),
        name="daily_broadcast",
    )
    logger.info("Daily broadcast scheduled at %02d:%02d %s", hour, minute, TZ_NAME)

# -----------------------
# Webhook run
# -----------------------
def stable_secret_from_token(token: str) -> str:
    import hashlib
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]

def main() -> None:
    if STORE.enabled():
        try:
            STORE.connect()
        except Exception as e:
            logger.warning("Google Sheets not ready: %s", e)

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("profile", cmd_profile))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    try:
        schedule_daily_job(app)
    except Exception as e:
        logger.warning("JobQueue not scheduled: %s", e)

    secret = WEBHOOK_SECRET or stable_secret_from_token(TELEGRAM_TOKEN)
    path = f"/telegram/webhook/{secret}"

    if not WEBHOOK_BASE_URL:
        logger.error("WEBHOOK_BASE_URL is not set. Example: https://<name>.onrender.com")

    webhook_url = (WEBHOOK_BASE_URL.rstrip("/") + path) if WEBHOOK_BASE_URL else None
    logger.info("Webhook server 0.0.0.0:%d path=%s => %s", PORT, path, webhook_url or "NO_WEBHOOK_URL")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=path.lstrip("/"),
        webhook_url=webhook_url,
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )

if __name__ == "__main__":
    main()
