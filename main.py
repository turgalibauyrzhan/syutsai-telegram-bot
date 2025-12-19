#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Syutsai Telegram bot (Render webhook-ready) + Google Sheets user registry.

Key features:
- Webhook mode for Render Web Service (binds PORT).
- Saves/updates users in Google Sheet with columns:
  telegram_user_id, status, plan, trial_expires, birth_date, created_at, last_seen_at,
  username, first_name, last_name, registered_on, last_full_ym
- User sends birth date once (ДД.ММ.ГГГГ) -> bot replies with forecast immediately (no /today needed).
- Daily forecast: full ОД + ЛД; ЛМ + ЛГ кратко.
- First ever forecast for user, and on 1st day of each month: full ОД + ЛД + ЛМ + ЛГ.
- Reply keyboard buttons for common commands.
"""

import base64
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

TZ = ZoneInfo("Asia/Almaty")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set (Render env var).")

# Render Web Service port (required for webhook)
PORT = int(os.getenv("PORT", "10000"))
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")  # e.g. https://syutsai-telegram-bot.onrender.com
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram/webhook").rstrip("/")  # fixed path
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "syutsai")  # just to make URL harder to guess

# Google Sheets
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "users").strip()
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()

# Plans
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "7"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - syucai - %(levelname)s - %(message)s",
)
log = logging.getLogger("syucai")

# -----------------------------------------------------------------------------
# Texts (Описания)
# -----------------------------------------------------------------------------

# Общий день (ОД) — полный
GENERAL_DAY_FULL: Dict[int, str] = {
    1: "День энергии, инициативы и начала. Хорош для стартов, знакомств, решений «с нуля».",
    2: "День партнерства и дипломатии. Лучше договариваться, слушать, делать аккуратные шаги.",
    3: "День удачи, простых решений и быстрых результатов. Хорош для стартов, поездок, встреч, общения.",
    4: "День порядка и дисциплины. Лучше закрывать хвосты, заниматься документами и рутиной.",
    5: "День перемен и движения. Подходит для дороги, новых идей, гибких решений.",
    6: "День семьи и заботы. Хорош для отношений, дома, помощи, красоты, гармонизации.",
    7: "День анализа и внутреннего фокуса. Лучше думать, планировать, учиться, не суетиться.",
    8: "День денег и результата. Подходит для переговоров, бизнеса, управления, финансовых решений.",
    9: "День завершения и очистки. Хорошо закрывать дела, отпускать лишнее, подводить итоги.",
}

# Личные циклы — FULL (встроены)
PERSONAL_YEAR_FULL: Dict[int, str] = {
    1: "Личный год 1. Это время старта, новых проектов и возможностей. Захочется перемен, самостоятельности, свободы. Хорошо начинать с нуля: смена работы, запуск бизнеса, новые цели. Важно не бояться и брать инициативу в свои руки.",
    2: "Личный год 2. Год партнерства, отношений и сотрудничества. Будет больше эмоций, чувствительности, желания близости. Хорошо строить союзы, налаживать контакты, заключать договоры. Важно учиться терпению и дипломатии.",
    3: "Личный год 3. Год общения, творчества и расширения круга знакомств. Легче проявлять себя, учиться, выступать, продвигать идеи. Возможны путешествия, новые хобби, публичность. Следи, чтобы не распыляться.",
    4: "Личный год 4. Год труда, дисциплины и укрепления фундамента. Подходит для системной работы, накопления, оформления документов, ремонта, обучения профессии. Может быть ощущение рутины — это нормально: ты строишь базу на годы вперед.",
    5: "Личный год 5. Год перемен, свободы, поездок и неожиданных возможностей. Часто меняется работа/окружение, появляются новые предложения. Хорошо пробовать новое, но важно держать рамки и не влезать в риск без расчета.",
    6: "Личный год 6. Год семьи, ответственности и гармонии. Вопросы отношений, дома, детей, заботы будут в фокусе. Хорошо укреплять связи, создавать уют, решать семейные дела. Важно не тащить всё одному.",
    7: "Личный год 7. Год анализа, обучения и внутреннего роста. Больше тяги к знаниям, одиночеству, переоценке ценностей. Хорошо учиться, планировать, работать глубоко. Не всегда «видимый» прогресс, но сильные внутренние изменения.",
    8: "Личный год 8. Год денег, власти и результата. Хорош для бизнеса, карьеры, роста дохода, управления. Возможны крупные сделки/покупки. Важно действовать честно и системно: 8-й год быстро возвращает последствия.",
    9: "Личный год 9. Год завершения и очищения. Закрываются старые циклы, уходят лишние связи и дела. Хорошо завершать проекты, отдавать долги, подводить итоги. Не лучший год для «приклеивания» к старому — освобождай место новому.",
}

PERSONAL_MONTH_FULL: Dict[int, str] = {
    1: "Личный месяц 1. Месяц инициативы и новых стартов. Хорошо начинать проекты, делать первые шаги, проявлять лидерство.",
    2: "Личный месяц 2. Месяц отношений и сотрудничества. Важно договариваться, укреплять связи, быть гибче и мягче.",
    3: "Личный месяц 3. Месяц общения и творчества. Подходит для выступлений, продвижения, знакомств, обучения и поездок.",
    4: "Личный месяц 4. Месяц дисциплины и рутины. Хорошо закрывать задачи, наводить порядок, заниматься документами.",
    5: "Личный месяц 5. Месяц перемен и движения. Хорош для поездок, изменений, быстрых решений, экспериментов.",
    6: "Личный месяц 6. Месяц семьи и заботы. Хорошо уделять внимание дому, отношениям, здоровью, красоте и комфорту.",
    7: "Личный месяц 7. Месяц анализа и обучения. Подходит для глубоких задач, чтения, планирования и спокойной работы.",
    8: "Личный месяц 8. Месяц денег и результатов. Хорош для бизнеса, переговоров, роста доходов, покупок и управления.",
    9: "Личный месяц 9. Месяц завершения. Хорошо закрывать хвосты, завершать проекты, отпускать лишнее и подводить итоги.",
}

PERSONAL_DAY_FULL: Dict[int, str] = {
    1: "Личный день 1. День инициативы и действий. Хорошо делать первые шаги, запускать, решать быстро и прямо.",
    2: "Личный день 2. День мягкости и взаимодействия. Лучше договариваться, просить помощь, работать в паре.",
    3: "Личный день 3. День общения и легкости. Подходит для встреч, переписок, презентаций, обучения и идей.",
    4: "Личный день 4. День дисциплины и порядка. Хорошо закрывать задачи, заниматься документами и рутиной.",
    5: "Личный день 5. День движения и перемен. Подходит для поездок, новых решений, переключения и экспериментов.",
    6: "Личный день 6. День заботы и гармонии. Хорошо уделить внимание семье, дому, отношениям и здоровью.",
    7: "Личный день 7. День анализа и тишины. Лучше думать, учиться, планировать, не перегружать общением.",
    8: "Личный день 8. День результата и денег. Подходит для бизнеса, переговоров, контроля, финансовых решений.",
    9: "Личный день 9. День завершения. Хорошо завершать, чистить, отпускать, подводить итоги и отдыхать.",
}

def _shorten(text: str, limit: int = 140) -> str:
    t = re.sub(r"\s+", " ", text).strip()
    parts = re.split(r"(?<=[.!?])\s+", t)
    first = parts[0] if parts else t
    if len(first) <= limit:
        return first
    return first[: limit - 1].rstrip() + "…"

PERSONAL_YEAR_SHORT = {k: _shorten(v) for k, v in PERSONAL_YEAR_FULL.items()}
PERSONAL_MONTH_SHORT = {k: _shorten(v) for k, v in PERSONAL_MONTH_FULL.items()}

# -----------------------------------------------------------------------------
# Google Sheet storage
# -----------------------------------------------------------------------------

COLUMNS = [
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
class UserRow:
    telegram_user_id: int
    status: str = "active"
    plan: str = "trial"
    trial_expires: str = ""
    birth_date: str = ""         # DD.MM.YYYY
    created_at: str = ""         # ISO datetime
    last_seen_at: str = ""       # ISO datetime
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    registered_on: str = ""      # ISO date
    last_full_ym: str = ""       # YYYY-MM

class SheetStore:
    def __init__(self) -> None:
        self.enabled = bool(GOOGLE_SHEET_ID and GOOGLE_SERVICE_ACCOUNT_B64)
        self.ws = None
        self.header_index = {}
        if self.enabled:
            self._connect()

    def _connect(self) -> None:
        try:
            raw = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64.encode("utf-8")).decode("utf-8")
            info = json.loads(raw)
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_info(info, scopes=scopes)
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(GOOGLE_SHEET_ID)
            self.ws = sh.worksheet(GOOGLE_SHEET_TAB)
            self._ensure_header()
            log.info("Google Sheet connected: tab=%s", GOOGLE_SHEET_TAB)
        except Exception as e:
            log.exception("Google Sheet connect failed, continuing without Sheets: %s", e)
            self.enabled = False
            self.ws = None

    def _ensure_header(self) -> None:
        assert self.ws is not None
        row1 = self.ws.row_values(1)
        if [c.strip() for c in row1] != COLUMNS:
            self.ws.resize(rows=max(self.ws.row_count, 2), cols=len(COLUMNS))
            self.ws.update("A1", [COLUMNS])
        self.header_index = {name: i + 1 for i, name in enumerate(COLUMNS)}

    def _find_row(self, telegram_user_id: int) -> Optional[int]:
        assert self.ws is not None
        col = self.ws.col_values(1)
        target = str(telegram_user_id)
        for i, v in enumerate(col[1:], start=2):
            if v == target:
                return i
        return None

    def get(self, telegram_user_id: int) -> UserRow:
        now = datetime.now(TZ).isoformat(timespec="seconds")
        if not self.enabled or self.ws is None:
            return UserRow(
                telegram_user_id=telegram_user_id,
                created_at=now,
                registered_on=date.today().isoformat(),
                last_seen_at=now,
            )
        row_idx = self._find_row(telegram_user_id)
        if row_idx is None:
            ur = UserRow(
                telegram_user_id=telegram_user_id,
                created_at=now,
                registered_on=date.today().isoformat(),
                last_seen_at=now,
            )
            self.upsert(ur)
            return ur
        values = self.ws.row_values(row_idx)
        data = {COLUMNS[i]: (values[i] if i < len(values) else "") for i in range(len(COLUMNS))}
        return UserRow(
            telegram_user_id=int(data["telegram_user_id"] or telegram_user_id),
            status=data["status"] or "active",
            plan=data["plan"] or "trial",
            trial_expires=data["trial_expires"] or "",
            birth_date=data["birth_date"] or "",
            created_at=data["created_at"] or now,
            last_seen_at=data["last_seen_at"] or now,
            username=data["username"] or "",
            first_name=data["first_name"] or "",
            last_name=data["last_name"] or "",
            registered_on=data["registered_on"] or date.today().isoformat(),
            last_full_ym=data["last_full_ym"] or "",
        )

    def upsert(self, ur: UserRow) -> None:
        if not self.enabled or self.ws is None:
            return
        self._ensure_header()
        row_idx = self._find_row(ur.telegram_user_id)
        row = [
            str(ur.telegram_user_id),
            ur.status,
            ur.plan,
            ur.trial_expires,
            ur.birth_date,
            ur.created_at,
            ur.last_seen_at,
            ur.username,
            ur.first_name,
            ur.last_name,
            ur.registered_on,
            ur.last_full_ym,
        ]
        if row_idx is None:
            self.ws.append_row(row, value_input_option="RAW")
        else:
            self.ws.update(f"A{row_idx}", [row], value_input_option="RAW")

store = SheetStore()

# -----------------------------------------------------------------------------
# Numerology helpers
# -----------------------------------------------------------------------------

def reduce_1_9(n: int) -> int:
    n = abs(int(n))
    while n > 9:
        s = 0
        for ch in str(n):
            s += ord(ch) - 48
        n = s
    return 9 if n == 0 else n

def general_day(d: date) -> int:
    s = sum(int(ch) for ch in d.strftime("%d%m%Y"))
    return reduce_1_9(s)

def personal_year(birth: date, d: date) -> int:
    s = sum(int(ch) for ch in birth.strftime("%d%m")) + sum(int(ch) for ch in d.strftime("%Y"))
    return reduce_1_9(s)

def personal_month(py: int, d: date) -> int:
    return reduce_1_9(py + d.month)

def personal_day(pm: int, d: date) -> int:
    return reduce_1_9(pm + d.day)

DATE_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")

def parse_birth_date(text: str) -> Optional[date]:
    m = DATE_RE.match(text.strip())
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None

# -----------------------------------------------------------------------------
# Message builder
# -----------------------------------------------------------------------------

def should_send_full_all(ur: UserRow, today: date) -> Tuple[bool, str]:
    ym = today.strftime("%Y-%m")
    if not ur.last_full_ym:
        return True, ym
    if today.day == 1 and ur.last_full_ym != ym:
        return True, ym
    return False, ur.last_full_ym

def format_forecast(today: date, birth: date, ur: UserRow) -> Tuple[str, str]:
    od = general_day(today)
    py = personal_year(birth, today)
    pm = personal_month(py, today)
    pd = personal_day(pm, today)

    full_all, new_last_full_ym = should_send_full_all(ur, today)

    od_text = GENERAL_DAY_FULL.get(od, "")
    pd_text = PERSONAL_DAY_FULL.get(pd, "")
    pm_full = PERSONAL_MONTH_FULL.get(pm, "")
    py_full = PERSONAL_YEAR_FULL.get(py, "")
    pm_short = PERSONAL_MONTH_SHORT.get(pm, pm_full)
    py_short = PERSONAL_YEAR_SHORT.get(py, py_full)

    lines = []
    lines.append(f"📅 <b>Дата:</b> {today.strftime('%d.%m.%Y')}")
    lines.append(f"🌐 <b>Общий день (ОД):</b> {od}")
    if od_text:
        lines.append(od_text)

    lines.append("")
    lines.append(f"🧮 <b>ЛГ / ЛМ / ЛД:</b> {py} / {pm} / {pd}")

    if full_all:
        if py_full:
            lines.append("")
            lines.append(f"📌 <b>Личный год (ЛГ) {py}:</b> {py_full}")
        if pm_full:
            lines.append("")
            lines.append(f"📌 <b>Личный месяц (ЛМ) {pm}:</b> {pm_full}")
        if pd_text:
            lines.append("")
            lines.append(f"📌 <b>Личный день (ЛД) {pd}:</b> {pd_text}")
    else:
        if pd_text:
            lines.append("")
            lines.append(f"📌 <b>Личный день (ЛД) {pd}:</b> {pd_text}")
        if pm_short:
            lines.append("")
            lines.append(f"🗓️ <b>Личный месяц (ЛМ) {pm}:</b> {pm_short}")
        if py_short:
            lines.append("")
            lines.append(f"📈 <b>Личный год (ЛГ) {py}:</b> {py_short}")

    plan_line = f"💳 <b>План:</b> {ur.plan}"
    if ur.plan == "trial" and ur.trial_expires:
        plan_line += f" • 🎁 Trial до {ur.trial_expires}"
    lines.append("")
    lines.append(plan_line)

    return "\n".join(lines).strip(), new_last_full_ym

def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("/today"), KeyboardButton("/status")],
            [KeyboardButton("/help"), KeyboardButton("/premium")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        selective=False,
    )

# -----------------------------------------------------------------------------
# Handlers
# -----------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = update.effective_user
    ur = store.get(u.id)

    now = datetime.now(TZ).isoformat(timespec="seconds")
    ur.last_seen_at = now
    ur.username = u.username or ""
    ur.first_name = u.first_name or ""
    ur.last_name = u.last_name or ""
    if not ur.created_at:
        ur.created_at = now
    if not ur.registered_on:
        ur.registered_on = date.today().isoformat()

    if not ur.trial_expires:
        ur.plan = "trial"
        ur.trial_expires = (date.today() + timedelta(days=TRIAL_DAYS)).isoformat()

    store.upsert(ur)

    msg = (
        "Привет! Пришли дату рождения в формате <b>ДД.ММ.ГГГГ</b> (например 05.11.1992) — "
        "и я сразу дам прогноз на сегодня.\n\n"
        "Команды: /today, /status"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=main_keyboard())

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = (
        "Команды:\n"
        "• /today — прогноз на сегодня\n"
        "• /status — твой план/триал и сохранённая дата рождения\n\n"
        "Можно просто отправить дату рождения: <b>ДД.ММ.ГГГГ</b> — прогноз придёт сразу."
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=main_keyboard())

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = update.effective_user
    ur = store.get(u.id)
    now = datetime.now(TZ).isoformat(timespec="seconds")
    ur.last_seen_at = now
    store.upsert(ur)

    bd = ur.birth_date or "не задана"
    trial = ur.trial_expires or "—"
    msg = (
        f"👤 <b>Пользователь:</b> {u.first_name or ''} {u.last_name or ''}\n"
        f"🆔 <b>telegram_user_id:</b> <code>{u.id}</code>\n"
        f"🎂 <b>Дата рождения:</b> {bd}\n"
        f"💳 <b>План:</b> {ur.plan}\n"
        f"🎁 <b>Trial до:</b> {trial}"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=main_keyboard())

async def cmd_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = "Премиум пока подключается вручную. Напиши администратору, и мы активируем доступ."
    await update.message.reply_text(msg, reply_markup=main_keyboard())

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = update.effective_user
    ur = store.get(u.id)
    now_dt = datetime.now(TZ)
    ur.last_seen_at = now_dt.isoformat(timespec="seconds")

    if not ur.birth_date:
        store.upsert(ur)
        await update.message.reply_text(
            "Чтобы посчитать ЛГ/ЛМ/ЛД, пришли дату рождения в формате <b>ДД.ММ.ГГГГ</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(),
        )
        return

    b = parse_birth_date(ur.birth_date)
    if not b:
        ur.birth_date = ""
        store.upsert(ur)
        await update.message.reply_text(
            "Не смог прочитать дату рождения. Пришли заново: <b>ДД.ММ.ГГГГ</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(),
        )
        return

    text, new_last_full_ym = format_forecast(now_dt.date(), b, ur)
    ur.last_full_ym = new_last_full_ym
    store.upsert(ur)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_keyboard())

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return

    txt = update.message.text.strip()
    u = update.effective_user
    ur = store.get(u.id)

    now_dt = datetime.now(TZ)
    ur.last_seen_at = now_dt.isoformat(timespec="seconds")
    ur.username = u.username or ""
    ur.first_name = u.first_name or ""
    ur.last_name = u.last_name or ""
    if not ur.created_at:
        ur.created_at = ur.last_seen_at
    if not ur.registered_on:
        ur.registered_on = date.today().isoformat()

    b = parse_birth_date(txt)
    if b:
        ur.birth_date = txt
        if not ur.trial_expires:
            ur.plan = "trial"
            ur.trial_expires = (date.today() + timedelta(days=TRIAL_DAYS)).isoformat()

        text, new_last_full_ym = format_forecast(now_dt.date(), b, ur)
        ur.last_full_ym = new_last_full_ym
        store.upsert(ur)
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        return

    store.upsert(ur)
    await update.message.reply_text(
        "Напиши /today или пришли дату рождения <b>ДД.ММ.ГГГГ</b> (например 05.11.1992).",
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
    )

# -----------------------------------------------------------------------------
# Daily broadcast (optional)
# -----------------------------------------------------------------------------

async def job_daily_broadcast(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not store.enabled or store.ws is None:
        return
    ws = store.ws
    col_user_id = ws.col_values(1)[1:]
    col_status = ws.col_values(2)[1:]
    col_birth = ws.col_values(5)[1:]
    col_last_full_ym = ws.col_values(12)[1:] if ws.col_values(12) else [""] * len(col_user_id)

    today = datetime.now(TZ).date()
    ym = today.strftime("%Y-%m")

    for i, uid_str in enumerate(col_user_id):
        try:
            uid = int(uid_str)
        except Exception:
            continue

        status = (col_status[i] if i < len(col_status) else "active") or "active"
        bd_str = (col_birth[i] if i < len(col_birth) else "") or ""
        if status != "active" or not bd_str:
            continue

        b = parse_birth_date(bd_str)
        if not b:
            continue

        ur = UserRow(
            telegram_user_id=uid,
            status=status,
            plan="trial",
            trial_expires="",
            birth_date=bd_str,
            created_at="",
            last_seen_at="",
            last_full_ym=(col_last_full_ym[i] if i < len(col_last_full_ym) else "") or "",
        )

        text, new_last_full_ym = format_forecast(today, b, ur)

        if new_last_full_ym != ur.last_full_ym:
            row_idx = i + 2
            ws.update_cell(row_idx, 12, new_last_full_ym)

        try:
            await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        except Exception as e:
            log.info("Broadcast to %s failed: %s", uid, e)

def schedule_daily(application: Application) -> None:
    application.job_queue.run_daily(
        job_daily_broadcast,
        time=datetime.strptime("09:00", "%H:%M").time(),
        name="daily_broadcast",
    )

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def build_webhook_url() -> str:
    if not PUBLIC_URL:
        raise ValueError("PUBLIC_URL env var is required for webhook mode (e.g. https://<service>.onrender.com)")
    return f"{PUBLIC_URL}{WEBHOOK_PATH}/{WEBHOOK_SECRET}"

def main() -> None:
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("today", cmd_today))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("premium", cmd_premium))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    try:
        schedule_daily(application)
        log.info("Daily broadcast scheduled at 09:00 %s", TZ)
    except Exception as e:
        log.info("JobQueue not available, skipping daily schedule: %s", e)

    webhook_url = build_webhook_url()
    log.info("Webhook server %s:%s path=%s/%s => %s", "0.0.0.0", PORT, WEBHOOK_PATH, WEBHOOK_SECRET, webhook_url)

    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=f"{WEBHOOK_PATH}/{WEBHOOK_SECRET}".lstrip("/"),
        webhook_url=webhook_url,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()
import os
import re
import json
import base64
import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =========================
# LOGGING
# =========================
LOG = logging.getLogger("syucai")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - syucai - %(levelname)s - %(message)s",
)

TZ = ZoneInfo("Asia/Almaty")

# =========================
# ENV / CONFIG
# =========================

def env_first(*names: str) -> str:
    """Return first non-empty env var among names."""
    for n in names:
        v = os.getenv(n, "").strip()
        if v:
            return v
    return ""

TELEGRAM_TOKEN = env_first("TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "TOKEN")
GSHEET_ID = env_first("GSHEET_ID", "GOOGLE_SHEET_ID")
SUBS_SHEET_NAME = env_first("SUBS_SHEET_NAME", "SHEET_NAME") or "subscriptions"

# Для webhook на Render:
# PUBLIC_URL = https://<твоя-ссылка>.onrender.com  (без / в конце)
PUBLIC_URL = env_first("PUBLIC_URL", "RENDER_EXTERNAL_URL", "SERVICE_URL")

# Путь для webhook — можно любой, но лучше рандомный
WEBHOOK_SECRET_PATH = env_first("WEBHOOK_SECRET_PATH") or "telegram/webhook/8f3b2c1a"
WEBHOOK_PATH = f"/{WEBHOOK_SECRET_PATH.lstrip('/')}"

PORT = int(env_first("PORT") or "10000")

# Google SA json: либо строкой JSON (в одну линию), либо base64
GOOGLE_SA_JSON = env_first("GOOGLE_SA_JSON")
GOOGLE_SA_JSON_B64 = env_first("GOOGLE_SA_JSON_B64")

# =========================
# UI (keyboard)
# =========================

KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📅 Сегодня"), KeyboardButton("🧾 Мой статус")],
        [KeyboardButton("🎂 Изменить ДР"), KeyboardButton("ℹ️ Помощь")],
    ],
    resize_keyboard=True,
)

# =========================
# TEXTS (логика из твоего описания)
# =========================

DATE_RE = re.compile(r"^\s*(\d{2})\.(\d{2})\.(\d{4})\s*$")

def reduce_1_9(n: int) -> int:
    """Digital root in 1..9."""
    n = abs(int(n))
    while n > 9:
        s = 0
        for ch in str(n):
            s += ord(ch) - 48
        n = s
    return 9 if n == 0 else n

def parse_ddmmyyyy(s: str) -> Optional[date]:
    m = DATE_RE.match(s or "")
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None

# ---- Общий день: считаем по сумме цифр всей даты.
# Плюс спец-правило: 10/20/30 любого месяца -> текст предупреждения для ОД=9.
OD_WARNING_TEXT = (
    "Сегодня Общий день: 9. Сегодня нежелательно начинать новые проекты, "
    "лучше завершать дела и закрывать хвосты. Перенеси на другой день крупные покупки, "
    "договоры, кредиты и т.д."
)

OD_TEXT_FULL = {
    1: "День инициативы и самостоятельности. Хорош для новых стартов, решений и лидерства. "
       "Действуй напрямую, без лишних согласований.",
    2: "День сотрудничества и дипломатии. Лучше договариваться, слушать, выстраивать контакт. "
       "Не дави — работай мягко.",
    3: "Благоприятный день через анализ, успех и мышление. Отлично подходит для серьезных решений, "
       "подписания договоров и совершения покупок.",
    4: "День порядка и дисциплины. Лучше закрывать хвосты, заниматься документами и рутиной. "
       "Планируй, структурируй, наводи порядок.",
    5: "День общения и движения. Хорош для встреч, переговоров, поездок, продаж и активностей. "
       "Главное — не распыляйся.",
    6: "Благоприятный день через любовь, успех и гармонию. Хорош для семьи, отношений, красоты, "
       "покупок и начала больших проектов. Подходит для подписания договоров.",
    7: "День анализа и уединения. Хорош для обучения, чтения, аналитики, внутренней настройки. "
       "Не перегружай себя шумом.",
    8: "День денег и результата. Хорош для работы, управления, финансовых решений, крупных задач. "
       "Действуй прагматично.",
    9: "День завершения. Закрывай дела, отдавай долги, подводи итоги, освобождай место под новое. "
       "Не стартуй лишнего."
}

OD_TEXT_SHORT = {
    k: v.split(".")[0] + "." for k, v in OD_TEXT_FULL.items()
}

# ---- Личный год/месяц/день: по твоей логике:
# ЛГ = ДР(день)+ДР(месяц)+текущий год
# ЛМ = ЛГ + текущий месяц
# ЛД = ЛМ + текущий день
def calc_personal_year(birth: date, today: date) -> int:
    return reduce_1_9(birth.day + birth.month + sum(int(c) for c in str(today.year)))

def calc_personal_month(py: int, today: date) -> int:
    return reduce_1_9(py + today.month)

def calc_personal_day(pm: int, today: date) -> int:
    return reduce_1_9(pm + today.day)

# Минимальные тексты (чтобы НЕ “пропали” и бот работал всегда).
# Если хочешь — потом заменим на твои “большие” описания 1..9 из твоего файла/методики.
LG_SHORT = {i: f"Личный год {i}." for i in range(1, 10)}
LM_SHORT = {i: f"Личный месяц {i}." for i in range(1, 10)}
LD_SHORT = {i: f"Личный день {i}." for i in range(1, 10)}

LG_FULL = {i: f"Ваш Личный год {i}. (полное описание)" for i in range(1, 10)}
LM_FULL = {i: f"Личный месяц {i}. (полное описание)" for i in range(1, 10)}
LD_FULL = {i: f"Личный день {i}. (полное описание)" for i in range(1, 10)}

# =========================
# GOOGLE SHEETS (старый формат колонок)
# =========================

COLUMNS = [
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
class UserRow:
    telegram_user_id: int
    status: str = "active"
    plan: str = "trial"
    trial_expires: str = ""         # YYYY-MM-DD
    birth_date: str = ""            # DD.MM.YYYY
    created_at: str = ""            # ISO
    last_seen_at: str = ""          # ISO
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    registered_on: str = ""         # YYYY-MM-DD
    last_full_ym: str = ""          # YYYY-MM

class SheetsStore:
    def __init__(self):
        self._ready = False
        self._ws = None

    def ready(self) -> bool:
        return self._ready and self._ws is not None

    def init(self) -> None:
        if not GSHEET_ID:
            LOG.warning("GSHEET_ID is empty; Google Sheets disabled.")
            return

        sa_json = ""
        if GOOGLE_SA_JSON.strip():
            sa_json = GOOGLE_SA_JSON.strip()
        elif GOOGLE_SA_JSON_B64.strip():
            try:
                sa_json = base64.b64decode(GOOGLE_SA_JSON_B64.strip()).decode("utf-8")
            except Exception as e:
                LOG.warning(f"Bad GOOGLE_SA_JSON_B64: {e}")
                return

        if not sa_json:
            LOG.warning("GOOGLE_SA_JSON / GOOGLE_SA_JSON_B64 is empty; Google Sheets disabled.")
            return

        try:
            sa_info = json.loads(sa_json)
        except Exception as e:
            LOG.warning(f"Google Sheets not ready: invalid JSON ({e})")
            return

        try:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(GSHEET_ID)
            ws = sh.worksheet(SUBS_SHEET_NAME)

            # ensure header
            header = ws.row_values(1)
            if [h.strip() for h in header] != COLUMNS:
                ws.update("A1", [COLUMNS])
                LOG.info("Header updated to old-format columns.")

            self._ws = ws
            self._ready = True
            LOG.info("Google Sheets ready.")
        except Exception as e:
            LOG.warning(f"Google Sheets init failed: {e}")

    def _find_row_index_by_user_id(self, user_id: int) -> Optional[int]:
        if not self.ready():
            return None
        try:
            col = self._ws.col_values(1)  # telegram_user_id
            # header at 1
            for i in range(2, len(col) + 1):
                if str(col[i - 1]).strip() == str(user_id):
                    return i
            return None
        except Exception as e:
            LOG.warning(f"_find_row_index_by_user_id failed: {e}")
            return None

    def get_user(self, user_id: int) -> Optional[UserRow]:
        if not self.ready():
            return None
        idx = self._find_row_index_by_user_id(user_id)
        if not idx:
            return None
        try:
            row = self._ws.row_values(idx)
            data = {COLUMNS[i]: (row[i] if i < len(row) else "") for i in range(len(COLUMNS))}
            return UserRow(
                telegram_user_id=int(data["telegram_user_id"] or user_id),
                status=data.get("status", "active") or "active",
                plan=data.get("plan", "trial") or "trial",
                trial_expires=data.get("trial_expires", "") or "",
                birth_date=data.get("birth_date", "") or "",
                created_at=data.get("created_at", "") or "",
                last_seen_at=data.get("last_seen_at", "") or "",
                username=data.get("username", "") or "",
                first_name=data.get("first_name", "") or "",
                last_name=data.get("last_name", "") or "",
                registered_on=data.get("registered_on", "") or "",
                last_full_ym=data.get("last_full_ym", "") or "",
            )
        except Exception as e:
            LOG.warning(f"get_user failed: {e}")
            return None

    def upsert_user(self, u: UserRow) -> None:
        if not self.ready():
            return
        idx = self._find_row_index_by_user_id(u.telegram_user_id)
        values = [
            str(u.telegram_user_id),
            u.status,
            u.plan,
            u.trial_expires,
            u.birth_date,
            u.created_at,
            u.last_seen_at,
            u.username,
            u.first_name,
            u.last_name,
            u.registered_on,
            u.last_full_ym,
        ]
        try:
            if idx:
                self._ws.update(f"A{idx}:L{idx}", [values])
            else:
                self._ws.append_row(values, value_input_option="USER_ENTERED")
        except Exception as e:
            LOG.warning(f"upsert_user failed: {e}")

STORE = SheetsStore()

# =========================
# BUSINESS RULES (показ полных/кратких)
# =========================

def now_iso() -> str:
    return datetime.now(TZ).replace(microsecond=0).isoformat()

def today_date() -> date:
    return datetime.now(TZ).date()

def ym(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"

def should_send_full_for_lg_lm(user: UserRow, today: date) -> bool:
    """Полные ЛГ/ЛМ: в первый раз (когда last_full_ym пуст) и 1-го числа каждого месяца."""
    if today.day == 1:
        return True
    if not (user.last_full_ym or "").strip():
        return True
    # если сменился месяц относительно last_full_ym — тоже шлём полные (чтобы не потерять)
    return user.last_full_ym.strip() != ym(today)

def format_forecast(
    birth: date,
    today: date,
    full_lg_lm: bool,
) -> Tuple[str, int, int, int, int]:
    od = reduce_1_9(sum(int(c) for c in f"{today.day:02d}{today.month:02d}{today.year:04d}"))
    # спец правило 10/20/30
    is_10_20_30 = today.day in (10, 20, 30)

    py = calc_personal_year(birth, today)
    pm = calc_personal_month(py, today)
    pd = calc_personal_day(pm, today)

    # ОД всегда “полное”, но 10/20/30 — предупреждение
    if is_10_20_30:
        od_line = OD_WARNING_TEXT
        od_desc = ""
    else:
        od_line = f"🌐 Общий день (ОД): {od}"
        od_desc = OD_TEXT_FULL.get(od, f"Общий день {od}.")

    # ЛД всегда полное
    ld_full = LD_FULL.get(pd, f"Личный день {pd}.")
    # ЛМ/ЛГ — полные по правилу, иначе кратко
    if full_lg_lm:
        lg_text = LG_FULL.get(py, f"Ваш Личный год {py}.")
        lm_text = LM_FULL.get(pm, f"Личный месяц {pm}.")
    else:
        lg_text = LG_SHORT.get(py, f"Личный год {py}.")
        lm_text = LM_SHORT.get(pm, f"Личный месяц {pm}.")

    text_parts = []
    text_parts.append(f"📅 Дата: {today.strftime('%d.%m.%Y')}")
    text_parts.append(od_line)
    if od_desc:
        text_parts.append(od_desc)

    # блок ЛГ/ЛМ/ЛД
    text_parts.append("")
    text_parts.append(f"🧮 ЛГ / ЛМ / ЛД: {py} / {pm} / {pd}")
    text_parts.append(lg_text)
    text_parts.append(lm_text)
    text_parts.append(ld_full)

    return "\n".join(text_parts).strip(), od, py, pm, pd

# =========================
# TELEGRAM HANDLERS
# =========================

def ensure_env_or_die() -> None:
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN (или TELEGRAM_TOKEN/TOKEN) не задан в Render Env Vars.")
    if not PUBLIC_URL:
        raise ValueError("PUBLIC_URL (https://<service>.onrender.com) не задан в Render Env Vars для webhook.")

async def upsert_from_update(update: Update) -> UserRow:
    """Безопасно обновляем last_seen + профиль пользователя, создаём если нет."""
    user = update.effective_user
    if not user:
        # fallback, чтобы не падало
        uid = 0
        ur = STORE.get_user(uid) or UserRow(telegram_user_id=uid)
        return ur

    uid = user.id
    existing = STORE.get_user(uid) or UserRow(telegram_user_id=uid)

    # created_at / registered_on — фиксируем только при первом создании
    if not existing.created_at:
        existing.created_at = now_iso()
    if not existing.registered_on:
        existing.registered_on = today_date().isoformat()

    existing.last_seen_at = now_iso()
    existing.username = user.username or existing.username or ""
    existing.first_name = user.first_name or existing.first_name or ""
    existing.last_name = user.last_name or existing.last_name or ""

    STORE.upsert_user(existing)
    return existing

def trial_expire_default(created_iso: str) -> str:
    """По умолчанию trial 7 дней с created_at (если надо — меняй)."""
    try:
        d = datetime.fromisoformat(created_iso).date()
    except Exception:
        d = today_date()
    return (d + timedelta(days=7)).isoformat()

async def send_help(update: Update) -> None:
    txt = (
        "Команды:\n"
        "• 📅 Сегодня — прогноз на сегодня\n"
        "• 🎂 Изменить ДР — заново задать дату рождения\n"
        "• 🧾 Мой статус — план/триал\n\n"
        "Можно просто отправить дату рождения в формате ДД.ММ.ГГГГ — прогноз придёт сразу."
    )
    await update.effective_chat.send_message(txt, reply_markup=KB)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = await upsert_from_update(update)

    # если в таблице нет trial_expires — ставим дефолт
    if u.plan == "trial" and not u.trial_expires:
        u.trial_expires = trial_expire_default(u.created_at)
        STORE.upsert_user(u)

    if u.birth_date and parse_ddmmyyyy(u.birth_date):
        await update.message.reply_text(
            "Ты уже зарегистрирован. Нажми «📅 Сегодня» или отправь новую дату рождения.",
            reply_markup=KB,
        )
        return

    await update.message.reply_text(
        "Привет! Отправь дату рождения в формате ДД.ММ.ГГГГ (например 16.09.1994) — и я сразу дам прогноз на сегодня.",
        reply_markup=KB,
    )

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = await upsert_from_update(update)
    if not u.birth_date:
        await update.message.reply_text("Сначала отправь дату рождения ДД.ММ.ГГГГ.", reply_markup=KB)
        return

    b = parse_ddmmyyyy(u.birth_date)
    if not b:
        await update.message.reply_text("Дата рождения в таблице битая. Отправь заново ДД.ММ.ГГГГ.", reply_markup=KB)
        return

    t = today_date()
    full_lg_lm = should_send_full_for_lg_lm(u, t)
    msg, *_ = format_forecast(b, t, full_lg_lm)

    # если отправили полный ЛГ/ЛМ — отметим месяц
    if full_lg_lm:
        u.last_full_ym = ym(t)
        STORE.upsert_user(u)

    await update.message.reply_text(msg, reply_markup=KB)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = await upsert_from_update(update)
    plan = u.plan or "trial"
    trial = u.trial_expires or "-"
    status = u.status or "active"
    b = u.birth_date or "-"
    txt = (
        f"🧾 Статус: {status}\n"
        f"💳 План: {plan}\n"
        f"🎁 Trial до: {trial}\n"
        f"🎂 ДР: {b}"
    )
    await update.message.reply_text(txt, reply_markup=KB)

async def cmd_setbirth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = await upsert_from_update(update)
    u.birth_date = ""
    # чтобы “первый раз” сработал снова — очищаем last_full_ym
    u.last_full_ym = ""
    STORE.upsert_user(u)
    await update.message.reply_text("Ок. Отправь дату рождения ДД.ММ.ГГГГ.", reply_markup=KB)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # защита от None
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    u = await upsert_from_update(update)

    # кнопки
    if text == "📅 Сегодня":
        await cmd_today(update, context)
        return
    if text == "🧾 Мой статус":
        await cmd_status(update, context)
        return
    if text == "🎂 Изменить ДР":
        await cmd_setbirth(update, context)
        return
    if text == "ℹ️ Помощь":
        await send_help(update)
        return

    # если прислали дату рождения — сразу сохраняем и сразу даём прогноз
    d = parse_ddmmyyyy(text)
    if d:
        # сохраняем
        u.birth_date = d.strftime("%d.%m.%Y")
        # если первый раз задают ДР — считаем это “первым разом” (полные ЛГ/ЛМ)
        u.last_full_ym = ""  # чтобы full_lg_lm=True сработал гарантированно
        # trial_expires если пустой
        if u.plan == "trial" and not u.trial_expires:
            u.trial_expires = trial_expire_default(u.created_at)

        STORE.upsert_user(u)

        # сразу прогноз
        t = today_date()
        full_lg_lm = True  # “в первый раз” — всё полное
        msg, *_ = format_forecast(d, t, full_lg_lm)

        u.last_full_ym = ym(t)
        STORE.upsert_user(u)

        await update.message.reply_text(msg, reply_markup=KB)
        return

    # неизвестный текст
    await update.message.reply_text(
        "Не понял. Отправь дату рождения ДД.ММ.ГГГГ или нажми «📅 Сегодня».",
        reply_markup=KB,
    )

# =========================
# MAIN (WEBHOOK)
# =========================

def main() -> None:
    ensure_env_or_die()

    # init sheets
    STORE.init()

    LOG.info(f"BOOT ENV: TOKEN_set={bool(TELEGRAM_TOKEN)} GSHEET_ID_set={bool(GSHEET_ID)} "
             f"PUBLIC_URL={PUBLIC_URL} PORT={PORT} WEBHOOK_PATH={WEBHOOK_PATH}")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("setbirth", cmd_setbirth))
    app.add_handler(CommandHandler("help", lambda u, c: send_help(u)))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # ВАЖНО:
    # run_webhook сам поднимет HTTP сервер (Render увидит открытый порт)
    # и сам выставит webhook в Telegram на url=PUBLIC_URL+WEBHOOK_PATH
    webhook_url = f"{PUBLIC_URL.rstrip('/')}{WEBHOOK_PATH}"
    LOG.info(f"Webhook server listen=0.0.0.0:{PORT} path={WEBHOOK_PATH} => {webhook_url}")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=WEBHOOK_PATH.lstrip("/"),
        webhook_url=webhook_url,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()
