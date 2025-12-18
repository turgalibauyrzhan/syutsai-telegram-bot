#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Syucai Telegram bot (Render-friendly)

Что умеет:
- Google Sheets (gspread + service account JSON) для хранения пользователей
- Trial/доступ/ежедневная рассылка (JobQueue)
- Прогноз сразу после ввода даты рождения (без /today)
- Кнопки для основных команд
- Два режима:
    - WEBHOOK (если задан WEBHOOK_URL)  -> Web Service на Render, открыт порт
    - POLLING (если WEBHOOK_URL не задан) -> Worker (или куда угодно), без порта

ENV:
- TELEGRAM_TOKEN (обязательно)
- GSHEET_ID (обязательно для таблиц)
- SUBS_SHEET_NAME (опционально, по умолчанию subscriptions)
- GOOGLE_SA_JSON  (json service account одной строкой)  ИЛИ
- GOOGLE_SA_JSON_B64 (base64 service account)
- WEBHOOK_URL (например https://<service>.onrender.com) -> включает webhook
- WEBHOOK_PATH (опционально, например /telegram/webhook/xxxx)
- PORT (Render даёт сам; по умолчанию 10000)
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Dict, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials
from telegram import ReplyKeyboardMarkup, Update
from telegram.error import Conflict
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------------- Logging ----------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper().strip()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - syucai - %(levelname)s - %(message)s",
)
log = logging.getLogger("syucai")

# ---------------- Timezone ----------------
# Без внешних зависимостей: используем фиксированную TZ Asia/Almaty (UTC+5)
# Если хочешь строго по IANA с DST и т.п. — ставь zoneinfo (в py3.9+ она встроена).
try:
    from zoneinfo import ZoneInfo

    TZ_NAME = os.getenv("TZ_NAME", "Asia/Almaty").strip() or "Asia/Almaty"
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    TZ_NAME = "Asia/Almaty"
    TZ = None  # fallback: naive date

# ---------------- ENV ----------------
TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
GSHEET_ID = (os.getenv("GSHEET_ID") or "").strip()
SUBS_SHEET_NAME = (os.getenv("SUBS_SHEET_NAME") or "subscriptions").strip()

WEBHOOK_URL = (os.getenv("WEBHOOK_URL") or "").strip().rstrip("/")
WEBHOOK_PATH = (os.getenv("WEBHOOK_PATH") or "").strip()
PORT = int((os.getenv("PORT") or "10000").strip())

# ---------------- Reply keyboard ----------------
MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        ["/today", "/me"],
        ["/start"],
    ],
    resize_keyboard=True,
    is_persistent=True,
)

# ---------------- Google Sheets ----------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_gs_client: Optional[gspread.Client] = None
_ws_cache: Optional[gspread.Worksheet] = None


def today_tz() -> date:
    if TZ is None:
        return datetime.utcnow().date()
    return datetime.now(TZ).date()


def now_tz() -> datetime:
    if TZ is None:
        return datetime.utcnow()
    return datetime.now(TZ)


def _looks_like_base64(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return False
    # json начинается с '{'. base64 service account часто начинается с 'ew' (это '{' в b64) или 'ey'
    if s.startswith("{"):
        return False
    if re.fullmatch(r"[A-Za-z0-9+/=\s]+", s) is None:
        return False
    # слишком короткое — скорее всего мусор
    return len(s) > 100


def _load_service_account_info() -> Optional[Dict[str, Any]]:
    """
    Поддерживает:
    - GOOGLE_SA_JSON: либо прям JSON, либо по ошибке base64 (попытаемся распознать и декодировать)
    - GOOGLE_SA_JSON_B64: base64
    """
    raw_json = (os.getenv("GOOGLE_SA_JSON") or "").strip()
    raw_b64 = (os.getenv("GOOGLE_SA_JSON_B64") or "").strip()

    # 1) Если дали GOOGLE_SA_JSON и он похож на base64 — пробуем decode
    if raw_json:
        if raw_json.startswith("{"):
            try:
                return json.loads(raw_json)
            except Exception as e:
                log.warning("GOOGLE_SA_JSON invalid JSON: %s", e)
        elif _looks_like_base64(raw_json):
            try:
                decoded = base64.b64decode(raw_json).decode("utf-8", "ignore")
                return json.loads(decoded)
            except Exception as e:
                log.warning("GOOGLE_SA_JSON looked like base64 but failed to decode/parse: %s", e)

    # 2) GOOGLE_SA_JSON_B64
    if raw_b64:
        try:
            decoded = base64.b64decode(raw_b64).decode("utf-8", "ignore")
            return json.loads(decoded)
        except Exception as e:
            log.warning("GOOGLE_SA_JSON_B64 invalid: %s", e)

    return None


def gs_init_safe() -> None:
    """Инициализация Google Sheets. Не валит бота, если что-то не так."""
    global _gs_client, _ws_cache

    if not GSHEET_ID:
        log.warning("GSHEET_ID is empty: Google Sheets disabled")
        _gs_client = None
        _ws_cache = None
        return

    info = _load_service_account_info()
    if not info:
        log.warning("Google Sheets not ready: service account json missing/invalid")
        _gs_client = None
        _ws_cache = None
        return

    try:
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        _gs_client = gspread.authorize(creds)
        sh = _gs_client.open_by_key(GSHEET_ID)
        _ws_cache = sh.worksheet(SUBS_SHEET_NAME)
        _ensure_headers(_ws_cache)
        log.info("Google Sheets ready: sheet '%s'", SUBS_SHEET_NAME)
    except Exception as e:
        log.warning("Google Sheets not ready: %s", e)
        _gs_client = None
        _ws_cache = None


def ws() -> Optional[gspread.Worksheet]:
    global _ws_cache
    if _ws_cache is None:
        gs_init_safe()
    return _ws_cache


HEADERS = [
    "user_id",
    "username",
    "first_name",
    "last_name",
    "birth_date",     # DD.MM.YYYY
    "status",         # active/blocked
    "trial_until",    # YYYY-MM-DD
    "last_full_ym",   # YYYY-MM
]


def _ensure_headers(_ws: gspread.Worksheet) -> None:
    try:
        first_row = _ws.row_values(1)
        if [h.strip() for h in first_row] == HEADERS:
            return
        if not first_row:
            _ws.append_row(HEADERS)
            return
        # если уже есть что-то, но не headers — не лезем агрессивно
        # лучше явно переименовать руками
        if len(first_row) < 2 or first_row[0] != "user_id":
            log.warning("Sheet first row doesn't look like headers. Expected user_id... Got: %s", first_row)
    except Exception:
        pass


def find_user_row(_ws: gspread.Worksheet, user_id: int) -> Optional[int]:
    """Возвращает индекс строки (1-based), где user_id."""
    try:
        col = _ws.col_values(1)  # user_id
        for i, v in enumerate(col, start=1):
            if str(v).strip() == str(user_id):
                return i
    except Exception:
        return None
    return None


def row_dict(_ws: gspread.Worksheet, row_idx: int) -> Dict[str, str]:
    vals = _ws.row_values(row_idx)
    out: Dict[str, str] = {}
    for i, h in enumerate(HEADERS):
        out[h] = vals[i] if i < len(vals) else ""
    return out


def update_row(_ws: gspread.Worksheet, row_idx: int, patch: Dict[str, str]) -> None:
    """Патчит отдельные поля по header-ам."""
    try:
        for k, v in patch.items():
            if k not in HEADERS:
                continue
            col_idx = HEADERS.index(k) + 1
            _ws.update_cell(row_idx, col_idx, v)
    except Exception as e:
        log.warning("update_row failed: %s", e)


def create_user_if_needed(_ws: gspread.Worksheet, u: Update) -> Tuple[int, Dict[str, str]]:
    uid = u.effective_user.id
    row_idx = find_user_row(_ws, uid)
    if row_idx is not None:
        return row_idx, row_dict(_ws, row_idx)

    user = u.effective_user
    # trial 7 дней по умолчанию (можешь поменять)
    td = today_tz()
    trial_until = (td.toordinal() + 7)
    trial_date = date.fromordinal(trial_until).isoformat()

    new_row = [
        str(uid),
        user.username or "",
        user.first_name or "",
        user.last_name or "",
        "",
        "active",
        trial_date,
        "",
    ]
    _ws.append_row(new_row)
    row_idx = find_user_row(_ws, uid)
    if row_idx is None:
        # крайне редко, но пусть будет
        row_idx = _ws.row_count
    return row_idx, row_dict(_ws, row_idx)


# ---------------- Domain logic ----------------
DATE_RE = re.compile(r"^\s*(\d{2})\.(\d{2})\.(\d{4})\s*$")


def parse_birth_date(s: str) -> Optional[date]:
    m = DATE_RE.match(s or "")
    if not m:
        return None
    dd, mm, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(yyyy, mm, dd)
    except Exception:
        return None


def ym_str(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _parse_iso_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        return None


def is_trial_active(urow: Dict[str, str], td: date) -> bool:
    tu = _parse_iso_date(urow.get("trial_until", ""))
    if not tu:
        return False
    return td <= tu


def enforce_trial(urow: Dict[str, str], td: date) -> bool:
    """True => надо заблокировать (trial закончился)."""
    status = (urow.get("status") or "").strip().lower()
    if status == "blocked":
        return True
    # если trial есть и он уже в прошлом -> блокируем
    tu = _parse_iso_date(urow.get("trial_until", ""))
    if tu and td > tu:
        return True
    return False


def is_allowed(urow: Dict[str, str], td: date) -> bool:
    status = (urow.get("status") or "").strip().lower()
    if status == "blocked":
        return False
    # если trial не активен — можно либо разрешать, либо запрещать.
    # Сейчас делаем просто: если trial закончился -> блок (через enforce_trial)
    return True


# ---------------- Forecast engine (заглушка/пример) ----------------
# Тут подставь твой реальный “сюцай” расчёт.
# Я оставил устойчивую структуру: ОД, ЛГ/ЛМ/ЛД на основе даты рождения + даты сегодня.

def digit_sum(n: int) -> int:
    return sum(int(ch) for ch in str(n))


def reduce_1_9(n: int) -> int:
    while n > 9:
        n = digit_sum(n)
    return n


def calc_general_day(d: date) -> int:
    return reduce_1_9(digit_sum(d.year) + digit_sum(d.month) + digit_sum(d.day))


def calc_personal_day(b: date, today: date) -> int:
    # примитивный пример: день + месяц рождения + текущий ОД
    return reduce_1_9(b.day + b.month + calc_general_day(today))


def build_forecast(urow: Dict[str, str], today: date, full_ym: bool = False) -> str:
    od = calc_general_day(today)

    birth = parse_birth_date(urow.get("birth_date", ""))
    pd = calc_personal_day(birth, today) if birth else None

    lines = []
    lines.append(f"📅 Дата: {today.strftime('%d.%m.%Y')}")
    lines.append(f"🌐 Общий день (ОД): {od}")

    # короткое описание по ОД (пример)
    if od in (1, 3, 5):
        lines.append("День удачи, простых решений и быстрых результатов. Хорош для стартов, встреч, общения.")
    elif od in (2, 4, 6):
        lines.append("День про дисциплину, документы, порядок и доведение дел до конца.")
    else:
        lines.append("День про завершение, чистку хвостов и перезагрузку.")

    if birth and pd is not None:
        lines.append(f"👤 Личный день (ЛД): {pd}")
        lines.append("Если нужно — добавлю ЛГ/ЛМ (сейчас стоит базовый расчет).")
    else:
        lines.append("Чтобы считать ЛГ/ЛМ/ЛД, пришли дату рождения в формате ДД.ММ.ГГГГ (например 05.11.1992).")

    if is_trial_active(urow, today):
        lines.append("🎁 Trial активен.")
    else:
        lines.append("⛔ Trial закончился или не активен.")

    return "\n".join(lines)


# ---------------- Handlers ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    td = today_tz()

    _ws = ws()
    if _ws is None:
        await update.message.reply_text(
            "Бот запущен, но Google Sheets пока не подключены. Проверь GSHEET_ID и GOOGLE_SA_JSON/GOOGLE_SA_JSON_B64.",
            reply_markup=MAIN_KB,
        )
        return

    row_idx, urow = create_user_if_needed(_ws, update)

    if enforce_trial(urow, td):
        update_row(_ws, row_idx, {"status": "blocked"})
        await update.message.reply_text("⛔ Trial закончился. Доступ закрыт.", reply_markup=MAIN_KB)
        return

    # если уже есть дата рождения — сразу покажем прогноз
    if parse_birth_date(urow.get("birth_date", "")):
        msg = build_forecast(urow, today=td, full_ym=False)
        await update.message.reply_text(msg, reply_markup=MAIN_KB)
    else:
        await update.message.reply_text(
            "Привет! Пришли дату рождения в формате ДД.ММ.ГГГГ (например 16.09.1994) — и я сразу дам прогноз на сегодня.",
            reply_markup=MAIN_KB,
        )


async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    td = today_tz()
    _ws = ws()
    if _ws is None:
        await update.message.reply_text("Google Sheets не готовы. Проверь переменные окружения.", reply_markup=MAIN_KB)
        return

    uid = update.effective_user.id
    row_idx = find_user_row(_ws, uid)
    if row_idx is None:
        row_idx, urow = create_user_if_needed(_ws, update)
    else:
        urow = row_dict(_ws, row_idx)

    if enforce_trial(urow, td):
        update_row(_ws, row_idx, {"status": "blocked"})
        await update.message.reply_text("⛔ Trial закончился. Доступ закрыт.", reply_markup=MAIN_KB)
        return

    if not is_allowed(urow, td):
        await update.message.reply_text("⛔ Доступ закрыт.", reply_markup=MAIN_KB)
        return

    msg = build_forecast(urow, today=td, full_ym=False)
    await update.message.reply_text(msg, reply_markup=MAIN_KB)


async def me(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    td = today_tz()
    _ws = ws()
    if _ws is None:
        await update.message.reply_text("Google Sheets не готовы.", reply_markup=MAIN_KB)
        return

    uid = update.effective_user.id
    row_idx = find_user_row(_ws, uid)
    if row_idx is None:
        row_idx, urow = create_user_if_needed(_ws, update)
    else:
        urow = row_dict(_ws, row_idx)

    if enforce_trial(urow, td):
        update_row(_ws, row_idx, {"status": "blocked"})
        await update.message.reply_text("⛔ Trial закончился. Доступ закрыт.", reply_markup=MAIN_KB)
        return

    b = (urow.get("birth_date") or "").strip() or "не задана"
    tu = (urow.get("trial_until") or "").strip() or "—"
    st = (urow.get("status") or "").strip() or "—"
    await update.message.reply_text(
        f"👤 Профиль\n"
        f"• Дата рождения: {b}\n"
        f"• Trial until: {tu}\n"
        f"• Status: {st}\n",
        reply_markup=MAIN_KB,
    )


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Любой текст. Если это дата рождения — сохраняем и сразу выдаем прогноз."""
    if not update.message or not update.message.text:
        return

    td = today_tz()
    text = update.message.text.strip()

    _ws = ws()
    if _ws is None:
        await update.message.reply_text("Google Sheets не готовы. Проверь переменные окружения.", reply_markup=MAIN_KB)
        return

    uid = update.effective_user.id
    row_idx = find_user_row(_ws, uid)
    if row_idx is None:
        row_idx, urow = create_user_if_needed(_ws, update)
    else:
        urow = row_dict(_ws, row_idx)

    if enforce_trial(urow, td):
        update_row(_ws, row_idx, {"status": "blocked"})
        await update.message.reply_text("⛔ Trial закончился. Доступ закрыт.", reply_markup=MAIN_KB)
        return

    # 1) Дата рождения
    b = parse_birth_date(text)
    if b:
        # сохраняем как оригинальную строку DD.MM.YYYY (как прислал)
        update_row(_ws, row_idx, {"birth_date": text})
        urow = row_dict(_ws, row_idx)

        msg = build_forecast(urow, today=td, full_ym=False)
        await update.message.reply_text(msg, reply_markup=MAIN_KB)
        return

    # 2) Любой другой текст
    await update.message.reply_text(
        "Напиши /today или пришли дату рождения ДД.ММ.ГГГГ (например 16.09.1994).",
        reply_markup=MAIN_KB,
    )


# ---------------- Daily broadcast ----------------
async def daily_broadcast(context: ContextTypes.DEFAULT_TYPE) -> None:
    td = today_tz()
    _ws = ws()
    if _ws is None:
        log.warning("Daily broadcast skipped: Google Sheets not ready")
        return

    ids = _ws.col_values(1)[1:]
    for s in ids:
        s = (s or "").strip()
        if not s.isdigit():
            continue
        uid = int(s)
        row_idx = find_user_row(_ws, uid)
        if row_idx is None:
            continue
        u = row_dict(_ws, row_idx)

        if enforce_trial(u, td):
            update_row(_ws, row_idx, {"status": "blocked"})
            continue
        if not is_allowed(u, td):
            continue

        cur_ym = ym_str(td)
        last_full_ym = (u.get("last_full_ym") or "").strip()
        full_ym = (td.day == 1) or (last_full_ym != cur_ym)

        msg = build_forecast(u, today=td, full_ym=full_ym)
        if full_ym and parse_birth_date(u.get("birth_date") or ""):
            update_row(_ws, row_idx, {"last_full_ym": cur_ym})

        try:
            await context.bot.send_message(chat_id=uid, text=msg, reply_markup=MAIN_KB)
        except Exception:
            continue


# ---------------- Error handler ----------------
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, Conflict):
        # два инстанса polling -> 409
        log.error("Polling conflict (409): another getUpdates is running with same token. Exiting.")
        try:
            await context.application.stop()
            await context.application.shutdown()
        finally:
            os._exit(0)

    log.error("Unhandled error: %s", err, exc_info=err)


def schedule_jobs(app: Application) -> None:
    if app.job_queue is None:
        log.warning('No JobQueue set up. Install: pip install "python-telegram-bot[job-queue]"')
        return
    # 09:00 Asia/Almaty
    tzinfo = TZ if TZ is not None else None
    app.job_queue.run_daily(daily_broadcast, time=time(9, 0, tzinfo=tzinfo), name="daily_broadcast")
    log.info("Daily broadcast scheduled at 09:00 %s", TZ_NAME)


def build_app() -> Application:
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("today", today_cmd))
    app.add_handler(CommandHandler("me", me))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    app.add_error_handler(on_error)
    return app


def _default_webhook_path() -> str:
    # стабильный короткий хэш (не светим токен)
    h = hashlib.sha256(TOKEN.encode("utf-8")).hexdigest()[:8]
    return f"/telegram/webhook/{h}"


def main() -> None:
    if not TOKEN:
        log.error("TELEGRAM_TOKEN is empty")
        return

    log.info(
        "BOOT ENV: TOKEN_set=%s GSHEET_ID_set=%s GOOGLE_SA_JSON_len=%d GOOGLE_SA_JSON_B64_len=%d",
        bool(TOKEN),
        bool(GSHEET_ID),
        len((os.getenv("GOOGLE_SA_JSON") or "").strip()),
        len((os.getenv("GOOGLE_SA_JSON_B64") or "").strip()),
    )

    # Не фатально: бот может работать и без Sheets (но профили/подписки не будут сохраняться)
    gs_init_safe()

    app = build_app()
    schedule_jobs(app)

    # WEBHOOK mode если задан WEBHOOK_URL
    if WEBHOOK_URL:
        path = WEBHOOK_PATH or _default_webhook_path()
        if not path.startswith("/"):
            path = "/" + path
        full_hook = f"{WEBHOOK_URL}{path}"
        log.info("Webhook server 0.0.0.0:%s path=%s => %s", PORT, path, full_hook)

        # ВАЖНО: требует установку python-telegram-bot[webhooks]
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=path.lstrip("/"),
            webhook_url=full_hook,
            drop_pending_updates=True,
        )
    else:
        log.info("Polling mode (WEBHOOK_URL not set)")
        app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
