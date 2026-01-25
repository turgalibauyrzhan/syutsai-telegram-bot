import os
import json
import base64
import logging
from datetime import datetime, date, timedelta

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ======================================================
# CONFIG
# ======================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL")
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")

if not all([TELEGRAM_TOKEN, PUBLIC_URL, GSHEET_ID, GOOGLE_SA_JSON_B64]):
    raise ValueError("Missing env vars")

TIMEZONE = "Asia/Almaty"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("syucai")

# ======================================================
# GOOGLE SHEETS
# ======================================================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
google_creds = json.loads(
    base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(SHEET_ID).worksheet("users")

HEADERS = sheet.row_values(1)

def get_user_row(user_id: int):
    try:
        cell = sheet.find(str(user_id))
        return cell.row
    except gspread.exceptions.CellNotFound:
        return None

def get_user(user_id: int):
    row = get_user_row(user_id)
    if not row:
        return None
    values = sheet.row_values(row)
    return dict(zip(HEADERS, values))

def save_user(data: dict):
    row = get_user_row(int(data["telegram_user_id"]))
    values = [data.get(h, "") for h in HEADERS]
    if row:
        sheet.update(f"A{row}", [values])
    else:
        sheet.append_row(values)

# ======================================================
# NUMEROLOGY
# ======================================================

def reduce_to_9(n: int) -> int:
    while n > 9:
        n = sum(map(int, str(n)))
    return n

def calc_lg(birth: date, today: date):
    return reduce_to_9(birth.day + birth.month + today.year)

def calc_lm(lg: int, today: date):
    return reduce_to_9(lg + today.month)

def calc_ld(lm: int, today: date):
    return reduce_to_9(lm + today.day)

def calc_od(today: date):
    return reduce_to_9(today.day + today.month + today.year)

# ======================================================
# TEXT DATA (КРАТКО, ПОЛНЫЕ ТЫ УЖЕ ДАЛ — ЗДЕСЬ ОСНОВА)
# ======================================================

LG = {i: f"Личный год {i}" for i in range(1, 10)}
LM = {i: f"Личный месяц {i}" for i in range(1, 10)}
LD = {i: f"Полное описание личного дня {i}" for i in range(1, 10)}

# ======================================================
# ACCESS
# ======================================================

def is_trial_active(user):
    if user["plan"] != "trial":
        return False
    return date.fromisoformat(user["trial_expires"]) >= date.today()

def has_full_access(user):
    return user["plan"] == "premium" or is_trial_active(user)

# ======================================================
# MESSAGE BUILD
# ======================================================

def build_message(user, birth: date, full: bool):
    today = date.today()

    lg = calc_lg(birth, today)
    lm = calc_lm(lg, today)
    ld = calc_ld(lm, today)

    msg = f"📅 {today.strftime('%d.%m.%Y')}\n\n"

    if full:
        msg += f"🔹 Личный год\n{LG[lg]}\n\n"
        msg += f"🔹 Личный месяц\n{LM[lm]}\n\n"
        msg += f"🔹 Личный день\n{LD[ld]}\n"
    else:
        msg += f"🔹 Личный день\n{LD[ld]}\n\n"
        msg += f"▫️ {LM[lm]}\n"
        msg += f"▫️ {LG[lg]}\n"

    return msg

# ======================================================
# HANDLERS
# ======================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введи дату рождения ДД.ММ.ГГГГ")

async def message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        birth = datetime.strptime(text, "%d.%m.%Y").date()
    except ValueError:
        await update.message.reply_text("Неверный формат")
        return

    tg = update.effective_user
    user = get_user(tg.id)

    if not user:
        today = date.today()
        user = {
            "telegram_user_id": tg.id,
            "status": "active",
            "plan": "trial",
            "trial_expires": (today + timedelta(days=3)).isoformat(),
            "birth_date": birth.isoformat(),
            "created_at": today.isoformat(),
            "last_seen_at": today.isoformat(),
            "username": tg.username or "",
            "first_name": tg.first_name or "",
            "last_name": tg.last_name or "",
            "registered_on": today.isoformat(),
            "last_full_ym": today.strftime("%Y-%m"),
        }
        save_user(user)

    full = has_full_access(user)
    msg = build_message(user, birth, full)

    await update.message.reply_text(msg)

# ======================================================
# DAILY BROADCAST
# ======================================================

async def morning_broadcast(app: Application):
    users = sheet.get_all_records()
    today = date.today()

    for u in users:
        if u["status"] != "active":
            continue

        if u["plan"] == "trial" and date.fromisoformat(u["trial_expires"]) < today:
            continue

        birth = date.fromisoformat(u["birth_date"])
        msg = build_message(u, birth, full=False)

        try:
            await app.bot.send_message(chat_id=int(u["telegram_user_id"]), text=msg)
        except Exception as e:
            logger.warning(e)

# ======================================================
# MAIN
# ======================================================

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message))

    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        morning_broadcast,
        CronTrigger(hour=9, minute=0),
        args=[app],
    )
    scheduler.start()

    app.run_webhook(
        listen="0.0.0.0",
        port=10000,
        webhook_url=f"{PUBLIC_URL}/telegram",
    )

if __name__ == "__main__":
    main()
