import os
import json
import base64
import logging
from datetime import date, datetime, timedelta

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


# =========================
# ENV
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL")
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")

print("BOT_TOKEN:", bool(TELEGRAM_TOKEN))
print("PUBLIC_URL:", bool(PUBLIC_URL))
print("GSHEET_ID:", bool(GSHEET_ID))
print("GOOGLE_SA_JSON_B64:", bool(GOOGLE_SA_JSON_B64))

if not all([TELEGRAM_TOKEN, PUBLIC_URL, GSHEET_ID, GOOGLE_SA_JSON_B64]):
    raise ValueError("Missing env vars")


TIMEZONE = "Asia/Almaty"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("syucai")


# =========================
# GOOGLE SHEETS
# =========================
service_account_info = json.loads(
    base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
)

creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)

gc = gspread.authorize(creds)
spreadsheet = gc.open_by_key(GSHEET_ID)

# === ВАЖНО: правильное имя листа ===
SHEET_NAME = "subscriptions"

try:
    sheet = spreadsheet.worksheet(SHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=20)
    sheet.append_row([
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
    ])

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


# =========================
# NUMEROLOGY (БАЗА)
# =========================
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


# =========================
# ACCESS
# =========================
def is_trial_active(user):
    return (
        user["plan"] == "trial"
        and date.fromisoformat(user["trial_expires"]) >= date.today()
    )


def has_full_access(user):
    return user["plan"] == "premium" or is_trial_active(user)


# =========================
# MESSAGE (заглушка, ты дальше сведёшь формулы)
# =========================
def build_message(birth: date):
    today = date.today()
    lg = calc_lg(birth, today)
    lm = calc_lm(lg, today)
    ld = calc_ld(lm, today)

    return (
        f"📅 Дата: {today.strftime('%d.%m.%Y')}\n\n"
        f"🧮 ЛГ / ЛМ / ЛД: {lg} / {lm} / {ld}"
    )


# =========================
# HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Пришли дату рождения в формате ДД.ММ.ГГГГ"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    try:
        birth = datetime.strptime(text, "%d.%m.%Y").date()
    except ValueError:
        await update.message.reply_text("Неверный формат даты.")
        return

    tg = update.effective_user
    user = get_user(tg.id)
    today = date.today()

    if not user:
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

    msg = build_message(birth)
    await update.message.reply_text(msg)


# =========================
# DAILY BROADCAST
# =========================
async def morning_broadcast(app: Application):
    users = sheet.get_all_records()
    today = date.today()

    for u in users:
        if u["status"] != "active":
            continue

        if u["plan"] == "trial" and date.fromisoformat(u["trial_expires"]) < today:
            continue

        birth = date.fromisoformat(u["birth_date"])
        msg = build_message(birth)

        try:
            await app.bot.send_message(
                chat_id=int(u["telegram_user_id"]),
                text=msg,
            )
        except Exception as e:
            logger.warning(e)


# =========================
# MAIN
# =========================
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

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
