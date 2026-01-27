import os
import json
import base64
import asyncio
import logging
from datetime import datetime, date, timedelta

from flask import Flask, request
import pytz

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ----------------- LOGGING -----------------
logging.basicConfig(level=logging.INFO)

# ----------------- ENV -----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL")
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")

print("TELEGRAM_TOKEN:", bool(TELEGRAM_TOKEN))
print("PUBLIC_URL:", bool(PUBLIC_URL))
print("GSHEET_ID:", bool(GSHEET_ID))
print("GOOGLE_SA_JSON_B64:", bool(GOOGLE_SA_JSON_B64))

if not all([TELEGRAM_TOKEN, PUBLIC_URL, GSHEET_ID, GOOGLE_SA_JSON_B64]):
    raise RuntimeError("❌ Missing required env vars")

TZ = pytz.timezone("Asia/Almaty")
TRIAL_DAYS = 3
BAD_DATES = {10, 20, 30}

# ----------------- GOOGLE SHEETS -----------------
sa_info = json.loads(
    base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
)

creds = Credentials.from_service_account_info(
    sa_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)

gc = gspread.authorize(creds)
sheet = gc.open_by_key(GSHEET_ID).worksheet("subscriptions")

# ----------------- NUMEROLOGY DATA -----------------
LD = {i: f"Полное описание личного дня {i}" for i in range(1, 10)}
LM = {i: f"Полное описание личного месяца {i}" for i in range(1, 10)}
LG = {i: f"Полное описание личного года {i}" for i in range(1, 10)}
OD = {
    1: "День начала и инициативы",
    2: "День партнерства и баланса",
    3: "День активности и идей",
    4: "День порядка и дисциплины",
    5: "День перемен",
    6: "День семьи и заботы",
    7: "День анализа",
    8: "День денег",
    9: "День завершений",
}

# ----------------- CALC -----------------
def reduce9(n: int) -> int:
    while n > 9:
        n = sum(map(int, str(n)))
    return n

def calculate(bd: date, today: date):
    od = reduce9(today.day + today.month + today.year)
    lg = reduce9(bd.day + bd.month + today.year)
    lm = reduce9(lg + today.month)
    ld = reduce9(lm + today.day)
    return od, lg, lm, ld

# ----------------- SHEET HELPERS -----------------
def get_user(uid: int):
    for r in sheet.get_all_records():
        if str(r["telegram_user_id"]) == str(uid):
            return r
    return None

def upsert_user(data: dict):
    headers = sheet.row_values(1)
    rows = sheet.get_all_records()
    for i, r in enumerate(rows, start=2):
        if str(r["telegram_user_id"]) == str(data["telegram_user_id"]):
            sheet.update(f"A{i}:L{i}", [[data[h] for h in headers]])
            return
    sheet.append_row([data[h] for h in headers])

# ----------------- MESSAGE -----------------
def build_message(user, bd):
    today = datetime.now(TZ).date()
    od, lg, lm, ld = calculate(bd, today)

    is_first = not user["birth_date"]
    current_ym = today.strftime("%Y-%m")
    is_first_month = user["last_full_ym"] != current_ym and today.day == 1

    text = f"📅 {today.strftime('%d.%m.%Y')}\n"

    if today.day in BAD_DATES:
        text += "⚠️ Неблагоприятная дата\n\n"

    text += f"🌐 ОД {od}\n{OD[od]}\n\n"

    if is_first:
        text += (
            f"🧮 ЛГ {lg}\n{LG[lg]}\n\n"
            f"📆 ЛМ {lm}\n{LM[lm]}\n\n"
            f"📍 ЛД {ld}\n{LD[ld]}"
        )
    elif is_first_month:
        text += (
            f"🧮 ЛГ {lg}\n{LG[lg]}\n\n"
            f"📆 ЛМ {lm}\n{LM[lm]}\n\n"
            f"📍 ЛД {ld}\n{LD[ld]}"
        )
    else:
        text += (
            f"📍 ЛД {ld}\n{LD[ld]}\n\n"
            f"Кратко:\nЛМ {lm} · ЛГ {lg}"
        )

    return text, current_ym

# ----------------- TELEGRAM -----------------
application = Application.builder().token(TELEGRAM_TOKEN).build()

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введите дату рождения: ДД.ММ.ГГГГ")

async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text.strip()
    now = datetime.now(TZ)

    user = get_user(uid)
    if not user:
        user = {
            "telegram_user_id": uid,
            "status": "trial",
            "plan": "trial",
            "trial_expires": (now + timedelta(days=TRIAL_DAYS)).strftime("%Y-%m-%d"),
            "birth_date": "",
            "created_at": now.isoformat(),
            "last_seen_at": now.isoformat(),
            "username": update.effective_user.username or "",
            "first_name": update.effective_user.first_name or "",
            "last_name": update.effective_user.last_name or "",
            "registered_on": now.strftime("%Y-%m-%d"),
            "last_full_ym": "",
        }

    if "." in text:
        user["birth_date"] = text

    if not user["birth_date"]:
        await update.message.reply_text("Введите дату рождения")
        return

    bd = datetime.strptime(user["birth_date"], "%d.%m.%Y").date()
    msg, ym = build_message(user, bd)

    user["last_full_ym"] = ym
    user["last_seen_at"] = now.isoformat()
    upsert_user(user)

    await update.message.reply_text(
        msg,
        reply_markup=ReplyKeyboardMarkup([["Сегодня"]], resize_keyboard=True),
    )

application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

# ----------------- SCHEDULER -----------------
scheduler = AsyncIOScheduler(timezone=TZ)

async def morning_job():
    users = sheet.get_all_records()
    for u in users:
        if not u["birth_date"]:
            continue
        bd = datetime.strptime(u["birth_date"], "%d.%m.%Y").date()
        msg, _ = build_message(u, bd)
        await application.bot.send_message(u["telegram_user_id"], msg)

scheduler.add_job(morning_job, "cron", hour=9, minute=0)

# ----------------- FLASK -----------------
app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    update = Update.de_json(request.json, application.bot)
    asyncio.create_task(application.process_update(update))
    return "ok"

# ----------------- MAIN -----------------
async def main():
    await application.initialize()
    await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")
    scheduler.start()

if __name__ == "__main__":
    asyncio.run(main())
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
