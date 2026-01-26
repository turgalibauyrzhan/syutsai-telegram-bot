import os
import json
import base64
import logging
from datetime import datetime, date, timedelta

from flask import Flask, request
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, ContextTypes

import gspread
from google.oauth2.service_account import Credentials

from apscheduler.schedulers.background import BackgroundScheduler
import pytz

# ----------------- CONFIG -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("syucai")

BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL")
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")

print("BOT_TOKEN:", bool(BOT_TOKEN))
print("PUBLIC_URL:", bool(PUBLIC_URL))
print("GSHEET_ID:", bool(GSHEET_ID))
print("GOOGLE_SA_JSON_B64:", bool(GOOGLE_SA_JSON_B64))

if not all([BOT_TOKEN, PUBLIC_URL, GSHEET_ID, GOOGLE_SA_JSON_B64]):
    raise ValueError("Missing env vars")

# ----------------- GOOGLE SHEETS -----------------
sa_info = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8"))
creds = Credentials.from_service_account_info(
    sa_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(GSHEET_ID).worksheet("subscriptions")

# ----------------- CONSTANTS -----------------
BAD_DATES = {10, 20, 30}
TRIAL_DAYS = 3
TZ = pytz.timezone("Asia/Almaty")

# ----------------- DESCRIPTIONS -----------------
# (сокращаю тут визуально — у тебя уже есть все тексты,
# логика подключения корректная)

LD = {i: f"Полное описание личного дня {i}" for i in range(1, 10)}
LM = {i: f"Полное описание личного месяца {i}" for i in range(1, 10)}
LG = {i: f"Полное описание личного года {i}" for i in range(1, 10)}
OD = {
    3: "Благоприятный день через анализ и успех.",
    6: "Благоприятный день через любовь и успех.",
}

# ----------------- CALCULATION -----------------
def reduce9(n):
    while n > 9:
        n = sum(map(int, str(n)))
    return n

def calculate(bd: date, today: date):
    od = reduce9(today.day + today.month + today.year)
    lg = reduce9(bd.day + bd.month + today.year)
    lm = reduce9(lg + today.month)
    ld = reduce9(lm + today.day)
    return od, lg, lm, ld

# ----------------- GOOGLE HELPERS -----------------
def get_user(uid):
    rows = sheet.get_all_records()
    for r in rows:
        if str(r["telegram_user_id"]) == str(uid):
            return r
    return None

def upsert_user(data: dict):
    headers = sheet.row_values(1)
    rows = sheet.get_all_records()
    for i, r in enumerate(rows, start=2):
        if str(r["telegram_user_id"]) == str(data["telegram_user_id"]):
            sheet.update(f"A{i}:L{i}", [data[h] for h in headers])
            return
    sheet.append_row([data[h] for h in headers])

# ----------------- MESSAGE BUILDER -----------------
def build_message(user, bd):
    today = datetime.now(TZ).date()
    od, lg, lm, ld = calculate(bd, today)

    is_first = user["birth_date"] == ""
    last_full = user["last_full_ym"]
    current_ym = today.strftime("%Y-%m")
    is_first_month = last_full != current_ym and today.day == 1

    text = f"📅 {today.strftime('%d.%m.%Y')}\n"

    if today.day in BAD_DATES:
        text += "⚠️ Неблагоприятная дата\n\n"

    if od in OD:
        text += f"🌐 Общий день: {od}\n{OD[od]}\n\n"

    if is_first:
        text += f"🧮 ЛГ {lg}\n{LG[lg]}\n\n📆 ЛМ {lm}\n{LM[lm]}\n\n📍 ЛД {ld}\n{LD[ld]}"
    elif is_first_month:
        text += f"🧮 ЛГ {lg}\n{LG[lg]}\n\n📆 ЛМ {lm}\n{LM[lm]}\n\n📍 ЛД {ld}\n{LD[ld]}"
    else:
        text += f"📍 ЛД {ld}\n{LD[ld]}\n\nКратко:\nЛМ {lm} · ЛГ {lg}"

    return text, current_ym

# ----------------- TELEGRAM -----------------
app = Flask(__name__)
application = Application.builder().token(BOT_TOKEN).build()

async def handle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text.strip()

    user = get_user(uid)
    if not user:
        now = datetime.now(TZ)
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

    user["last_seen_at"] = datetime.now(TZ).isoformat()

    if "." in text and len(text) == 10:
        bd = datetime.strptime(text, "%d.%m.%Y").date()
        user["birth_date"] = text
    elif user["birth_date"]:
        bd = datetime.strptime(user["birth_date"], "%d.%m.%Y").date()
    else:
        await update.message.reply_text("Введите дату рождения ДД.ММ.ГГГГ")
        return

    msg, ym = build_message(user, bd)
    user["last_full_ym"] = ym
    upsert_user(user)

    await update.message.reply_text(
        msg,
        reply_markup=ReplyKeyboardMarkup([["Сегодня"]], resize_keyboard=True)
    )

from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram import Update
from flask import Flask, request

# ---------- Telegram handlers ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Пришли дату рождения в формате ДД.ММ.ГГГГ"
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    # тут у тебя будет логика ЛГ / ЛМ / ЛД / ОД
    await update.message.reply_text(f"Принял: {text}")

# ---------- Telegram application ----------

application = Application.builder().token(TELEGRAM_TOKEN).build()

application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("today", handle_text))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

# ---------- Flask webhook ----------

app = Flask(__name__)

@app.post("/webhook")
async def webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return "ok"


# ----------------- WEBHOOK -----------------
@app.route(f"/webhook", methods=["POST"])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    application.update_queue.put_nowait(update)
    return "ok"

# ----------------- SCHEDULER -----------------
def morning_job():
    users = sheet.get_all_records()
    for u in users:
        if not u["birth_date"]:
            continue
        bd = datetime.strptime(u["birth_date"], "%d.%m.%Y").date()
        msg, _ = build_message(u, bd)
        application.bot.send_message(u["telegram_user_id"], msg)

scheduler = BackgroundScheduler(timezone=TZ)
scheduler.add_job(morning_job, "cron", hour=9, minute=0)
scheduler.start()

# ----------------- MAIN -----------------
def main():
    application.bot.set_webhook(f"{PUBLIC_URL}/webhook")
    app.run(host="0.0.0.0", port=10000)

if __name__ == "__main__":
    main()
