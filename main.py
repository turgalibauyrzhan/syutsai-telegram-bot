import os
import json
import base64
import logging
import asyncio
import threading
from datetime import datetime, timedelta

import pytz
import gspread
from google.oauth2.service_account import Credentials

from flask import Flask, request

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ================= НАСТРОЙКИ =================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")

DEFAULT_TZ = "Asia/Almaty"

# FSM
WAIT_TZ = "WAIT_TZ"
WAIT_NOTIFY_TIME = "WAIT_NOTIFY_TIME"
WAIT_BIRTH = "WAIT_BIRTH"
CHANGE_TZ = "CHANGE_TZ"
CHANGE_NOTIFY_TIME = "CHANGE_NOTIFY_TIME"
READY = "READY"

ROW_SIZE = 9

# ================= КЛАВИАТУРЫ =================
def tz_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🇰🇿 Алматы"), KeyboardButton("🇷🇺 Москва")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def time_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("06:00"), KeyboardButton("08:00")],
            [KeyboardButton("09:00"), KeyboardButton("11:00")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def main_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📅 Мой прогноз")],
            [KeyboardButton("⏰ Изменить время уведомлений")],
            [KeyboardButton("🌍 Изменить часовой пояс")],
            [KeyboardButton("💳 Мой тариф")],
        ],
        resize_keyboard=True,
    )

# ================= УТИЛИТЫ =================
def normalize_row(r):
    return r + [""] * (ROW_SIZE - len(r))

def validate_date(text):
    try:
        return datetime.strptime(text, "%d.%m.%Y")
    except:
        return None

def validate_time(text):
    try:
        datetime.strptime(text, "%H:%M")
        return True
    except:
        return False

# ================= GOOGLE SHEETS =================
_ws = None

def get_ws():
    global _ws
    if _ws:
        return _ws

    creds_json = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode())
    creds = Credentials.from_service_account_info(
        creds_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GSHEET_ID)

    try:
        ws = sh.worksheet("users")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="users", rows=1000, cols=ROW_SIZE)
        ws.append_row([
            "user_id",
            "status",
            "trial_until",
            "birth_date",
            "timezone",
            "notify_time",
            "step",
            "created_at",
            "updated_at",
        ])

    _ws = ws
    return ws

def get_user(update: Update):
    ws = get_ws()
    uid = str(update.effective_user.id)
    rows = ws.get_all_values()

    for r in rows[1:]:
        if r and r[0] == uid:
            return normalize_row(r)

    return None

def update_user(update: Update, **fields):
    ws = get_ws()
    uid = str(update.effective_user.id)
    rows = ws.get_all_values()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    col_map = {
        "status": 2,
        "trial_until": 3,
        "birth_date": 4,
        "timezone": 5,
        "notify_time": 6,
        "step": 7,
    }

    for i, r in enumerate(rows[1:], start=2):
        if r and r[0] == uid:
            for k, v in fields.items():
                if k in col_map:
                    ws.update_cell(i, col_map[k], v)
            ws.update_cell(i, 9, now)
            return normalize_row(ws.row_values(i))

    row = [
        uid,
        "trial",
        (datetime.now() + timedelta(days=3)).strftime("%d.%m.%Y"),
        "",
        "",
        "",
        WAIT_TZ,
        datetime.now().strftime("%d.%m.%Y"),
        now,
    ]
    ws.append_row(row)
    return normalize_row(row)

# ================= HANDLERS =================
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    row = get_user(u)
    if not row:
        update_user(u, step=WAIT_TZ)
        await u.message.reply_text(
            "Выбери часовой пояс:",
            reply_markup=tz_keyboard()
        )
    else:
        await u.message.reply_text(
            "Главное меню:",
            reply_markup=main_keyboard()
        )

async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    text = u.message.text.strip()
    row = get_user(u)

    if not row:
        log.warning(f"user {u.effective_user.id} recreated")
        update_user(u, step=WAIT_TZ)
        await u.message.reply_text(
            "Давай начнём сначала 🙂\nВыбери часовой пояс:",
            reply_markup=tz_keyboard()
        )
        return

    step = row[6]

    if step in [WAIT_TZ, CHANGE_TZ]:
        if "Алматы" in text or "Москва" in text:
            tz = "Asia/Almaty" if "Алматы" in text else "Europe/Moscow"
            next_step = WAIT_NOTIFY_TIME if step == WAIT_TZ else READY
            update_user(u, timezone=tz, step=next_step)
            await u.message.reply_text(
                "Выбери время уведомлений:",
                reply_markup=time_keyboard(),
            )
        else:
            await u.message.reply_text("Выбери часовой пояс кнопкой.")
        return

    if step in [WAIT_NOTIFY_TIME, CHANGE_NOTIFY_TIME]:
        if validate_time(text):
            next_step = WAIT_BIRTH if step == WAIT_NOTIFY_TIME else READY
            update_user(u, notify_time=text, step=next_step)
            if step == WAIT_NOTIFY_TIME:
                await u.message.reply_text("Введи дату рождения (ДД.ММ.ГГГГ):")
            else:
                await u.message.reply_text("Время обновлено.", reply_markup=main_keyboard())
        else:
            await u.message.reply_text("Введите время ЧЧ:ММ")
        return

    if step == WAIT_BIRTH:
        if validate_date(text):
            update_user(u, birth_date=text, step=READY)
            await send_full_forecast(u, get_user(u))
        else:
            await u.message.reply_text("Неверный формат даты.")
        return


    if text == "🌍 Изменить часовой пояс":
        update_user(u, step=CHANGE_TZ)
        await u.message.reply_text("Выбери часовой пояс:", reply_markup=tz_keyboard())
        return

    if text == "⏰ Изменить время уведомлений":
        update_user(u, step=CHANGE_NOTIFY_TIME)
        await u.message.reply_text("Введите новое время:", reply_markup=time_keyboard())
        return
    if text == "📅 Мой прогноз":
        await send_full_forecast(u, row)
        return

    if text == "💳 Мой тариф":
        await u.message.reply_text(
            f"💳 Тариф: {row[1].upper()}\n"
            f"⏳ До: {row[2]}"
        )
        return
def reduce9(n: int) -> int:
    while n > 9:
        n = sum(map(int, str(n)))
    return n


async def send_full_forecast(u: Update, row):
    if not row or not row[3]:
        await u.message.reply_text(
            "Сначала укажи дату рождения 🙂",
            reply_markup=main_keyboard()
        )
        return

    bd = datetime.strptime(row[3], "%d.%m.%Y")
    tz = pytz.timezone(row[4] or DEFAULT_TZ)
    now = datetime.now(tz)

    lg = reduce9(bd.day + bd.month + now.year)
    lm = reduce9(lg + now.month)
    ld = reduce9(lm + now.day)
    od = reduce9(now.day + now.month + now.year)

    msg = (
        msg = f"📅 *ПРОГНОЗ НА {now.strftime('%d.%m.%Y')}*\n\n"
        msg += f"🌐 *Общий день {od}:*\n{DESC_OD.get(str(od), '')}\n\n"
        msg += f"📍 *Личный день {ld}:*\n{DESC_LD.get(str(ld), '')}\n\n"
        y = DESC_LG.get(str(lg), {})
        m = DESC_LM.get(str(lm), {})
        msg += f"✨ *Личный год {lg}: {y.get('n','')}*\n_{y.get('d','')}_\n"
        msg += f"*Рекомендации:* {y.get('r','')}\n"
        msg += f"*В минусе:* {y.get('m','')}\n\n"
        msg += f"🌙 *Личный месяц {lm}: {m.get('n','')}*\n_{m.get('d','')}_\n"
        msg += f"*В минусе:* {m.get('m','')}\n"
    )

    await u.message.reply_text(
        msg,
        parse_mode="Markdown",
        reply_markup=main_keyboard()
    )

# ================= SERVER =================
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()

application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

loop = asyncio.new_event_loop()
threading.Thread(target=loop.run_forever, daemon=True).start()

@app.route("/webhook", methods=["POST"])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    asyncio.run_coroutine_threadsafe(application.process_update(update), loop)
    return "OK", 200

if __name__ == "__main__":
    asyncio.run_coroutine_threadsafe(application.initialize(), loop)
    asyncio.run_coroutine_threadsafe(application.start(), loop)
    asyncio.run_coroutine_threadsafe(
        application.bot.set_webhook(f"{PUBLIC_URL}/webhook"),
        loop,
    )
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
