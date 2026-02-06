import os
import json
import base64
import logging
import asyncio
import threading
from datetime import datetime, timedelta
from desc_lg import DESC_LG
from desc_lm import DESC_LM
from desc_ld import DESC_LD
from desc_od import DESC_OD
import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from flask import Flask, request

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

import gspread
from google.oauth2.service_account import Credentials


# ================= НАСТРОЙКИ =================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")

DEFAULT_TZ = "Asia/Almaty"

# step значения
WAIT_TZ = "WAIT_TZ"
WAIT_NOTIFY_TIME = "WAIT_NOTIFY_TIME"
WAIT_BIRTH = "WAIT_BIRTH"
READY = "READY"

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
            [KeyboardButton("09:00"), KeyboardButton("12:00")],
            [KeyboardButton("18:00"), KeyboardButton("21:00")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def main_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📅 Мой прогноз")]],
        resize_keyboard=True,
    )


# ================= УТИЛИТЫ =================
def reduce9(n: int) -> int:
    while n > 9:
        n = sum(map(int, str(n)))
    return n


def validate_date(text: str):
    try:
        d = datetime.strptime(text, "%d.%m.%Y")
        if d > datetime.now():
            return None
        return d
    except ValueError:
        return None


def validate_time(text: str) -> bool:
    try:
        datetime.strptime(text, "%H:%M")
        return True
    except ValueError:
        return False


# ================= GOOGLE SHEETS =================
def get_ws():
    creds_json = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode())
    creds = Credentials.from_service_account_info(
        creds_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds).open_by_key(GSHEET_ID).worksheet("subscriptions")


def sync_user(update: Update, **fields):
    """
    Храним:
    0 uid
    4 birth
    12 tz
    13 notify_time
    14 step
    """
    try:
        ws = get_ws()
        uid = str(update.effective_user.id)
        rows = ws.get_all_values()
        now = datetime.now().strftime("%d.%m.%Y %H:%M")

        for i, r in enumerate(rows, start=1):
            if r and r[0] == uid:
                if "birth" in fields:
                    ws.update_cell(i, 5, fields["birth"])
                if "tz" in fields:
                    ws.update_cell(i, 13, fields["tz"])
                if "notify_time" in fields:
                    ws.update_cell(i, 14, fields["notify_time"])
                if "step" in fields:
                    ws.update_cell(i, 15, fields["step"])

                ws.update_cell(i, 7, now)

                row = ws.row_values(i)
                row += [""] * (15 - len(row))
                return row

        # новый пользователь
        row = [
            uid,
            "active",
            "trial",
            (datetime.now() + timedelta(days=3)).strftime("%d.%m.%Y"),
            fields.get("birth", ""),
            now,
            now,
            update.effective_user.username or "",
            update.effective_user.first_name or "",
            update.effective_user.last_name or "",
            datetime.now().strftime("%d.%m.%Y"),
            "",
            fields.get("tz", ""),
            fields.get("notify_time", ""),
            fields.get("step", WAIT_TZ),
        ]
        ws.append_row(row)
        return row

    except Exception:
        log.exception("GSheet error")
        return None
def has_access(row) -> bool:
    """
    row:
    1  -> status
    3  -> trial_until (ДД.ММ.ГГГГ)
    """
    status = row[1].strip().lower()

    if status == "premium":
        return True

    try:
        trial_until = datetime.strptime(row[3], "%d.%m.%Y").date()
    except Exception:
        return False

    return datetime.now().date() <= trial_until


# ================= ПРОГНОЗ =================
async def send_full_forecast(u: Update, row):
    if not has_access(row):
        await u.message.reply_text(
            "⛔ Пробный период завершён.\n\n"
            "Для продолжения доступа обратитесь:\n"
            "📞 +7 778 990 01 14"
        )
    return

    try:
        birth_raw = row[4].strip()
        tz_name = row[12] or DEFAULT_TZ

        bd = datetime.strptime(birth_raw, "%d.%m.%Y")
        tz = pytz.timezone(tz_name)
        now = datetime.now(tz)

        lg = reduce9(bd.day + bd.month + now.year)
        lm = reduce9(lg + now.month)
        ld = reduce9(lm + now.day)
        od = reduce9(now.day + now.month + now.year)

        msg = f"📅 *ПРОГНОЗ НА {now.strftime('%d.%m.%Y')}*\n\n"
        msg += (
            f"🌐 *Общий день {od}:*\n"
            f"{DESC_OD.get(str(od), '')}\n\n"
        msg += f"📍 *Личный день {ld}:*\n{DESC_LD.get(str(ld),'')}"
        y = DESC_LG.get(str(lg), {})
        m = DESC_LM.get(str(lm), {})
        msg += f"✨ *Личный год {lg}: {y.get('n','')}*\n_{y.get('d','')}_\n"
        msg += f"*Рекомендации:* {y.get('r','')}\n"
        msg += f"*В минусе:* {y.get('m','')}\n\n"
        msg += f"🌙 *Личный месяц {lm}: {m.get('n','')}*\n_{m.get('d','')}_\n"
        msg += f"*В минусе:* {m.get('m','')}\n\n"
        )
        
        await u.message.reply_text(
            msg,
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )

    except Exception:
        log.exception("Forecast error")
        await u.message.reply_text("Ошибка генерации прогноза.")


# ================= HANDLERS =================
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    sync_user(u, step=WAIT_TZ)
    await u.message.reply_text(
        "Выбери часовой пояс:",
        reply_markup=tz_keyboard(),
    )


async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    text = u.message.text.strip()
    row = sync_user(u)
    step = row[14] if row and len(row) > 14 else WAIT_TZ

    # -------- WAIT_TZ --------
    if step == WAIT_TZ:
        if text in ["🇰🇿 Алматы", "🇷🇺 Москва"]:
            tz = "Asia/Almaty" if "Алматы" in text else "Europe/Moscow"
            sync_user(u, tz=tz, step=WAIT_NOTIFY_TIME)
            await u.message.reply_text(
                "Выбери время уведомления или введи своё (ЧЧ:ММ):",
                reply_markup=time_keyboard(),
            )
        else:
            await u.message.reply_text(
                "Пожалуйста, выбери часовой пояс кнопкой.",
                reply_markup=tz_keyboard(),
            )
        return

    # -------- WAIT_NOTIFY_TIME --------
    if step == WAIT_NOTIFY_TIME:
        if validate_time(text):
            sync_user(u, notify_time=text, step=WAIT_BIRTH)
            await u.message.reply_text(
                "Время сохранено.\nВведи дату рождения (ДД.ММ.ГГГГ):",
                reply_markup=ReplyKeyboardRemove(),
            )
        else:
            await u.message.reply_text(
                "Введите время в формате ЧЧ:ММ, например 08:30",
                reply_markup=time_keyboard(),
            )
        return

    # -------- WAIT_BIRTH --------
    if step == WAIT_BIRTH:
        bd = validate_date(text)
        if bd:
            row = sync_user(u, birth=text, step=READY)
            await send_full_forecast(u, row)
        else:
            await u.message.reply_text(
                "Дата должна быть в формате ДД.ММ.ГГГГ",
            )
        return

    # -------- READY --------
    if step == READY:
        if not has_access(row):
            await u.message.reply_text(
                "⛔ Пробный период завершён.\n\n"
                "Для продолжения доступа обратитесь:\n"
                "📞 +7 778 990 01 14"
            )
            return

        if text == "📅 Мой прогноз":
            await send_full_forecast(u, row)

async def send_daily_forecast(application: Application, row):
    try:
        fake_update = Update(
            update_id=0,
            message=None,
        )
        fake_update._effective_user = type(
            "User", (), {"id": int(row[0])}
        )

        class FakeMessage:
            async def reply_text(self, *args, **kwargs):
                await application.bot.send_message(
                    chat_id=row[0],
                    text=args[0],
                    parse_mode=kwargs.get("parse_mode"),
                    reply_markup=kwargs.get("reply_markup"),
                )

        fake_update.message = FakeMessage()

        await send_full_forecast(fake_update, row)

    except Exception:
        log.exception("Daily forecast error")
def daily_job():
    try:
        ws = get_ws()
        rows = ws.get_all_values()

        for r in rows[1:]:
            if len(r) < 15:
                continue
            if r[14] != READY:
                continue

            if not has_access(r):
                continue


            uid = r[0]
            tz_name = r[12]
            notify_time = r[13]

            if not tz_name or not notify_time:
                continue

            tz = pytz.timezone(tz_name)
            now = datetime.now(tz)

            hh, mm = map(int, notify_time.split(":"))

            if now.hour == hh and now.minute == mm:
                asyncio.run_coroutine_threadsafe(
                    send_daily_forecast(application, r),
                    loop,
                )

    except Exception:
        log.exception("Scheduler error")


# ================= SERVER =================
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()

application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))


# ================= EVENT LOOP =================
loop = asyncio.new_event_loop()

def run_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=run_loop, daemon=True).start()


@app.route("/webhook", methods=["POST"])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    asyncio.run_coroutine_threadsafe(application.process_update(update), loop)
    return "OK", 200


@app.route("/")
def index():
    return "Bot is running", 200


if __name__ == "__main__":
    asyncio.run_coroutine_threadsafe(application.initialize(), loop)
    asyncio.run_coroutine_threadsafe(application.start(), loop)
    asyncio.run_coroutine_threadsafe(
        application.bot.set_webhook(f"{PUBLIC_URL}/webhook"),
        loop,
    )
scheduler = BackgroundScheduler()
scheduler.add_job(daily_job, "interval", minutes=1)
scheduler.start()

app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000)),
        )
    
    