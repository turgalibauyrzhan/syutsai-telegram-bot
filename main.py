import os
import json
import base64
import logging
import asyncio
import threading
from datetime import datetime, timedelta

import pytz
from flask import Flask, request
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
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


# ================= ОПИСАНИЯ =================
DESC_LG = {
    "1": {"n": "Начало нового цикла", "d": "Выбор направления на 9 лет.", "r": "Бери ответственность.", "m": "Пустота."},
    "2": {"n": "Отношения", "d": "Перемены в связях.", "r": "Гибкость.", "m": "Разрывы."},
    "3": {"n": "Анализ", "d": "Успех через расчёт.", "r": "Планируй.", "m": "Лень."},
    "4": {"n": "Цели", "d": "Постановка целей.", "r": "Честность.", "m": "Риски."},
    "5": {"n": "Масштаб", "d": "Коммуникации.", "r": "Расширяйся.", "m": "Экстрим."},
    "6": {"n": "Комфорт", "d": "Любовь и деньги.", "r": "Инвестируй.", "m": "Долги."},
    "7": {"n": "Кризис", "d": "Карма.", "r": "Дисциплина.", "m": "Хаос."},
    "8": {"n": "Труд", "d": "Фундамент.", "r": "Учись.", "m": "Перегруз."},
    "9": {"n": "Завершение", "d": "Итоги.", "r": "Отпусти.", "m": "Эмоции."},
}

DESC_LM = {
    "1": {"n": "Начало", "d": "Новые проекты.", "m": "Эго."},
    "2": {"n": "Дипломатия", "d": "Спокойствие.", "m": "Сомнения."},
    "3": {"n": "Анализ", "d": "Обучение.", "m": "Лень."},
    "4": {"n": "Мистика", "d": "Цели.", "m": "Паника."},
    "5": {"n": "Рост", "d": "Бизнес.", "m": "Хаос."},
    "6": {"n": "Любовь", "d": "Интуиция.", "m": "Излишества."},
    "7": {"n": "Трансформация", "d": "Практики.", "m": "Срывы."},
    "8": {"n": "Работа", "d": "Контроль.", "m": "Жёсткость."},
    "9": {"n": "Благодарность", "d": "Завершение.", "m": "Воинственность."},
}

DESC_LD = {
    "1": "Новые начинания.",
    "2": "Дипломатия.",
    "3": "Планирование.",
    "4": "Честность.",
    "5": "Сделки.",
    "6": "Творчество.",
    "7": "Дисциплина.",
    "8": "Обучение.",
    "9": "Здоровье.",
}


# ================= УТИЛИТЫ =================
def reduce9(n: int) -> int:
    while n > 9:
        n = sum(map(int, str(n)))
    return n


def validate_date(text: str):
    try:
        return datetime.strptime(text, "%d.%m.%Y")
    except ValueError:
        return None


def get_ws():
    creds_json = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8"))
    creds = Credentials.from_service_account_info(
        creds_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds).open_by_key(GSHEET_ID).worksheet("subscriptions")


# ================= БАЗА =================
def sync_user(update: Update, birth=None, tz=None):
    try:
        ws = get_ws()
        uid = str(update.effective_user.id)
        rows = ws.get_all_values()

        now = datetime.now().strftime("%d.%m.%Y %H:%M")

        for i, r in enumerate(rows, start=1):
            if r and r[0] == uid:
                if birth:
                    ws.update_cell(i, 5, birth)
                if tz:
                    ws.update_cell(i, 13, tz)
                ws.update_cell(i, 7, now)

                trial_until = r[3]
                if trial_until:
                    if datetime.strptime(trial_until, "%d.%m.%Y") < datetime.now():
                        return {"expired": True}

                r_dict = r + [""] * (13 - len(r))
                return {
                    "row": r_dict,
                    "tz": r_dict[12] or DEFAULT_TZ,
                }

        # новый пользователь
        trial_until = (datetime.now() + timedelta(days=3)).strftime("%d.%m.%Y")
        row = [
            uid, "active", "trial", trial_until,
            birth or "", now, now,
            update.effective_user.username or "",
            update.effective_user.first_name or "",
            update.effective_user.last_name or "",
            datetime.now().strftime("%d.%m.%Y"),
            "",
            tz or DEFAULT_TZ,
        ]
        ws.append_row(row)
        return {"row": row, "tz": row[12]}

    except Exception as e:
        log.error(f"GSheet error: {e}")
        return {"error": True}


# ================= ПРОГНОЗ =================
async def send_full_forecast(update: Update, user):
    try:
        row = user["row"]
        tz = pytz.timezone(user["tz"])

        bd_raw = (row[4] or "").strip()
        bd = datetime.strptime(bd_raw, "%d.%m.%Y")

        now = datetime.now(tz)

        lg = reduce9(bd.day + bd.month + now.year)
        lm = reduce9(lg + now.month)
        ld = reduce9(lm + now.day)
        od = reduce9(now.day + now.month + now.year)

        msg = f"📅 *Прогноз на {now.strftime('%d.%m.%Y')}*\n\n"
        msg += f"🌐 *Общий день:* {od}\n\n"

        y = DESC_LG.get(str(lg), {})
        m = DESC_LM.get(str(lm), {})
        d = DESC_LD.get(str(ld), "Нет данных для дня")

        msg += (
            f"✨ *Личный год {lg}: {y.get('n','')}*\n"
            f"{y.get('d','')}\n"
            f"*Рекомендации:* {y.get('r','')}\n"
            f"*В минусе:* {y.get('m','')}\n\n"
        )

        msg += (
            f"🌙 *Личный месяц {lm}: {m.get('n','')}*\n"
            f"{m.get('d','')}\n"
            f"*В минусе:* {m.get('m','')}\n\n"
        )

        msg += f"📍 *Личный день {ld}:*\n{d}"

        await update.effective_message.reply_text(
            msg,
            parse_mode="Markdown"
        )

    except Exception as e:
        log.exception("Forecast error")
        await update.effective_message.reply_text(
            "Ошибка генерации прогноза."
        )


# ================= HANDLERS =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🇰🇿 Алматы", callback_data="tz_Asia/Almaty")],
        [InlineKeyboardButton("🇷🇺 Москва", callback_data="tz_Europe/Moscow")],
    ])
    await update.message.reply_text("Выбери часовой пояс:", reply_markup=kb)


async def tz_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tz = update.callback_query.data.replace("tz_", "")
    sync_user(update, tz=tz)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Мой прогноз", callback_data="forecast")]
    ])
    await update.callback_query.message.reply_text(
        "Часовой пояс сохранён. Введи дату рождения (ДД.ММ.ГГГГ):",
        reply_markup=kb,
    )


async def forecast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = sync_user(update)
    if user.get("error"):
        await update.callback_query.message.reply_text("Ошибка данных.")
        return
    if user.get("expired"):
        await update.callback_query.message.reply_text("⛔ Пробный период закончился.")
        return
    if not user["row"][4]:
        await update.callback_query.message.reply_text("Сначала введи дату рождения.")
        return

    await send_full_forecast(update, user)


async def handle_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    bd = validate_date(text)
    if not bd:
        await update.message.reply_text("Неверная дата. Формат ДД.ММ.ГГГГ")
        return

    user = sync_user(update, birth=text)
    if user.get("error"):
        await update.message.reply_text("Ошибка сохранения данных.")
        return

    await send_full_forecast(update, user)


# ================= SERVER =================
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()

application.add_handler(CommandHandler("start", start))
application.add_handler(CallbackQueryHandler(tz_callback, pattern="^tz_"))
application.add_handler(CallbackQueryHandler(forecast_callback, pattern="^forecast$"))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))


loop = asyncio.new_event_loop()
threading.Thread(target=lambda: loop.run_forever(), daemon=True).start()


@app.route("/webhook", methods=["POST"])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    asyncio.run_coroutine_threadsafe(application.process_update(update), loop)
    return "OK", 200


@app.route("/")
def index():
    return "Bot is running", 200


if __name__ == "__main__":
    # init telegram
    loop.call_soon_threadsafe(asyncio.create_task, application.initialize())
    loop.call_soon_threadsafe(asyncio.create_task, application.bot.set_webhook(
        f"{PUBLIC_URL}/webhook"
    ))

    # ВАЖНО: Flask должен стартовать СРАЗУ
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000)),
    )
