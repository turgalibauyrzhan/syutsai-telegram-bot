import os, json, base64, logging, asyncio
from datetime import datetime, date, timedelta
import pytz
from flask import Flask, request
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
import gspread
from google.oauth2.service_account import Credentials

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip('/')
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")
ADMIN_CONTACT = "@knaddisyucai"
TZ_DEFAULT = "Asia/Almaty"

# --- ТЕКСТЫ (УПРОЩЕНО ДЛЯ СТАБИЛЬНОСТИ) ---
TEXTS_DATA = {
    "UNFAVORABLE": "⚠️ Неблагоприятная дата. Рекомендуется отложить важные дела.",
    "LG": {"1": "ЛГ 1: Начало цикла. Время стратегии.", "2": "ЛГ 2: Дипломатия и отношения.", "3": "ЛГ 3: Анализ и успех."}, # Добавьте остальные по аналогии
    "LM": {"1": "Месяц планирования.", "2": "Месяц дипломатии."},
    "LD": {"1": "День начинаний.", "2": "День понимания."}
}

# --- ЛОГИКА НУМЕРОЛОГИИ ---
def reduce9(n: int) -> int:
    while n > 9: n = sum(map(int, str(n)))
    return n

def calculate_numerology(bd_str: str, user_tz_str: str):
    tz = pytz.timezone(user_tz_str)
    today = datetime.now(tz).date()
    bd = datetime.strptime(bd_str, "%d.%m.%Y").date()
    
    od = reduce9(today.day + today.month + today.year)
    lg = reduce9(bd.day + bd.month + today.year)
    lm = reduce9(lg + today.month)
    ld = reduce9(lm + today.day)
    return od, lg, lm, ld, today

def get_prognoz(bd_str, tz_str):
    od, lg, lm, ld, today = calculate_numerology(bd_str, tz_str)
    res = f"📅 *Прогноз на {today.strftime('%d.%m.%Y')}*\n(Часовой пояс: {tz_str})\n\n"
    res += f"🌐 Общий день: {od}\n✨ Личный год: {lg}\n🌙 Личный месяц: {lm}\n📍 Личный день: {ld}\n\n"
    res += "Для получения полного описания используйте расширенную версию."
    return res

# --- GOOGLE SHEETS ---
def get_gs_ws():
    sa_info = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8"))
    creds = Credentials.from_service_account_info(sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc = gspread.authorize(creds)
    return gc.open_by_key(GSHEET_ID).worksheet("subscriptions")

def upsert_user(uid, updates: dict):
    try:
        ws = get_gs_ws()
        data = ws.get_all_values()
        uid_str = str(uid)
        row_idx = -1
        current_row = []

        for i, row in enumerate(data[1:], start=2):
            if row and str(row[0]) == uid_str:
                row_idx = i
                current_row = row
                break

        if row_idx != -1:
            new_row = list(current_row)
            while len(new_row) < 14: new_row.append("")
        else:
            exp_date = (datetime.now() + timedelta(days=3)).strftime("%d.%m.%Y")
            new_row = [uid_str, "active", "trial", exp_date, "", datetime.now().isoformat(), "", "", "", "", datetime.now().strftime("%d.%m.%Y"), "", TZ_DEFAULT, ""]
            row_idx = len(data) + 1

        mapping = {"status":1, "plan":2, "trial_expires":3, "birth_date":4, "timezone":12, "phone":13}
        for k, v in updates.items():
            if k in mapping: new_row[mapping[k]] = v
        
        new_row[6] = datetime.now().isoformat()
        ws.update(f"A{row_idx}:N{row_idx}", [new_row])
        return new_row
    except Exception as e:
        log.error(f"GS Error: {e}")
        return None

# --- КОМАНДЫ ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот Сюцай. Даю прогноз на день по дате рождения.\n\n"
        "Сначала введите дату рождения в формате: *16.09.1994*",
        parse_mode="Markdown"
    )

async def handle_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.effective_user
    
    # Кнопки
    if text == "⚙️ Сменить часовой пояс":
        kb = [[KeyboardButton("Алматы (UTC+5)"), KeyboardButton("Москва (UTC+3)")]]
        await update.message.reply_text("Выберите пояс:", reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))
        return

    if "UTC+" in text:
        tz = "Asia/Almaty" if "Алматы" in text else "Europe/Moscow"
        upsert_user(user.id, {"timezone": tz})
        await update.message.reply_text(f"✅ Установлен пояс: {text}", reply_markup=main_kb())
        return

    if text == "📅 Сменить дату рождения":
        await update.message.reply_text("Введите новую дату рождения (ДД.ММ.ГГГГ):")
        return

    # Прогноз
    if text == "Сегодня":
        ws = get_gs_ws()
        rows = ws.get_all_values()
        user_row = next((r for r in rows if r[0] == str(user.id)), None)
        
        if not user_row or not user_row[4]:
            await update.message.reply_text("Введите дату рождения!")
            return

        # Проверка триала
        trial_exp = datetime.strptime(user_row[3], "%d.%m.%Y")
        if user_row[1] != "paid" and datetime.now() > trial_exp:
            await update.message.reply_text(f"⌛️ Триал истек. Для доступа напишите {ADMIN_CONTACT}")
            return

        res = get_prognoz(user_row[4], user_row[12] or TZ_DEFAULT)
        await update.message.reply_text(res, parse_mode="Markdown", reply_markup=main_kb())
        return

    # Ввод даты
    try:
        datetime.strptime(text, "%d.%m.%Y")
        upsert_user(user.id, {"birth_date": text})
        await update.message.reply_text(f"✅ Дата {text} сохранена!", reply_markup=main_kb())
    except ValueError:
        await update.message.reply_text("❌ Используйте формат ДД.ММ.ГГГГ")

def main_kb():
    return ReplyKeyboardMarkup([[KeyboardButton("Сегодня")], [KeyboardButton("⚙️ Сменить часовой пояс"), KeyboardButton("📅 Сменить дату рождения")]], resize_keyboard=True)

# --- ЗАПУСК ---
application = Application.builder().token(TELEGRAM_TOKEN).build()
flask_app = Flask(__name__)

@flask_app.route('/webhook', methods=['POST'])
async def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    await application.process_update(update)
    return "OK", 200

@flask_app.route('/')
def index(): return "OK", 200

application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

async def setup():
    await application.initialize()
    await application.start()
    await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup())
    flask_app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))