import os, json, base64, logging, asyncio
from datetime import datetime, timedelta
import pytz
from flask import Flask, request
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
import gspread
from google.oauth2.service_account import Credentials

# --- КОНФИГУРАЦИЯ ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip('/')
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")
ADMIN_CONTACT = "@knaddisyucai"

# --- КНОПКИ ---
def main_kb():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📅 Мой прогноз на сегодня")],
        [KeyboardButton("⚙️ Настройки"), KeyboardButton("🆘 Поддержка")]
    ], resize_keyboard=True)

def settings_kb():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🌍 Сменить часовой пояс")],
        [KeyboardButton("🎂 Сменить дату рождения")],
        [KeyboardButton("⬅️ Назад")]
    ], resize_keyboard=True)

# --- ЛОГИКА РАСЧЕТОВ ---
def reduce9(n: int) -> int:
    while n > 9: n = sum(map(int, str(n)))
    return n

def get_numerology(bd_str, tz_name):
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    today = now.date()
    bd = datetime.strptime(bd_str, "%d.%m.%Y").date()
    
    od = reduce9(today.day + today.month + today.year)
    lg = reduce9(bd.day + bd.month + today.year)
    lm = reduce9(lg + today.month)
    ld = reduce9(lm + today.day)
    return {"od": od, "lg": lg, "lm": lm, "ld": ld, "dt": today}

# --- GOOGLE SHEETS (14 СТОЛБЦОВ) ---
def get_ws():
    decoded = base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
    creds = Credentials.from_service_account_info(json.loads(decoded), 
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds).open_by_key(GSHEET_ID).worksheet("subscriptions")

def sync_user(uid, updates=None):
    ws = get_ws()
    rows = ws.get_all_values()
    uid_str = str(uid)
    idx = -1
    user_data = []

    for i, row in enumerate(rows[1:], start=2):
        if row and row[0] == uid_str:
            idx, user_data = i, row
            break
    
    if idx == -1:
        trial_exp = (datetime.now() + timedelta(days=3)).strftime("%d.%m.%Y")
        user_data = [uid_str, "active", "trial", trial_exp, "", datetime.now().isoformat(), "", "", "", "", datetime.now().strftime("%d.%m.%Y"), "", "Asia/Almaty", ""]
        idx = len(rows) + 1
    
    if updates:
        mapping = {"status":1, "plan":2, "trial_expires":3, "birth_date":4, "last_ym":11, "timezone":12, "phone":13}
        for k, v in updates.items():
            if k in mapping:
                while len(user_data) <= mapping[k]: user_data.append("")
                user_data[mapping[k]] = v
        ws.update(f"A{idx}:N{idx}", [user_data])
    
    return user_data

# --- ОБРАБОТКА ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🌟 *Добро пожаловать в Сюцай Бот!*\n\nВведите дату рождения (ДД.ММ.ГГГГ), чтобы получить прогноз и активировать 3 дня доступа.",
        parse_mode="Markdown", reply_markup=main_kb())

async def handle_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid = update.effective_user.id

    if text == "⬅️ Назад":
        await update.message.reply_text("Главное меню", reply_markup=main_kb())
        return
    
    if text == "⚙️ Настройки":
        await update.message.reply_text("Настройки:", reply_markup=settings_kb())
        return

    # Ввод даты
    if len(text) == 10 and text.count(".") == 2:
        try:
            datetime.strptime(text, "%d.%m.%Y")
            sync_user(uid, {"birth_date": text})
            await update.message.reply_text(f"✅ Дата {text} сохранена! Проверьте ваш прогноз.", reply_markup=main_kb())
        except:
            await update.message.reply_text("❌ Ошибка формата. Нужно: 16.09.1994")
        return

    # ПРОГНОЗ
    if text == "📅 Мой прогноз на сегодня":
        user = sync_user(uid)
        if not user[4]:
            await update.message.reply_text("Сначала введите дату рождения!")
            return

        # Проверка триала
        trial_dt = datetime.strptime(user[3], "%d.%m.%Y")
        if user[1] != "paid" and datetime.now() > trial_dt:
            await update.message.reply_text(f"⌛️ Доступ истек. Напишите {ADMIN_CONTACT} для оплаты.")
            return

        # Расчет
        res = get_numerology(user[4], user[12] or "Asia/Almaty")
        cur_ym = res['dt'].strftime("%m.%Y")
        is_full = (user[11] != cur_ym) # Если месяц сменился — даем полное описание

        msg = f"📅 *Прогноз на {res['dt'].strftime('%d.%m.%Y')}*\n\n"
        
        # Логика ОД (из CSV)
        if res['dt'].day in [10, 20, 30]:
            msg += "⚠️ *Неблагоприятная дата (10/20/30):* Нежелательно начинать новые проекты.\n\n"
        elif res['od'] in [3, 6]:
            msg += f"🌟 *Общий день {res['od']}:* Успех и удача в делах!\n\n"
        else:
            msg += f"🌐 *Общий день:* {res['od']}\n\n"

        msg += f"✨ *Личный год {res['lg']}:* { 'ПОЛНОЕ ОПИСАНИЕ ИЗ CSV' if is_full else 'Краткая суть...'}\n\n"
        msg += f"🌙 *Личный месяц {res['lm']}:* { 'ПОЛНОЕ ОПИСАНИЕ' if is_full else 'Энергия месяца...'}\n\n"
        msg += f"📍 *Личный день {res['ld']}:* Описание дня..."

        if is_full:
            sync_user(uid, {"last_ym": cur_ym}) # Помечаем, что полное описание за этот месяц выдано

        await update.message.reply_text(msg, parse_mode="Markdown")

# --- FLASK ---
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()

@app.route("/webhook", methods=["POST"])
async def webhook():
    await application.process_update(Update.de_json(request.get_json(force=True), application.bot))
    return "OK", 200

async def setup():
    await application.initialize()
    await application.start()
    await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup())
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))