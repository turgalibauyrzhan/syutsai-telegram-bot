import os, json, base64, logging, asyncio
from datetime import datetime, timedelta
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

# --- БАЗА ДАННЫХ ОПИСАНИЙ (Из ваших CSV) ---
DESC_LG = {
    "1": {"n": "Начало нового цикла", "t": "Время выбора направления на 9 лет. Самый мощный энергетический поток..."},
    "2": {"n": "Год дипломатии", "t": "Подвижность в отношениях, не принимайте кардинальных решений..."},
    "3": {"n": "Год анализа и успеха", "t": "Пробуждается аналитическое мышление, время планирования..."},
    "7": {"n": "Год трансформации", "t": "Время глубокой внутренней трансформации, отработка кармы..."},
    "8": {"n": "Год труда и обучения", "t": "Успех через дисциплину. Хорошо для недвижимости..."},
    "9": {"n": "Год служения и разрушения", "t": "Подведение итогов, освобождение от ненужного..."}
}

DESC_LM = {
    "1": "Стратегия и лидерство. Хорошее время для начала проектов.",
    "2": "Дипломатия и чувственность. Серьезные решения лучше отложить.",
    "3": "Анализ и успех. Действовать через расчет, а не эмоции."
}

DESC_LD = {
    "1": "День новых начинаний. Поддержка в любых делах.",
    "2": "День понимания. Налаживайте связи, пейте больше воды.",
    "7": "День кризиса/трансформации. Дисциплина тела, йога, молитва.",
    "8": "День труда. Обучайтесь, не берите кредиты.",
    "9": "День благодарности. Массаж, баня, помощь людям."
}

# --- ЛОГИКА СЮЦАЙ ---
def reduce9(n):
    while n > 9: n = sum(map(int, str(n)))
    return n

def get_prognoz_data(bd_str, tz_name):
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    today = now.date()
    bd = datetime.strptime(bd_str, "%d.%m.%Y").date()
    
    # Расчеты
    od = reduce9(today.day + today.month + today.year)
    lg = reduce9(bd.day + bd.month + today.year)
    lm = reduce9(lg + today.month)
    ld = reduce9(lm + today.day)
    
    return {"od": od, "lg": lg, "lm": lm, "ld": ld, "day": today.day, "date_str": today.strftime("%d.%m.%Y"), "ym": today.strftime("%m.%Y")}

# --- GOOGLE SHEETS ---
def get_user(uid, updates=None):
    try:
        creds = Credentials.from_service_account_info(json.loads(base64.b64decode(GOOGLE_SA_JSON_B64)), 
                scopes=["https://www.googleapis.com/auth/spreadsheets"])
        ws = gspread.authorize(creds).open_by_key(GSHEET_ID).worksheet("subscriptions")
        rows = ws.get_all_values()
        uid_str, idx, u_row = str(uid), -1, []
        for i, row in enumerate(rows[1:], start=2):
            if row and row[0] == uid_str: idx, u_row = i, row; break
        if idx == -1:
            u_row = [uid_str, "active", "trial", (datetime.now()+timedelta(days=3)).strftime("%d.%m.%Y"), "", "", "", "", "", "", "", "", "Asia/Almaty", ""]
            idx = len(rows)+1
        if updates:
            m = {"status":1, "birth_date":4, "last_ym":11, "timezone":12}
            for k, v in updates.items(): u_row[m[k]] = v
            ws.update(f"A{idx}:N{idx}", [u_row])
        return u_row
    except: return None

# --- КОМАНДЫ ---
async def start(u: Update, c):
    await u.message.reply_text("✨ Введите дату рождения (ДД.ММ.ГГГГ):", reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📅 Мой прогноз на сегодня")]], resize_keyboard=True))

async def handle_msg(u: Update, c):
    text, uid = u.message.text.strip(), u.effective_user.id
    user = get_user(uid)

    if len(text) == 10 and "." in text:
        get_user(uid, {"birth_date": text})
        await u.message.reply_text(f"✅ Дата {text} сохранена!")
        return

    if text == "📅 Мой прогноз на сегодня":
        if not user[4]: await u.message.reply_text("Введите дату рождения!"); return
        
        # Проверка триала
        exp = datetime.strptime(user[3], "%d.%m.%Y")
        if user[1] != "paid" and datetime.now() > exp:
            await u.message.reply_text(f"💳 Доступ закрыт. Пишите {ADMIN_CONTACT}"); return

        res = get_prognoz_data(user[4], user[12] or "Asia/Almaty")
        is_full = (user[11] != res["ym"])
        
        msg = f"📅 *Прогноз на {res['date_str']}*\n\n"
        if res['day'] in [10, 20, 30]: msg += "⚠️ *Неблагоприятная дата:* Не начинайте новых дел!\n\n"
        if res['od'] in [3, 6]: msg += f"🌟 *Общий день {res['od']}:* День успеха!\n\n"
        else: msg += f"🌐 *Общий день:* {res['od']}\n\n"

        lg = DESC_LG.get(str(res['lg']), {"n": "Год", "t": "..."})
        msg += f"✨ *Личный год {res['lg']}: {lg['n']}*\n"
        if is_full: msg += f"{lg['t']}\n\n"
        
        lm = DESC_LM.get(str(res['lm']), "...")
        msg += f"🌙 *Личный месяц {res['lm']}:*\n"
        if is_full: msg += f"{lm}\n\n"
        
        msg += f"📍 *Личный день {res['ld']}:*\n{DESC_LD.get(str(res['ld']), '...')}"

        if is_full: get_user(uid, {"last_ym": res["ym"]})
        await u.message.reply_text(msg, parse_mode="Markdown")

# --- FLASK ---
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()

@app.route("/webhook", methods=["POST"])
async def webhook():
    await application.process_update(Update.de_json(request.get_json(force=True), application.bot))
    return "OK", 200

async def setup():
    await application.initialize(); await application.start()
    await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(setup())
    app.run(host="0.0.0.0", port=10000)