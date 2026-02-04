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

# --- ДАННЫЕ ИЗ ВАШИХ CSV ---
DATA = {
    "LG": {
        "1": "Начало нового цикла. Время выбора направления на 9 лет. Самый мощный энергетический поток.",
        "2": "Год построения отношений и дипломатии. Не принимайте кардинальных решений.",
        "3": "Год анализа и успеха. Пробуждается аналитическое мышление: планируйте, ведите учет.",
        "7": "Год трансформации и кризиса. Время глубокой внутренней работы и отработки кармы.",
        "8": "Год труда и обучения. Успех через дисциплину. Хорошо для операций с недвижимостью.",
        "9": "Год служения и разрушения. Подведение итогов, освобождение от старого."
    },
    "LM": {
        "1": "Месяц лидерства. Стратегия и планирование. Хорошо для новых проектов.",
        "2": "Месяц дипломатии. Активизируется энергия воспоминаний. Пейте больше воды.",
        "3": "Месяц анализа. Действуйте через расчет, а не через эмоции. Хорошо для экзаменов."
    },
    "LD": {
        "1": "День начинаний. Любое дело получит поддержку энергии дня.",
        "7": "День кризиса/трансформации. Начните утро с дисциплины тела: ходьба, йога.",
        "8": "День обучения и труда. Избегайте пустого отдыха. Кредиты брать нельзя.",
        "9": "День благодарности. Полезны баня, массаж. Помогайте людям и отдавайте долги."
    }
}

# --- ЛОГИКА ---
def reduce9(n):
    while n > 9: n = sum(map(int, str(n)))
    return n

def get_calc(bd_str, tz_name="Asia/Almaty"):
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    today = now.date()
    bd = datetime.strptime(bd_str, "%d.%m.%Y").date()
    
    od = reduce9(today.day + today.month + today.year)
    lg = reduce9(bd.day + bd.month + today.year)
    lm = reduce9(lg + today.month)
    ld = reduce9(lm + today.day)
    return {"od": od, "lg": lg, "lm": lm, "ld": ld, "day": today.day, "date": today.strftime("%d.%m.%Y"), "ym": today.strftime("%m.%Y")}

# --- ТАБЛИЦА ---
def sync_user(uid, updates=None):
    try:
        creds = Credentials.from_service_account_info(json.loads(base64.b64decode(GOOGLE_SA_JSON_B64)), 
                scopes=["https://www.googleapis.com/auth/spreadsheets"])
        ws = gspread.authorize(creds).open_by_key(GSHEET_ID).worksheet("subscriptions")
        rows = ws.get_all_values()
        uid_str, idx, u_row = str(uid), -1, []
        for i, r in enumerate(rows[1:], start=2):
            if r and r[0] == uid_str: idx, u_row = i, r; break
        if idx == -1:
            u_row = [uid_str, "active", "trial", (datetime.now()+timedelta(days=3)).strftime("%d.%m.%Y"), "", "", "", "", "", "", "", "", "Asia/Almaty", ""]
            idx = len(rows)+1
        if updates:
            m = {"status":1, "birth":4, "last_ym":11, "tz":12}
            for k, v in updates.items(): u_row[m[k]] = v
            ws.update(f"A{idx}:N{idx}", [u_row])
        return u_row
    except: return None

# --- КОМАНДЫ ---
async def start(u: Update, c):
    await u.message.reply_text("✨ Добро пожаловать! Введите дату рождения (ДД.ММ.ГГГГ):", 
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📅 Мой прогноз")]], resize_keyboard=True))

async def handle_msg(u: Update, c):
    text, uid = u.message.text.strip(), u.effective_user.id
    user = sync_user(uid)

    if len(text) == 10 and "." in text:
        sync_user(uid, {"birth": text})
        await u.message.reply_text(f"✅ Дата {text} сохранена! Нажмите 'Мой прогноз'.")
        return

    if text == "📅 Мой прогноз":
        if not user or not user[4]: await u.message.reply_text("Введите дату рождения!"); return
        
        # Проверка триала
        exp = datetime.strptime(user[3], "%d.%m.%Y")
        if user[1] != "paid" and datetime.now() > exp:
            await u.message.reply_text(f"💳 Доступ закрыт. Пишите {ADMIN_CONTACT}"); return

        res = get_calc(user[4], user[12] or "Asia/Almaty")
        is_full = (user[11] != res["ym"]) # Если месяц новый - даем полные тексты
        
        msg = f"📅 *Прогноз на {res['date']}*\n\n"
        
        # Общий день
        if res['day'] in [10, 20, 30]:
            msg += "⚠️ *Внимание!* 10, 20, 30 числа — неблагоприятные даты. Риск обнуления результатов.\n\n"
        elif res['od'] in [3, 6]:
            msg += f"🌟 *Общий день {res['od']}:* Благоприятный день для сделок и начинаний!\n\n"
        else:
            msg += f"🌐 *Общий день:* {res['od']}\n\n"

        # Тексты (ЛГ, ЛМ, ЛД)
        msg += f"✨ *Личный год {res['lg']}:*\n"
        msg += f"{DATA['LG'].get(str(res['lg']), '... ')}\n\n" if is_full else "_Энергия года (описание было 1-го числа)_\n\n"
        
        msg += f"🌙 *Личный месяц {res['lm']}:*\n"
        msg += f"{DATA['LM'].get(str(res['lm']), '... ')}\n\n" if is_full else "_Фокус месяца остается прежним._\n\n"
        
        msg += f"📍 *Личный день {res['ld']}:*\n{DATA['LD'].get(str(res['ld']), '...')}"

        if is_full: sync_user(uid, {"last_ym": res["ym"]})
        await u.message.reply_text(msg, parse_mode="Markdown")

# --- ЗАПУСК ---
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