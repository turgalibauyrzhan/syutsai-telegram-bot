import os, json, base64, logging, asyncio
from datetime import datetime, timedelta
import pytz
from flask import Flask, request
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
import gspread
from google.oauth2.service_account import Credentials

# --- LOGGING & KONFIGURATION ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip('/')
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")
ADMIN_CONTACT = "@knaddisyucai"

# --- DATEN AUS DEINEN DATEIEN (STRUKTURIERT) ---
# Hier habe ich die wichtigsten Beschreibungen aus deinen CSVs zusammengeführt
SYUTSAI_DATA = {
    "LG": { # Persönliches Jahr
        "1": "Начало нового цикла. Время выбора направления на 9 лет. Мощный поток энергии.",
        "2": "Год построения отношений и дипломатии. Не принимайте кардинальных решений.",
        "3": "Год анализа и успеха. Пробуждается аналитическое мышление, планируйте шаги.",
        "7": "Год трансформации и кризиса. Глубокая внутренняя трансформация.",
        "8": "Год труда и обучения. Успех через дисциплину, хорошо для недвижимости.",
        "9": "Год служения и завершения. Подведение итогов, освобождение от старого."
    },
    "LM": { # Persönlicher Monat
        "1": "Хороший месяц для начала дел. Лидерство, стратегия и планирование.",
        "2": "Месяц дипломатии и выстраивания отношений. Пейте больше воды.",
        "3": "Месяц анализа и успеха. Действуйте через расчет, а не эмоции.",
        "6": "Месяц любви и успеха. Творчество, удача, инвестиции.",
        "7": "Месяц кризиса или трансформации. Дисциплина и духовные практики."
    },
    "LD": { # Persönlicher Tag
        "1": "День новых начинаний. Любое дело получит поддержку энергии дня.",
        "2": "День понимания и дипломатии. Налаживайте старые связи.",
        "7": "День кризиса или трансформации. Дисциплина тела, йога, молитва.",
        "8": "День обучения и труда. Финансовый результат через навыки.",
        "9": "День здоровья и благодарности. Баня, массаж, помощь людям."
    },
    "OD": { # Allgemeiner Tag
        "3": "Благоприятный день через анализ. Подходит для сделок и документов.",
        "6": "Благоприятный день через любовь. Успех в новых начинаниях.",
        "bad_dates": "Нежелательно начинать новые проекты. Риск обнуления результатов."
    }
}

# --- HILFSFUNKTIONEN ---
def reduce9(n):
    while n > 9: n = sum(map(int, str(n)))
    return n

def calculate_all(bd_str, tz_name="Asia/Almaty"):
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    today = now.date()
    bd = datetime.strptime(bd_str, "%d.%m.%Y").date()
    
    od = reduce9(today.day + today.month + today.year)
    lg = reduce9(bd.day + bd.month + today.year)
    lm = reduce9(lg + today.month)
    ld = reduce9(lm + today.day)
    
    return {"od": od, "lg": lg, "lm": lm, "ld": ld, "day": today.day, "date": today.strftime("%d.%m.%Y"), "ym": today.strftime("%m.%Y")}

# --- GOOGLE SHEETS LOGIK ---
def get_user_row(uid, updates=None):
    try:
        decoded = base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
        creds = Credentials.from_service_account_info(json.loads(decoded), scopes=["https://www.googleapis.com/auth/spreadsheets"])
        ws = gspread.authorize(creds).open_by_key(GSHEET_ID).worksheet("subscriptions")
        
        data = ws.get_all_values()
        uid_str = str(uid)
        idx, row = -1, []
        for i, r in enumerate(data[1:], start=2):
            if r and r[0] == uid_str: idx, row = i, r; break
        
        if idx == -1: # Neu-Registrierung
            row = [uid_str, "active", "trial", (datetime.now()+timedelta(days=3)).strftime("%d.%m.%Y"), "", "", "", "", "", "", "", "", "Asia/Almaty", ""]
            idx = len(data) + 1
        
        if updates:
            m = {"status":1, "trial":3, "birth":4, "last_ym":11, "tz":12}
            for k, v in updates.items():
                if k in m: row[m[k]] = v
            ws.update(f"A{idx}:N{idx}", [row])
        return row
    except Exception as e:
        log.error(f"Sheet Error: {e}")
        return None

# --- BOT HANDLER ---
async def start(update: Update, context):
    await update.message.reply_text("✨ Добро пожаловать! Введите дату рождения (ДД.ММ.ГГГГ):", 
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📅 Мой прогноз")]], resize_keyboard=True))

async def handle_text(update: Update, context):
    text = update.message.text.strip()
    uid = update.effective_user.id
    user = get_user_row(uid)

    if len(text) == 10 and "." in text: # Datumseingabe
        get_user_row(uid, {"birth": text})
        await update.message.reply_text(f"✅ Дата {text} сохранена! Нажмите 'Мой прогноз'.")
        return

    if text == "📅 Мой прогноз":
        if not user or not user[4]:
            await update.message.reply_text("Сначала введите дату рождения!"); return
        
        # Trial-Check
        exp = datetime.strptime(user[3], "%d.%m.%Y")
        if user[1] != "paid" and datetime.now() > exp:
            await update.message.reply_text(f"💳 Доступ истек. Напишите {ADMIN_CONTACT}"); return

        res = calculate_all(user[4], user[12] or "Asia/Almaty")
        is_full = (user[11] != res["ym"]) # Vollständiger Text am 1. oder bei Registrierung
        
        msg = f"📅 *Прогноз на {res['date']}*\n\n"
        
        # OD & Kritische Tage
        if res['day'] in [10, 20, 30]:
            msg += f"⚠️ *{SYUTSAI_DATA['OD']['bad_dates']}*\n\n"
        elif res['od'] in [3, 6]:
            msg += f"🌟 *Общий день {res['od']}: {SYUTSAI_DATA['OD'][str(res['od'])]}*\n\n"
        else:
            msg += f"🌐 *Общий день:* {res['od']}\n\n"

        # LG
        msg += f"✨ *Личный год {res['lg']}:*\n"
        msg += f"{SYUTSAI_DATA['LG'].get(str(res['lg']), 'Описание года...')}\n\n" if is_full else "_Энергия года в действии._\n\n"
        
        # LM
        msg += f"🌙 *Личный месяц {res['lm']}:*\n"
        msg += f"{SYUTSAI_DATA['LM'].get(str(res['lm']), 'Описание месяца...')}\n\n" if is_full else "_Фокус месяца остается прежним._\n\n"
        
        # LD
        msg += f"📍 *Личный день {res['ld']}:*\n{SYUTSAI_DATA['LD'].get(str(res['ld']), 'Описание дня...')}"

        if is_full: get_user_row(uid, {"last_ym": res["ym"]})
        await update.message.reply_text(msg, parse_mode="Markdown")

# --- SERVER START ---
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