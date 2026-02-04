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

# --- ДАННЫЕ ИЗ ВАШИХ ФАЙЛОВ ---
DATA = {
    "LG": {
        "3": "Ваш Личный год 3. Год анализа и успеха.\nВ этот период пробуждается аналитическое мышление: человек начинает планировать и подводить итоги. Рекомендации: действуй через анализ, планируй шаги на год вперед. В минусе: лень и азарт.",
        "7": "Личный год 7. Год трансформации и кризиса. Лучшее время для глубокого развития. Не начинай новое дело, избегай операций с недвижимостью.",
        "8": "Личный год 8. Год труда и обучения. Успех через дисциплину. Хорошо для покупки недвижимости. Избегайте кредитов."
    },
    "LM": {
        "1": "Личный месяц 1. Хороший месяц для начала дел. Стратегия и планирование.",
        "2": "Личный месяц 2. Месяц дипломатии и выстраивания отношений. Полезно пить больше воды, серьезные решения отложите.",
        "3": "Личный месяц 3. Месяц анализа и успеха. Думайте, прежде чем делать."
    },
    "LD": {
        "7": "Личный день 7. День кризиса или трансформации. Начните утро с дисциплины тела: ходьба, йога. Принимайте всё спокойно.",
        "8": "Личный день 8. День обучения и труда. Навыки принесут финансовый результат. Кредиты брать не рекомендуется.",
        "9": "Личный день 9. День здоровья и благодарности. Полезны массаж и баня. Отпускайте старое с миром."
    },
    "OD": {
        "3": "Благоприятный день через анализ. Успех в документах и покупках.",
        "6": "Благоприятный день через любовь. Успех в браке и инвестициях.",
        "bad": "Нежелательно начинать новые проекты (10, 20, 30 число). Риск обнуления результатов."
    }
}

# --- ФУНКЦИИ РАСЧЕТА ---
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

# --- GOOGLE SHEETS ---
def sync_user(uid, updates=None):
    decoded = base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
    creds = Credentials.from_service_account_info(json.loads(decoded), 
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
        m = {"status":1, "birth":4, "last_ym":11}
        for k, v in updates.items(): u_row[m[k]] = v
        ws.update(f"A{idx}:N{idx}", [u_row])
    return u_row

# --- ОБРАБОТЧИКИ ---
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text("✨ Введите дату рождения (ДД.ММ.ГГГГ):", 
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📅 Мой прогноз")]], resize_keyboard=True))

async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    try:
        text, uid = u.message.text.strip(), u.effective_user.id
        user = sync_user(uid)

        if len(text) == 10 and "." in text:
            sync_user(uid, {"birth": text})
            await u.message.reply_text(f"✅ Дата {text} сохранена!")
            return

        if text == "📅 Мой прогноз":
            if not user[4]: 
                await u.message.reply_text("Сначала введите дату рождения!"); return
            
            res = get_calc(user[4])
            is_full = (user[11] != res["ym"])
            
            msg = f"📅 *Прогноз на {res['date']}*\n\n"
            if res['day'] in [10, 20, 30]: msg += f"⚠️ {DATA['OD']['bad']}\n\n"
            
            msg += f"🌐 *Общий день:* {res['od']}\n\n"
            
            # ЛГ и ЛМ (только 1-го числа или при регистрации)
            if is_full:
                msg += f"✨ {DATA['LG'].get(str(res['lg']), 'Энергия года...')}\n\n"
                msg += f"🌙 {DATA['LM'].get(str(res['lm']), 'Энергия месяца...')}\n\n"
                sync_user(uid, {"last_ym": res["ym"]})
            
            msg += f"📍 {DATA['LD'].get(str(res['ld']), 'Описание дня...')}"
            await u.message.reply_text(msg, parse_mode="Markdown")
            
    except Exception as e:
        await u.message.reply_text(f"Ошибка: {str(e)}")

# --- ЗАПУСК ---
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

@app.route("/webhook", methods=["POST"])
async def webhook():
    await application.process_update(Update.de_json(request.get_json(force=True), application.bot))
    return "OK", 200

async def setup():
    await application.initialize()
    await application.start()
    await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(setup())
    app.run(host="0.0.0.0", port=10000)