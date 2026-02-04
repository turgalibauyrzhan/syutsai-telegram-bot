import os, json, base64, logging, asyncio
from datetime import datetime, date
import pytz
from flask import Flask, request
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip('/')
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")
TZ = pytz.timezone("Asia/Almaty")

# --- ВШИТЫЕ ДАННЫЕ ИЗ ТАБЛИЦ ---
TEXTS_DATA = {
    "UNFAVORABLE": "⚠️ Нежелательно начинать новые проекты и события. Есть высокая вероятность обнуления всех результатов ваших действий. Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и т.д.",
    "OD": {
        "3": "🌟 *Благоприятный день (ОД 3)*\nБлагоприятный день через анализ, успех. Подходит для важных решений, регистрации брака, оформления договоров, крупных покупок.",
        "6": "💖 *Благоприятный день (ОД 6)*\nБлагоприятный день через любовь и успех. Подходит для инвестиций, больших проектов и семейных дел."
    },
    "LG": {
        "1": {"t": "ЛГ 1. Начало нового цикла", "d": "Время выбора направления на 9 лет. Мощный энергетический поток.", "r": "Открывайте свое дело, развивайте лидерство, сохраняйте позитив."},
        "2": {"t": "ЛГ 2. Год дипломатии", "d": "Период перемен в отношениях. Старое уходит, новое строится.", "r": "Развивайте гибкость, не цепляйтесь за старые связи."},
        "3": {"t": "ЛГ 3. Год анализа и успеха", "d": "Год творческого подъема и реализации через расчет.", "r": "Анализируйте действия через логику."},
        "4": {"t": "ЛГ 4. Год трансформации", "d": "Год мистических событий и глубоких изменений.", "r": "Принимайте перемены, работайте над дисциплиной."},
        "5": {"t": "ЛГ 5. Год коммуникаций", "d": "Время расширения связей и новых возможностей.", "r": "Будьте открыты новому."},
        "6": {"t": "ЛГ 6. Год любви и успеха", "d": "Год семейных ценностей и комфорта.", "r": "Укрепляйте отношения, проявляйте заботу."},
        "7": {"t": "ЛГ 7. Год трансформации", "d": "Глубинная работа над собой, отработка кармы.", "r": "Используйте кризис как точку роста."},
        "8": {"t": "ЛГ 8. Год труда и обучения", "d": "Успех через дисциплину и новые навыки.", "r": "Трудитесь, инвестируйте в знания."},
        "9": {"t": "ЛГ 9. Год служения", "d": "Подведение итогов, освобождение пространства.", "r": "Прощайте обиды, помогайте другим."}
    },
    "LM": {
        "1": "Месяц стратегии и планирования.", "2": "Месяц дипломатии и связей.", "3": "Месяц анализа и успеха.",
        "4": "Месяц неожиданных событий.", "5": "Месяц расширения и идей.", "6": "Месяц творчества и любви.",
        "7": "Месяц дисциплины.", "8": "Месяц контроля и мудрости.", "9": "Месяц завершения."
    },
    "LD": {
        "1": "День новых начинаний.", "2": "День понимания и терпения.", "3": "День анализа и планов.",
        "4": "День мистических событий.", "5": "День общения и знакомств.", "6": "День заботы и тепла.",
        "7": "День трансформации (ходьба, медитация).", "8": "День обучения и труда.", "9": "День благодарности."
    }
}

# --- ЛОГИКА ---
def reduce9(n: int) -> int:
    while n > 9: n = sum(map(int, str(n)))
    return n

def calculate_numerology(bd: date, target_date: date):
    od = reduce9(target_date.day + target_date.month + target_date.year)
    lg = reduce9(bd.day + bd.month + target_date.year)
    lm = reduce9(lg + target_date.month)
    ld = reduce9(lm + target_date.day)
    return od, lg, lm, ld

def get_prognoz(bd_str: str):
    bd = datetime.strptime(bd_str, "%d.%m.%Y").date()
    today = datetime.now(TZ).date()
    od, lg, lm, ld = calculate_numerology(bd, today)
    
    res = f"📅 *Прогноз на {today.strftime('%d.%m.%Y')}*\n\n"
    if today.day in {10, 20, 30}: res += f"{TEXTS_DATA['UNFAVORABLE']}\n\n"
    elif str(od) in TEXTS_DATA["OD"]: res += f"{TEXTS_DATA['OD'][str(od)]}\n\n"
    else: res += f"🌐 *Общий день:* {od}\n\n"

    g = TEXTS_DATA["LG"][str(lg)]
    res += f"✨ *{g['t']}*\n_{g['d']}_\n💡 {g['r']}\n\n"
    res += f"🌙 *Личный месяц {lm}:* {TEXTS_DATA['LM'][str(lm)]}\n\n"
    res += f"📍 *Личный день {ld}:* {TEXTS_DATA['LD'][str(ld)]}"
    return res

# --- ИНИЦИАЛИЗАЦИЯ ---
application = Application.builder().token(TELEGRAM_TOKEN).build()
flask_app = Flask(__name__)

@flask_app.route('/webhook', methods=['POST'])
async def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    await application.process_update(update)
    return "OK", 200

@flask_app.route('/')
def index(): return "Bot is running", 200

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Введите дату рождения в формате ДД.ММ.ГГГГ")

async def handle_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        datetime.strptime(text, "%d.%m.%Y")
        msg = get_prognoz(text)
        await update.message.reply_text(msg, parse_mode="Markdown", 
            reply_markup=ReplyKeyboardMarkup([["Сегодня"]], resize_keyboard=True))
    except:
        await update.message.reply_text("❌ Введите дату: ДД.ММ.ГГГГ")

application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

async def setup():
    await application.initialize()
    await application.start()
    await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup())
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host='0.0.0.0', port=port)