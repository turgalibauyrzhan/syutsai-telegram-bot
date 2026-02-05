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

# --- БАЗА ДАННЫХ ТЕКСТОВ (Из ваших CSV) ---
DESC_LG = {
    "1": "✨ *Личный год 1: Начало нового цикла*\nЭто время выбора направления на ближайшие 9 лет. Самый мощный энергетический поток. Отличный период для открытия дела. Развивайте лидерство.",
    "2": "✨ *Личный год 2: Год дипломатии*\nПериод перемен в отношениях. Не принимайте кардинальных решений. Учитесь строить новые связи и мягко отпускать старое.",
    "3": "✨ *Личный год 3: Год анализа и успеха*\nПробуждается аналитическое мышление. Время планирования и ведения учета. Действуйте через расчет. В минусе — лень и азарт.",
    "7": "✨ *Личный год 7: Год трансформации*\nЛучшее время для глубокого внутреннего развития. Год отработки кармы. Не начинайте новое, избегайте сделок с недвижимостью.",
    "8": "✨ *Личный год 8: Год труда и обучения*\nУспех через дисциплину. Всё, что наработаете, будет служить долго. Хорошо для покупки недвижимости. Избегайте кредитов.",
    "9": "✨ *Личный год 9: Год служения и разрушения*\nПодведение итогов. Позвольте уйти устаревшему. Простите обиды, уделите внимание здоровью."
}

DESC_LM = {
    "1": "🌙 *Личный месяц 1: Стратегия*\nВремя для лидерства и новых проектов. Укрепляйте внутреннюю дисциплину.",
    "2": "🌙 *Личный месяц 2: Дипломатия*\nАктивизируется энергия воспоминаний. Серьезные решения лучше отложить. Пейте больше воды.",
    "3": "🌙 *Личный месяц 3: Анализ*\nСначала думайте, потом делайте. Благоприятно для экзаменов и структурирования планов."
}

DESC_LD = {
    "1": "📍 *Личный день 1: Новые начинания*\nЛюбое дело сегодня получит поддержку. Сохраняйте спокойствие и реализуйте задуманное.",
    "2": "📍 *Личный день 2: Понимание*\nПроявляйте терпение. Свяжитесь с близкими. В минусе — сомнения и депрессия. Поможет вода.",
    "7": "📍 *Личный день 7: Трансформация*\nДисциплина тела: ходьба, йога. Принимайте события спокойно — как опыт для роста.",
    "8": "📍 *Личный день 8: Труд*\nПолученные навыки принесут доход. Избегайте пустого отдыха. Кредиты сегодня брать нельзя.",
    "9": "📍 *Личный день 9: Благодарность*\nУделите внимание телу (баня, массаж). Отпускайте старое, отдавайте долги и помогайте людям."
}

# --- ЛОГИКА РАСЧЕТОВ ---
def reduce9(n):
    while n > 9: n = sum(map(int, str(n)))
    return n

def calculate_syutsai(bd_str, tz_name="Asia/Almaty"):
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    bd = datetime.strptime(bd_str, "%d.%m.%Y").date()
    
    od = reduce9(now.day + now.month + now.year)
    lg = reduce9(bd.day + bd.month + now.year)
    lm = reduce9(lg + now.month)
    ld = reduce9(lm + now.day)
    return {"od": od, "lg": lg, "lm": lm, "ld": ld, "day": now.day, "date": now.strftime("%d.%m.%Y"), "ym": now.strftime("%m.%Y")}

# --- РАБОТА С ТАБЛИЦЕЙ ---
def sync_user(uid, updates=None):
    try:
        creds_info = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8"))
        creds = Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
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
    except Exception as e:
        log.error(f"GS Error: {e}")
        return None

# --- ОБРАБОТЧИКИ ТЕЛЕГРАМ ---
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text("✨ Добро пожаловать! Введите дату рождения в формате ДД.ММ.ГГГГ:", 
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📅 Мой прогноз")]], resize_keyboard=True))

async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    text, uid = u.message.text.strip(), u.effective_user.id
    user = sync_user(uid)

    if len(text) == 10 and "." in text:
        sync_user(uid, {"birth": text})
        await u.message.reply_text(f"✅ Дата {text} сохранена! Нажмите кнопку снизу.")
        return

    if text == "📅 Мой прогноз":
        if not user or not user[4]:
            await u.message.reply_text("Сначала введите дату рождения!"); return
        
        res = calculate_syutsai(user[4])
        is_full = (user[11] != res["ym"])
        
        msg = f"📅 *Прогноз на {res['date']}*\n\n"
        if res['day'] in [10, 20, 30]:
            msg += "⚠️ *Неблагоприятная дата!* Нежелательно начинать новые проекты — риск обнуления результатов.\n\n"
        elif res['od'] in [3, 6]:
            msg += f"🌟 *Общий день {res['od']}:* Благоприятный день для успеха и начинаний!\n\n"
        else:
            msg += f"🌐 *Общий день:* {res['od']}\n\n"

        if is_full:
            msg += f"{DESC_LG.get(str(res['lg']), 'Описание года...')}\n\n"
            msg += f"{DESC_LM.get(str(res['lm']), 'Описание месяца...')}\n\n"
            sync_user(uid, {"last_ym": res["ym"]})
        else:
            msg += "_Полное описание ЛГ и ЛМ доступно 1-го числа._\n\n"
            
        msg += f"{DESC_LD.get(str(res['ld']), 'Описание дня...')}"
        await u.message.reply_text(msg, parse_mode="Markdown")

# --- FLASK И ЗАПУСК ---
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

@app.route("/webhook", methods=["POST"])
def webhook():
    if request.method == "POST":
        update = Update.de_json(request.get_json(force=True), application.bot)
        # Создаем новый loop для каждого запроса, чтобы избежать RuntimeError
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(application.process_update(update))
        finally:
            loop.close()
    return "OK", 200

async def setup_bot():
    await application.initialize()
    await application.start()
    await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")
    log.info("Webhook set up successfully")

if __name__ == "__main__":
    # Инициализация бота
    init_loop = asyncio.get_event_loop()
    init_loop.run_until_complete(setup_bot())
    
    # Запуск сервера
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))