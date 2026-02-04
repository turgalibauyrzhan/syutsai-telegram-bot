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

# --- ДАННЫЕ (ВШИТЫ ДЛЯ СТАБИЛЬНОСТИ) ---
TEXTS_DATA = {
    "UNFAVORABLE": "⚠️ Нежелательно начинать новые проекты и события. Есть высокая вероятность обнуления всех результатов ваших действий.",
    "OD": {
        "3": "🌟 *ОД 3: Успех через анализ.* Подходит для договоров и крупных покупок.",
        "6": "💖 *ОД 6: Успех через любовь.* День комфорта и выгодных инвестиций."
    },
    "LG": {
        "1": {"t": "ЛГ 1. Начало цикла", "d": "Время выбора пути на 9 лет.", "r": "Действуйте смело."},
        "2": {"t": "ЛГ 2. Дипломатия", "d": "Год выстраивания отношений.", "r": "Будьте гибкими."},
        "3": {"t": "ЛГ 3. Успех", "d": "Реализация через холодный расчет.", "r": "Анализируйте планы."},
        "4": {"t": "ЛГ 4. Трансформация", "d": "Год мистических перемен.", "r": "Соблюдайте дисциплину."},
        "5": {"t": "ЛГ 5. Коммуникация", "d": "Расширение возможностей.", "r": "Заводите связи."},
        "6": {"t": "ЛГ 6. Комфорт", "d": "Год любви и успеха.", "r": "Заботьтесь о близких."},
        "7": {"t": "ЛГ 7. Глубина", "d": "Работа над сознанием.", "r": "Больше двигайтесь."},
        "8": {"t": "ЛГ 8. Труд", "d": "Успех через обучение.", "r": "Работайте на результат."},
        "9": {"t": "ЛГ 9. Завершение", "d": "Очищение пространства.", "r": "Прощайте обиды."}
    },
    "LM": {
        "1": "Месяц стратегии.", "2": "Месяц дипломатии.", "3": "Месяц успеха.",
        "4": "Месяц перемен.", "5": "Месяц идей.", "6": "Месяц удачи.",
        "7": "Месяц дисциплины.", "8": "Месяц контроля.", "9": "Месяц тишины."
    },
    "LD": {
        "1": "День начинаний.", "2": "День мягкости.", "3": "День расчетов.",
        "4": "День интуиции.", "5": "День общения.", "6": "День уюта.",
        "7": "День йоги.", "8": "День учебы.", "9": "День отдачи."
    }
}

# --- ЛОГИКА РАСЧЕТА ---
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
    try:
        bd = datetime.strptime(bd_str, "%d.%m.%Y").date()
        today = datetime.now(TZ).date()
        od, lg, lm, ld = calculate_numerology(bd, today)
        
        res = f"📅 *Прогноз на {today.strftime('%d.%m.%Y')}*\n\n"
        if today.day in {10, 20, 30}: res += f"{TEXTS_DATA['UNFAVORABLE']}\n\n"
        elif str(od) in TEXTS_DATA["OD"]: res += f"{TEXTS_DATA['OD'][str(od)]}\n\n"
        else: res += f"🌐 *Общий день:* {od}\n\n"

        g = TEXTS_DATA["LG"][str(lg)]
        res += f"✨ *{g['t']}*\n_{g['d']}_\n💡 {g['r']}\n\n"
        res += f"🌙 *ЛМ {lm}:* {TEXTS_DATA['LM'][str(lm)]}\n\n"
        res += f"📍 *ЛД {ld}:* {TEXTS_DATA['LD'][str(ld)]}"
        return res
    except: return "Ошибка расчета. Проверьте дату."

# --- GOOGLE SHEETS (ЗАЩИЩЕННАЯ ЛОГИКА) ---
def upsert_user(uid, updates: dict):
    try:
        sa_info = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8"))
        creds = Credentials.from_service_account_info(sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(GSHEET_ID).worksheet("subscriptions")
        
        all_rows = ws.get_all_values()
        header = all_rows[0]
        uid = str(uid)
        
        row_idx = -1
        current_row = []
        for i, row in enumerate(all_rows[1:], start=2):
            if row and str(row[0]) == uid:
                row_idx = i
                current_row = row
                break

        now_iso = datetime.now(TZ).isoformat()
        
        if row_idx != -1:
            # Обновляем существующую строку, не затирая другие данные
            # Структура: ID(0), Status(1), Plan(2), Trial(3), Birth(4), Created(5), LastSeen(6), User(7), First(8), Last(9), RegDate(10), LastYM(11)
            new_row = list(current_row)
            # Дополняем список, если он короче 12 столбцов
            while len(new_row) < 12: new_row.append("")
            
            if "birth_date" in updates: new_row[4] = updates["birth_date"]
            new_row[6] = now_iso # Обновляем LastSeen
            if "username" in updates: new_row[7] = updates["username"]
            if "first_name" in updates: new_row[8] = updates["first_name"]
            if "last_name" in updates: new_row[9] = updates["last_name"]
            
            ws.update(f"A{row_idx}:L{row_idx}", [new_row])
        else:
            # Создаем новую строку
            new_row = [uid, "active", "trial", "", updates.get("birth_date", ""), now_iso, now_iso, 
                       updates.get("username", ""), updates.get("first_name", ""), updates.get("last_name", ""), 
                       datetime.now(TZ).strftime("%d.%m.%Y"), ""]
            ws.append_row(new_row)
    except Exception as e: log.error(f"GS Error: {e}")

# --- ИНИЦИАЛИЗАЦИЯ БОТА ---
application = Application.builder().token(TELEGRAM_TOKEN).build()
flask_app = Flask(__name__)

@flask_app.route('/webhook', methods=['POST'])
async def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    await application.process_update(update)
    return "OK", 200

@flask_app.route('/')
def index(): return "Bot is live", 200

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Введите вашу дату рождения в формате ДД.ММ.ГГГГ")

async def handle_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.effective_user
    
    # 1. Валидация даты
    try:
        # Проверяем сам формат
        birth_date_dt = datetime.strptime(text, "%d.%m.%Y")
    except ValueError:
        # Если это не дата, и не кнопка "Сегодня" — ругаемся
        if text != "Сегодня":
            await update.message.reply_text("❌ Неверный формат. Пожалуйста, введите дату как 16.09.1994")
        return

    # 2. Попытка расчета (делаем первым, чтобы пользователь получил ответ в любом случае)
    try:
        prognoz = get_prognoz(text)
        await update.message.reply_text(
            prognoz, 
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardMarkup([["Сегодня"]], resize_keyboard=True)
        )
    except Exception as e:
        log.error(f"Ошибка расчета: {e}")
        await update.message.reply_text("😔 Произошла ошибка при расчете прогноза.")
        return

    # 3. Фоновая запись в Google Sheets (теперь ошибка здесь не блокирует ответ пользователю)
    try:
        # Запускаем в отдельном потоке, чтобы не тормозить бота
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, upsert_user, user.id, {
            "birth_date": text, 
            "username": user.username or "",
            "first_name": user.first_name or "", 
            "last_name": user.last_name or ""
        })
    except Exception as e:
        log.error(f"Ошибка записи в таблицу: {e}")
        # Пользователю об этом знать не обязательно, он уже получил прогноз
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