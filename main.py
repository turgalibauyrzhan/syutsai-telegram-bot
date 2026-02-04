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

# --- КНОПКИ МЕНЮ ---
def main_menu():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📅 Мой прогноз на сегодня")],
        [KeyboardButton("⚙️ Настройки"), KeyboardButton("🆘 Поддержка")]
    ], resize_keyboard=True)

def settings_menu():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🌍 Сменить часовой пояс")],
        [KeyboardButton("🎂 Сменить дату рождения")],
        [KeyboardButton("⬅️ Назад")]
    ], resize_keyboard=True)

def tz_menu():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🇰🇿 Алматы (UTC+5)"), KeyboardButton("🇷🇺 Москва (UTC+3)")],
        [KeyboardButton("⬅️ Назад")]
    ], resize_keyboard=True)

# --- ЛОГИКА СЮЦАЙ ---
def reduce9(n: int) -> int:
    while n > 9: n = sum(map(int, str(n)))
    return n

def calculate_syutsai(bd_str, tz_name):
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    # По Сюцай день часто меняется в 4:00 утра, но для базы берем 00:00
    today = now.date()
    bd = datetime.strptime(bd_str, "%d.%m.%Y").date()
    
    od = reduce9(today.day + today.month + today.year)
    lg = reduce9(bd.day + bd.month + today.year)
    lm = reduce9(lg + today.month)
    ld = reduce9(lm + today.day)
    return od, lg, lm, ld, today

# --- РАБОТА С GOOGLE SHEETS ---
def get_worksheet():
    try:
        decoded = base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
        info = json.loads(decoded)
        creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        return gc.open_by_key(GSHEET_ID).worksheet("subscriptions")
    except Exception as e:
        log.error(f"GS Auth Error: {e}")
        return None

def upsert_user(uid, updates: dict):
    ws = get_worksheet()
    if not ws: return None
    
    data = ws.get_all_values()
    uid_str = str(uid)
    row_idx = -1
    user_row = []

    for i, row in enumerate(data[1:], start=2):
        if row and str(row[0]) == uid_str:
            row_idx = i
            user_row = row
            break

    if row_idx == -1:
        # Новая регистрация: даем триал 3 дня
        trial_exp = (datetime.now() + timedelta(days=3)).strftime("%d.%m.%Y")
        user_row = [uid_str, "active", "trial", trial_exp, "", datetime.now().isoformat(), "", "", "", "", datetime.now().strftime("%d.%m.%Y"), "", "Asia/Almaty", ""]
        row_idx = len(data) + 1
    
    # Маппинг столбцов (0-ID, 1-Status, 2-Plan, 3-TrialExp, 4-Birth, 12-TZ)
    mapping = {"status":1, "plan":2, "trial_expires":3, "birth_date":4, "timezone":12, "phone":13}
    for k, v in updates.items():
        if k in mapping: 
            while len(user_row) <= mapping[k]: user_row.append("")
            user_row[mapping[k]] = v
    
    ws.update(f"A{row_idx}:N{row_idx}", [user_row])
    return user_row

# --- ХЕНДЛЕРЫ ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🌟 *Добро пожаловать в Сюцай Бот!*\n\nВведите вашу дату рождения в формате *ДД.ММ.ГГГГ* (например, 16.09.1994), чтобы активировать бесплатный доступ на 3 дня.",
        parse_mode="Markdown"
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid = update.effective_user.id

    # Навигация
    if text == "⬅️ Назад":
        await update.message.reply_text("Главное меню", reply_markup=main_menu())
        return
    if text == "⚙️ Настройки":
        await update.message.reply_text("Настройки профиля:", reply_markup=settings_menu())
        return
    if text == "🌍 Сменить часовой пояс":
        await update.message.reply_text("Выберите пояс:", reply_markup=tz_menu())
        return
    if text == "🆘 Поддержка":
        await update.message.reply_text(f"По всем вопросам пишите: {ADMIN_CONTACT}")
        return

    # Смена пояса
    if "UTC+" in text:
        new_tz = "Asia/Almaty" if "Алматы" in text else "Europe/Moscow"
        upsert_user(uid, {"timezone": new_tz})
        await update.message.reply_text(f"✅ Установлен пояс: {text}", reply_markup=main_menu())
        return

    # Ввод даты рождения
    if len(text) == 10 and text.count(".") == 2:
        try:
            datetime.strptime(text, "%d.%m.%Y")
            upsert_user(uid, {"birth_date": text})
            await update.message.reply_text(f"✅ Дата {text} сохранена! Вам начислено 3 дня триала.", reply_markup=main_menu())
        except:
            await update.message.reply_text("❌ Ошибка в дате. Введите еще раз: ДД.ММ.ГГГГ")
        return

    # ПРОГНОЗ
    if text == "📅 Мой прогноз на сегодня":
        ws = get_worksheet()
        all_data = ws.get_all_values()
        user_row = next((r for r in all_data if r[0] == str(uid)), None)

        if not user_row or not user_row[4]:
            await update.message.reply_text("Сначала введите дату рождения!")
            return

        # Проверка оплаты/триала
        try:
            trial_dt = datetime.strptime(user_row[3], "%d.%m.%Y")
            if user_row[1] != "paid" and datetime.now() > trial_dt:
                await update.message.reply_text(f"⌛️ Ваш доступ истек. Для продления напишите {ADMIN_CONTACT}")
                return
        except: pass

        # Расчет
        od, lg, lm, ld, today = calculate_syutsai(user_row[4], user_row[12] or "Asia/Almaty")
        
        # Формируем сообщение (можно добавить детали из CSV)
        msg = f"📅 *Прогноз на {today.strftime('%d.%m.%Y')}*\n\n"
        msg += f"🌐 *Общий день:* {od}\n"
        msg += f"✨ *Личный год:* {lg}\n"
        msg += f"🌙 *Личный месяц:* {lm}\n"
        msg += f"📍 *Личный день:* {ld}\n\n"
        msg += "_Полное описание доступно в рамках вашего тарифа._"
        
        await update.message.reply_text(msg, parse_mode="Markdown")

# --- FLASK & WEBHOOK ---
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()

@app.route("/webhook", methods=["POST"])
async def webhook():
    await application.process_update(Update.de_json(request.get_json(force=True), application.bot))
    return "OK", 200

@app.route("/")
def index(): return "Bot is running", 200

async def setup():
    await application.initialize()
    await application.start()
    await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup())
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))