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

# --- ЛОГИКА РАСЧЕТОВ ---
def reduce9(n):
    while n > 9: n = sum(map(int, str(n)))
    return n

def get_calc(bd_str):
    tz = pytz.timezone("Asia/Almaty")
    now = datetime.now(tz)
    bd = datetime.strptime(bd_str, "%d.%m.%Y")
    # Базовые расчеты
    lg = reduce9(bd.day + bd.month + now.year)
    lm = reduce9(lg + now.month)
    ld = reduce9(lm + now.day)
    od = reduce9(now.day + now.month + now.year)
    return {"lg": lg, "lm": lm, "ld": ld, "od": od, "ym": now.strftime("%m.%Y"), "today": now.strftime("%d.%m.%Y")}

# --- РАБОТА С ТАБЛИЦЕЙ (ВАШ ФОРМАТ) ---
def sync_user_data(update: Update, birth_date=None, last_ym=None):
    try:
        user = update.effective_user
        uid_str = str(user.id)
        
        creds_info = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8"))
        creds = Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        ws = gspread.authorize(creds).open_by_key(GSHEET_ID).worksheet("subscriptions")
        
        all_data = ws.get_all_values()
        headers = all_data[0]
        rows = all_data[1:]
        
        idx = -1
        for i, row in enumerate(rows, start=2):
            if row and row[0] == uid_str:
                idx = i
                current_row = row
                break
        
        now_str = datetime.now(pytz.timezone("Asia/Almaty")).strftime("%d.%m.%Y %H:%M")
        
        # Если пользователя нет - создаем структуру под ваш формат
        if idx == -1:
            reg_date = datetime.now().strftime("%d.%m.%Y")
            trial_end = (datetime.now() + timedelta(days=3)).strftime("%d.%m.%Y")
            # Формат: telegram_user_id(0), status(1), plan(2), trial_expires(3), birth_date(4), 
            # created_at(5), last_seen_at(6), username(7), first_name(8), last_name(9), 
            # registered_on(10), last_full_ym(11), Timezone(12), Phone(13)
            new_row = [
                uid_str, "active", "trial", trial_end, birth_date or "", 
                now_str, now_str, user.username or "", user.first_name or "", user.last_name or "",
                reg_date, last_ym or "", "Asia/Almaty", ""
            ]
            ws.append_row(new_row)
            return new_row
        else:
            # Обновляем существующего
            updates = []
            # Обновляем last_seen_at (колонка G / индекс 6)
            ws.update_cell(idx, 7, now_str)
            
            if birth_date:
                ws.update_cell(idx, 5, birth_date)
            if last_ym:
                ws.update_cell(idx, 12, last_ym)
                
            # Синхронизируем имя/юзернейм на случай перемен
            ws.update_cell(idx, 8, user.username or "")
            ws.update_cell(idx, 9, user.first_name or "")
            ws.update_cell(idx, 10, user.last_name or "")
            
            # Возвращаем обновленную строку (перечитываем для логики)
            return ws.row_values(idx)

    except Exception as e:
        log.error(f"GS Sync Error: {e}")
        return None

# --- ОБРАБОТЧИКИ ---
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    sync_user_data(u) # Сохраняем первичные данные
    await u.message.reply_text(
        "✨ Приветствую! Я рассчитаю твой прогноз по системе Сюцай.\n\n"
        "Введи дату рождения в формате: **ДД.ММ.ГГГГ** (например 16.09.1994)",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📅 Мой прогноз")]], resize_keyboard=True)
    )

async def handle_message(u: Update, c: ContextTypes.DEFAULT_TYPE):
    text = u.message.text.strip()
    
    # Обработка даты рождения
    if len(text) == 10 and text.count(".") == 2:
        sync_user_data(u, birth_date=text)
        await u.message.reply_text(f"✅ Дата {text} зафиксирована! Нажми на кнопку 'Мой прогноз'.")
        return

    if text == "📅 Мой прогноз":
        user_row = sync_user_data(u)
        if not user_row or not user_row[4]:
            await u.message.reply_text("Сначала напиши дату рождения (ДД.ММ.ГГГГ)"); return
        
        # Проверка подписки (статус и триал)
        status = user_row[1]
        trial_exp = datetime.strptime(user_row[3], "%d.%m.%Y")
        if status != "paid" and datetime.now() > trial_exp:
            await u.message.reply_text(f"💳 Твой пробный период окончен. Напиши {ADMIN_CONTACT} для продления.")
            return

        # Расчеты
        res = get_calc(user_row[4])
        last_full_ym = user_row[11]
        is_new_month = (last_full_ym != res["ym"])

        msg = f"📅 *Прогноз на {res['today']}*\n\n"
        msg += f"🌐 *Общий день:* {res['od']}\n"
        
        # Если новый месяц - даем полные данные и сохраняем ym в таблицу
        if is_new_month:
            msg += f"\n✨ *Твой личный год:* {res['lg']}\n🌙 *Твой личный месяц:* {res['lm']}\n"
            msg += "\n(Полное описание года и месяца доступно 1-го числа или при регистрации)"
            sync_user_data(u, last_ym=res["ym"])
        
        msg += f"\n📍 *Личный день:* {res['ld']}"
        
        await u.message.reply_text(msg, parse_mode="Markdown")

# --- СТАНДАРТНЫЙ ЗАПУСК ---
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

@app.route("/webhook", methods=["POST"])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(application.process_update(update))
    loop.close()
    return "OK", 200

if __name__ == "__main__":
    async def setup():
        await application.initialize()
        await application.start()
        await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")
    
    asyncio.get_event_loop().run_until_complete(setup())
    app.run(host="0.0.0.0", port=10000)