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

# --- СЛОВАРИ ОПИСАНИЙ (Данные из ваших файлов) ---
DESC_LG = {
    "1": {"name": "Начало нового цикла", "text": "Время выбора направления на ближайшие 9 лет. Самый мощный энергетический поток."},
    "2": {"name": "Год построения отношений", "text": "Связан с подвижностью и переменами в отношениях. Учитесь дипломатии."},
    "3": {"name": "Год анализа и успеха", "text": "Пробуждается аналитическое мышление. Время планирования и учета."},
    # Добавьте остальные 4-9 аналогично
}

DESC_LM = {
    "1": "Хороший месяц для начала дел. Стратегия и планирование.",
    "2": "Месяц дипломатии. Активизируется энергия воспоминаний, важна чувственность.",
    # Добавьте остальные 3-9
}

DESC_LD = {
    "1": "День новых начинаний. Любое дело получит поддержку энергии дня.",
    "7": "День кризиса или трансформации. Начните утро с дисциплины тела.",
    "8": "День обучения и труда. Избегайте пустого времяпрепровождения.",
    "9": "День здоровья и благодарности. Отпускайте старое, помогайте людям.",
    # Добавьте остальные 2-6
}

# --- ЛОГИКА РАСЧЕТА ---
def reduce9(n):
    while n > 9:
        n = sum(map(int, str(n)))
    return n

def get_syutsai_numbers(bd_str, tz_name):
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

# --- РАБОТА С ТАБЛИЦЕЙ ---
def sync_user_data(uid, updates=None):
    try:
        decoded = base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
        creds = Credentials.from_service_account_info(json.loads(decoded), 
                scopes=["https://www.googleapis.com/auth/spreadsheets"])
        ws = gspread.authorize(creds).open_by_key(GSHEET_ID).worksheet("subscriptions")
        
        rows = ws.get_all_values()
        uid_str = str(uid)
        idx = -1
        u_row = []

        for i, row in enumerate(rows[1:], start=2):
            if row and row[0] == uid_str:
                idx, u_row = i, row
                break
        
        if idx == -1:
            trial_exp = (datetime.now() + timedelta(days=3)).strftime("%d.%m.%Y")
            u_row = [uid_str, "active", "trial", trial_exp, "", "", "", "", "", "", "", "", "Asia/Almaty", ""]
            idx = len(rows) + 1
        
        if updates:
            mapping = {"status":1, "trial_expires":3, "birth_date":4, "last_ym":11, "timezone":12}
            for k, v in updates.items():
                if k in mapping:
                    while len(u_row) <= mapping[k]: u_row.append("")
                    u_row[mapping[k]] = v
            ws.update(f"A{idx}:N{idx}", [u_row])
        
        return u_row
    except Exception as e:
        log.error(f"GS error: {e}")
        return None

# --- ГЛАВНЫЙ ХЕНДЛЕР ПРОГНОЗА ---
async def send_forecast(update: Update, user_row):
    uid = update.effective_user.id
    bd_str = user_row[4]
    tz_name = user_row[12] or "Asia/Almaty"
    
    res = get_numerology_data(bd_str, tz_name)
    is_first_time_this_month = (user_row[11] != res["ym"])
    
    msg = f"📅 *Прогноз на {res['date_str']}*\n\n"
    
    # 1. Проверка неблагоприятных дат (10, 20, 30)
    if res['day'] in [10, 20, 30]:
        msg += "⚠️ *Неблагоприятная дата!* Нежелательно начинать новые проекты, высока вероятность обнуления результатов.\n\n"
    
    # 2. Общий день
    msg += f"🌐 *Общий день: {res['od']}*\n"
    if res['od'] in [3, 6]:
        msg += "_Благоприятный день для важных решений и начинаний!_\n\n"
    else:
        msg += "\n"

    # 3. Личный год (Полное 1-го числа)
    lg_data = DESC_LG.get(str(res['lg']), {"name": "Год цикла", "text": "Энергия года..."})
    msg += f"✨ *Ваш Личный год {res['lg']}: {lg_data['name']}*\n"
    if is_first_time_this_month:
        msg += f"{lg_data['text']}\n\n"
    else:
        msg += "_Описание было доступно 1-го числа._\n\n"

    # 4. Личный месяц (Полное 1-го числа)
    lm_text = DESC_LM.get(str(res['lm']), "Энергия месяца...")
    msg += f"🌙 *Личный месяц {res['lm']}:*\n"
    if is_first_time_this_month:
        msg += f"{lm_text}\n\n"
    else:
        msg += "_Фокус месяца остается прежним._\n\n"

    # 5. Личный день (Всегда полное)
    ld_text = DESC_LD.get(str(res['ld']), "Описание дня...")
    msg += f"📍 *Личный день {res['ld']}:*\n{ld_text}"

    # Сохраняем, что за этот месяц полное описание выдано
    if is_first_time_this_month:
        sync_user_data(uid, {"last_ym": res["ym"]})

    await update.message.reply_text(msg, parse_mode="Markdown")

# --- ОСТАЛЬНЫЕ ХЕНДЛЕРЫ ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid = update.effective_user.id
    
    if text == "📅 Мой прогноз на сегодня":
        user = sync_user_data(uid)
        if not user or not user[4]:
            await update.message.reply_text("Пожалуйста, сначала введите дату рождения в формате ДД.ММ.ГГГГ")
            return
            
        # Проверка триала
        try:
            exp_date = datetime.strptime(user[3], "%d.%m.%Y")
            if user[1] != "paid" and datetime.now() > exp_date:
                await update.message.reply_text(f"💳 Срок бесплатного доступа (3 дня) истек. Напишите {ADMIN_CONTACT} для оплаты.")
                return
        except: pass
        
        await send_forecast(update, user)
    
    elif len(text) == 10 and text.count(".") == 2: # Ввод даты
        sync_user_data(uid, {"birth_date": text})
        await update.message.reply_text(f"✅ Дата {text} сохранена! Нажмите кнопку 'Мой прогноз'.", 
                                       reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📅 Мой прогноз на сегодня")]], resize_keyboard=True))

# (Стандартная Flask-часть и запуск бота остаются без изменений)