import os, json, base64, logging, asyncio
from datetime import datetime, date, timedelta
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

# Поля таблицы: 0:id, 1:status, 2:plan, 3:trial_expires, 4:birth_date, 5:created_at, 6:last_seen, 7:user, 8:first, 9:last, 10:reg_on, 11:last_ym, 12:timezone, 13:phone

# --- КНОПКИ МЕНЮ ---
def get_main_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("Сегодня")],
        [KeyboardButton("⚙️ Сменить часовой пояс"), KeyboardButton("📅 Сменить дату рождения")]
    ], resize_keyboard=True)

def get_tz_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("Алматы (UTC+5)"), KeyboardButton("Москва (UTC+3)")]
    ], resize_keyboard=True)

# --- УТИЛИТЫ ---
def get_now(user_tz_str="Asia/Almaty"):
    tz = pytz.timezone(user_tz_str)
    return datetime.now(tz)

# --- РАБОТА С ТАБЛИЦЕЙ ---
def get_user_from_sheet(uid):
    try:
        sa_info = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8"))
        creds = Credentials.from_service_account_info(sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(GSHEET_ID).worksheet("subscriptions")
        
        all_rows = ws.get_all_values()
        for i, row in enumerate(all_rows[1:], start=2):
            if row and str(row[0]) == str(uid):
                # Возвращаем словарь для удобства
                return i, row
        return None, None
    except Exception as e:
        log.error(f"Error fetching user: {e}")
        return None, None

def save_user(uid, data_dict):
    """Умное обновление: сохраняет существующие данные, меняет только переданные ключи"""
    try:
        sa_info = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8"))
        creds = Credentials.from_service_account_info(sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(GSHEET_ID).worksheet("subscriptions")
        
        idx, current_row = get_user_from_sheet(uid)
        
        if idx:
            new_row = list(current_row)
            while len(new_row) < 14: new_row.append("")
        else:
            # Новый пользователь: ID, Status, Plan, Trial_Exp, Birth, Created, LastSeen, User, First, Last, Reg, LYM, TZ, Phone
            new_row = [str(uid), "active", "trial", (datetime.now() + timedelta(days=3)).strftime("%d.%m.%Y"), 
                       "", datetime.now().isoformat(), "", "", "", "", "", "", "Asia/Almaty", ""]
            idx = len(ws.get_all_values()) + 1

        # Маппинг обновлений
        mapping = {"status":1, "plan":2, "trial_expires":3, "birth_date":4, "timezone":12, "phone":13}
        for k, v in data_dict.items():
            if k in mapping: new_row[mapping[k]] = v
        
        new_row[6] = datetime.now().isoformat() # LastSeen
        ws.update(f"A{idx}:N{idx}", [new_row])
    except Exception as e:
        log.error(f"Error saving user: {e}")

# --- ХЕНДЛЕРЫ ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Добро пожаловать в бот Сюцай! 🌟\nДля начала работы, пожалуйста, введите ваш номер телефона или дату рождения.")
    await update.message.reply_text("Введите дату рождения в формате: ДД.ММ.ГГГГ")

async def handle_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid = update.effective_user.id
    idx, user_data = get_user_from_sheet(uid)

    # 1. Смена часового пояса
    if text == "⚙️ Сменить часовой пояс":
        await update.message.reply_text("Выберите ваш город:", reply_markup=get_tz_keyboard())
        return

    if text in ["Алматы (UTC+5)", "Москва (UTC+3)"]:
        tz_name = "Asia/Almaty" if "Алматы" in text else "Europe/Moscow"
        save_user(uid, {"timezone": tz_name})
        await update.message.reply_text(f"✅ Часовой пояс изменен на {text}", reply_markup=get_main_keyboard())
        return

    # 2. Смена даты рождения
    if text == "📅 Сменить дату рождения":
        await update.message.reply_text("Введите новую дату рождения (ДД.ММ.ГГГГ):")
        return

    # 3. Обработка ввода даты
    try:
        datetime.strptime(text, "%d.%m.%Y")
        save_user(uid, {"birth_date": text})
        await update.message.reply_text("✅ Дата сохранена! Теперь вы можете получать прогноз.", reply_markup=get_main_keyboard())
        return
    except ValueError:
        pass

    # 4. Кнопка СЕГОДНЯ / Расчет
    if text == "Сегодня" or text == "прогноз":
        if not user_data or not user_data[4]:
            await update.message.reply_text("Сначала введите дату рождения (ДД.ММ.ГГГГ)")
            return
        
        # ПРОВЕРКА ТРИАЛА
        plan = user_data[2]
        trial_exp_str = user_data[3]
        is_paid = user_data[1] == "paid"
        
        try:
            trial_exp_dt = datetime.strptime(trial_exp_str, "%d.%m.%Y")
            if not is_paid and datetime.now() > trial_exp_dt:
                await update.message.reply_text(
                    f"💳 Ваш пробный период (3 дня) закончился.\n"
                    f"Для оплаты полного доступа и продолжения использования бота, пожалуйста, напишите администратору: {ADMIN_CONTACT}"
                )
                return
        except: pass

        # Если всё ок — даем прогноз (используем ваш метод get_prognoz)
        from main import get_prognoz # Предполагая, что функция расчета в этом же файле
        res = get_prognoz(user_data[4]) 
        await update.message.reply_text(res, parse_mode="Markdown", reply_markup=get_main_keyboard())

# --- ЗАПУСК (FLASK ЧАСТЬ ОСТАЕТСЯ ПРЕЖНЕЙ) ---