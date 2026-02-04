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

# --- КНОПКИ ---
def main_menu_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📅 Мой прогноз на сегодня")],
        [KeyboardButton("⚙️ Настройки"), KeyboardButton("🆘 Поддержка")]
    ], resize_keyboard=True)

def settings_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🌍 Сменить часовой пояс")],
        [KeyboardButton("🎂 Изменить дату рождения")],
        [KeyboardButton("⬅️ Назад")]
    ], resize_keyboard=True)

def tz_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🇰🇿 Алматы (UTC+5)"), KeyboardButton("🇷🇺 Москва (UTC+3)")],
        [KeyboardButton("⬅️ Назад")]
    ], resize_keyboard=True)

# --- ЛОГИКА НУМЕРОЛОГИИ (СЮЦАЙ) ---
def reduce9(n: int) -> int:
    while n > 9: n = sum(map(int, str(n)))
    return n

def get_numerology_data(birth_date_str, user_tz):
    tz = pytz.timezone(user_tz)
    now = datetime.now(tz)
    # Если сейчас до 4 утра, по Сюцай может считаться еще предыдущий день
    # Но для стандарта берем календарный день
    today = now.date()
    bd = datetime.strptime(birth_date_str, "%d.%m.%Y").date()
    
    od = reduce9(today.day + today.month + today.year)
    lg = reduce9(bd.day + bd.month + today.year)
    lm = reduce9(lg + today.month)
    ld = reduce9(lm + today.day)
    
    return {"od": od, "lg": lg, "lm": lm, "ld": ld, "date": today, "is_first_day": today.day == 1}

# --- ФУНКЦИЯ ФОРМИРОВАНИЯ ТЕКСТА ---
def build_message(u_data, n_data, force_full=False):
    """
    u_data: строка из таблицы
    n_data: расчетные цифры
    force_full: если True, даем полное описание (1-е число или регистрация)
    """
    # Здесь логика подтягивания текстов из ваших CSV
    # Для примера краткая сборка:
    res = f"✨ *Ваш персональный прогноз на {n_data['date'].strftime('%d.%m.%Y')}*\n\n"
    
    # ОД
    res += f"🌐 *Общий день {n_data['od']}:* "
    if n_data['od'] in [3, 6]: res += "Благоприятный день для начинаний! ✅\n"
    else: res += "Обычный день. ⚪️\n"
    
    # ЛГ, ЛМ, ЛД
    res += f"📅 *Личный год {n_data['lg']}:* Энергия года направлена на..."
    if force_full or n_data['is_first_day']:
        res += "\n_(Полное описание года из файла...)_\n"
        
    res += f"\n🌙 *Личный месяц {n_data['lm']}:* "
    if force_full or n_data['is_first_day']:
        res += "\n_(Полное описание месяца из файла...)_\n"
    else: res += "Месяц дипломатии.\n"
        
    res += f"\n📍 *Личный день {n_data['ld']}:* Описание дня..."
    
    return res

# --- GOOGLE SHEETS ---
def upsert_user(uid, updates: dict):
    # Код из предыдущего шага, который сохраняет в 14 столбцов
    # Добавляем логику: если это новый пользователь, ставим trial_expires = today + 3 days
    pass 

# --- ОБРАБОТКА КОМАНД ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🌟 *Добро пожаловать в Сюцай Бот!*\n\n"
        "Я помогу вам понять энергию каждого дня. Для начала мне нужна ваша дата рождения.",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Ввести дату рождения")]], resize_keyboard=True)
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user = update.effective_user
    uid = str(user.id)
    
    # Загружаем данные пользователя из таблицы (кеширование можно добавить позже)
    # user_row = get_user_by_id(uid)

    if text == "📅 Мой прогноз на сегодня":
        # 1. Проверка регистрации
        # 2. Проверка триала (3 дня) / Оплаты
        # 3. Расчет и Вывод
        await update.message.reply_text("Ваш прогноз...")

    elif text == "⚙️ Настройки":
        await update.message.reply_text("Что вы хотите изменить?", reply_markup=settings_keyboard())

    elif text == "🌍 Сменить часовой пояс":
        await update.message.reply_text("Выберите ваш регион:", reply_markup=tz_keyboard())

    elif text == "🎂 Изменить дату рождения":
        await update.message.reply_text("Введите дату рождения в формате ДД.ММ.ГГГГ (например, 15.05.1990):")

    elif text == "⬅️ Назад":
        await update.message.reply_text("Главное меню", reply_markup=main_menu_keyboard())
    
    # Проверка на ввод даты (регулярное выражение)
    elif len(text) == 10 and text.count(".") == 2:
        # Валидация и сохранение даты + выдача первого прогноза (force_full=True)
        await update.message.reply_text("✅ Дата сохранена! Ваш первый прогноз за 3 дня триала:", reply_markup=main_menu_keyboard())

# --- ЗАПУСК (FLASK + BOT) ---