import os
import logging
import sqlite3
from datetime import datetime, time
from zoneinfo import ZoneInfo

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# =========================
# НАСТРОЙКИ
# =========================
TOKEN = os.environ.get("8293279514:AAEcTtUeB9kXaLn3viOVty7jzFgboAC1l8Q")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set (Render env var).")

TZ = ZoneInfo("Asia/Almaty")
DB_PATH = os.environ.get("BOT_DB_PATH", "bot.db")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# ТЕКСТЫ ТРАКТОВОК
# (оставил твою структуру 1:1)
# =========================
UNFAVORABLE_DAYS = {10, 20, 30}

GENERAL_DAY_INTERPRETATIONS = {
    3: "Благоприятный день через анализ, успех. Хороший день для принятия серьёзных решений, подписания договоров и совершения покупок.",
    6: "Благоприятный день через любовь, успех. Хороший день для принятия решений, для подписания договоров. Делайте покупки, начинайте большие проекты.",
    "unfavorable": (
        "Сегодня нежелательно начинать новые проекты и события. Есть высокая вероятность обнуления всех результатов ваших действий. "
        "Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и т.д."
    ),
}

# ВАЖНО: Ниже словари можно оставить твоими большими текстами.
# Я оставляю минимальные заглушки-структуры, чтобы код был цельный.
# Просто вставь сюда свои полные тексты из текущего main.py (они у тебя уже есть).
PERSONAL_YEAR_INTERPRETATIONS = {
    # пример структуры:
    1: {"title": "Личный год 1. Начало нового цикла.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    2: {"title": "Личный год 2.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    3: {"title": "Личный год 3.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    4: {"title": "Личный год 4.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    5: {"title": "Личный год 5.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    6: {"title": "Личный год 6.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    7: {"title": "Личный год 7.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    8: {"title": "Личный год 8.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    9: {"title": "Личный год 9.", "description": "…", "recommendations": "…", "if_not_used": "…"},
}

PERSONAL_MONTH_INTERPRETATIONS = {
    1: {"title": "Личный месяц 1.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    2: {"title": "Личный месяц 2.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    3: {"title": "Личный месяц 3.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    4: {"title": "Личный месяц 4.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    5: {"title": "Личный месяц 5.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    6: {"title": "Личный месяц 6.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    7: {"title": "Личный месяц 7.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    8: {"title": "Личный месяц 8.", "description": "…", "recommendations": "…", "if_not_used": "…"},
    9: {"title": "Личный месяц 9.", "description": "…", "recommendations": "…", "if_not_used": "…"},
}

PERSONAL_DAY_INTERPRETATIONS = {
    1: "…", 2: "…", 3: "…", 4: "…", 5: "…", 6: "…", 7: "…", 8: "…", 9: "…"
}

# =========================
# БД (SQLite) — быстро и просто
# =========================
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def db_init() -> None:
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                birth_date TEXT NOT NULL,
                subscribed INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.commit()

def db_get_user(user_id: int):
    with db_connect() as conn:
        cur = conn.execute("SELECT user_id, birth_date, subscribed FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row

def db_upsert_user(user_id: int, birth_date: str) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, birth_date, subscribed)
            VALUES (?, ?, COALESCE((SELECT subscribed FROM users WHERE user_id=?), 0))
            ON CONFLICT(user_id) DO UPDATE SET birth_date=excluded.birth_date
            """,
            (user_id, birth_date, user_id),
        )
        conn.commit()

def db_set_subscribed(user_id: int, subscribed: bool) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, birth_date, subscribed)
            VALUES (?, '01.01.2000', ?)
            ON CONFLICT(user_id) DO UPDATE SET subscribed=excluded.subscribed
            """,
            (user_id, 1 if subscribed else 0),
        )
        conn.commit()

def db_all_subscribed_users():
    with db_connect() as conn:
        cur = conn.execute("SELECT user_id, birth_date FROM users WHERE subscribed=1")
        return cur.fetchall()

# =========================
# РАСЧЁТЫ
# =========================
def reduce_to_single_digit(number_str: str) -> int:
    cleaned = "".join(ch for ch in number_str if ch.isdigit())
    if not cleaned:
        return 0
    s = sum(int(d) for d in cleaned)
    while s > 9:
        s = sum(int(d) for d in str(s))
    return s

def calculate_general_day(today: datetime) -> int:
    # сумма всех цифр даты ДД.ММ.ГГГГ
    return reduce_to_single_digit(today.strftime("%d.%m.%Y"))

def calculate_personal_year(birth_date: datetime, today: datetime) -> int:
    all_digits = birth_date.strftime("%d%m") + today.strftime("%Y")
    return reduce_to_single_digit(all_digits)

def calculate_personal_month(personal_year: int, today: datetime) -> int:
    month_digit = reduce_to_single_digit(today.strftime("%m"))  # 10->1, 11->2, 12->3
    return reduce_to_single_digit(str(personal_year + month_digit))

def calculate_personal_day(personal_month: int, today: datetime) -> int:
    day_digit = reduce_to_single_digit(today.strftime("%d"))  # 29->2, 30->3 и т.д.
    return reduce_to_single_digit(str(personal_month + day_digit))

# =========================
# ФОРМАТИРОВАНИЕ ВЫВОДА
# =========================
def build_result_message(birth_date_str: str, now_dt: datetime) -> str:
    birth_dt = datetime.strptime(birth_date_str, "%d.%m.%Y")

    general_day = calculate_general_day(now_dt)
    personal_year = calculate_personal_year(birth_dt, now_dt)
    personal_month = calculate_personal_month(personal_year, now_dt)
    personal_day = calculate_personal_day(personal_month, now_dt)

    # трактовка общего дня
    if now_dt.day in UNFAVORABLE_DAYS:
        general_desc = GENERAL_DAY_INTERPRETATIONS["unfavorable"]
    else:
        general_desc = GENERAL_DAY_INTERPRETATIONS.get(general_day, "")

    personal_day_desc = PERSONAL_DAY_INTERPRETATIONS.get(personal_day, "")

    lines = []
    lines.append(f"<b>Дата:</b> {now_dt.strftime('%d.%m.%Y')}")
    lines.append("")
    lines.append(f"<b>Общий день:</b> {general_day}")
    if general_desc:
        lines.append(f"— {general_desc}")
    lines.append("")
    lines.append(f"<b>Личный год:</b> {personal_year}")
    lines.append(f"<b>Личный месяц:</b> {personal_month}")
    lines.append(f"<b>Личный день:</b> {personal_day}")
    lines.append("")
    if personal_day_desc:
        lines.append(f"<b>Трактовка личного дня {personal_day}:</b> {personal_day_desc}")

    # Полные описания ЛГ/ЛМ — только 1-го числа
    if now_dt.day == 1:
        py = PERSONAL_YEAR_INTERPRETATIONS.get(personal_year)
        pm = PERSONAL_MONTH_INTERPRETATIONS.get(personal_month)

        if py or pm:
            lines.append("")
            lines.append("━━━━━━━━━━━━━━━━━━━━")
            lines.append("<b>Полное описание периодов (выдаётся 1-го числа)</b>")

        if py:
            lines.append("")
            lines.append(f"<b>{py.get('title','')}</b>")
            desc = py.get("description", "")
            rec = py.get("recommendations", "")
            bad = py.get("if_not_used", "")
            if desc:
                lines.append(desc)
            if rec:
                lines.append("")
                lines.append("<b>Рекомендации:</b>")
                lines.append(rec)
            if bad:
                lines.append("")
                lines.append("<b>Если не проживать энергию:</b>")
                lines.append(bad)

        if pm:
            lines.append("")
            lines.append(f"<b>{pm.get('title','')}</b>")
            desc = pm.get("description", "")
            rec = pm.get("recommendations", "")
            bad = pm.get("if_not_used", "")
            if desc:
                lines.append(desc)
            if rec:
                lines.append("")
                lines.append("<b>Рекомендации:</b>")
                lines.append(rec)
            if bad:
                lines.append("")
                lines.append("<b>Если не проживать энергию:</b>")
                lines.append(bad)

    return "\n".join(lines)

def menu_keyboard(subscribed: bool) -> InlineKeyboardMarkup:
    sub_text = "🔕 Отключить ежедневные сообщения" if subscribed else "🔔 Включить ежедневные сообщения"
    keyboard = [
        [InlineKeyboardButton("🔁 Рассчитать на сегодня", callback_data="calc_today")],
        [InlineKeyboardButton("✏️ Изменить дату рождения", callback_data="change_birth")],
        [InlineKeyboardButton(sub_text, callback_data="toggle_sub")],
    ]
    return InlineKeyboardMarkup(keyboard)

# =========================
# ДИАЛОГИ (Conversation)
# =========================
ASK_BIRTHDATE = 1

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    row = db_get_user(user_id)

    if row:
        _, birth_date, subscribed = row
        await update.message.reply_text(
            "Меню:",
            reply_markup=menu_keyboard(bool(subscribed)),
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "Привет! Введите вашу дату рождения в формате <b>ДД.ММ.ГГГГ</b> (пример: 05.03.1994).",
        parse_mode=ParseMode.HTML,
    )
    return ASK_BIRTHDATE

def parse_birthdate_strict(text: str) -> str:
    # строгий формат и реальная дата
    dt = datetime.strptime(text.strip(), "%d.%m.%Y")
    # логичная проверка: ДР не в будущем
    now = datetime.now(TZ)
    if dt.date() > now.date():
        raise ValueError("Birth date is in the future.")
    return dt.strftime("%d.%m.%Y")

async def set_birthdate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    try:
        birth_str = parse_birthdate_strict(text)
    except Exception:
        await update.message.reply_text(
            "❌ Неверная дата. Введите строго в формате <b>ДД.ММ.ГГГГ</b> и убедитесь, что дата существует (пример: 05.03.1994).",
            parse_mode=ParseMode.HTML,
        )
        return ASK_BIRTHDATE

    db_upsert_user(user_id, birth_str)

    now_dt = datetime.now(TZ)
    msg = build_result_message(birth_str, now_dt)

    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    row = db_get_user(user_id)
    subscribed = bool(row[2]) if row else False
    await update.message.reply_text("Меню:", reply_markup=menu_keyboard(subscribed))
    return ConversationHandler.END

async def on_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    row = db_get_user(user_id)
    if not row:
        await query.edit_message_text("Нужно сначала ввести дату рождения. Напишите /start")
        return

    _, birth_str, subscribed = row
    subscribed = bool(subscribed)

    if query.data == "calc_today":
        now_dt = datetime.now(TZ)
        msg = build_result_message(birth_str, now_dt)
        await query.edit_message_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        await query.message.reply_text("Меню:", reply_markup=menu_keyboard(subscribed))
        return

    if query.data == "change_birth":
        await query.edit_message_text(
            "Введите новую дату рождения в формате <b>ДД.ММ.ГГГГ</b> (пример: 05.03.1994).",
            parse_mode=ParseMode.HTML,
        )
        # переключаемся в режим ожидания даты рождения
        context.user_data["awaiting_birthdate"] = True
        return

    if query.data == "toggle_sub":
        new_state = not subscribed
        db_set_subscribed(user_id, new_state)
        if new_state:
            await query.edit_message_text("✅ Ежедневные сообщения включены.")
        else:
            await query.edit_message_text("✅ Ежедневные сообщения отключены.")
        await query.message.reply_text("Меню:", reply_markup=menu_keyboard(new_state))
        return

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Ловим текст вне ConversationHandler:
    - если пользователь нажал "Изменить дату рождения", ждём дату тут.
    """
    if not context.user_data.get("awaiting_birthdate"):
        await update.message.reply_text("Используйте меню: /start")
        return

    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    try:
        birth_str = parse_birthdate_strict(text)
    except Exception:
        await update.message.reply_text(
            "❌ Неверная дата. Введите строго в формате <b>ДД.ММ.ГГГГ</b> и убедитесь, что дата существует (пример: 05.03.1994).",
            parse_mode=ParseMode.HTML,
        )
        return

    db_upsert_user(user_id, birth_str)
    context.user_data["awaiting_birthdate"] = False

    now_dt = datetime.now(TZ)
    msg = build_result_message(birth_str, now_dt)
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    row = db_get_user(user_id)
    subscribed = bool(row[2]) if row else False
    await update.message.reply_text("Меню:", reply_markup=menu_keyboard(subscribed))

# =========================
# ЕЖЕДНЕВНАЯ РАССЫЛКА
# =========================
async def daily_broadcast(context: ContextTypes.DEFAULT_TYPE) -> None:
    users = db_all_subscribed_users()
    now_dt = datetime.now(TZ)

    for user_id, birth_str in users:
        try:
            msg = build_result_message(birth_str, now_dt)
            await context.bot.send_message(
                chat_id=user_id,
                text=msg,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.exception("Failed to send daily message to %s: %s", user_id, e)

# =========================
# MAIN
# =========================
def main() -> None:
    db_init()

    app = Application.builder().token(TOKEN).build()

    # Conversation только для первого ввода даты рождения через /start
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={ASK_BIRTHDATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_birthdate)]},
        fallbacks=[],
    )

    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(on_menu))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    # Запланируем рассылку каждый день в 09:00 по Asia/Almaty
    app.job_queue.run_daily(daily_broadcast, time=time(9, 0, tzinfo=TZ), name="daily_broadcast")

    logger.info("Bot started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
