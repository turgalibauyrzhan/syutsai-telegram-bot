import os
import json
import time as pytime
import logging
import sqlite3
from datetime import datetime, time
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

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
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set (Render env var).")

TZ = ZoneInfo("Asia/Almaty")
DB_PATH = os.environ.get("BOT_DB_PATH", "bot.db")

GSHEET_ID = os.environ.get("GSHEET_ID")
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON")
TEXT_CACHE_TTL_SECONDS = int(os.environ.get("TEXT_CACHE_TTL_SECONDS", "300"))

ADMIN_USER_IDS = set()
_admin_raw = os.environ.get("ADMIN_USER_IDS", "").strip()
if _admin_raw:
    for x in _admin_raw.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_USER_IDS.add(int(x))

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# FALLBACK ТЕКСТЫ (если Sheets недоступен)
# =========================
UNFAVORABLE_DAYS = {10, 20, 30}

DEFAULT_GENERAL_DAY_INTERPRETATIONS = {
    1: "День новых начинаний и инициатив. Благоприятно начинать проекты, принимать самостоятельные решения, брать ответственность.",
    2: "День взаимодействия и партнёрства. Хорошо для переговоров, совместной работы, примирения и дипломатии.",
    3: "Благоприятный день через анализ и успех. Подходит для принятия серьёзных решений, подписания договоров и совершения покупок.",
    4: "День структуры и порядка. Благоприятно заниматься планированием, документами, финансами и рутинными задачами.",
    5: "День перемен и активности. Хорошо для поездок, общения, новых знакомств, гибких решений.",
    6: "Благоприятный день через любовь и гармонию. Подходит для важных решений, подписания договоров, покупок и начала больших проектов.",
    7: "День анализа и уединения. Лучше посвятить время размышлениям, обучению, внутренней работе.",
    8: "День материальных вопросов и власти. Хорош для бизнеса, финансовых операций, управления и карьерных решений.",
    9: "День завершения и подведения итогов. Благоприятно закрывать дела, отпускать старое, заниматься благотворительностью.",
    "unfavorable": (
        "Сегодня нежелательно начинать новые проекты и события. Есть высокая вероятность обнуления всех результатов ваших действий. "
        "Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и важные решения."
    ),
}

DEFAULT_PERSONAL_DAY_INTERPRETATIONS = {
    1: "День личной инициативы. Проявляйте самостоятельность, принимайте решения, действуйте смело.",
    2: "День чувствительности и взаимодействия. Будьте внимательны к эмоциям — своим и чужим.",
    3: "День общения и творчества. Благоприятен для самовыражения, встреч и лёгких решений.",
    4: "День дисциплины и порядка. Лучше сосредоточиться на делах, обязанностях и завершении задач.",
    5: "День свободы и движения. Возможны неожиданные события, гибкость даст лучший результат.",
    6: "День семьи и ответственности. Подходит для заботы о близких, домашних и личных дел.",
    7: "День внутренней работы. Хорошо замедлиться, подумать, понаблюдать.",
    8: "День силы и контроля. Благоприятен для финансовых и рабочих решений.",
    9: "День отпускания и завершения. Не держитесь за старое — освобождение даст облегчение.",
}

DEFAULT_PERSONAL_YEAR_INTERPRETATIONS = {
    1: {"title": "Личный год 1 — Начало нового цикла", "description": "Год новых возможностей, инициатив и стартов. Формируется вектор на ближайшие 9 лет.", "recommendations": "Начинайте проекты, принимайте решения, действуйте самостоятельно.", "if_not_used": "Ощущение застоя, упущенные возможности, отсутствие направления."},
    2: {"title": "Личный год 2 — Партнёрство и ожидание", "description": "Год взаимодействия, терпения и эмоциональной чувствительности.", "recommendations": "Учитесь сотрудничать, договариваться, выстраивать отношения.", "if_not_used": "Зависимость от чужого мнения, внутренние конфликты."},
    3: {"title": "Личный год 3 — Самовыражение", "description": "Год творчества, общения, публичности и радости.", "recommendations": "Проявляйте себя, развивайте таланты, расширяйте круг общения.", "if_not_used": "Разбросанность, пустая трата энергии."},
    4: {"title": "Личный год 4 — Структура и фундамент", "description": "Год труда, дисциплины и создания устойчивой базы.", "recommendations": "Наводите порядок, стройте систему, работайте на результат.", "if_not_used": "Перегрузка, ощущение тяжести и стагнации."},
    5: {"title": "Личный год 5 — Перемены", "description": "Год свободы, изменений и неожиданных поворотов.", "recommendations": "Будьте гибкими, открытыми к новому, путешествуйте.", "if_not_used": "Хаос, нестабильность, импульсивные ошибки."},
    6: {"title": "Личный год 6 — Ответственность", "description": "Год семьи, заботы и гармонизации жизни.", "recommendations": "Уделяйте внимание близким, дому, здоровью.", "if_not_used": "Чувство долга без радости, эмоциональное выгорание."},
    7: {"title": "Личный год 7 — Осмысление", "description": "Год внутреннего роста, анализа и поиска смысла.", "recommendations": "Учитесь, исследуйте, развивайтесь.", "if_not_used": "Изоляция, сомнения, потеря мотивации."},
    8: {"title": "Личный год 8 — Реализация", "description": "Год денег, карьеры, управления и результатов.", "recommendations": "Берите ответственность, управляйте ресурсами, укрепляйте финансы.", "if_not_used": "Финансовые сложности, конфликты из-за контроля."},
    9: {"title": "Личный год 9 — Завершение", "description": "Год подведения итогов и освобождения от прошлого.", "recommendations": "Завершайте дела, отпускайте старое, готовьтесь к новому циклу.", "if_not_used": "Застревание в прошлом, эмоциональная тяжесть."},
}

DEFAULT_PERSONAL_MONTH_INTERPRETATIONS = {
    1: {"title": "Личный месяц 1", "description": "Месяц инициативы и новых шагов.", "recommendations": "Начинайте, пробуйте, действуйте.", "if_not_used": "Прокрастинация, упущенные возможности."},
    2: {"title": "Личный месяц 2", "description": "Месяц партнёрства и чувств.", "recommendations": "Проявляйте мягкость и терпение.", "if_not_used": "Обида, зависимость от других."},
    3: {"title": "Личный месяц 3", "description": "Месяц общения и творчества.", "recommendations": "Говорите, проявляйтесь, общайтесь.", "if_not_used": "Поверхностность, суета."},
    4: {"title": "Личный месяц 4", "description": "Месяц дисциплины и работы.", "recommendations": "Наводите порядок, фокусируйтесь.", "if_not_used": "Усталость, перегрузка."},
    5: {"title": "Личный месяц 5", "description": "Месяц перемен и свободы.", "recommendations": "Будьте гибкими и открытыми.", "if_not_used": "Импульсивность, нестабильность."},
    6: {"title": "Личный месяц 6", "description": "Месяц семьи и ответственности.", "recommendations": "Заботьтесь о близких и себе.", "if_not_used": "Чувство долга без радости."},
    7: {"title": "Личный месяц 7", "description": "Месяц размышлений и анализа.", "recommendations": "Замедляйтесь и осмысливайте.", "if_not_used": "Замкнутость, сомнения."},
    8: {"title": "Личный месяц 8", "description": "Месяц денег и результатов.", "recommendations": "Смело берите ответственность.", "if_not_used": "Конфликты из-за контроля."},
    9: {"title": "Личный месяц 9", "description": "Месяц завершений.", "recommendations": "Закрывайте дела и отпускайте.", "if_not_used": "Эмоциональная тяжесть."},
}

# =========================
# GOOGLE SHEETS ЗАГРУЗКА + КЭШ + FALLBACK
# =========================
_TEXT_CACHE: Dict[str, Any] = {"loaded_at": 0, "data": None}

def _gs_client() -> gspread.Client:
    if not GOOGLE_SA_JSON:
        raise ValueError("GOOGLE_SA_JSON is not set")
    info = json.loads(GOOGLE_SA_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def _normalize_key(raw: Any) -> Any:
    s = str(raw).strip()
    if s.isdigit():
        return int(s)
    return s

def _read_kv_sheet(sh: gspread.Spreadsheet, sheet_name: str) -> Dict[Any, str]:
    ws = sh.worksheet(sheet_name)
    rows = ws.get_all_records()  # headers in first row
    out: Dict[Any, str] = {}
    for r in rows:
        k = _normalize_key(r.get("key", ""))
        v = str(r.get("text", "")).strip()
        if k == "" or k is None:
            continue
        if v == "":
            continue
        out[k] = v
    return out

def _read_struct_sheet(sh: gspread.Spreadsheet, sheet_name: str) -> Dict[int, Dict[str, str]]:
    ws = sh.worksheet(sheet_name)
    rows = ws.get_all_records()
    out: Dict[int, Dict[str, str]] = {}
    for r in rows:
        k = _normalize_key(r.get("key", ""))
        if not isinstance(k, int):
            continue
        out[k] = {
            "title": str(r.get("title", "")).strip(),
            "description": str(r.get("description", "")).strip(),
            "recommendations": str(r.get("recommendations", "")).strip(),
            "if_not_used": str(r.get("if_not_used", "")).strip(),
        }
    return out

def default_texts() -> Dict[str, Any]:
    return {
        "GENERAL_DAY_INTERPRETATIONS": DEFAULT_GENERAL_DAY_INTERPRETATIONS,
        "PERSONAL_DAY_INTERPRETATIONS": DEFAULT_PERSONAL_DAY_INTERPRETATIONS,
        "PERSONAL_YEAR_INTERPRETATIONS": DEFAULT_PERSONAL_YEAR_INTERPRETATIONS,
        "PERSONAL_MONTH_INTERPRETATIONS": DEFAULT_PERSONAL_MONTH_INTERPRETATIONS,
    }

def load_texts(force: bool = False) -> Dict[str, Any]:
    now = int(pytime.time())
    if (not force) and _TEXT_CACHE["data"] and (now - _TEXT_CACHE["loaded_at"] < TEXT_CACHE_TTL_SECONDS):
        return _TEXT_CACHE["data"]

    # если Sheets не настроен — сразу fallback
    if not GSHEET_ID or not GOOGLE_SA_JSON:
        data = default_texts()
        _TEXT_CACHE["data"] = data
        _TEXT_CACHE["loaded_at"] = now
        return data

    try:
        gc = _gs_client()
        sh = gc.open_by_key(GSHEET_ID)

        data = {
            "GENERAL_DAY_INTERPRETATIONS": _read_kv_sheet(sh, "general_day"),
            "PERSONAL_DAY_INTERPRETATIONS": _read_kv_sheet(sh, "personal_day"),
            "PERSONAL_YEAR_INTERPRETATIONS": _read_struct_sheet(sh, "personal_year"),
            "PERSONAL_MONTH_INTERPRETATIONS": _read_struct_sheet(sh, "personal_month"),
        }

        # минимальная проверка: если пусто — не ломаемся, а fallback
        if not data["PERSONAL_DAY_INTERPRETATIONS"] or not data["GENERAL_DAY_INTERPRETATIONS"]:
            raise ValueError("Sheets returned empty critical dictionaries")

        _TEXT_CACHE["data"] = data
        _TEXT_CACHE["loaded_at"] = now
        logger.info("Texts loaded from Google Sheets.")
        return data

    except Exception as e:
        logger.exception("Failed to load texts from Google Sheets, using fallback. Reason: %s", e)
        data = default_texts()
        _TEXT_CACHE["data"] = data
        _TEXT_CACHE["loaded_at"] = now
        return data

# =========================
# БД (SQLite)
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

def db_get_user(user_id: int) -> Optional[Tuple[int, str, int]]:
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
            UPDATE users SET subscribed = ? WHERE user_id = ?
            """,
            (1 if subscribed else 0, user_id),
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
    return reduce_to_single_digit(today.strftime("%d.%m.%Y"))

def calculate_personal_year(birth_date: datetime, today: datetime) -> int:
    all_digits = birth_date.strftime("%d%m") + today.strftime("%Y")
    return reduce_to_single_digit(all_digits)

def calculate_personal_month(personal_year: int, today: datetime) -> int:
    month_digit = reduce_to_single_digit(today.strftime("%m"))  # 10->1, 11->2, 12->3
    return reduce_to_single_digit(str(personal_year + month_digit))

def calculate_personal_day(personal_month: int, today: datetime) -> int:
    day_digit = reduce_to_single_digit(today.strftime("%d"))  # 29->2, 30->3
    return reduce_to_single_digit(str(personal_month + day_digit))

# =========================
# ФОРМАТИРОВАНИЕ ВЫВОДА
# =========================
def build_result_message(birth_date_str: str, now_dt: datetime) -> str:
    texts = load_texts()

    GENERAL_DAY_INTERPRETATIONS = texts["GENERAL_DAY_INTERPRETATIONS"]
    PERSONAL_DAY_INTERPRETATIONS = texts["PERSONAL_DAY_INTERPRETATIONS"]
    PERSONAL_YEAR_INTERPRETATIONS = texts["PERSONAL_YEAR_INTERPRETATIONS"]
    PERSONAL_MONTH_INTERPRETATIONS = texts["PERSONAL_MONTH_INTERPRETATIONS"]

    birth_dt = datetime.strptime(birth_date_str, "%d.%m.%Y")

    general_day = calculate_general_day(now_dt)
    personal_year = calculate_personal_year(birth_dt, now_dt)
    personal_month = calculate_personal_month(personal_year, now_dt)
    personal_day = calculate_personal_day(personal_month, now_dt)

    # общий день трактовка
    if now_dt.day in UNFAVORABLE_DAYS:
        general_desc = GENERAL_DAY_INTERPRETATIONS.get("unfavorable", DEFAULT_GENERAL_DAY_INTERPRETATIONS["unfavorable"])
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

    # Полные тексты ЛГ/ЛМ — только 1-го числа
    if now_dt.day == 1:
        py = PERSONAL_YEAR_INTERPRETATIONS.get(personal_year)
        pm = PERSONAL_MONTH_INTERPRETATIONS.get(personal_month)

        if py or pm:
            lines.append("")
            lines.append("━━━━━━━━━━━━━━━━━━━━")
            lines.append("<b>Полное описание периодов (выдаётся 1-го числа)</b>")

        if py:
            lines.append("")
            if py.get("title"):
                lines.append(f"<b>{py.get('title')}</b>")
            if py.get("description"):
                lines.append(py.get("description"))
            if py.get("recommendations"):
                lines.append("")
                lines.append("<b>Рекомендации:</b>")
                lines.append(py.get("recommendations"))
            if py.get("if_not_used"):
                lines.append("")
                lines.append("<b>Если не проживать энергию:</b>")
                lines.append(py.get("if_not_used"))

        if pm:
            lines.append("")
            if pm.get("title"):
                lines.append(f"<b>{pm.get('title')}</b>")
            if pm.get("description"):
                lines.append(pm.get("description"))
            if pm.get("recommendations"):
                lines.append("")
                lines.append("<b>Рекомендации:</b>")
                lines.append(pm.get("recommendations"))
            if pm.get("if_not_used"):
                lines.append("")
                lines.append("<b>Если не проживать энергию:</b>")
                lines.append(pm.get("if_not_used"))

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
# ДИАЛОГИ
# =========================
ASK_BIRTHDATE = 1

def parse_birthdate_strict(text: str) -> str:
    dt = datetime.strptime(text.strip(), "%d.%m.%Y")
    now = datetime.now(TZ)
    if dt.date() > now.date():
        raise ValueError("Birth date is in the future.")
    return dt.strftime("%d.%m.%Y")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    row = db_get_user(user_id)

    if row:
        _, _, subscribed = row
        await update.message.reply_text("Меню:", reply_markup=menu_keyboard(bool(subscribed)))
        return ConversationHandler.END

    await update.message.reply_text(
        "Привет! Введите вашу дату рождения в формате <b>ДД.ММ.ГГГГ</b> (пример: 05.03.1994).",
        parse_mode=ParseMode.HTML,
    )
    return ASK_BIRTHDATE

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
# ADMIN: RELOAD TEXTS
# =========================
async def reload_texts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if ADMIN_USER_IDS and user_id not in ADMIN_USER_IDS:
        await update.message.reply_text("❌ Нет доступа.")
        return

    load_texts(force=True)
    await update.message.reply_text("✅ Тексты перезагружены (или применён fallback).")

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

    # прогреем кэш (необязательно, но полезно)
    _ = load_texts(force=False)

    app = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={ASK_BIRTHDATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_birthdate)]},
        fallbacks=[],
    )

    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(on_menu))
    app.add_handler(CommandHandler("reload_texts", reload_texts))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    # Рассылка каждый день в 09:00 (Asia/Almaty)
    app.job_queue.run_daily(
        daily_broadcast,
        time=time(9, 0, tzinfo=TZ),
        name="daily_broadcast",
    )

    logger.info("Bot started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
