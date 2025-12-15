import os
import json
import logging
import sqlite3
from datetime import datetime, date, timedelta, time
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Dict, Any

import gspread
from google.oauth2.service_account import Credentials
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import Conflict
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
# CONFIG
# =========================
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set")

GSHEET_ID = os.environ.get("GSHEET_ID")
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON")

TZ = ZoneInfo("Asia/Almaty")
DB_PATH = os.environ.get("BOT_DB_PATH", "bot.db")

TRIAL_DAYS = 3
UNFAVORABLE_DAYS = {10, 20, 30}

ADMIN_CHAT_IDS = set()
_admin_raw = os.environ.get("ADMIN_CHAT_IDS", "").strip()
if _admin_raw:
    for x in _admin_raw.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_CHAT_IDS.add(int(x))

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
async def sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    try:
        created = ensure_user_in_sheet(user)
        if created:
            await update.message.reply_text("✅ Добавил тебя в Google Sheets (subscriptions).")
        else:
            await update.message.reply_text("ℹ️ Ты уже есть в Google Sheets (или запись не нужна).")
    except Exception as e:
        await update.message.reply_text(f"❌ Не смог записать в Google Sheets: {type(e).__name__}: {e}")
app.add_handler(CommandHandler("sync", sync))

# =========================
# FALLBACK TEXTS (if Sheets/text store fails)
# =========================
GENERAL_DAY_INTERPRETATIONS = {
    1: "День новых начинаний и инициатив. Благоприятно начинать проекты, принимать самостоятельные решения.",
    2: "День партнёрства и дипломатии. Хорошо для переговоров и совместных задач.",
    3: "День анализа и успеха. Подходит для решений, договоров и покупок.",
    4: "День структуры и порядка. Планирование, документы, дисциплина.",
    5: "День перемен. Движение, коммуникации, гибкость.",
    6: "День гармонии. Хорош для договоров, покупок и важных шагов.",
    7: "День размышлений. Лучше замедлиться, учиться, анализировать.",
    8: "День денег и управления. Карьера, финансы, ответственность.",
    9: "День завершения. Закрывайте дела, подводите итоги.",
    "unfavorable": (
        "Сегодня нежелательно начинать новые проекты и события. Есть высокая вероятность обнуления результатов. "
        "Лучше перенести крупные покупки, договоры, кредиты и важные решения."
    ),
}

PERSONAL_DAY_INTERPRETATIONS = {
    1: "День инициативы и самостоятельных решений.",
    2: "День чувствительности и взаимодействия.",
    3: "День общения и самовыражения.",
    4: "День дисциплины и порядка.",
    5: "День перемен и гибкости.",
    6: "День ответственности и заботы.",
    7: "День размышлений и анализа.",
    8: "День силы, денег и контроля.",
    9: "День завершения и отпускания.",
}

# (коротко, без простыней; при желании эти тексты тоже можно вынести в Sheets позже)
PERSONAL_YEAR_SHORT = {
    1: "Новый цикл, старт и инициативы.",
    2: "Партнёрство, терпение, выстраивание отношений.",
    3: "Творчество, общение, самовыражение.",
    4: "Фундамент, дисциплина, системность.",
    5: "Перемены, свобода, гибкость.",
    6: "Ответственность, семья, баланс.",
    7: "Осмысление, анализ, обучение.",
    8: "Результаты, деньги, карьера.",
    9: "Завершение, итоги, отпускание.",
}

PERSONAL_MONTH_SHORT = {
    1: "Инициатива и новый старт.",
    2: "Партнёрство и мягкость.",
    3: "Общение и творчество.",
    4: "Порядок и дисциплина.",
    5: "Перемены и движение.",
    6: "Ответственность и забота.",
    7: "Анализ и осмысление.",
    8: "Результаты и финансы.",
    9: "Завершение и очищение.",
}

# =========================
# SQLite DB
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
                birth_date TEXT,
                notify INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.commit()

def db_set_birthdate(user_id: int, birth: str) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, birth_date, notify)
            VALUES (?, ?, COALESCE((SELECT notify FROM users WHERE user_id=?), 0))
            ON CONFLICT(user_id) DO UPDATE SET birth_date=excluded.birth_date
            """,
            (user_id, birth, user_id),
        )
        conn.commit()

def db_get_user(user_id: int) -> Tuple[Optional[str], int]:
    with db_connect() as conn:
        cur = conn.execute("SELECT birth_date, notify FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        if not row:
            return (None, 0)
        return (row[0], int(row[1]))

def db_set_notify(user_id: int, notify: bool) -> None:
    with db_connect() as conn:
        conn.execute("UPDATE users SET notify=? WHERE user_id=?", (1 if notify else 0, user_id))
        conn.commit()

def db_get_notify_users() -> list[Tuple[int, str]]:
    with db_connect() as conn:
        cur = conn.execute("SELECT user_id, birth_date FROM users WHERE notify=1 AND birth_date IS NOT NULL")
        return cur.fetchall()

# =========================
# Google Sheets subscriptions (source of truth for access)
# =========================
def gs_client() -> gspread.Client:
    if not GOOGLE_SA_JSON or not GSHEET_ID:
        raise ValueError("GSHEET_ID / GOOGLE_SA_JSON not set")
    info = json.loads(GOOGLE_SA_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]  # read/write
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def _parse_ymd(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def find_user_row(ws: gspread.Worksheet, user_id: int) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    """
    Returns (row_index, record_dict) where row_index is 2-based (because row 1 is headers),
    or (None, None) if not found.
    """
    records = ws.get_all_records()
    for i, r in enumerate(records, start=2):
        rid = str(r.get("telegram_user_id", "")).strip()
        if rid.isdigit() and int(rid) == user_id:
            return i, r
    return None, None

def ensure_user_in_sheet(user) -> bool:
    """
    Adds user to subscriptions if not exists.
    Returns True if created new row.
    """
    if not GSHEET_ID or not GOOGLE_SA_JSON:
        return False

    gc = gs_client()
    sh = gc.open_by_key(GSHEET_ID)
    ws = sh.worksheet("subscriptions")

    row_idx, _ = find_user_row(ws, user.id)
    if row_idx is not None:
        return False

    today = date.today()
    trial_until = (today + timedelta(days=TRIAL_DAYS)).strftime("%Y-%m-%d")

    ws.append_row(
        [
            user.id,
            "active",
            "trial",
            trial_until,
            datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
            user.username or "",
            user.first_name or "",
            user.last_name or "",
        ],
        value_input_option="USER_ENTERED",
    )
    return True

def get_access_level_and_autoblock(user_id: int) -> str:
    """
    Source of truth: subscriptions sheet.
    Returns: 'premium' | 'trial' | 'blocked'
    Also performs auto-block: if trial expired -> set status=inactive.
    """
    # If sheets not configured -> allow trial to avoid hard fail
    if not GSHEET_ID or not GOOGLE_SA_JSON:
        return "trial"

    try:
        gc = gs_client()
        sh = gc.open_by_key(GSHEET_ID)
        ws = sh.worksheet("subscriptions")

        row_idx, r = find_user_row(ws, user_id)
        if row_idx is None or not r:
            return "blocked"

        status = str(r.get("status", "")).strip().lower()
        plan = str(r.get("plan", "")).strip().lower()
        until = _parse_ymd(str(r.get("access_until", "")).strip())

        if status != "active":
            return "blocked"

        # expiry check (trial or premium can have until)
        if until and date.today() > until:
            # AUTO-BLOCK for trial expiry (and also for premium if you set until)
            # requirement asked: "автопереход trial → blocked"
            if plan == "trial":
                try:
                    # column B is status (telegram_user_id=A, status=B, plan=C, access_until=D)
                    ws.update_cell(row_idx, 2, "inactive")
                    logger.info("Auto-blocked expired trial user_id=%s", user_id)
                except Exception as e:
                    logger.exception("Failed to auto-block in sheet: %s", e)
            return "blocked"

        if plan == "premium":
            return "premium"
        if plan == "trial":
            return "trial"
        # unknown plan -> block
        return "blocked"

    except Exception as e:
        logger.exception("Sheets access check failed: %s", e)
        # safe fallback: trial (so bot works even if google temporary down)
        return "trial"

# =========================
# Numerology calculations
# =========================
def reduce_to_digit(s: str) -> int:
    nums = [int(c) for c in s if c.isdigit()]
    total = sum(nums)
    while total > 9:
        total = sum(int(c) for c in str(total))
    return total

def calc_general_day(today: datetime) -> int:
    return reduce_to_digit(today.strftime("%d.%m.%Y"))

def calc_personal_year(birth: datetime, today: datetime) -> int:
    return reduce_to_digit(birth.strftime("%d%m") + today.strftime("%Y"))

def calc_personal_month(personal_year: int, today: datetime) -> int:
    month_digit = reduce_to_digit(today.strftime("%m"))  # 12 -> 3
    return reduce_to_digit(str(personal_year + month_digit))

def calc_personal_day(personal_month: int, today: datetime) -> int:
    day_digit = reduce_to_digit(today.strftime("%d"))  # 30 -> 3
    return reduce_to_digit(str(personal_month + day_digit))

# =========================
# Message builders
# =========================
def build_trial_message(birth_str: str, now_dt: datetime) -> str:
    birth = datetime.strptime(birth_str, "%d.%m.%Y")
    py = calc_personal_year(birth, now_dt)
    pm = calc_personal_month(py, now_dt)
    pd = calc_personal_day(pm, now_dt)
    return (
        f"<b>Дата:</b> {now_dt.strftime('%d.%m.%Y')}\n\n"
        f"<b>Личный день:</b> {pd}\n"
        f"{PERSONAL_DAY_INTERPRETATIONS.get(pd, '')}\n\n"
        f"⏳ <b>Trial:</b> доступ ограничен — только личный день."
    )
async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("pong ✅")

def build_premium_message(birth_str: str, now_dt: datetime) -> str:
    birth = datetime.strptime(birth_str, "%d.%m.%Y")

    gd = calc_general_day(now_dt)
    py = calc_personal_year(birth, now_dt)
    pm = calc_personal_month(py, now_dt)
    pd = calc_personal_day(pm, now_dt)

    if now_dt.day in UNFAVORABLE_DAYS:
        gd_text = GENERAL_DAY_INTERPRETATIONS.get("unfavorable", "")
    else:
        gd_text = GENERAL_DAY_INTERPRETATIONS.get(gd, "")

    lines = [
        f"<b>Дата:</b> {now_dt.strftime('%d.%m.%Y')}",
        "",
        f"<b>Общий день:</b> {gd}",
    ]
    if gd_text:
        lines.append(f"— {gd_text}")

    lines += [
        "",
        f"<b>Личный год:</b> {py} — {PERSONAL_YEAR_SHORT.get(py, '')}",
        f"<b>Личный месяц:</b> {pm} — {PERSONAL_MONTH_SHORT.get(pm, '')}",
        f"<b>Личный день:</b> {pd}",
        f"{PERSONAL_DAY_INTERPRETATIONS.get(pd, '')}",
    ]
    return "\n".join(lines)

# =========================
# UI
# =========================
ASK_BIRTH = 1

def menu_keyboard(access: str, notify: bool) -> InlineKeyboardMarkup:
    if access == "premium":
        sub_text = "🔕 Отключить ежедневные сообщения" if notify else "🔔 Включить ежедневные сообщения"
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🔁 Прогноз на сегодня", callback_data="calc")],
            [InlineKeyboardButton("✏️ Изменить дату рождения", callback_data="change_birth")],
            [InlineKeyboardButton(sub_text, callback_data="toggle_notify")],
        ])
    if access == "trial":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🔁 Личный день на сегодня", callback_data="calc")],
            [InlineKeyboardButton("✏️ Изменить дату рождения", callback_data="change_birth")],
            [InlineKeyboardButton("⭐️ Premium", callback_data="upgrade")],
        ])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐️ Premium", callback_data="upgrade")]
    ])

# =========================
# Handlers
# =========================
def parse_birth_strict(text: str) -> str:
    dt = datetime.strptime(text.strip(), "%d.%m.%Y")
    if dt.date() > datetime.now(TZ).date():
        raise ValueError("Birth date is in the future")
    return dt.strftime("%d.%m.%Y")

async def notify_admins_new_user(context: ContextTypes.DEFAULT_TYPE, user) -> None:
    if not ADMIN_CHAT_IDS:
        return
    msg = (
        "🆕 <b>Новый пользователь</b>\n"
        f"ID: <code>{user.id}</code>\n"
        f"Username: @{user.username}" if user.username else f"ID: <code>{user.id}</code>"
    )
    # безопаснее собрать нормально:
    uname = f"@{user.username}" if user.username else "(нет)"
    name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "(без имени)"
    msg = (
        "🆕 <b>Новый пользователь</b>\n"
        f"ID: <code>{user.id}</code>\n"
        f"Name: {name}\n"
        f"Username: {uname}\n"
        f"Дата: {datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    for admin_id in ADMIN_CHAT_IDS:
        try:
            await context.bot.send_message(admin_id, msg, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.exception("Failed to notify admin %s: %s", admin_id, e)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user

    created = False
    try:
        created = ensure_user_in_sheet(user)
    except Exception as e:
        logger.exception("ensure_user_in_sheet failed: %s", e)

    if created:
        await notify_admins_new_user(context, user)

    access = get_access_level_and_autoblock(user.id)
    birth_str, notify = db_get_user(user.id)

    if birth_str:
        await update.message.reply_text("Меню:", reply_markup=menu_keyboard(access, bool(notify)))
        return ConversationHandler.END

    await update.message.reply_text(
        "Введите дату рождения в формате <b>ДД.ММ.ГГГГ</b> (пример: 05.03.1994).",
        parse_mode=ParseMode.HTML,
    )
    return ASK_BIRTH

async def set_birth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    try:
        birth_str = parse_birth_strict(update.message.text or "")
    except Exception:
        await update.message.reply_text(
            "❌ Неверная дата. Формат <b>ДД.ММ.ГГГГ</b> (пример: 05.03.1994).",
            parse_mode=ParseMode.HTML,
        )
        return ASK_BIRTH

    db_set_birthdate(user.id, birth_str)

    access = get_access_level_and_autoblock(user.id)
    _, notify = db_get_user(user.id)
    await update.message.reply_text("✅ Дата сохранена.", reply_markup=menu_keyboard(access, bool(notify)))
    return ConversationHandler.END

async def on_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    user = q.from_user

    # always ensure in sheet (in case user skipped /start somehow)
    try:
        created = ensure_user_in_sheet(user)
        if created:
            await notify_admins_new_user(context, user)
    except Exception:
        pass

    access = get_access_level_and_autoblock(user.id)
    birth_str, notify = db_get_user(user.id)

    if access == "blocked":
        await q.edit_message_text("⛔ Доступ ограничен. Trial закончился или статус выключен.")
        return

    if q.data == "upgrade":
        await q.edit_message_text(
            "⭐️ <b>Premium</b> включает полный прогноз и ежедневные сообщения.\n"
            "Чтобы включить Premium — админ меняет вам план в таблице.",
            parse_mode=ParseMode.HTML,
        )
        return

    if q.data == "change_birth":
        context.user_data["awaiting_birth"] = True
        await q.edit_message_text(
            "Введите новую дату рождения в формате <b>ДД.ММ.ГГГГ</b> (пример: 05.03.1994).",
            parse_mode=ParseMode.HTML,
        )
        return

    if q.data == "toggle_notify":
        if access != "premium":
            await q.edit_message_text("⛔ Ежедневные сообщения доступны только в Premium.")
            return
        new_notify = not bool(notify)
        db_set_notify(user.id, new_notify)
        await q.edit_message_text("✅ Готово.")
        await q.message.reply_text("Меню:", reply_markup=menu_keyboard(access, new_notify))
        return

    # calc
    if not birth_str:
        await q.edit_message_text("Сначала введите дату рождения: /start")
        return

    now_dt = datetime.now(TZ)

    if access == "trial":
        msg = build_trial_message(birth_str, now_dt)  # ONLY LD
    else:
        msg = build_premium_message(birth_str, now_dt)  # FULL

    await q.edit_message_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    await q.message.reply_text("Меню:", reply_markup=menu_keyboard(access, bool(db_get_user(user.id)[1])))

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get("awaiting_birth"):
        await update.message.reply_text("Используйте /start")
        return

    user = update.effective_user
    try:
        birth_str = parse_birth_strict(update.message.text or "")
    except Exception:
        await update.message.reply_text(
            "❌ Неверная дата. Формат <b>ДД.ММ.ГГГГ</b> (пример: 05.03.1994).",
            parse_mode=ParseMode.HTML,
        )
        return

    db_set_birthdate(user.id, birth_str)
    context.user_data["awaiting_birth"] = False

    access = get_access_level_and_autoblock(user.id)
    _, notify = db_get_user(user.id)

    now_dt = datetime.now(TZ)
    if access == "trial":
        msg = build_trial_message(birth_str, now_dt)
    else:
        msg = build_premium_message(birth_str, now_dt)

    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    await update.message.reply_text("Меню:", reply_markup=menu_keyboard(access, bool(notify)))

# =========================
# DAILY PREMIUM BROADCAST
# =========================
async def daily_broadcast(context: ContextTypes.DEFAULT_TYPE) -> None:
    users = db_get_notify_users()
    now_dt = datetime.now(TZ)

    for user_id, birth_str in users:
        access = get_access_level_and_autoblock(user_id)
        if access != "premium":
            # no daily for trial/blocked
            continue
        try:
            msg = build_premium_message(birth_str, now_dt)
            await context.bot.send_message(
                chat_id=user_id,
                text=msg,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.exception("daily_broadcast failed for %s: %s", user_id, e)

# =========================
# ERROR HANDLER (409 conflict)
# =========================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, Conflict):
        logger.error("409 Conflict: another instance is running. Exiting to let Render restart.")
        os._exit(1)
    logger.exception("Unhandled error: %s", err)

# =========================
# MAIN
# =========================
def main() -> None:
    db_init()

    app = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={ASK_BIRTH: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_birth)]},
        fallbacks=[],
    )

    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(on_menu))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    app.add_error_handler(on_error)

    # Daily premium broadcast at 09:00 Asia/Almaty
    app.job_queue.run_daily(
        daily_broadcast,
        time=time(9, 0, tzinfo=TZ),
        name="daily_broadcast",
    )

    logger.info("Bot started")
    app.add_handler(CommandHandler("ping", ping))
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
