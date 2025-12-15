import os
import json
import base64
import logging
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update
from telegram.constants import ParseMode
from telegram.error import Conflict
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ===================== CONFIG =====================
TZ = ZoneInfo("Asia/Almaty")
TRIAL_DAYS = 3

TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
GSHEET_ID = (os.environ.get("GSHEET_ID") or "").strip()
GOOGLE_SA_JSON = (os.environ.get("GOOGLE_SA_JSON") or "").strip()

ADMIN_CHAT_IDS = set()
_admin_raw = (os.environ.get("ADMIN_CHAT_IDS") or "").strip()
if _admin_raw:
    for x in _admin_raw.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_CHAT_IDS.add(int(x))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("syucai_bot")

# убрать спам сетевых логов
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)
logging.getLogger("telegram.ext").setLevel(logging.INFO)


# ===================== NUMEROLOGY (LD only) =====================
LD_TEXT = {
    1: "День инициативы и начала. Хорошо запускать новое и брать ответственность.",
    2: "День взаимодействия и дипломатии. Лучше договариваться, чем давить.",
    3: "День общения и творчества. Полезно проявляться и продвигать идеи.",
    4: "День порядка и дисциплины. Делай по плану, закрывай хвосты.",
    5: "День перемен и движения. Гибкость важнее контроля.",
    6: "День семьи и ответственности. Хорошо наводить баланс и заботиться.",
    7: "День анализа и уединения. Меньше суеты, больше смысла и выводов.",
    8: "День силы и денег. Управляй ресурсами, принимай взрослые решения.",
    9: "День завершений. Закрывай циклы, подводи итоги, освобождай место новому.",
}


def reduce_to_digit(n: int) -> int:
    while n > 9:
        n = sum(int(c) for c in str(n))
    return n


def calc_personal_day(birth_ddmmyyyy: str, now_dt: datetime) -> int:
    # birth_ddmmyyyy: "05.03.1994"
    d, m, y = map(int, birth_ddmmyyyy.split("."))
    total = d + m + sum(int(c) for c in str(now_dt.year)) + now_dt.month + now_dt.day
    return reduce_to_digit(total)


def validate_birth(text: str) -> Optional[str]:
    text = (text or "").strip()
    try:
        dt = datetime.strptime(text, "%d.%m.%Y")
        if dt.date() > datetime.now(TZ).date():
            return None
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return None


# ===================== GOOGLE SHEETS =====================
def load_sa_info() -> dict:
    if not GOOGLE_SA_JSON:
        raise ValueError("GOOGLE_SA_JSON env is empty")

    raw = GOOGLE_SA_JSON.strip()

    # 1) base64 first
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        if decoded.strip().startswith("{"):
            return json.loads(decoded)
    except Exception:
        pass

    # 2) plain json (and unescape)
    raw = raw.replace("\\n", "\n")
    return json.loads(raw)


def gs_open_ws() -> gspread.Worksheet:
    if not GSHEET_ID:
        raise ValueError("GSHEET_ID env is empty")

    info = load_sa_info()
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GSHEET_ID)
    return sh.worksheet("subscriptions")


def find_user_row(ws: gspread.Worksheet, user_id: int) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    records = ws.get_all_records()
    for i, r in enumerate(records, start=2):  # row1 = headers
        rid = str(r.get("telegram_user_id", "")).strip()
        if rid.isdigit() and int(rid) == user_id:
            return i, r
    return None, None


def ensure_user_in_sheet(user) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Returns (created, record_or_none)
    """
    ws = gs_open_ws()
    row_idx, rec = find_user_row(ws, user.id)
    if row_idx is not None and rec:
        return False, rec

    now = datetime.now(TZ)
    access_until = (date.today() + timedelta(days=TRIAL_DAYS)).isoformat()

    ws.append_row(
        [
            user.id,
            "active",             # status
            "trial",              # plan
            access_until,         # access_until (YYYY-MM-DD)
            now.strftime("%Y-%m-%d %H:%M:%S"),  # created_at
            user.username or "",
            user.first_name or "",
            user.last_name or "",
        ],
        value_input_option="USER_ENTERED",
    )

    # fetch again
    row_idx2, rec2 = find_user_row(ws, user.id)
    return True, rec2


def parse_iso_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def get_access_level(user_id: int) -> str:
    """
    Uses Google Sheet as source of truth.
    Returns: 'premium' | 'trial' | 'blocked'
    Auto-blocks expired trial by setting status=inactive.
    Fallback: trial (if sheets broken)
    """
    if not (GSHEET_ID and GOOGLE_SA_JSON):
        return "trial"

    try:
        ws = gs_open_ws()
        row_idx, rec = find_user_row(ws, user_id)
        if row_idx is None or not rec:
            return "blocked"

        status = str(rec.get("status", "")).strip().lower()
        plan = str(rec.get("plan", "")).strip().lower()
        until = parse_iso_date(str(rec.get("access_until", "")))

        if status != "active":
            return "blocked"

        if until and date.today() > until:
            if plan == "trial":
                # auto-block
                try:
                    ws.update_cell(row_idx, 2, "inactive")  # status column (2)
                except Exception:
                    pass
            return "blocked"

        if plan == "premium":
            return "premium"
        if plan == "trial":
            return "trial"

        return "blocked"

    except Exception as e:
        logger.exception("Sheets access failed, fallback to trial: %s", e)
        return "trial"


# ===================== ADMIN NOTIFY =====================
async def notify_admins_new_user(context: ContextTypes.DEFAULT_TYPE, user) -> None:
    if not ADMIN_CHAT_IDS:
        return

    uname = f"@{user.username}" if user.username else "(нет)"
    name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "(без имени)"
    msg = (
        "🆕 <b>Новый пользователь</b>\n"
        f"ID: <code>{user.id}</code>\n"
        f"Name: {name}\n"
        f"Username: {uname}\n"
        f"Time: {datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')}"
    )

    for admin_id in ADMIN_CHAT_IDS:
        try:
            await context.bot.send_message(admin_id, msg, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.exception("Failed to notify admin %s: %s", admin_id, e)


# ===================== BOT STATE (in-memory) =====================
# birthdates stored in-memory: user_id -> "DD.MM.YYYY"
BIRTHDATES: Dict[int, str] = {}


# ===================== HANDLERS =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user

    # try add to sheets (and notify admins)
    if GSHEET_ID and GOOGLE_SA_JSON:
        try:
            created, _rec = ensure_user_in_sheet(user)
            if created:
                await notify_admins_new_user(context, user)
        except Exception as e:
            logger.exception("ensure_user_in_sheet failed: %s", e)

    access = get_access_level(user.id)
    if access == "blocked":
        await update.message.reply_text(
            "⛔ Доступ ограничен.\n"
            "Trial закончился или доступ отключён.\n"
            "Обратитесь к администратору.",
            parse_mode=ParseMode.HTML,
        )
        return

    await update.message.reply_text(
        "Введите дату рождения в формате <b>ДД.ММ.ГГГГ</b>\n"
        "Пример: <code>05.03.1994</code>",
        parse_mode=ParseMode.HTML,
    )


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("pong ✅")


async def sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    try:
        created = False
        rec = None
        if GSHEET_ID and GOOGLE_SA_JSON:
            created, rec = ensure_user_in_sheet(user)
            if created:
                await notify_admins_new_user(context, user)
        access = get_access_level(user.id)
        await update.message.reply_text(
            f"✅ sync ok\ncreated={created}\naccess={access}\nrecord={bool(rec)}"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ sync failed: {type(e).__name__}: {e}")


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    access = get_access_level(user.id)
    if access == "blocked":
        await update.message.reply_text(
            "⛔ Доступ ограничен.\n"
            "Trial закончился или доступ отключён.\n"
            "Обратитесь к администратору."
        )
        return

    birth = validate_birth(update.message.text)
    if not birth:
        await update.message.reply_text("❌ Неверный формат. Пример: 05.03.1994")
        return

    # сохраняем дату рождения в памяти (без БД)
    BIRTHDATES[user.id] = birth

    now_dt = datetime.now(TZ)
    ld = calc_personal_day(birth, now_dt)
    text = (
        f"<b>Дата:</b> {now_dt.strftime('%d.%m.%Y')}\n\n"
        f"<b>Личный день:</b> {ld}\n"
        f"{LD_TEXT.get(ld, '')}\n\n"
    )

    if access == "trial":
        text += "⏳ <b>Trial:</b> доступ ограничен (только личный день)."
    else:
        text += "⭐️ <b>Premium:</b> активен."

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ===================== ERROR HANDLER =====================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, Conflict):
        logger.error("409 Conflict: another getUpdates is running. Exiting to let Render restart.")
        os._exit(1)
    logger.exception("Unhandled error: %s", err)


# ===================== MAIN =====================
def main() -> None:
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    app = Application.builder().token(TOKEN).build()

    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("sync", sync))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    logger.info("Bot started")
    # drop_pending_updates помогает после перезапусков/дублирующих апдейтов
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
