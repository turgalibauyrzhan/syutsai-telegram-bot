import os
import json
import base64
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ================= НАСТРОЙКИ =================

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GSHEET_ID = os.environ.get("GSHEET_ID")

ADMIN_IDS = {123456789}  # <-- замени на свой telegram user_id
TRIAL_DAYS = 3
TZ = ZoneInfo("Asia/Almaty")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================= GOOGLE SHEETS =================

def load_sa_info() -> dict:
    raw = os.environ.get("GOOGLE_SA_JSON", "")
    if not raw:
        raise ValueError("GOOGLE_SA_JSON empty")

    raw = raw.strip()

    # 1) base64
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        if decoded.strip().startswith("{"):
            return json.loads(decoded)
    except Exception:
        pass

    # 2) plain json
    raw = raw.replace("\\n", "\n")
    return json.loads(raw)


def get_sheet():
    info = load_sa_info()
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GSHEET_ID)
    return sh.worksheet("subscriptions")

# ================= USERS =================

def ensure_user(user):
    try:
        ws = get_sheet()
        rows = ws.get_all_records()
    except Exception as e:
        logger.warning("GS unavailable: %s", e)
        return {
            "status": "trial",
            "access_until": (datetime.now(TZ) + timedelta(days=TRIAL_DAYS)).date().isoformat(),
        }

    for r in rows:
        if str(r.get("telegram_user_id")) == str(user.id):
            return r

    now = datetime.now(TZ)
    until = now + timedelta(days=TRIAL_DAYS)

    ws.append_row([
        user.id,
        "trial",
        "basic",
        until.date().isoformat(),
        now.strftime("%Y-%m-%d %H:%M:%S"),
        user.username or "",
        user.first_name or "",
        user.last_name or "",
    ])

    return {
        "telegram_user_id": user.id,
        "status": "trial",
        "access_until": until.date().isoformat(),
    }


def has_access(user):
    data = ensure_user(user)
    status = data.get("status", "trial")

    if status == "premium":
        return True
    if status == "blocked":
        return False

    until = data.get("access_until")
    if not until:
        return False

    if datetime.now(TZ).date() <= datetime.fromisoformat(until).date():
        return True

    try:
        ws = get_sheet()
        cells = ws.findall(str(user.id))
        for c in cells:
            ws.update_cell(c.row, 2, "blocked")
    except Exception:
        pass

    return False

# ================= НУМЕРОЛОГИЯ =================

LD_TEXT = {
    1: "День инициативы и начала.",
    2: "День взаимодействия и дипломатии.",
    3: "День общения и творчества.",
    4: "День порядка и дисциплины.",
    5: "День перемен.",
    6: "День семьи и ответственности.",
    7: "День анализа и уединения.",
    8: "День силы и денег.",
    9: "День завершений.",
}

def digit_sum(n: int) -> int:
    while n > 9:
        n = sum(map(int, str(n)))
    return n

def personal_day(birth: str) -> int:
    d, m, y = map(int, birth.split("."))
    today = datetime.now(TZ)
    return digit_sum(d + m + sum(map(int, str(today.year))) + today.month + today.day)

# ================= HANDLERS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update.effective_user)
    await update.message.reply_text(
        "👋 Введите дату рождения в формате ДД.ММ.ГГГГ\n"
        "Пример: 05.03.1994"
    )

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not has_access(user):
        await update.message.reply_text(
            "⛔ Доступ ограничен. Trial закончился.\n"
            "Обратитесь к администратору."
        )
        return

    text = update.message.text.strip()

    if len(text) != 10 or text[2] != "." or text[5] != ".":
        await update.message.reply_text("❌ Формат: ДД.ММ.ГГГГ")
        return

    ld = personal_day(text)
    await update.message.reply_text(
        f"🔢 *Личный день: {ld}*\n\n{LD_TEXT[ld]}",
        parse_mode="Markdown"
    )

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅")

async def sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = ensure_user(update.effective_user)
    await update.message.reply_text(f"OK. Статус: {data.get('status')}")

# ================= MAIN =================

def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("sync", sync))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("Bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
