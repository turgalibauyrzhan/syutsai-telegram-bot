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

# ================== НАСТРОЙКИ ==================

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GSHEET_ID = os.environ.get("GSHEET_ID")

ADMIN_IDS = {123456789}  # ← сюда свой telegram user_id
TZ = ZoneInfo("Asia/Almaty")

TRIAL_DAYS = 3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================== GOOGLE SHEETS ==================

def load_sa_info() -> dict:
    raw = os.environ.get("GOOGLE_SA_JSON", "")
    if not raw:
        raise ValueError("GOOGLE_SA_JSON empty")

    raw = raw.strip()

    # base64 first
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        if decoded.strip().startswith("{"):
            return json.loads(decoded)
    except Exception:
        pass

    raw = raw.replace("\\n", "\n")
    return json.loads(raw)


def gs_client():
    info = load_sa_info()
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def get_sheet():
    gc = gs_client()
    sh = gc.open_by_key(GSHEET_ID)
    return sh.worksheet("subscriptions")


# ================== USERS ==================

def ensure_user(user):
    try:
        ws = get_sheet()
    except Exception as e:
        logger.warning("GS unavailable: %s", e)
        return {"status": "trial", "access_until": datetime.now(TZ) + timedelta(days=TRIAL_DAYS)}

    rows = ws.get_all_records()
    for r in rows:
        if str(r.get("telegram_user_id")) == str(user.id):
            return r

    now = datetime.now(TZ)
    access_until = now + timedelta(days=TRIAL_DAYS)

    ws.append_row([
        user.id,
        "trial",
        "basic",
        access_until.strftime("%Y-%m-%d"),
        now.strftime("%Y-%m-%d %H:%M:%S"),
        user.username or "",
        user.first_name or "",
        user.last_name or "",
    ])

    for admin in ADMIN_IDS:
        try:
            app.bot.send_message(
                admin,
                f"👤 Новый пользователь\nID: {user.id}\nUsername: @{user.username}"
            )
        except Exception:
            pass

    return {
        "telegram_user_id": user.id,
        "status": "trial",
        "access_until": access_until.strftime("%Y-%m-%d"),
    }


def check_access(user):
    data = ensure_user(user)
    status = data.get("status", "trial")

    if status == "premium":
        return True

    if status == "blocked":
        return False

    if status == "trial":
        until = data.get("access_until")
        if not until:
            return False
        if datetime.now(TZ).date() <= datetime.fromisoformat(until).date():
            return True
        else:
            block_user(user)
            return False

    return False


def block_user(user):
    try:
        ws = get_sheet()
        cells = ws.findall(str(user.id))
        for c in cells:
            ws.update_cell(c.row, 2, "blocked")
    except Exception:
        pass


# ================== НУМЕРОЛОГИЯ ==================

LD_TEXT = {
    1: "День инициативы. Хорошо начинать новое.",
    2: "День взаимодействия и дипломатии.",
    3: "День общения и творчества.",
    4: "День порядка и дисциплины.",
    5: "День перемен и движения.",
    6: "День семьи и ответственности.",
    7: "День анализа и уединения.",
    8: "День силы и денег.",
    9: "День завершений и выводов.",
}


def digit_sum(n: int) -> int:
    while n > 9:
        n = sum(map(int, str(n)))
    return n


def personal_day(birth: str) -> int:
    d, m, y = map(int, birth.split("."))
    today = datetime.now(TZ)
    total = d + m + sum(map(int, str(today.year))) + today.month + today.day
    return digit_sum(total)


# ================== HANDLERS ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user)

    await update.message.reply_text(
        "👋 Введите дату рождения в формате ДД.ММ.ГГГГ\n"
        "Пример: 05.03.1994"
    )


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not check_access(user):
        await update.message.reply_text(
            "⛔ Доступ ограничен.\n"
            "Ваш trial закончился. Обратитесь к администратору."
        )
        return

    text = update.message.text.strip()
    if len(text) != 10 or text[2] != "." or text[5] != ".":
        await update.message.reply_text("❌ Неверный формат. Используйте ДД.ММ.ГГГГ")
        return

    try:
        ld = personal_day(text)
    except Exception:
        await update.message.reply_text("❌ Ошибка расчёта даты")
        return

    await update.message.reply_text(
        f"🔢 *Личный день: {ld}*\n\n{LD_TEXT[ld]}",
        parse_mode="Markdown"
    )


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅")


async def sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    data = ensure_user(user)
    await update.message.reply_text(f"OK. Статус: {data.get('status')}")


# ================== MAIN ==================

def main():
    global app

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
