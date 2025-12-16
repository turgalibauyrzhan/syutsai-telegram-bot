# =========================
# SYUCAI TELEGRAM BOT
# Final stable version
# =========================

import os
import json
import base64
import logging
import asyncio
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

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

# =========================
# CONFIG
# =========================

TZ = ZoneInfo("Asia/Almaty")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")
TRIAL_DAYS = 3

ADMIN_CHAT_IDS = {
    int(x) for x in os.getenv("ADMIN_CHAT_IDS", "").split(",") if x.strip().isdigit()
}

SHEET_NAME = "subscriptions"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("syucai")

# =========================
# NUMEROLOGY TEXTS (DOC)
# =========================

UNFAVORABLE_DAYS = [10, 20, 30]
UNFAVORABLE_TEXT = (
    "Нежелательно начинать новые проекты и события.\n"
    "Есть высокая вероятность обнуления всех результатов ваших действий.\n"
    "Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и т.д."
)

GENERAL_DAY = {
    1: "День перезапуска и обнуления. Важно не спешить с новыми решениями.",
    2: "День взаимодействия, чувствительности и дипломатии.",
    3: "Благоприятный день анализа и успеха.",
    4: "День мистических событий. Важно быть в позитиве.",
    5: "День перемен и движения.",
    6: "Благоприятный день любви и гармонии.",
    7: "День анализа, тишины и глубины.",
    8: "День ресурсов, денег и управления.",
    9: "День завершений и подведения итогов.",
}

PERSONAL_DAY = {
    1: "Инициатива и старт.",
    2: "Отношения и мягкость.",
    3: "Общение и творчество.",
    4: "Мистические события, визуализация целей.",
    5: "Изменения и гибкость.",
    6: "Любовь и ответственность.",
    7: "Анализ и уединение.",
    8: "День ресурсов и денег.",
    9: "Завершения и итоги.",
}

PERSONAL_YEAR_SHORT = {
    1: "Начало нового цикла.",
    2: "Год отношений.",
    3: "Год анализа и успеха.",
    4: "Год внутренних трансформаций.",
    5: "Год перемен.",
    6: "Год семьи и любви.",
    7: "Год глубины.",
    8: "Год денег и управления.",
    9: "Год завершений.",
}

PERSONAL_MONTH_SHORT = {
    1: "Месяц стартов.",
    2: "Месяц отношений.",
    3: "Месяц общения.",
    4: "Месяц мистики.",
    5: "Месяц движения.",
    6: "Месяц семьи.",
    7: "Месяц анализа.",
    8: "Месяц ресурсов.",
    9: "Месяц завершений.",
}

# =========================
# HELPERS
# =========================

def reduce_digit(n: int) -> int:
    while n > 9:
        n = sum(map(int, str(n)))
    return n

def parse_date(s: str):
    try:
        return datetime.strptime(s, "%d.%m.%Y").date()
    except:
        return None

def calc_general_day(d: date):
    return reduce_digit(sum(map(int, f"{d.day:02d}{d.month:02d}{d.year}")))

def calc_personal_year(birth: str, year: int):
    d, m, _ = map(int, birth.split("."))
    return reduce_digit(reduce_digit(d) + reduce_digit(m) + reduce_digit(year))

def calc_personal_month(py: int, m: int):
    return reduce_digit(py + reduce_digit(m))

def calc_personal_day(pm: int, d: int):
    return reduce_digit(pm + reduce_digit(d))

# =========================
# GOOGLE SHEETS
# =========================

def gs_client():
    raw = GOOGLE_SA_JSON
    try:
        raw = base64.b64decode(raw).decode()
    except:
        pass
    creds = Credentials.from_service_account_info(
        json.loads(raw),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

def get_ws():
    sh = gs_client().open_by_key(GSHEET_ID)
    return sh.worksheet(SHEET_NAME)

def ensure_user(user):
    ws = get_ws()
    users = ws.get_all_records()
    for i, r in enumerate(users, start=2):
        if str(r["telegram_user_id"]) == str(user.id):
            return r, i

    ws.append_row([
        user.id,
        "active",
        "trial",
        (date.today() + timedelta(days=TRIAL_DAYS)).isoformat(),
        "",
        date.today().isoformat()
    ])
    return None, None

def access_level(rec):
    if rec["status"] != "active":
        return "blocked"
    if rec["plan"] == "premium":
        return "premium"
    if rec["plan"] == "trial":
        if date.today() > date.fromisoformat(rec["trial_expires"]):
            return "blocked"
        return "trial"
    return "blocked"

# =========================
# MESSAGE BUILD
# =========================

def build_message(rec, birth, today, first_day):
    py = calc_personal_year(birth, today.year)
    pm = calc_personal_month(py, today.month)
    ld = calc_personal_day(pm, today.day)

    parts = [f"📅 Дата: {today.strftime('%d.%m.%Y')}"]

    if today.day in UNFAVORABLE_DAYS:
        parts.append(f"\n⚠️ {UNFAVORABLE_TEXT}")
    else:
        od = calc_general_day(today)
        parts.append(f"\n🌐 Общий день: {od}\n{GENERAL_DAY[od]}")

    parts.append(f"\n🗓 Личный год {py}. {PERSONAL_YEAR_SHORT[py]}")
    parts.append(f"🗓 Личный месяц {pm}. {PERSONAL_MONTH_SHORT[pm]}")
    parts.append(f"\n🔢 Личный день {ld}. {PERSONAL_DAY[ld]}")

    return "\n".join(parts)

# =========================
# HANDLERS
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    rec, row = ensure_user(user)

    ws = get_ws()
    if not rec:
        rec = ws.get_all_records()[-1]

    if not rec["birth_date"]:
        await update.message.reply_text("Введите дату рождения (ДД.ММ.ГГГГ)")
        return

    level = access_level(rec)
    if level == "blocked":
        await update.message.reply_text("⛔️ Доступ ограничен.")
        return

    today = date.today()
    msg = build_message(rec, rec["birth_date"], today, False)
    await update.message.reply_text(msg)

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    birth = parse_date(update.message.text)
    if not birth:
        await update.message.reply_text("Неверный формат.")
        return

    ws = get_ws()
    rec, row = ensure_user(user)
    ws.update_cell(row, 5, birth.strftime("%d.%m.%Y"))

    await start(update, context)

# =========================
# MAIN
# =========================

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
