import os
import json
import base64
import logging
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

# ================= CONFIG =================

TZ = ZoneInfo("Asia/Almaty")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")
TRIAL_DAYS = 3

SHEET_NAME = "subscriptions"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("syucai")

# ================= TEXTS =================

UNFAVORABLE_DAYS = [10, 20, 30]
UNFAVORABLE_TEXT = (
    "Нежелательно начинать новые проекты и события.\n"
    "Есть высокая вероятность обнуления всех результатов ваших действий.\n"
    "Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и т.д."
)

GENERAL_DAY = {
    1: "День перезапуска и обнуления. Важно не спешить с новыми решениями.",
    2: "День взаимодействия, чувствительности и дипломатии.",
    3: "День анализа и успеха.",
    4: "День мистических событий, важно быть в позитиве.",
    5: "День перемен и движения.",
    6: "День любви и гармонии.",
    7: "День анализа, тишины и глубины.",
    8: "День ресурсов и денег.",
    9: "День завершений и подведения итогов.",
}

PERSONAL_DAY_FULL = {
    1: "День инициативы. Хорошо начинать новые дела.",
    2: "День отношений. Важно проявлять мягкость.",
    3: "День общения и творчества.",
    4: (
        "День мистических событий, как положительных, так и отрицательных. "
        "Важно сохранять позитивное мышление. "
        "Посвяти день целям и мечтам, визуализируй их."
    ),
    5: "День перемен и гибкости.",
    6: "День любви, семьи и ответственности.",
    7: "День анализа, тишины и фокуса.",
    8: "День ресурсов, денег и управления.",
    9: "День завершений и подведения итогов.",
}

PERSONAL_YEAR_FULL = {
    1: "Год начала нового цикла. Формирование направления жизни.",
    2: "Год отношений, дипломатии и партнёрства.",
    3: "Год анализа и успеха. Важно действовать осознанно.",
    4: "Год мистических событий и внутренних трансформаций.",
    5: "Год перемен, движения и свободы.",
    6: "Год любви, семьи и ответственности.",
    7: "Год глубины, обучения и внутреннего роста.",
    8: "Год денег, управления и карьеры.",
    9: "Год завершений и подведения итогов.",
}

PERSONAL_MONTH_FULL = {
    1: "Месяц стартов и инициатив.",
    2: "Месяц отношений и взаимодействия.",
    3: "Месяц общения и самовыражения.",
    4: "Месяц мистических процессов.",
    5: "Месяц движения и изменений.",
    6: "Месяц семьи и заботы.",
    7: "Месяц анализа и тишины.",
    8: "Месяц ресурсов и финансов.",
    9: "Месяц завершений.",
}

# ================= CALC =================

def reduce_digit(n: int) -> int:
    while n > 9:
        n = sum(map(int, str(n)))
    return n

def calc_general_day(d: date):
    return reduce_digit(sum(map(int, f"{d.day:02d}{d.month:02d}{d.year}")))

def calc_personal_year(birth: str, year: int):
    d, m, _ = map(int, birth.split("."))
    return reduce_digit(reduce_digit(d) + reduce_digit(m) + reduce_digit(year))

def calc_personal_month(py: int, m: int):
    return reduce_digit(py + reduce_digit(m))

def calc_personal_day(pm: int, d: int):
    return reduce_digit(pm + reduce_digit(d))

# ================= GOOGLE SHEETS =================

def gs_ws():
    raw = GOOGLE_SA_JSON
    try:
        raw = base64.b64decode(raw).decode()
    except:
        pass
    creds = Credentials.from_service_account_info(
        json.loads(raw),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds).open_by_key(GSHEET_ID).worksheet(SHEET_NAME)

def ensure_user(user):
    ws = gs_ws()
    rows = ws.get_all_records()
    for i, r in enumerate(rows, start=2):
        if str(r["telegram_user_id"]) == str(user.id):
            return r, i

    ws.append_row([
        user.id,
        "active",
        "trial",
        (date.today() + timedelta(days=TRIAL_DAYS)).isoformat(),
        "",
        date.today().isoformat(),  # registered_on
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

# ================= MESSAGE =================

def build_message(rec, birth, today):
    first_day = rec["registered_on"] == today.isoformat()

    py = calc_personal_year(birth, today.year)
    pm = calc_personal_month(py, today.month)
    ld = calc_personal_day(pm, today.day)

    parts = [f"📅 Дата: {today.strftime('%d.%m.%Y')}"]

    if today.day in UNFAVORABLE_DAYS:
        parts.append(f"\n⚠️ {UNFAVORABLE_TEXT}")
    else:
        od = calc_general_day(today)
        parts.append(f"\n🌐 Общий день: {od}\n{GENERAL_DAY[od]}")

    # Личный год
    parts.append(f"\n🗓 Личный год {py}.")
    parts.append(PERSONAL_YEAR_FULL[py] if first_day else PERSONAL_YEAR_FULL[py].split(".")[0])

    # Личный месяц
    parts.append(f"\n🗓 Личный месяц {pm}.")
    parts.append(PERSONAL_MONTH_FULL[pm] if first_day else PERSONAL_MONTH_FULL[pm].split(".")[0])

    # Личный день — всегда расширенно
    parts.append(f"\n🔢 Личный день {ld}.")
    parts.append(PERSONAL_DAY_FULL[ld])

    return "\n".join(parts)

# ================= HANDLERS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    rec, row = ensure_user(user)
    ws = gs_ws()
    if not rec:
        rec = ws.get_all_records()[-1]

    if not rec["birth_date"]:
        await update.message.reply_text("Введите дату рождения (ДД.ММ.ГГГГ)")
        return

    if access_level(rec) == "blocked":
        await update.message.reply_text("⛔️ Доступ ограничен.")
        return

    msg = build_message(rec, rec["birth_date"], date.today())
    await update.message.reply_text(msg)

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        birth = datetime.strptime(update.message.text, "%d.%m.%Y").strftime("%d.%m.%Y")
    except:
        await update.message.reply_text("Неверный формат даты.")
        return

    ws = gs_ws()
    rec, row = ensure_user(user)
    ws.update_cell(row, 5, birth)
    await start(update, context)

# ================= MAIN =================

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
