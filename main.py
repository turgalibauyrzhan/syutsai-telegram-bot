import os
import json
import base64
import logging
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, Optional, Any, List

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

# ================= CONFIG =================

TZ = ZoneInfo("Asia/Almaty")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "3"))

SHEET_NAME = os.getenv("SHEET_NAME", "subscriptions")
TEXTS_PATH = os.getenv("TEXTS_JSON_PATH", "texts.json")

ADMIN_CHAT_IDS = {
    int(x) for x in os.getenv("ADMIN_CHAT_IDS", "").split(",") if x.strip().isdigit()
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("syucai")

# ==== Sheet schema you requested ====
HEADERS = [
    "telegram_user_id",
    "status",
    "plan",
    "trial_expires",
    "birth_date",
    "created_at",
    "last_seen_at",
    "username",
    "first_name",
    "last_name",
    "registered_on",
    "last_full_ym",
]

# =============== FALLBACK TEXTS (если texts.json не прочитается) ===============

FALLBACK_TEXTS = {
    "unfavorable_days": [10, 20, 30],
    "unfavorable_text": (
        "Нежелательно начинать новые проекты и события.\n"
        "Есть высокая вероятность обнуления всех результатов ваших действий.\n"
        "Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и т.д."
    ),
    "general_day": {
        "1": "День перезапуска и обнуления. Важно не спешить с новыми решениями.",
        "2": "День взаимодействия, чувствительности и дипломатии.",
        "3": "День анализа и успеха.",
        "4": "День мистических событий: важно быть в позитиве.",
        "5": "День перемен и движения.",
        "6": "День любви и гармонии.",
        "7": "День анализа, тишины и глубины.",
        "8": "День ресурсов и денег.",
        "9": "День завершений и подведения итогов.",
    },
    "personal_day_full": {
        "1": "День инициативы. Хорошо начинать новые дела.",
        "2": "День отношений. Важно проявлять мягкость и слышать других.",
        "3": "День общения и творчества. Легко договариваться и проявляться.",
        "4": "День мистических событий, как положительных, так и отрицательных... Визуализируйте цели.",
        "5": "День перемен, движения и гибкости. Хорошо менять подход и пробовать новое.",
        "6": "День любви, семьи и ответственности. Благоприятно заботиться о близких.",
        "7": "День анализа, тишины, фокуса и глубины. Хорошо учиться и планировать.",
        "8": "День ресурсов, денег и управления. Хорошо решать финансовые и рабочие вопросы.",
        "9": "День завершений и итогов. Закрывайте хвосты, подводите результаты.",
    },
    "personal_year_full": {
        "1": "Год начала нового цикла. Формирование направления и целей.",
        "2": "Год отношений, дипломатии и партнёрства.",
        "3": "Год анализа и успеха. Важно действовать осознанно.",
        "4": "Год мистических событий и внутренних трансформаций.",
        "5": "Год перемен, движения и свободы.",
        "6": "Год любви, семьи и ответственности.",
        "7": "Год глубины, обучения и внутреннего роста.",
        "8": "Год денег, управления и карьеры.",
        "9": "Год завершений и подведения итогов.",
    },
    "personal_year_short": {
        "1": "Начало нового цикла.",
        "2": "Год отношений.",
        "3": "Год анализа и успеха.",
        "4": "Год трансформаций.",
        "5": "Год перемен.",
        "6": "Год семьи и любви.",
        "7": "Год глубины.",
        "8": "Год денег и управления.",
        "9": "Год завершений.",
    },
    "personal_month_full": {
        "1": "Месяц стартов и инициатив.",
        "2": "Месяц отношений и взаимодействия.",
        "3": "Месяц общения и самовыражения.",
        "4": "Месяц мистических процессов и внимательности к знакам.",
        "5": "Месяц движения и изменений.",
        "6": "Месяц семьи и заботы.",
        "7": "Месяц анализа и тишины.",
        "8": "Месяц ресурсов и финансов.",
        "9": "Месяц завершений.",
    },
    "personal_month_short": {
        "1": "Месяц стартов.",
        "2": "Месяц отношений.",
        "3": "Месяц общения.",
        "4": "Месяц мистики.",
        "5": "Месяц движения.",
        "6": "Месяц семьи.",
        "7": "Месяц анализа.",
        "8": "Месяц ресурсов.",
        "9": "Месяц завершений.",
    },
}


def load_texts() -> dict:
    try:
        with open(TEXTS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        for k in ["general_day", "personal_day_full", "personal_year_full", "personal_month_full"]:
            if k not in data:
                raise ValueError(f"Missing key in texts.json: {k}")
        return data
    except Exception as e:
        logger.warning("texts.json load failed, using fallback. Error=%s", e)
        return FALLBACK_TEXTS


TEXTS = load_texts()


def _int_key_map(d: dict) -> dict:
    return {int(k): v for k, v in d.items()}


UNFAVORABLE_DAYS = set(TEXTS.get("unfavorable_days", [10, 20, 30]))
UNFAVORABLE_TEXT = TEXTS.get("unfavorable_text", FALLBACK_TEXTS["unfavorable_text"])

GENERAL_DAY = _int_key_map(TEXTS["general_day"])
PERSONAL_DAY_FULL = _int_key_map(TEXTS["personal_day_full"])
PERSONAL_YEAR_FULL = _int_key_map(TEXTS["personal_year_full"])
PERSONAL_YEAR_SHORT = _int_key_map(TEXTS.get("personal_year_short", {})) or {
    k: v.split(".")[0] for k, v in PERSONAL_YEAR_FULL.items()
}
PERSONAL_MONTH_FULL = _int_key_map(TEXTS["personal_month_full"])
PERSONAL_MONTH_SHORT = _int_key_map(TEXTS.get("personal_month_short", {})) or {
    k: v.split(".")[0] for k, v in PERSONAL_MONTH_FULL.items()
}

# ================= CALC =================

def now_iso() -> str:
    return datetime.now(TZ).replace(microsecond=0).isoformat()

def today_ym() -> str:
    d = date.today()
    return f"{d.year:04d}-{d.month:02d}"

def reduce_digit(n: int) -> int:
    while n > 9:
        n = sum(int(c) for c in str(n))
    return n

def parse_birth(s: str) -> Optional[str]:
    try:
        dt = datetime.strptime(s.strip(), "%d.%m.%Y")
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return None

def calc_general_day(d: date) -> int:
    return reduce_digit(sum(int(c) for c in f"{d.day:02d}{d.month:02d}{d.year}"))

def calc_personal_year(birth_ddmmyyyy: str, year: int) -> int:
    d, m, _ = map(int, birth_ddmmyyyy.split("."))
    return reduce_digit(reduce_digit(d) + reduce_digit(m) + reduce_digit(year))

def calc_personal_month(py: int, month: int) -> int:
    return reduce_digit(py + reduce_digit(month))

def calc_personal_day(pm: int, day: int) -> int:
    return reduce_digit(pm + reduce_digit(day))

# ================= GOOGLE SHEETS =================

def _sa_json_raw() -> str:
    if not GOOGLE_SA_JSON:
        raise ValueError("GOOGLE_SA_JSON is not set")
    raw = GOOGLE_SA_JSON.strip()
    # base64 or direct JSON
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        if decoded.lstrip().startswith("{"):
            return decoded
    except Exception:
        pass
    return raw

def gs_ws():
    if not GSHEET_ID:
        raise ValueError("GSHEET_ID is not set")
    raw = _sa_json_raw()
    creds = Credentials.from_service_account_info(
        json.loads(raw),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(GSHEET_ID)
    return sh.worksheet(SHEET_NAME)

def ensure_sheet_schema(ws) -> Dict[str, int]:
    """
    Ensures header row exists and matches required HEADERS.
    Returns mapping header->col_index (1-based).
    """
    header_row = ws.row_values(1)
    if not header_row:
        ws.insert_row(HEADERS, 1)
        header_row = HEADERS

    # If headers differ, we won't destructively rewrite; we only ensure required headers exist in some order.
    # But easiest/cleanest: enforce exact order if the row is empty or obviously wrong length.
    if header_row != HEADERS:
        # If it already contains all required headers, keep as is.
        if all(h in header_row for h in HEADERS):
            pass
        else:
            # Hard reset header to required schema
            ws.update("A1", [HEADERS])
            header_row = HEADERS

    return {h: (header_row.index(h) + 1) for h in header_row}

def find_user_row(ws, header_map: Dict[str, int], user_id: int) -> Optional[int]:
    # naive scan: ok for small/moderate sheets
    uid_col = header_map["telegram_user_id"]
    col_values = ws.col_values(uid_col)[1:]  # skip header
    uid_str = str(user_id)
    for idx, v in enumerate(col_values, start=2):
        if str(v).strip() == uid_str:
            return idx
    return None

def read_user_record(ws, header_map: Dict[str, int], row: int) -> Dict[str, Any]:
    vals = ws.row_values(row)
    rec = {}
    for h, col in header_map.items():
        rec[h] = vals[col-1] if col-1 < len(vals) else ""
    return rec

def update_row_fields(ws, header_map: Dict[str, int], row: int, fields: Dict[str, Any]) -> None:
    """
    Batch update specific cells in the row using header_map.
    """
    updates: List[tuple[int, int, Any]] = []
    for k, v in fields.items():
        if k not in header_map:
            continue
        updates.append((row, header_map[k], "" if v is None else str(v)))

    if not updates:
        return

    # Build A1 ranges per cell; use batch_update for fewer requests
    data = []
    for r, c, value in updates:
        a1 = gspread.utils.rowcol_to_a1(r, c)
        data.append({"range": a1, "values": [[value]]})

    ws.batch_update(data)

def ensure_user(update: Update) -> Tuple[Dict[str, Any], int, bool]:
    """
    Ensure user exists in sheet. Returns (record, row, is_new).
    Also updates last_seen_at + profile fields.
    """
    ws = gs_ws()
    header_map = ensure_sheet_schema(ws)

    user = update.effective_user
    user_id = user.id
    row = find_user_row(ws, header_map, user_id)
    is_new = False

    now = now_iso()
    username = user.username or ""
    first_name = user.first_name or ""
    last_name = user.last_name or ""

    if row is None:
        is_new = True
        created = now
        registered_on = date.today().isoformat()
        trial_expires = (date.today() + timedelta(days=TRIAL_DAYS)).isoformat()

        new_row_dict = {
            "telegram_user_id": str(user_id),
            "status": "active",
            "plan": "trial",
            "trial_expires": trial_expires,
            "birth_date": "",
            "created_at": created,
            "last_seen_at": now,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
            "registered_on": registered_on,
            "last_full_ym": "",
        }

        # append row in exact header order
        ws.append_row([new_row_dict.get(h, "") for h in HEADERS])
        # find inserted row
        row = find_user_row(ws, header_map, user_id)
        if row is None:
            # very rare; fallback assume last row
            row = ws.row_count

        rec = new_row_dict

    else:
        rec = read_user_record(ws, header_map, row)
        # update last_seen_at and profile fields every time
        update_row_fields(ws, header_map, row, {
            "last_seen_at": now,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
        })
        rec.update({
            "last_seen_at": now,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
        })

    return rec, row, is_new

def access_level(rec: dict) -> str:
    if rec.get("status") != "active":
        return "blocked"
    plan = rec.get("plan", "blocked")
    if plan == "premium":
        return "premium"
    if plan == "trial":
        exp = rec.get("trial_expires")
        if not exp:
            return "blocked"
        try:
            if date.today() > date.fromisoformat(exp):
                return "blocked"
        except Exception:
            return "blocked"
        return "trial"
    return "blocked"

def is_first_day(rec: dict, today: date) -> bool:
    ro = (rec.get("registered_on") or "").strip()
    if not ro:
        return False
    try:
        return date.fromisoformat(ro) == today
    except Exception:
        return False

# ================= MESSAGE BUILD =================

def build_forecast_message(rec: dict, birth: str, today: date, first_day: bool) -> str:
    py = calc_personal_year(birth, today.year)
    pm = calc_personal_month(py, today.month)
    ld = calc_personal_day(pm, today.day)

    parts: list[str] = [f"📅 Дата: {today.strftime('%d.%m.%Y')}"]

    # priority: 10/20/30 message
    if today.day in UNFAVORABLE_DAYS:
        parts.append(f"\n⚠️ {UNFAVORABLE_TEXT}")
    else:
        od = calc_general_day(today)
        parts.append(f"\n🌐 Общий день: {od}\n{GENERAL_DAY.get(od, '')}")

    # LG/LM: FULL only first day, else SHORT (как ты зафиксировал)
    if first_day:
        parts.append(f"\n🗓 Личный год {py}.\n{PERSONAL_YEAR_FULL.get(py, '')}")
        parts.append(f"\n🗓 Личный месяц {pm}.\n{PERSONAL_MONTH_FULL.get(pm, '')}")
    else:
        parts.append(f"\n🗓 Личный год {py}. {PERSONAL_YEAR_SHORT.get(py, '')}")
        parts.append(f"🗓 Личный месяц {pm}. {PERSONAL_MONTH_SHORT.get(pm, '')}")

    # LD: ALWAYS FULL
    parts.append(f"\n🔢 Личный день {ld}.\n{PERSONAL_DAY_FULL.get(ld, '')}")

    level = access_level(rec)
    if level == "trial":
        exp = rec.get("trial_expires", "")
        parts.append(f"\n🧪 Trial активен до: {exp}")
    elif level == "premium":
        parts.append("\n⭐️ Premium активен.")

    return "\n".join(parts).strip()

# ================= HANDLERS =================

async def notify_admins_new_user(app: Application, rec: dict):
    if not ADMIN_CHAT_IDS:
        return
    txt = (
        "🆕 Новый пользователь\n"
        f"telegram_user_id: {rec.get('telegram_user_id')}\n"
        f"username: @{rec.get('username')}\n"
        f"name: {rec.get('first_name')} {rec.get('last_name')}\n"
        f"plan: {rec.get('plan')}, trial_expires: {rec.get('trial_expires')}"
    )
    for admin_id in ADMIN_CHAT_IDS:
        try:
            await app.bot.send_message(chat_id=admin_id, text=txt)
        except Exception as e:
            logger.warning("Admin notify failed: %s", e)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    rec, row, is_new = ensure_user(update)
    if is_new:
        await notify_admins_new_user(context.application, rec)

    level = access_level(rec)
    if level == "blocked":
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору."
        )
        return

    birth = (rec.get("birth_date") or "").strip()
    if not birth:
        await update.message.reply_text(
            "Введите дату рождения в формате ДД.ММ.ГГГГ\nПример: 05.03.1994"
        )
        return

    today = date.today()
    first_day = is_first_day(rec, today)

    msg = build_forecast_message(rec, birth, today, first_day)

    # if FULL was shown today (first day), stamp last_full_ym
    if first_day:
        try:
            ws = gs_ws()
            header_map = ensure_sheet_schema(ws)
            user_row = find_user_row(ws, header_map, int(rec["telegram_user_id"]))
            if user_row:
                update_row_fields(ws, header_map, user_row, {"last_full_ym": today_ym()})
        except Exception as e:
            logger.warning("Failed to update last_full_ym: %s", e)

    await update.message.reply_text(msg)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    text = update.message.text.strip()
    birth = parse_birth(text)
    if not birth:
        await update.message.reply_text("Неверный формат. Введите дату рождения: ДД.ММ.ГГГГ")
        return

    rec, row, is_new = ensure_user(update)
    if is_new:
        await notify_admins_new_user(context.application, rec)

    # save birth_date
    try:
        ws = gs_ws()
        header_map = ensure_sheet_schema(ws)
        user_row = find_user_row(ws, header_map, update.effective_user.id)
        if not user_row:
            await update.message.reply_text("❌ Не смог найти вашу строку в таблице. Проверь доступ к Google Sheets.")
            return
        update_row_fields(ws, header_map, user_row, {"birth_date": birth})
        rec["birth_date"] = birth
    except Exception as e:
        logger.exception("Failed to save birth_date to Sheets: %s", e)
        await update.message.reply_text("❌ Не смог сохранить дату рождения. Проверь доступ к Google Sheets.")
        return

    # IMPORTANT: send ONE message (no calling /start again)
    level = access_level(rec)
    if level == "blocked":
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору."
        )
        return

    today = date.today()
    first_day = is_first_day(rec, today)
    msg = build_forecast_message(rec, birth, today, first_day)

    if first_day:
        try:
            ws = gs_ws()
            header_map = ensure_sheet_schema(ws)
            user_row = find_user_row(ws, header_map, update.effective_user.id)
            if user_row:
                update_row_fields(ws, header_map, user_row, {"last_full_ym": today_ym()})
        except Exception as e:
            logger.warning("Failed to update last_full_ym: %s", e)

    await update.message.reply_text(msg)

# ================= MAIN =================

def main():
    if not TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is not set")
    if not GSHEET_ID:
        raise ValueError("GSHEET_ID is not set")
    if not GOOGLE_SA_JSON:
        raise ValueError("GOOGLE_SA_JSON is not set")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # Helps prevent old queued updates after redeploy
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
