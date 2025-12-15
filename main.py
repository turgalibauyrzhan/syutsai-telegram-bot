import os
import json
import base64
import logging
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Tuple, List

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

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


# ===================== CONFIG =====================
TZ = ZoneInfo("Asia/Almaty")
TRIAL_DAYS = 3

TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
GSHEET_ID = (os.environ.get("GSHEET_ID") or "").strip()
GOOGLE_SA_JSON = (os.environ.get("GOOGLE_SA_JSON") or "").strip()

# админы (telegram user_id) через ENV: "123,456"
ADMIN_CHAT_IDS: set[int] = set()
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
logging.getLogger("httpx").setLevel(logging.WARNING)


# ===================== TEXTS =====================
TRIAL_ONLY_MSG = "⏳ *Trial:* доступ ограничен — показываю только *Личный день (ЛД).*"
PREMIUM_ON_MSG = "⭐️ *Premium активен:* полный прогноз доступен."

UNFAVORABLE_TEXT = (
    "⚠️ *Неблагоприятный день.*\n"
    "Сегодня нежелательно начинать новые проекты и события. "
    "Есть высокая вероятность обнуления результатов. "
    "Рекомендуется отложить крупные покупки, договоры, кредиты и т.д."
)

# Полные трактовки (1–9) — можешь заменить на свои тексты
GENERAL_DAY_INTERPRETATIONS = {
    1: "День лидерства и начала.",
    2: "День сотрудничества и баланса.",
    3: "День общения и креатива.",
    4: "День порядка и системности.",
    5: "День перемен и гибкости.",
    6: "День ответственности и заботы.",
    7: "День анализа и глубины.",
    8: "День ресурсов, власти и денег.",
    9: "День завершений и итогов.",
}

PERSONAL_YEAR_INTERPRETATIONS = {
    1: "Личный год 1 — старт нового цикла, инициативы, новые проекты.",
    2: "Личный год 2 — партнёрства, терпение, согласование.",
    3: "Личный год 3 — публичность, творчество, коммуникации.",
    4: "Личный год 4 — фундамент, дисциплина, системная работа.",
    5: "Личный год 5 — изменения, движение, адаптация.",
    6: "Личный год 6 — семья/ответственность, укрепление позиций.",
    7: "Личный год 7 — обучение, анализ, углубление.",
    8: "Личный год 8 — деньги/карьера, управление ресурсами.",
    9: "Личный год 9 — завершение, чистка, закрытие циклов.",
}

PERSONAL_MONTH_INTERPRETATIONS = {
    1: "Личный месяц 1 — инициатива, запуски.",
    2: "Личный месяц 2 — переговоры, мягкое продвижение.",
    3: "Личный месяц 3 — активная коммуникация, креатив.",
    4: "Личный месяц 4 — порядок, дедлайны, структура.",
    5: "Личный месяц 5 — изменения, поездки, эксперименты.",
    6: "Личный месяц 6 — забота, отношения, ответственность.",
    7: "Личный месяц 7 — анализ, обучение, спокойный темп.",
    8: "Личный месяц 8 — амбиции, деньги, управление.",
    9: "Личный месяц 9 — завершения, итоги, освобождение.",
}

PERSONAL_DAY_INTERPRETATIONS = {
    1: "Личный день 1 — действуй первым, начинай.",
    2: "Личный день 2 — договаривайся, слушай.",
    3: "Личный день 3 — общайся, проявляйся.",
    4: "Личный день 4 — дисциплина, рутина, порядок.",
    5: "Личный день 5 — гибкость, движение, перемены.",
    6: "Личный день 6 — забота, дом, ответственность.",
    7: "Личный день 7 — анализ, тишина, фокус.",
    8: "Личный день 8 — деньги/ресурсы, твёрдые решения.",
    9: "Личный день 9 — завершай, закрывай хвосты.",
}


# ===================== NUMEROLOGY (rules) =====================
def reduce_to_digit(n: int) -> int:
    while n > 9:
        n = sum(int(c) for c in str(n))
    return n


def sum_digits_of_int(n: int) -> int:
    return sum(int(c) for c in str(n))


def calc_general_day(dt: date) -> int:
    # ОД = сумма цифр даты (ДДММГГГГ) -> 1..9
    s = sum_digits_of_int(dt.day) + sum_digits_of_int(dt.month) + sum_digits_of_int(dt.year)
    return reduce_to_digit(s)


def calc_personal_year(birth_ddmmyyyy: str, current_year: int) -> int:
    d, m, _y = map(int, birth_ddmmyyyy.split("."))
    s = sum_digits_of_int(d) + sum_digits_of_int(m) + sum_digits_of_int(current_year)
    return reduce_to_digit(s)


def calc_personal_month(personal_year: int, current_month: int) -> int:
    # по твоему примеру: месяц сначала приводим к цифре (12 -> 3), затем складываем с ЛГ
    month_digit = reduce_to_digit(sum_digits_of_int(current_month))
    return reduce_to_digit(personal_year + month_digit)


def calc_personal_day(personal_month: int, current_day: int) -> int:
    day_digit = reduce_to_digit(sum_digits_of_int(current_day))
    return reduce_to_digit(personal_month + day_digit)


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
SHEET_NAME = "subscriptions"

# Рекомендуемые заголовки (можно расширять)
HEADERS = [
    "telegram_user_id",
    "status",        # active / inactive
    "plan",          # trial / premium
    "access_until",  # YYYY-MM-DD (для trial)
    "created_at",
    "username",
    "first_name",
    "last_name",
    "birth_date",    # DD.MM.YYYY
    "last_seen_at",
]


def load_sa_info() -> dict:
    if not GOOGLE_SA_JSON:
        raise ValueError("GOOGLE_SA_JSON env is empty")

    raw = GOOGLE_SA_JSON.strip()

    # base64 first
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        if decoded.strip().startswith("{"):
            return json.loads(decoded)
    except Exception:
        pass

    # plain JSON
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
    return sh.worksheet(SHEET_NAME)


def ensure_headers(ws: gspread.Worksheet) -> None:
    row1 = ws.row_values(1)
    if row1:
        return
    ws.append_row(HEADERS, value_input_option="USER_ENTERED")


def find_user_row(ws: gspread.Worksheet, user_id: int) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    records = ws.get_all_records()
    for i, r in enumerate(records, start=2):  # row1 headers
        rid = str(r.get("telegram_user_id", "")).strip()
        if rid.isdigit() and int(rid) == user_id:
            return i, r
    return None, None


def parse_iso_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def sheet_safe_get_user(user_id: int) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    ws = gs_open_ws()
    ensure_headers(ws)
    return find_user_row(ws, user_id)


def sheet_safe_update_cell(row: int, col: int, value: Any) -> None:
    ws = gs_open_ws()
    ws.update_cell(row, col, value)


def ensure_user_in_sheet(user) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Creates user if not exists:
      status=active, plan=trial, access_until=today+TRIAL_DAYS, birth_date empty
    Returns (created, record)
    """
    ws = gs_open_ws()
    ensure_headers(ws)

    row_idx, rec = find_user_row(ws, user.id)
    if row_idx and rec:
        return False, rec

    now = datetime.now(TZ)
    until = (date.today() + timedelta(days=TRIAL_DAYS)).isoformat()

    ws.append_row(
        [
            user.id,
            "active",
            "trial",
            until,
            now.strftime("%Y-%m-%d %H:%M:%S"),
            user.username or "",
            user.first_name or "",
            user.last_name or "",
            "",  # birth_date
            now.strftime("%Y-%m-%d %H:%M:%S"),
        ],
        value_input_option="USER_ENTERED",
    )

    row_idx2, rec2 = find_user_row(ws, user.id)
    return True, rec2


def get_access_level(user_id: int) -> str:
    """
    Source of truth: Google Sheet.
    Returns: 'trial' | 'premium' | 'blocked'
    Auto trial->inactive if expired.
    """
    try:
        row_idx, rec = sheet_safe_get_user(user_id)
        if not row_idx or not rec:
            return "blocked"

        status = str(rec.get("status", "")).strip().lower()
        plan = str(rec.get("plan", "")).strip().lower()
        until = parse_iso_date(str(rec.get("access_until", "")))

        if status != "active":
            return "blocked"

        if plan == "trial":
            if until and date.today() > until:
                # auto-block (inactive)
                try:
                    sheet_safe_update_cell(row_idx, 2, "inactive")  # status col=2
                except Exception:
                    pass
                return "blocked"
            return "trial"

        if plan == "premium":
            return "premium"

        return "blocked"
    except Exception as e:
        # если GS упал — безопасный fallback: trial (но без premium-функций)
        logger.exception("Sheets failure, fallback to trial: %s", e)
        return "trial"


def get_birth_date(user_id: int) -> Optional[str]:
    try:
        _row, rec = sheet_safe_get_user(user_id)
        if not rec:
            return None
        bd = str(rec.get("birth_date", "")).strip()
        return bd or None
    except Exception:
        return None


def set_birth_date(user_id: int, birth_ddmmyyyy: str) -> bool:
    try:
        ws = gs_open_ws()
        ensure_headers(ws)
        row_idx, rec = find_user_row(ws, user_id)
        if not row_idx:
            return False
        # birth_date column index = HEADERS index + 1
        col_birth = HEADERS.index("birth_date") + 1
        col_seen = HEADERS.index("last_seen_at") + 1
        ws.update_cell(row_idx, col_birth, birth_ddmmyyyy)
        ws.update_cell(row_idx, col_seen, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
        return True
    except Exception as e:
        logger.exception("Failed to set birth_date: %s", e)
        return False


def touch_last_seen(user_id: int) -> None:
    try:
        ws = gs_open_ws()
        ensure_headers(ws)
        row_idx, _rec = find_user_row(ws, user_id)
        if not row_idx:
            return
        col_seen = HEADERS.index("last_seen_at") + 1
        ws.update_cell(row_idx, col_seen, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        pass


# ===================== RENDER / TELEGRAM SAFETY =====================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, Conflict):
        logger.error("409 Conflict: another getUpdates is running. Exiting to let Render restart.")
        os._exit(1)
    logger.exception("Unhandled error: %s", err)


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
        except Exception:
            pass


# ===================== FORMATTING =====================
def format_trial_ld(birth: str, today: date) -> str:
    py = calc_personal_year(birth, today.year)
    pm = calc_personal_month(py, today.month)
    ld = calc_personal_day(pm, today.day)

    return (
        f"📅 *Дата:* {today.strftime('%d.%m.%Y')}\n\n"
        f"🔢 *Личный день (ЛД):* {ld}\n"
        f"{PERSONAL_DAY_INTERPRETATIONS.get(ld, '')}\n\n"
        f"{TRIAL_ONLY_MSG}"
    )


def format_premium_full(birth: str, today: date) -> str:
    # OD with special rule 10/20/30
    lines: List[str] = [f"📅 *Дата:* {today.strftime('%d.%m.%Y')}"]

    if today.day in (10, 20, 30):
        lines.append("\n" + UNFAVORABLE_TEXT)
    else:
        od = calc_general_day(today)
        # по ТЗ “описание нужно только для 3 и 6” — но ты просил “всё”, поэтому даю для всех 1–9
        lines.append(f"\n🌐 *Общий день (ОД):* {od}\n{GENERAL_DAY_INTERPRETATIONS.get(od, '')}")

    py = calc_personal_year(birth, today.year)
    pm = calc_personal_month(py, today.month)
    ld = calc_personal_day(pm, today.day)

    # правило 1-го числа: полный текст ЛГ/ЛМ только 1-го
    if today.day == 1:
        lines.append(f"\n🗓 *Личный год (ЛГ):* {py}\n{PERSONAL_YEAR_INTERPRETATIONS.get(py, '')}")
        lines.append(f"\n🗓 *Личный месяц (ЛМ):* {pm}\n{PERSONAL_MONTH_INTERPRETATIONS.get(pm, '')}")
    else:
        lines.append(f"\n🗓 *Личный год (ЛГ):* {py}")
        lines.append(f"🗓 *Личный месяц (ЛМ):* {pm}")

    lines.append(f"\n🔢 *Личный день (ЛД):* {ld}\n{PERSONAL_DAY_INTERPRETATIONS.get(ld, '')}")
    lines.append(f"\n{PREMIUM_ON_MSG}")

    return "\n".join(lines)


# ===================== HANDLERS =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user

    # ensure user exists + notify admins if new
    try:
        created, _rec = ensure_user_in_sheet(user)
        if created:
            await notify_admins_new_user(context, user)
    except Exception as e:
        logger.exception("ensure_user_in_sheet failed: %s", e)

    touch_last_seen(user.id)

    access = get_access_level(user.id)
    if access == "blocked":
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору."
        )
        return

    bd = get_birth_date(user.id)
    if not bd:
        await update.message.reply_text(
            "Введите дату рождения в формате *ДД.ММ.ГГГГ*\nПример: `05.03.1994`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # если ДР уже есть — сразу покажем прогноз на сегодня по тарифу
    today = datetime.now(TZ).date()
    if access == "trial":
        msg = format_trial_ld(bd, today)
    else:
        msg = format_premium_full(bd, today)

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    touch_last_seen(user.id)

    access = get_access_level(user.id)
    if access == "blocked":
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору."
        )
        return

    bd = get_birth_date(user.id)
    if not bd:
        await update.message.reply_text(
            "Сначала введи дату рождения в формате *ДД.ММ.ГГГГ*\nПример: `05.03.1994`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    today = datetime.now(TZ).date()
    if access == "trial":
        msg = format_trial_ld(bd, today)  # ✅ trial = ONLY LD
    else:
        msg = format_premium_full(bd, today)  # ✅ premium = full

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def setbirth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Введите новую дату рождения в формате *ДД.ММ.ГГГГ*\nПример: `05.03.1994`",
        parse_mode=ParseMode.MARKDOWN,
    )


async def sync_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    try:
        created, rec = ensure_user_in_sheet(user)
        if created:
            await notify_admins_new_user(context, user)
        access = get_access_level(user.id)
        bd = get_birth_date(user.id)
        await update.message.reply_text(
            f"✅ sync ok\ncreated={created}\naccess={access}\nbirth_date={bd}\nrecord={bool(rec)}"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ sync failed: {type(e).__name__}: {e}")


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    touch_last_seen(user.id)

    access = get_access_level(user.id)
    if access == "blocked":
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору."
        )
        return

    birth = validate_birth(update.message.text)
    if not birth:
        await update.message.reply_text("❌ Неверный формат. Пример: 05.03.1994")
        return

    ok = set_birth_date(user.id, birth)
    if not ok:
        await update.message.reply_text("❌ Не смог сохранить дату рождения. Проверь доступ к Google Sheets.")
        return

    today = datetime.now(TZ).date()
    if access == "trial":
        msg = format_trial_ld(birth, today)  # ✅ trial = ONLY LD
    else:
        msg = format_premium_full(birth, today)  # ✅ premium = full

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ===================== PREMIUM DAILY BROADCAST =====================
async def _send_daily_premium(app: Application) -> None:
    """
    Runs in PTB event loop (async). Sends only to active premium users with birth_date.
    """
    try:
        ws = gs_open_ws()
        ensure_headers(ws)
        records = ws.get_all_records()
    except Exception as e:
        logger.exception("daily: cannot open sheet: %s", e)
        return

    today = datetime.now(TZ).date()

    for r in records:
        try:
            status = str(r.get("status", "")).strip().lower()
            plan = str(r.get("plan", "")).strip().lower()
            uid = r.get("telegram_user_id")
            bd = str(r.get("birth_date", "")).strip()

            if status != "active" or plan != "premium":
                continue
            if not uid or not str(uid).isdigit():
                continue
            user_id = int(uid)
            if not bd:
                continue

            msg = format_premium_full(bd, today)  # ✅ premium = full
            await app.bot.send_message(user_id, msg, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            # не валим рассылку из-за одного пользователя
            continue


def _schedule_daily(app: Application) -> BackgroundScheduler:
    """
    APScheduler runs in a separate thread; we marshal to PTB loop.
    """
    scheduler = BackgroundScheduler(timezone=str(TZ))

    def job():
        try:
            loop = app.loop
            fut = asyncio.run_coroutine_threadsafe(_send_daily_premium(app), loop)
            fut.result(timeout=120)
        except Exception as e:
            logger.exception("daily job error: %s", e)

    trigger = CronTrigger(hour=9, minute=0, timezone=str(TZ))
    scheduler.add_job(job, trigger=trigger, id="daily_premium", replace_existing=True)
    scheduler.start()
    return scheduler


async def post_init(app: Application) -> None:
    # daily premium only
    try:
        app.bot_data["scheduler"] = _schedule_daily(app)
        logger.info("Scheduler started (premium daily 09:00).")
    except Exception as e:
        logger.exception("Failed to start scheduler: %s", e)


async def post_shutdown(app: Application) -> None:
    sch = app.bot_data.get("scheduler")
    try:
        if sch:
            sch.shutdown(wait=False)
            logger.info("Scheduler shutdown.")
    except Exception:
        pass


# ===================== MAIN =====================
def main() -> None:
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    app = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("today", today_cmd))
    app.add_handler(CommandHandler("setbirth", setbirth_cmd))
    app.add_handler(CommandHandler("sync", sync_cmd))

    # Любой текст — трактуем как ввод даты рождения (и обновление)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("Bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
