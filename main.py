import os
import json
import base64
import logging
import asyncio
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

# админы через ENV: ADMIN_CHAT_IDS="123,456"
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
UNFAVORABLE_TEXT = (
    "⚠️ *Неблагоприятный день.*\n"
    "Сегодня нежелательно начинать новые проекты и события. "
    "Есть высокая вероятность обнуления всех результатов ваших действий. "
    "Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и т.д."
)

GENERAL_DAY_TEXTS = {
    3: "✅ *ОД=3:* благоприятный день через анализ и успех. Хорошо принимать серьёзные решения, подписывать договоры и совершать покупки.",
    6: "✅ *ОД=6:* благоприятный день через любовь и успех. Хорошо принимать решения, подписывать договоры. Можно делать покупки и начинать большие проекты.",
}

PERSONAL_DAY_TEXTS = {
    1: "ЛД=1 — действуй первым, начинай.",
    2: "ЛД=2 — договаривайся, слушай, действуй мягко.",
    3: "ЛД=3 — общайся, проявляйся, продвигай идеи.",
    4: "ЛД=4 — дисциплина, рутина, порядок, закрывай хвосты.",
    5: "ЛД=5 — гибкость, движение, перемены.",
    6: "ЛД=6 — забота, дом, ответственность, отношения.",
    7: "ЛД=7 — анализ, тишина, фокус, глубина.",
    8: "ЛД=8 — ресурсы/деньги, твёрдые решения, управление.",
    9: "ЛД=9 — завершай, подводи итоги, освобождай место новому.",
}

PERSONAL_YEAR_TEXTS = {
    1: "ЛГ=1 — старт нового цикла, новые проекты, инициатива.",
    2: "ЛГ=2 — партнёрства, терпение, согласование.",
    3: "ЛГ=3 — публичность, творчество, коммуникации.",
    4: "ЛГ=4 — фундамент, дисциплина, системная работа.",
    5: "ЛГ=5 — изменения, движение, адаптация.",
    6: "ЛГ=6 — ответственность, семья/отношения, укрепление.",
    7: "ЛГ=7 — обучение, анализ, углубление.",
    8: "ЛГ=8 — деньги/карьера, управление ресурсами.",
    9: "ЛГ=9 — завершение, чистка, закрытие циклов.",
}

PERSONAL_MONTH_TEXTS = {
    1: "ЛМ=1 — инициатива, запуски.",
    2: "ЛМ=2 — переговоры, мягкое продвижение.",
    3: "ЛМ=3 — активная коммуникация, креатив.",
    4: "ЛМ=4 — порядок, дедлайны, структура.",
    5: "ЛМ=5 — изменения, поездки, эксперименты.",
    6: "ЛМ=6 — забота, отношения, ответственность.",
    7: "ЛМ=7 — анализ, обучение, спокойный темп.",
    8: "ЛМ=8 — амбиции, деньги, управление.",
    9: "ЛМ=9 — завершения, итоги, освобождение.",
}


# ===================== MATH (по ТЗ) =====================
def reduce_to_digit(n: int) -> int:
    while n > 9:
        n = sum(int(c) for c in str(n))
    return n


def digits_sum_of_date(dt: date) -> int:
    s = sum(int(c) for c in f"{dt.day:02d}{dt.month:02d}{dt.year:04d}")
    return reduce_to_digit(s)


def digits_sum_int(n: int) -> int:
    return reduce_to_digit(sum(int(c) for c in str(n)))


def validate_birth(text: str) -> Optional[str]:
    text = (text or "").strip()
    try:
        dt = datetime.strptime(text, "%d.%m.%Y").date()
        if dt > datetime.now(TZ).date():
            return None
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return None


def calc_personal_year(birth_ddmmyyyy: str, current_year: int) -> int:
    d, m, _y = map(int, birth_ddmmyyyy.split("."))
    total = digits_sum_int(d) + digits_sum_int(m) + reduce_to_digit(sum(int(c) for c in str(current_year)))
    return reduce_to_digit(total)


def calc_personal_month(personal_year: int, current_month: int) -> int:
    # месяц: 12 -> 1+2=3
    month_digit = reduce_to_digit(sum(int(c) for c in str(current_month)))
    return reduce_to_digit(personal_year + month_digit)


def calc_personal_day(personal_month: int, current_day: int) -> int:
    # день: 29 -> 2+9=11 -> 2
    day_digit = reduce_to_digit(sum(int(c) for c in str(current_day)))
    return reduce_to_digit(personal_month + day_digit)


# ===================== GOOGLE SHEETS (admin-only) =====================
SHEET_NAME = "subscriptions"
HEADERS = [
    "telegram_user_id",
    "status",        # active/inactive
    "plan",          # trial/premium
    "trial_expires", # YYYY-MM-DD (для trial)
    "birth_date",    # DD.MM.YYYY
    "created_at",
    "last_seen_at",
    "username",
    "first_name",
    "last_name",
]


def load_sa_info() -> dict:
    if not GOOGLE_SA_JSON:
        raise ValueError("GOOGLE_SA_JSON env is empty")

    raw = GOOGLE_SA_JSON.strip()

    # base64
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        if decoded.strip().startswith("{"):
            return json.loads(decoded)
    except Exception:
        pass

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


def ensure_user(user) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Авто-добавляет всех, кто запускает бота:
      status=active, plan=trial, trial_expires=today+3, birth_date=""
    """
    ws = gs_open_ws()
    ensure_headers(ws)

    row_idx, rec = find_user_row(ws, user.id)
    if row_idx and rec:
        return False, rec

    now = datetime.now(TZ)
    trial_expires = (date.today() + timedelta(days=TRIAL_DAYS)).isoformat()

    ws.append_row(
        [
            user.id,
            "active",
            "trial",
            trial_expires,
            "",  # birth_date
            now.strftime("%Y-%m-%d %H:%M:%S"),
            now.strftime("%Y-%m-%d %H:%M:%S"),
            user.username or "",
            user.first_name or "",
            user.last_name or "",
        ],
        value_input_option="USER_ENTERED",
    )

    # reread
    _, rec2 = find_user_row(ws, user.id)
    return True, rec2


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


def get_user_record(user_id: int) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    ws = gs_open_ws()
    ensure_headers(ws)
    return find_user_row(ws, user_id)


def set_birth_date(user_id: int, birth_ddmmyyyy: str) -> bool:
    try:
        ws = gs_open_ws()
        ensure_headers(ws)
        row_idx, _rec = find_user_row(ws, user_id)
        if not row_idx:
            return False
        col_birth = HEADERS.index("birth_date") + 1
        col_seen = HEADERS.index("last_seen_at") + 1
        ws.update_cell(row_idx, col_birth, birth_ddmmyyyy)
        ws.update_cell(row_idx, col_seen, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
        return True
    except Exception as e:
        logger.exception("Failed to set birth_date: %s", e)
        return False


def get_access_level(user_id: int) -> str:
    """
    Returns: trial | premium | blocked
    trial истёк → status=inactive → blocked
    """
    try:
        ws = gs_open_ws()
        ensure_headers(ws)
        row_idx, rec = find_user_row(ws, user_id)
        if not row_idx or not rec:
            return "blocked"

        status = str(rec.get("status", "")).strip().lower()
        plan = str(rec.get("plan", "")).strip().lower()
        trial_expires = parse_iso_date(str(rec.get("trial_expires", "")))

        if status != "active":
            return "blocked"

        if plan == "premium":
            return "premium"

        if plan == "trial":
            if trial_expires and date.today() > trial_expires:
                # auto-block: status=inactive
                try:
                    col_status = HEADERS.index("status") + 1
                    ws.update_cell(row_idx, col_status, "inactive")
                except Exception:
                    pass
                return "blocked"
            return "trial"

        return "blocked"
    except Exception as e:
        # fallback безопасный: trial (но без premium-функций)
        logger.exception("Sheets failure, fallback to trial: %s", e)
        return "trial"


def get_birth_date(user_id: int) -> Optional[str]:
    try:
        _row, rec = get_user_record(user_id)
        if not rec:
            return None
        bd = str(rec.get("birth_date", "")).strip()
        return bd or None
    except Exception:
        return None


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
        except Exception:
            pass


# ===================== FORMATTING =====================
def build_trial_message(birth: str, today: date) -> str:
    py = calc_personal_year(birth, today.year)
    pm = calc_personal_month(py, today.month)
    ld = calc_personal_day(pm, today.day)
    return (
        f"📅 *Дата:* {today.strftime('%d.%m.%Y')}\n\n"
        f"🔢 *Личный день (ЛД):* {ld}\n"
        f"{PERSONAL_DAY_TEXTS.get(ld, '')}\n\n"
        f"⏳ *Trial:* доступ ограничен — показываю только *ЛД*."
    )


def build_premium_message(birth: str, today: date) -> str:
    parts: List[str] = [f"📅 *Дата:* {today.strftime('%d.%m.%Y')}"]

    # неблагоприятные дни 10/20/30
    if today.day in (10, 20, 30):
        parts.append("\n" + UNFAVORABLE_TEXT)
    else:
        od = digits_sum_of_date(today)
        # по ТЗ описания ОД только для 3 и 6
        if od in (3, 6):
            parts.append(f"\n🌐 *Общий день (ОД):* {od}\n{GENERAL_DAY_TEXTS.get(od, '')}")
        else:
            parts.append(f"\n🌐 *Общий день (ОД):* {od}")

    py = calc_personal_year(birth, today.year)
    pm = calc_personal_month(py, today.month)
    ld = calc_personal_day(pm, today.day)

    # правило 1-го числа: полный текст ЛГ/ЛМ только 1-го
    if today.day == 1:
        parts.append(f"\n🗓 *Личный год (ЛГ):* {py}\n{PERSONAL_YEAR_TEXTS.get(py, '')}")
        parts.append(f"\n🗓 *Личный месяц (ЛМ):* {pm}\n{PERSONAL_MONTH_TEXTS.get(pm, '')}")
    else:
        parts.append(f"\n🗓 *Личный год (ЛГ):* {py}")
        parts.append(f"🗓 *Личный месяц (ЛМ):* {pm}")

    parts.append(f"\n🔢 *Личный день (ЛД):* {ld}\n{PERSONAL_DAY_TEXTS.get(ld, '')}")
    parts.append("\n⭐️ *Premium активен:* полный прогноз доступен + ежедневка 09:00.")
    return "\n".join(parts)


# ===================== HANDLERS =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user

    try:
        created, _rec = ensure_user(user)
        if created:
            await notify_admins_new_user(context, user)
    except Exception as e:
        logger.exception("ensure_user failed: %s", e)

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

    today = datetime.now(TZ).date()
    msg = build_trial_message(bd, today) if access == "trial" else build_premium_message(bd, today)
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
            "Сначала введи дату рождения *ДД.ММ.ГГГГ*\nПример: `05.03.1994`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    today = datetime.now(TZ).date()
    msg = build_trial_message(bd, today) if access == "trial" else build_premium_message(bd, today)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def setbirth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Введите новую дату рождения в формате *ДД.ММ.ГГГГ*\nПример: `05.03.1994`",
        parse_mode=ParseMode.MARKDOWN,
    )


async def sync_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    try:
        created, rec = ensure_user(user)
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

    if not set_birth_date(user.id, birth):
        await update.message.reply_text("❌ Не смог сохранить дату рождения. Проверь доступ к Google Sheets.")
        return

    today = datetime.now(TZ).date()
    msg = build_trial_message(birth, today) if access == "trial" else build_premium_message(birth, today)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ===================== PREMIUM DAILY BROADCAST =====================
async def _send_daily_premium(app: Application) -> None:
    """
    Ежедневка ТОЛЬКО premium и status=active.
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
            if not bd:
                continue

            user_id = int(uid)
            msg = build_premium_message(bd, today)  # ✅ premium full
            await app.bot.send_message(user_id, msg, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            continue


def _schedule_daily(app: Application) -> BackgroundScheduler:
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

    # Любой текст — считаем как ввод даты рождения
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("Bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
