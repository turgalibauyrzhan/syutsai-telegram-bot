import os
import json
import base64
import logging
import asyncio
from datetime import datetime, timedelta, date, time
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


# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("syucai_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)


# ===================== ENV =====================
TZ = ZoneInfo("Asia/Almaty")

TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
GSHEET_ID = (os.environ.get("GSHEET_ID") or "").strip()
GOOGLE_SA_JSON = (os.environ.get("GOOGLE_SA_JSON") or "").strip()

TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "3").strip() or "3")

# ADMIN_CHAT_IDS="123,456"
ADMIN_CHAT_IDS: set[int] = set()
_admin_raw = (os.environ.get("ADMIN_CHAT_IDS") or "").strip()
if _admin_raw:
    for x in _admin_raw.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_CHAT_IDS.add(int(x))


# ===================== INTERPRETATIONS (из твоего старого main.py) =====================
UNFAVORABLE_DAYS = [10, 20, 30]

GENERAL_DAY_INTERPRETATIONS = {
    3: "Благоприятный день через анализ, успех. Хороший день для принятия серьезных решений, подписания договоров и совершения покупок.",
    6: "Благоприятный день через любовь, успех. Хороший день для принятия решений, для подписания договоров. Делайте покупки, начинайте большие проекты.",
}

UNFAVORABLE_TEXT = (
    "Сегодня нежелательно начинать новые проекты и события. Есть высокая вероятность обнуления всех результатов ваших действий. "
    "Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и т.д."
)

# Полные тексты (ЛГ/ЛМ/ЛД) — вставь сюда 1-в-1 из твоего main.py
# Я оставляю структуру именно такую, как у тебя было:
PERSONAL_YEAR_INTERPRETATIONS = {
    1: {
        "title": "Личный год 1. Начало нового цикла.",
        "description": "Это время выбора направления, в котором ты хочешь реализовать себя. Сейчас приходит самый мощный энергетический поток за весь цикл.",
        "recommendations": "– Отличный период для открытия собственного дела и новых проектов.\n– Развивай лидерские качества и учись брать ответственность на себя.\n– Старайся сохранять внутренний позитивный настрой: тогда энергия будет работать на результат.",
        "if_not_used": "Может ощущаться жжение в теле, раздражение, чувство пустоты от непонимания, куда направить этот мощный поток.",
    },
    2: {
        "title": "Личный год 2. Год построения отношений и дипломатии.",
        "description": "Год учит терпению, гибкости и умению договариваться. Важно слышать других и выстраивать партнерства.",
        "recommendations": "– Укрепляй отношения и создавай союзы.\n– Избегай резких решений.\n– Учись принимать помощь и делиться.",
        "if_not_used": "Сомнения, затягивание решений, эмоциональные качели, зависимость от чужого мнения.",
    },
    3: {
        "title": "Личный год 3. Год анализа и успеха.",
        "description": "В этот период пробуждается аналитическое мышление: человек начинает лучше понимать причинно-следственные связи. Это время планирования и ведения учета.",
        "recommendations": "– Действуй через анализ и расчет.\n– Веди учет доходов/расходов.\n– Следи за временем: куда оно уходит и какие результаты приносит.",
        "if_not_used": "Лень, апатия, хаос в делах. В итоге это приводит к разрушению планов и потере ресурсов.",
    },
    4: {
        "title": "Личный год 4. Год мистических событий.",
        "description": "Год может приносить неожиданные повороты, важные инсайты и проверки на честность с собой.",
        "recommendations": "– Доверяй интуиции, но проверяй фактами.\n– Очищай окружение и привычки.\n– Доводи начатое до конца.",
        "if_not_used": "Страх перемен, закрытость, внутренние конфликты, потеря энергии.",
    },
    5: {
        "title": "Личный год 5. Год энергии и перемен.",
        "description": "Период движения, новых впечатлений и роста через изменения. Хорошо учиться и расширять горизонты.",
        "recommendations": "– Пробуй новое.\n– Больше общения и движения.\n– Не застревай в рутине.",
        "if_not_used": "Нервозность, разбрасывание, отсутствие результата из-за хаотичных действий.",
    },
    6: {
        "title": "Личный год 6. Год любви и ответственности.",
        "description": "Акцент на семье, отношениях, заботе и ответственности. Важно укреплять связи и создавать комфорт.",
        "recommendations": "– Уделяй внимание близким.\n– Закрывай обещания.\n– Создавай устойчивые привычки.",
        "if_not_used": "Конфликты, обиды, эмоциональная перегрузка, выгорание.",
    },
    7: {
        "title": "Личный год 7. Год духовности и самоанализа.",
        "description": "Время внутреннего роста, обучения и глубины. Хорошо заниматься саморазвитием и исследованием.",
        "recommendations": "– Учись и углубляйся.\n– Меньше суеты.\n– Выстраивай внутренний фундамент.",
        "if_not_used": "Ощущение пустоты, уход в изоляцию без роста, тревожность.",
    },
    8: {
        "title": "Личный год 8. Год денег и управления.",
        "description": "Фокус на карьере, ресурсах, деньгах, управлении. Хорошо ставить амбициозные цели и достигать.",
        "recommendations": "– Планируй финансы.\n– Бери управление в руки.\n– Думай стратегически.",
        "if_not_used": "Финансовые потери из-за импульсивности, конфликты из-за контроля.",
    },
    9: {
        "title": "Личный год 9. Год завершений.",
        "description": "Период закрытия циклов, завершения проектов, освобождения от лишнего. Подготовка к новому старту.",
        "recommendations": "– Заверши начатое.\n– Отпусти лишнее.\n– Подводи итоги.",
        "if_not_used": "Зависание в прошлом, сожаления, ощущение, что жизнь стоит на месте.",
    },
}

PERSONAL_MONTH_INTERPRETATIONS = {
    1: {
        "title": "Личный месяц 1.",
        "plus": "Месяц инициативы и стартов. Хорошо начинать новые дела.",
        "minus": "Импульсивность и конфликтность при давлении.",
    },
    2: {
        "title": "Личный месяц 2.",
        "plus": "Дипломатия, отношения, мягкое продвижение.",
        "minus": "Сомнения, медлительность, манипуляции.",
    },
    3: {
        "title": "Личный месяц 3.",
        "plus": "Коммуникации, творчество, продвижение.",
        "minus": "Поверхностность и расфокус.",
    },
    4: {
        "title": "Личный месяц 4.",
        "plus": "Структура, дисциплина, порядок.",
        "minus": "Жесткость и рутина.",
    },
    5: {
        "title": "Личный месяц 5.",
        "plus": "Перемены, движение, гибкость.",
        "minus": "Хаос и скачки настроения.",
    },
    6: {
        "title": "Личный месяц 6.",
        "plus": "Семья, забота, ответственность.",
        "minus": "Перегруз и обиды.",
    },
    7: {
        "title": "Личный месяц 7.",
        "plus": "Анализ, обучение, глубина.",
        "minus": "Закрытость, одиночество.",
    },
    8: {
        "title": "Личный месяц 8.",
        "plus": "Деньги, карьера, управление.",
        "minus": "Конфликты из-за контроля.",
    },
    9: {
        "title": "Личный месяц 9.",
        "plus": "Завершения, итоги, очищение.",
        "minus": "Ностальгия, зависание в прошлом.",
    },
}

PERSONAL_DAY_INTERPRETATIONS = {
    1: "День инициативы и стартов.",
    2: "День мягкости, дипломатии, отношений.",
    3: "День общения, творчества, продвижения.",
    4: "День порядка, дисциплины и системности.",
    5: "День перемен и гибкости.",
    6: "День любви, семьи и ответственности.",
    7: "День анализа, тишины и глубины.",
    8: "День денег, ресурсов, управления.",
    9: "День завершений и подведения итогов.",
}


# ===================== CALC (по ТЗ) =====================
def reduce_to_digit(n: int) -> int:
    while n > 9:
        n = sum(int(c) for c in str(n))
    return n


def parse_ddmmyyyy(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s.strip(), "%d.%m.%Y").date()
    except Exception:
        return None


def validate_birth(text: str) -> Optional[str]:
    dt = parse_ddmmyyyy(text or "")
    if not dt:
        return None
    if dt > datetime.now(TZ).date():
        return None
    return dt.strftime("%d.%m.%Y")


def calc_general_day(today: date) -> int:
    s = sum(int(c) for c in f"{today.day:02d}{today.month:02d}{today.year:04d}")
    return reduce_to_digit(s)


def digits_sum_int(n: int) -> int:
    return reduce_to_digit(sum(int(c) for c in str(n)))


def calc_personal_year(birth_ddmmyyyy: str, current_year: int) -> int:
    d, m, _y = map(int, birth_ddmmyyyy.split("."))
    total = digits_sum_int(d) + digits_sum_int(m) + reduce_to_digit(sum(int(c) for c in str(current_year)))
    return reduce_to_digit(total)


def calc_personal_month(personal_year: int, current_month: int) -> int:
    month_digit = reduce_to_digit(sum(int(c) for c in str(current_month)))
    return reduce_to_digit(personal_year + month_digit)


def calc_personal_day(personal_month: int, current_day: int) -> int:
    day_digit = reduce_to_digit(sum(int(c) for c in str(current_day)))
    return reduce_to_digit(personal_month + day_digit)


# ===================== SHEETS (admin only) =====================
SHEET_NAME = "subscriptions"

# добавил registered_on / last_full_ym (для правила “полное ЛГ/ЛМ 1-го или в день регистрации”)
HEADERS = [
    "telegram_user_id",
    "status",         # active/inactive
    "plan",           # trial/premium
    "trial_expires",  # YYYY-MM-DD
    "birth_date",     # DD.MM.YYYY
    "created_at",
    "last_seen_at",
    "username",
    "first_name",
    "last_name",
    "registered_on",  # YYYY-MM-DD
    "last_full_ym",   # YYYY-MM
]


def load_sa_info() -> dict:
    if not GOOGLE_SA_JSON:
        raise ValueError("GOOGLE_SA_JSON env is empty")

    raw = GOOGLE_SA_JSON.strip()

    # base64?
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        if decoded.strip().startswith("{"):
            return json.loads(decoded)
    except Exception:
        pass

    # normal json (with escaped newlines)
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
    if not row1:
        ws.append_row(HEADERS, value_input_option="USER_ENTERED")
        return

    # мягкая миграция: добавим недостающие колонки справа
    missing = [h for h in HEADERS if h not in row1]
    if missing:
        new_headers = row1 + missing
        ws.delete_rows(1)
        ws.insert_row(new_headers, 1)


def find_user_row(ws: gspread.Worksheet, user_id: int) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    records = ws.get_all_records()
    for i, r in enumerate(records, start=2):
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


def ym_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def ensure_user_exists(user) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    ВАЖНО: пользователя добавляем в таблицу всегда.
    По умолчанию: active + trial + trial_expires=today+TRIAL_DAYS
    """
    ws = gs_open_ws()
    ensure_headers(ws)

    row_idx, rec = find_user_row(ws, user.id)
    if row_idx and rec:
        return False, rec

    now = datetime.now(TZ)
    trial_expires = (date.today() + timedelta(days=TRIAL_DAYS)).isoformat()
    reg = date.today().isoformat()

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
            reg,
            "",   # last_full_ym
        ],
        value_input_option="USER_ENTERED",
    )

    _, rec2 = find_user_row(ws, user.id)
    return True, rec2


def touch_last_seen(user_id: int) -> None:
    try:
        ws = gs_open_ws()
        ensure_headers(ws)
        row_idx, _rec = find_user_row(ws, user_id)
        if not row_idx:
            return
        col_seen = ws.row_values(1).index("last_seen_at") + 1
        ws.update_cell(row_idx, col_seen, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        pass


def get_user_record(user_id: int) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    ws = gs_open_ws()
    ensure_headers(ws)
    return find_user_row(ws, user_id)


def set_birth_date_anyway(user_id: int, birth_ddmmyyyy: str) -> bool:
    """
    КРИТИЧНО: сохраняем ДР независимо от статуса/плана.
    Это чинит твой кейс со скрина.
    """
    try:
        ws = gs_open_ws()
        ensure_headers(ws)
        row_idx, _rec = find_user_row(ws, user_id)
        if not row_idx:
            return False

        headers = ws.row_values(1)
        col_birth = headers.index("birth_date") + 1
        col_seen = headers.index("last_seen_at") + 1

        ws.update_cell(row_idx, col_birth, birth_ddmmyyyy)
        ws.update_cell(row_idx, col_seen, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
        return True
    except Exception as e:
        logger.exception("Failed to set birth_date: %s", e)
        return False


def get_access_level(user_id: int) -> str:
    """
    trial | premium | blocked
    trial истёк -> автоматически status=inactive
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
                # auto-block
                try:
                    headers = ws.row_values(1)
                    col_status = headers.index("status") + 1
                    ws.update_cell(row_idx, col_status, "inactive")
                except Exception:
                    pass
                return "blocked"
            return "trial"

        return "blocked"
    except Exception as e:
        # безопасный fallback: trial (не даём premium-функции при проблеме Sheets)
        logger.exception("Sheets error, fallback=trial: %s", e)
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


def should_send_full_year_month(rec: Dict[str, Any], today: date) -> bool:
    """
    Полный ЛГ/ЛМ:
    - 1-го числа всегда
    - или в день регистрации (если не 1-е) — один раз в месяц
    """
    if today.day == 1:
        return True

    reg = str(rec.get("registered_on", "")).strip()
    last_full = str(rec.get("last_full_ym", "")).strip()
    cur_ym = ym_key(today)

    if reg == today.isoformat() and last_full != cur_ym and today.day != 1:
        return True

    return False


def mark_full_sent(user_id: int, today: date) -> None:
    try:
        ws = gs_open_ws()
        ensure_headers(ws)
        row_idx, _rec = find_user_row(ws, user_id)
        if not row_idx:
            return
        headers = ws.row_values(1)
        if "last_full_ym" not in headers:
            return
        col = headers.index("last_full_ym") + 1
        ws.update_cell(row_idx, col, ym_key(today))
    except Exception:
        pass


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


# ===================== MESSAGES =====================
def build_trial_message(birth: str, today: date) -> str:
    py = calc_personal_year(birth, today.year)
    pm = calc_personal_month(py, today.month)
    ld = calc_personal_day(pm, today.day)
    ld_text = PERSONAL_DAY_INTERPRETATIONS.get(ld, "")

    return (
        f"📅 *Дата:* {today.strftime('%d.%m.%Y')}\n\n"
        f"🔢 *Личный день (ЛД):* {ld}\n"
        f"{ld_text}\n\n"
        f"⏳ *Trial:* доступ ограничен — показываю только *ЛД*."
    )


def build_premium_message(user_id: int, rec: Dict[str, Any], birth: str, today: date) -> str:
    parts: List[str] = [f"📅 *Дата:* {today.strftime('%d.%m.%Y')}"]

    # общий день
    if today.day in UNFAVORABLE_DAYS:
        parts.append(f"\n⚠️ *Неблагоприятный день.*\n{UNFAVORABLE_TEXT}")
    else:
        od = calc_general_day(today)
        parts.append(f"\n🌐 *Общий день (ОД):* {od}")
        if od in (3, 6):
            parts.append(GENERAL_DAY_INTERPRETATIONS[od])

    py = calc_personal_year(birth, today.year)
    pm = calc_personal_month(py, today.month)
    ld = calc_personal_day(pm, today.day)

    parts.append(f"\n🗓 *Личный год (ЛГ):* {py}")
    parts.append(f"🗓 *Личный месяц (ЛМ):* {pm}")

    # Полные тексты ЛГ/ЛМ — только 1-го или в день регистрации (1 раз в месяц)
    if should_send_full_year_month(rec, today):
        y = PERSONAL_YEAR_INTERPRETATIONS.get(py, {})
        m = PERSONAL_MONTH_INTERPRETATIONS.get(pm, {})

        if y:
            parts.append(f"\n*{y.get('title','')}*\n{y.get('description','')}")
            recs = y.get("recommendations", "")
            if recs:
                parts.append(f"\n*Рекомендации:*\n{recs}")
            inu = y.get("if_not_used", "")
            if inu:
                parts.append(f"\n*Если энергия года не используется:*\n{inu}")

        if m:
            parts.append(f"\n*{m.get('title','')}*")
            plus = m.get("plus", "")
            minus = m.get("minus", "")
            if plus:
                parts.append(f"\n*В плюсе:*\n{plus}")
            if minus:
                parts.append(f"\n*В минусе:*\n{minus}")

        mark_full_sent(user_id, today)

    # ЛД всегда
    ld_text = PERSONAL_DAY_INTERPRETATIONS.get(ld, "")
    parts.append(f"\n🔢 *Личный день (ЛД):* {ld}\n{ld_text}")
    parts.append("\n⭐️ *Premium активен:* полный прогноз доступен + ежедневка 09:00.")
    return "\n".join(parts)


# ===================== HANDLERS =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user

    # 1) гарантированно создаём запись (даже если потом blocked)
    try:
        created, _rec = ensure_user_exists(user)
        if created:
            await notify_admins_new_user(context, user)
    except Exception as e:
        logger.exception("ensure_user_exists failed: %s", e)

    touch_last_seen(user.id)

    # 2) если ДР нет — просим, независимо от тарифа (чтобы не было ловушки)
    bd = get_birth_date(user.id)
    if not bd:
        await update.message.reply_text(
            "Введите дату рождения в формате *ДД.ММ.ГГГГ*\nПример: `05.03.1994`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # 3) доступ влияет только на выдачу
    access = get_access_level(user.id)
    if access == "blocked":
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору."
        )
        return

    today = datetime.now(TZ).date()
    if access == "trial":
        msg = build_trial_message(bd, today)
    else:
        _row, rec = get_user_record(user.id)
        msg = build_premium_message(user.id, rec or {}, bd, today)

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    touch_last_seen(user.id)

    bd = get_birth_date(user.id)
    if not bd:
        await update.message.reply_text(
            "Сначала введи дату рождения *ДД.ММ.ГГГГ*\nПример: `05.03.1994`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    access = get_access_level(user.id)
    if access == "blocked":
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору."
        )
        return

    today = datetime.now(TZ).date()
    if access == "trial":
        msg = build_trial_message(bd, today)
    else:
        _row, rec = get_user_record(user.id)
        msg = build_premium_message(user.id, rec or {}, bd, today)

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def profile_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    touch_last_seen(user.id)

    _row, rec = get_user_record(user.id)
    bd = get_birth_date(user.id)
    access = get_access_level(user.id)

    if not rec:
        await update.message.reply_text("Профиль не найден в таблице. Используй /start.")
        return

    msg = (
        f"👤 *Профиль*\n"
        f"ID: `{user.id}`\n"
        f"Username: @{user.username or '—'}\n"
        f"Дата рождения: `{bd or '—'}`\n"
        f"Доступ: *{access}*\n"
        f"План: `{rec.get('plan','')}`\n"
        f"Статус: `{rec.get('status','')}`\n"
        f"Trial до: `{rec.get('trial_expires','')}`"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def sync_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    try:
        created, rec = ensure_user_exists(user)
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

    # гарантированно запись существует
    try:
        ensure_user_exists(user)
    except Exception:
        pass

    touch_last_seen(user.id)

    birth = validate_birth(update.message.text)
    if not birth:
        await update.message.reply_text("❌ Неверный формат. Пример: 05.03.1994")
        return

    # КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: сохраняем ДР всегда
    if not set_birth_date_anyway(user.id, birth):
        await update.message.reply_text("❌ Не смог сохранить дату рождения. Проверь доступ к Google Sheets.")
        return

    access = get_access_level(user.id)
    if access == "blocked":
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору."
        )
        return

    today = datetime.now(TZ).date()
    if access == "trial":
        msg = build_trial_message(birth, today)
    else:
        _row, rec = get_user_record(user.id)
        msg = build_premium_message(user.id, rec or {}, birth, today)

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ===================== PREMIUM DAILY =====================
async def _send_daily_premium(app: Application) -> None:
    """
    Ежедневка ТОЛЬКО premium+active и только если есть birth_date.
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
            msg = build_premium_message(user_id, r, bd, today)
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
    app.add_handler(CommandHandler("profile", profile_cmd))
    app.add_handler(CommandHandler("sync", sync_cmd))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("Bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
