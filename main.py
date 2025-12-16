import os
import json
import re
import logging
import asyncio
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

from apscheduler.schedulers.asyncio import AsyncIOScheduler


# =========================
# CONFIG
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("syucai")

TZ = ZoneInfo(os.getenv("TZ_NAME", "Asia/Almaty"))

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set")

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()  # spreadsheet ID only
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON", "").strip()  # raw JSON string
SUBS_SHEET_NAME = os.getenv("SUBS_SHEET_NAME", "subscriptions").strip()

ADMIN_CHAT_IDS = []
_admin_raw = os.getenv("ADMIN_CHAT_IDS", "").strip()
if _admin_raw:
    for x in _admin_raw.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_CHAT_IDS.append(int(x))

TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "3"))

# Columns you requested (must match your sheet header)
SUBS_COLUMNS = [
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

STATUS_ACTIVE = "active"
STATUS_BLOCKED = "blocked"
PLAN_TRIAL = "trial"
PLAN_PREMIUM = "premium"
PLAN_BLOCKED = "blocked"

MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📅 Прогноз на сегодня")],
        [KeyboardButton("👤 Профиль")],
    ],
    resize_keyboard=True,
)

BIRTHDATE_STATE = 1


# =========================
# TEXTS (FULL) — from your doc
# =========================
TEXTS: Dict[str, Any] = {
    "general_day": {
        "3": "Благоприятный день через анализ, успех. Хороший день для принятия серьёзных решений, подписания договоров и совершения покупок.",
        "6": "Благоприятный день через любовь, успех. Хороший день для принятия решений, для подписания договоров. Делайте покупки, начинайте большие проекты.",
    },
    "unfavorable_day_text": (
        "Сегодня нежелательно начинать новые проекты и события. Есть высокая вероятность обнуления всех результатов ваших действий. "
        "Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и т.д."
    ),
    "personal_year": {
        "1": {
            "title": "Личный год 1. Начало нового цикла.",
            "full": (
                "Это время выбора направления, в котором ты хочешь реализоваться в ближайшие 9 лет. Именно сейчас приходит самый мощный "
                "энергетический поток за весь цикл.\n\n"
                "Рекомендации:\n"
                "– Отличный период для открытия собственного дела или запуска нового проекта.\n"
                "– Определи для себя одно ключевое направление и сосредоточься на нем, не распыляясь.\n"
                "– Развивай лидерские качества и учись брать ответственность на себя.\n\n"
                "– Старайся сохранять внутренний позитивный настрой: тогда энергия будет работать на результат."
            ),
        },
        "2": {
            "title": "Личный год 2. Год дипломатии и отношений.",
            "full": (
                "Появляется больше реалистичности и стремления докопаться до сути. Активизируется энергия воспоминаний, усиливается чувственность. "
                "Во всём важно проявлять дипломатию. Серьёзные решения лучше отложить до следующего года. Полезно пить больше воды.\n\n"
                "Может проявляться медлительность, сомнения, усиление депрессивных состояний. Мысли и действия часто направлены на разрыв отношений. "
                "Усиливается желание манипулировать."
            ),
        },
        "3": {
            "title": "Личный год 3. Год анализа и успеха.",
            "full": (
                "В этот период пробуждается аналитическое мышление: человек начинает планировать, подводить итоги и более осознанно подходить к своим действиям. "
                "Это время планирования и ведения учета.\n\n"
                "Рекомендации:\n"
                "– Действуй через анализ и расчет.\n"
                "– В бизнесе и совместных делах выстраивай справедливое и прозрачное управление.\n"
                "– Планируй шаги на день, месяц и год вперед.\n"
                "– Подводи промежуточные итоги, корректируй и обновляй планы по мере необходимости.\n"
                "– Следи за своим временем: куда оно уходит и какие результаты приносит.\n\n"
                "Если энергия года не используется:\n"
                "Могут проявляться лень, азарт, корысть и стремление к быстрой выгоде. В итоге это приводит к разрушению планов и потере ресурсов."
            ),
        },
        "4": {
            "title": "Личный год 4. Год трансформации и перемен.",
            "full": (
                "В этот период активно происходят перемены, трансформация личности, переоценка ценностей. Может уйти из жизни что-то дорогое или значимое, "
                "к чему вы были привязаны."
            ),
        },
        "5": {
            "title": "Личный год 5. Год общения и открытых возможностей.",
            "full": (
                "Хороший период, чтобы заводить новые знакомства, общаться, заниматься бизнесом, делиться мыслями и выкладывать посты в социальных сетях. "
                "Активное общение приносит новые возможности, успех, карьерный рост и материальные блага.\n\n"
                "Всё тайное становится явным — могут открыться тайны и секреты.\n"
                "Может проявляться беспечность.\n\n"
                "Главное — не вступать в борьбу и сопротивление, а говорить «да» и соглашаться. Тогда энергия возможностей не будет блокироваться."
            ),
        },
        "6": {
            "title": "Личный год 6. Год любви и успеха.",
            "full": (
                "Работает энергия любви и счастья. Проявляйте творчество и любовь, ищите креативный подход даже в простых делах. "
                "Дарите заботу, говорите близким тёплые слова, проявляйте тактильность и внимание. Старайтесь создавать комфорт для других.\n\n"
                "В минусе может проявляться стремление к лени, мстительности и заботе только о собственном комфорте."
            ),
        },
        "7": {
            "title": "Личный год 7. Год глубины и обучения.",
            "full": (
                "Год глубины, обучения и внутреннего роста. Хорошо идти в изучение, прокачивать навыки, усиливать дисциплину и фокус. "
                "Подходит для уединения, работы над собой, поиска смысла и нового уровня осознанности."
            ),
        },
        "8": {
            "title": "Личный год 8. Год ресурсов и денег.",
            "full": (
                "Год ресурсов, денег и управления. Хорошо заниматься финансами, карьерой, масштабированием, усилением личной силы. "
                "Важно действовать системно и держать фокус на результат."
            ),
        },
        "9": {
            "title": "Личный год 9. Год завершений и очищения.",
            "full": (
                "Год завершений и очищения. Подходит для закрытия хвостов, завершения проектов, отпускания лишнего. "
                "Важно подвести итоги и подготовить почву для нового цикла."
            ),
        },
    },
    "personal_month": {
        "1": {
            "title": "Личный месяц 1. Месяц стартов и инициатив.",
            "full": (
                "Месяц стартов и инициатив. Хорошо начинать новые задачи, делать первые шаги, пробовать. "
                "Важно не распыляться и двигаться в выбранном направлении."
            ),
        },
        "2": {
            "title": "Личный месяц 2. Месяц дипломатии и выстраивания отношений.",
            "full": (
                "Появляется больше реалистичности и стремления докопаться до сути. Активизируется энергия воспоминаний, усиливается чувственность. "
                "Во всём важно проявлять дипломатию. Серьёзные решения лучше отложить до следующего месяца. Полезно пить больше воды.\n\n"
                "Может проявляться медлительность, сомнения, усиление депрессивных состояний. Мысли и действия часто направлены на разрыв отношений. "
                "Усиливается желание манипулировать."
            ),
        },
        "3": {
            "title": "Личный месяц 3. Месяц анализа и успеха.",
            "full": (
                "Месяц анализа и успеха. Хорошо планировать, считать, улучшать процессы, наводить порядок, подводить итоги и корректировать курс."
            ),
        },
        "4": {
            "title": "Личный месяц 4. Месяц трансформации и перемен.",
            "full": (
                "Месяц трансформации и перемен. Могут происходить резкие развороты, переоценка, завершения и обновления. "
                "Важна гибкость и готовность отпустить старое."
            ),
        },
        "5": {
            "title": "Личный месяц 5. Месяц общения и возможностей.",
            "full": (
                "Месяц общения и возможностей. Больше контактов, движений, договорённостей и новых шансов. "
                "Хорошо проявляться публично и расширять круг общения."
            ),
        },
        "6": {
            "title": "Личный месяц 6. Месяц любви и успеха.",
            "full": (
                "Месяц любви и успеха. Хорошо укреплять отношения, создавать красоту и комфорт, заниматься творчеством и важными решениями."
            ),
        },
        "7": {
            "title": "Личный месяц 7. Месяц глубины и роста.",
            "full": (
                "Месяц глубины и роста. Хорошо учиться, углубляться, анализировать, выстраивать личные смыслы и укреплять дисциплину."
            ),
        },
        "8": {
            "title": "Личный месяц 8. Месяц ресурсов и денег.",
            "full": (
                "Месяц ресурсов и денег. Хорошо управлять финансами, усиливать доход, строить систему, договариваться о выгодных условиях."
            ),
        },
        "9": {
            "title": "Личный месяц 9. Месяц завершений и очищения.",
            "full": (
                "Месяц завершений и очищения. Подходит для закрытия проектов, расхламления, наведения порядка и подготовки к новому этапу."
            ),
        },
    },
    "personal_day": {
        "1": {
            "title": "Личный день 1. День новых начинаний.",
            "full": (
                "День новых начинаний. Любое начинание сегодня будет благоприятным и получит поддержку энергии дня."
            ),
        },
        "2": {
            "title": "Личный день 2. День понимания и дипломатии.",
            "full": (
                "Проявляйте терпение и понимание. Если вас не понимают — дайте пространство, слушайте искренне и без осуждения. "
                "Свяжитесь с кем-то важным: позвоните тем, с кем давно не общались. Это поможет восстановить ценные связи. "
                "Будьте особенно аккуратны в отношениях. Может появиться желание разорвать их, но задача дня — налаживать и укреплять. "
                "В минусе — день сомнений и возможного упадка настроения, депрессия. Обратите внимание на воду: контрастный душ, ванна или прогулка у воды "
                "обновят энергию и снимут напряжение. Проживайте день через дипломатию и мягкость."
            ),
        },
        "3": {
            "title": "Личный день 3. День анализа и планирования.",
            "full": (
                "Анализируйте каждое действие. Планируйте события — сегодня энергия анализа помогает принимать верные решения. "
                "Благоприятный день для медицинских процедур, операций и визита к врачу. Может возникнуть желание получить лёгкую выгоду через азартные действия. "
                "Действуйте через холодный анализ — иначе возможны потери."
            ),
        },
        "4": {
            "title": "Личный день 4. День мистических событий.",
            "full": (
                "День мистических событий — как положительных, так и отрицательных. Может появляться чувство неудовлетворенности, поэтому важно сохранять "
                "позитивный настрой, чтобы были положительные мистические события. Иначе могут быть мистические потери. Посвятите день своим целям и мечтам. "
                "Визуализируйте желаемое, позволяйте себе мечтать без ограничений — именно это сегодня даст мощный импульс."
            ),
        },
        "5": {
            "title": "Личный день 5. День общения и открытых возможностей.",
            "full": (
                "Хороший день, чтобы заводить новые знакомства, общаться, заниматься бизнесом, делиться мыслями и выкладывать посты в социальных сетях. "
                "Активное общение принесёт новые возможности, успех, карьерный рост и материальные блага. Всё тайное становится явным — могут открыться тайны и секреты. "
                "Может проявляться беспечность. Главное — не вступать в борьбу и сопротивление, а говорить «да» и соглашаться. "
                "Тогда энергия возможностей не будет блокироваться."
            ),
        },
        "6": {
            "title": "Личный день 6. День любви и успеха.",
            "full": (
                "Работает энергия любви и счастья. Проявляйте творчество и любовь, ищите креативный подход даже в простых делах. "
                "Дарите заботу, говорите близким тёплые слова, проявляйте тактильность и внимание. Старайтесь создавать комфорт для других. "
                "В минусе может проявляться стремление к лени, мстительности и заботе только о собственном комфорте."
            ),
        },
        "7": {
            "title": "Личный день 7. День кризиса или осознанной трансформации.",
            "full": (
                "День кризиса или осознанной трансформации. Хорошо уходить в тишину, анализировать и делать выводы. "
                "Важно не конфликтовать, а направлять энергию в осознанные изменения."
            ),
        },
        "8": {
            "title": "Личный день 8. День ресурсов и денег.",
            "full": (
                "День ресурсов, денег и управления. Хорошо заниматься финансами, делами, дисциплиной и ответственными решениями."
            ),
        },
        "9": {
            "title": "Личный день 9. День завершений и очищения.",
            "full": (
                "День завершений и очищения. Хорошо закрывать хвосты, завершать проекты, отпускать лишнее и подводить итоги."
            ),
        },
    },
}


# =========================
# UTIL: numerology
# =========================
def digits_sum_to_1_9(n: int) -> int:
    # reduce to 1..9 (no 11/22 handling)
    while n > 9:
        s = 0
        for ch in str(n):
            s += ord(ch) - 48
        n = s
    if n == 0:
        # should not happen for valid dates, but keep safe
        return 9
    return n


def sum_digits_of_date(d: date) -> int:
    s = 0
    for ch in d.strftime("%d%m%Y"):
        s += ord(ch) - 48
    return digits_sum_to_1_9(s)


def personal_year(birth: date, today: date) -> int:
    s = 0
    for ch in birth.strftime("%d%m"):
        s += ord(ch) - 48
    for ch in today.strftime("%Y"):
        s += ord(ch) - 48
    return digits_sum_to_1_9(s)


def personal_month(py: int, today: date) -> int:
    # month number reduced to 1..9 via digits sum (e.g., 12 -> 3)
    m = today.month
    m_red = digits_sum_to_1_9(sum(int(c) for c in str(m)))
    return digits_sum_to_1_9(py + m_red)


def personal_day(pm: int, today: date) -> int:
    dd = today.day
    dd_red = digits_sum_to_1_9(sum(int(c) for c in str(dd)))
    return digits_sum_to_1_9(pm + dd_red)


# =========================
# GOOGLE SHEETS
# =========================
@dataclass
class UserRec:
    telegram_user_id: int
    status: str
    plan: str
    trial_expires: Optional[date]
    birth_date: Optional[date]
    created_at: datetime
    last_seen_at: datetime
    username: str
    first_name: str
    last_name: str
    registered_on: date
    last_full_ym: str  # YYYY-MM


_gs_client: Optional[gspread.Client] = None
_subs_ws: Optional[gspread.Worksheet] = None


def _parse_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def _now() -> datetime:
    return datetime.now(TZ)


def _today() -> date:
    return _now().date()


def gs_init() -> None:
    global _gs_client, _subs_ws
    if _subs_ws is not None:
        return

    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON env var is not set")

    if not GSHEET_ID:
        raise RuntimeError("GSHEET_ID env var is not set")

    try:
        sa_info = json.loads(GOOGLE_SA_JSON)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GOOGLE_SA_JSON parse error: {e}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    _gs_client = gspread.authorize(creds)

    sh = _gs_client.open_by_key(GSHEET_ID)
    _subs_ws = sh.worksheet(SUBS_SHEET_NAME)

    # ensure header
    header = _subs_ws.row_values(1)
    if header != SUBS_COLUMNS:
        # If sheet is empty or wrong header, enforce it.
        if any(cell.strip() for cell in header):
            log.warning("Sheet header differs; rewriting to expected SUBS_COLUMNS")
        _subs_ws.resize(rows=max(_subs_ws.row_count, 2), cols=len(SUBS_COLUMNS))
        _subs_ws.update("A1", [SUBS_COLUMNS])


def gs_find_row_by_user_id(user_id: int) -> Optional[int]:
    assert _subs_ws is not None
    try:
        col = _subs_ws.col_values(1)  # telegram_user_id
        # row 1 is header
        for idx, v in enumerate(col[1:], start=2):
            if str(user_id) == str(v).strip():
                return idx
        return None
    except Exception as e:
        log.exception("gs_find_row_by_user_id failed: %s", e)
        return None


def gs_get_user(user_id: int) -> Optional[UserRec]:
    assert _subs_ws is not None
    row_idx = gs_find_row_by_user_id(user_id)
    if not row_idx:
        return None
    row = _subs_ws.row_values(row_idx)
    # pad
    row += [""] * (len(SUBS_COLUMNS) - len(row))
    data = dict(zip(SUBS_COLUMNS, row))

    created_at = _parse_dt(data["created_at"]) or _now()
    last_seen_at = _parse_dt(data["last_seen_at"]) or _now()
    trial_expires = _parse_date(data["trial_expires"])
    birth_date = _parse_date(data["birth_date"])
    registered_on = _parse_date(data["registered_on"]) or created_at.date()

    return UserRec(
        telegram_user_id=int(data["telegram_user_id"]),
        status=(data["status"] or STATUS_ACTIVE).strip(),
        plan=(data["plan"] or PLAN_TRIAL).strip(),
        trial_expires=trial_expires,
        birth_date=birth_date,
        created_at=created_at,
        last_seen_at=last_seen_at,
        username=(data["username"] or "").strip(),
        first_name=(data["first_name"] or "").strip(),
        last_name=(data["last_name"] or "").strip(),
        registered_on=registered_on,
        last_full_ym=(data["last_full_ym"] or "").strip(),
    )


def gs_upsert_user_from_update(update: Update) -> Tuple[UserRec, bool]:
    """
    Returns (user_rec, is_new_user_row)
    """
    assert update.effective_user is not None
    u = update.effective_user

    gs_init()
    assert _subs_ws is not None

    row_idx = gs_find_row_by_user_id(u.id)
    now = _now()
    is_new = False

    if not row_idx:
        is_new = True
        registered_on = now.date()
        created_at = now
        last_seen_at = now
        trial_expires = (registered_on + timedelta(days=TRIAL_DAYS))
        values = {
            "telegram_user_id": str(u.id),
            "status": STATUS_ACTIVE,
            "plan": PLAN_TRIAL,
            "trial_expires": trial_expires.strftime("%Y-%m-%d"),
            "birth_date": "",
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "last_seen_at": last_seen_at.strftime("%Y-%m-%d %H:%M:%S"),
            "username": u.username or "",
            "first_name": u.first_name or "",
            "last_name": u.last_name or "",
            "registered_on": registered_on.strftime("%Y-%m-%d"),
            "last_full_ym": "",
        }
        row = [values[c] for c in SUBS_COLUMNS]
        _subs_ws.append_row(row, value_input_option="USER_ENTERED")
        row_idx = gs_find_row_by_user_id(u.id)

    # update last_seen + identity fields
    assert row_idx is not None
    rec = gs_get_user(u.id)
    if rec is None:
        # should not happen
        raise RuntimeError("Failed to read user after upsert")

    updates = {
        "last_seen_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "username": u.username or "",
        "first_name": u.first_name or "",
        "last_name": u.last_name or "",
    }

    # write minimal update
    row_vals = _subs_ws.row_values(row_idx)
    row_vals += [""] * (len(SUBS_COLUMNS) - len(row_vals))
    for k, v in updates.items():
        col_idx = SUBS_COLUMNS.index(k) + 1
        row_vals[col_idx - 1] = v
    _subs_ws.update(f"A{row_idx}", [row_vals], value_input_option="USER_ENTERED")

    rec2 = gs_get_user(u.id)
    if rec2 is None:
        raise RuntimeError("Failed to read user after update")
    return rec2, is_new


def gs_set_birth_date(user_id: int, birth: date) -> None:
    assert _subs_ws is not None
    row_idx = gs_find_row_by_user_id(user_id)
    if not row_idx:
        return
    row_vals = _subs_ws.row_values(row_idx)
    row_vals += [""] * (len(SUBS_COLUMNS) - len(row_vals))
    row_vals[SUBS_COLUMNS.index("birth_date")] = birth.strftime("%Y-%m-%d")
    _subs_ws.update(f"A{row_idx}", [row_vals], value_input_option="USER_ENTERED")


def gs_set_plan_status(user_id: int, *, status: Optional[str] = None, plan: Optional[str] = None) -> None:
    assert _subs_ws is not None
    row_idx = gs_find_row_by_user_id(user_id)
    if not row_idx:
        return
    row_vals = _subs_ws.row_values(row_idx)
    row_vals += [""] * (len(SUBS_COLUMNS) - len(row_vals))
    if status is not None:
        row_vals[SUBS_COLUMNS.index("status")] = status
    if plan is not None:
        row_vals[SUBS_COLUMNS.index("plan")] = plan
    _subs_ws.update(f"A{row_idx}", [row_vals], value_input_option="USER_ENTERED")


def gs_set_last_full_ym(user_id: int, ym: str) -> None:
    assert _subs_ws is not None
    row_idx = gs_find_row_by_user_id(user_id)
    if not row_idx:
        return
    row_vals = _subs_ws.row_values(row_idx)
    row_vals += [""] * (len(SUBS_COLUMNS) - len(row_vals))
    row_vals[SUBS_COLUMNS.index("last_full_ym")] = ym
    _subs_ws.update(f"A{row_idx}", [row_vals], value_input_option="USER_ENTERED")


def gs_all_users() -> List[UserRec]:
    assert _subs_ws is not None
    rows = _subs_ws.get_all_values()
    if not rows or rows[0] != SUBS_COLUMNS:
        return []
    res: List[UserRec] = []
    for r in rows[1:]:
        r += [""] * (len(SUBS_COLUMNS) - len(r))
        data = dict(zip(SUBS_COLUMNS, r))
        try:
            uid = int(data["telegram_user_id"])
        except Exception:
            continue
        created_at = _parse_dt(data["created_at"]) or _now()
        last_seen_at = _parse_dt(data["last_seen_at"]) or _now()
        trial_expires = _parse_date(data["trial_expires"])
        birth_date = _parse_date(data["birth_date"])
        registered_on = _parse_date(data["registered_on"]) or created_at.date()
        res.append(
            UserRec(
                telegram_user_id=uid,
                status=(data["status"] or STATUS_ACTIVE).strip(),
                plan=(data["plan"] or PLAN_TRIAL).strip(),
                trial_expires=trial_expires,
                birth_date=birth_date,
                created_at=created_at,
                last_seen_at=last_seen_at,
                username=(data["username"] or "").strip(),
                first_name=(data["first_name"] or "").strip(),
                last_name=(data["last_name"] or "").strip(),
                registered_on=registered_on,
                last_full_ym=(data["last_full_ym"] or "").strip(),
            )
        )
    return res


# =========================
# ACCESS LOGIC
# =========================
def is_trial_active(rec: UserRec, today: date) -> bool:
    if rec.plan != PLAN_TRIAL or rec.status != STATUS_ACTIVE:
        return False
    if rec.trial_expires is None:
        return False
    return today <= rec.trial_expires


def is_premium_active(rec: UserRec) -> bool:
    return rec.status == STATUS_ACTIVE and rec.plan == PLAN_PREMIUM


def has_full_access(rec: UserRec, today: date) -> bool:
    return is_premium_active(rec) or is_trial_active(rec, today)


def maybe_autoblock(rec: UserRec, today: date) -> Optional[str]:
    """
    Auto trial -> blocked when expired.
    Returns reason string if changed.
    """
    if rec.status != STATUS_ACTIVE:
        return None
    if rec.plan == PLAN_TRIAL and rec.trial_expires and today > rec.trial_expires:
        return "trial_expired"
    return None


def should_show_full_year_month(rec: UserRec, today: date) -> bool:
    """
    Full Y/M text only:
    - on 1st day of month (once per month) OR
    - on registration day if not 1st (also once per that month)
    tracked by last_full_ym.
    """
    ym = today.strftime("%Y-%m")
    if rec.last_full_ym == ym:
        return False

    if today.day == 1:
        return True

    # day of registration (only in that month)
    if rec.registered_on == today:
        return True

    return False


# =========================
# MESSAGE RENDERING
# =========================
def render_forecast(rec: UserRec, today: date, full_year_month: bool) -> str:
    assert rec.birth_date is not None

    od = sum_digits_of_date(today)
    py = personal_year(rec.birth_date, today)
    pm = personal_month(py, today)
    pd = personal_day(pm, today)

    # OD line + description rules
    od_desc = ""
    if today.day in (10, 20, 30):
        od_desc = TEXTS["unfavorable_day_text"]
    else:
        od_desc = TEXTS["general_day"].get(str(od), "")

    lines: List[str] = []
    lines.append(f"📅 Дата: {today.strftime('%d.%m.%Y')}")
    lines.append("")
    if od_desc:
        lines.append(f"🌐 Сегодня Общий день: {od}. {od_desc}")
    else:
        lines.append(f"🌐 Сегодня Общий день: {od}.")
    lines.append("")
    # Year / Month
    y = TEXTS["personal_year"][str(py)]
    m = TEXTS["personal_month"][str(pm)]
    if full_year_month:
        lines.append(f"🗓 {y['title']}")
        lines.append(y["full"])
        lines.append("")
        lines.append(f"🗓 {m['title']}")
        lines.append(m["full"])
    else:
        # short titles only (as in your example “Ваш Личный год 3. ...”, but keep consistent)
        lines.append(f"🗓 {y['title']}")
        lines.append(f"🗓 {m['title']}")
    lines.append("")
    # Personal day always expanded
    d = TEXTS["personal_day"][str(pd)]
    lines.append(f"🔢 {d['title']}")
    if d["full"]:
        lines.append(d["full"])

    # Access footer
    if is_premium_active(rec):
        lines.append("")
        lines.append("⭐️ Premium активен: полный прогноз доступен + ежедневка 09:00.")
    elif is_trial_active(rec, today):
        lines.append("")
        lines.append(f"🎁 Trial активен до {rec.trial_expires.strftime('%d.%m.%Y')}: полный прогноз доступен + ежедневка 09:00.")
    return "\n".join(lines).strip()


def render_profile(rec: UserRec, today: date) -> str:
    bd = rec.birth_date.strftime("%d.%m.%Y") if rec.birth_date else "не задана"
    te = rec.trial_expires.strftime("%d.%m.%Y") if rec.trial_expires else "-"
    return (
        "👤 Профиль\n\n"
        f"ID: {rec.telegram_user_id}\n"
        f"Статус: {rec.status}\n"
        f"План: {rec.plan}\n"
        f"Trial до: {te}\n"
        f"Дата рождения: {bd}\n"
    ).strip()


# =========================
# ADMIN NOTIFY
# =========================
async def notify_admins(app: Application, text: str) -> None:
    if not ADMIN_CHAT_IDS:
        return
    for admin_id in ADMIN_CHAT_IDS:
        try:
            await app.bot.send_message(chat_id=admin_id, text=text)
        except Exception:
            log.exception("Failed to notify admin %s", admin_id)


# =========================
# HANDLERS
# =========================
def parse_birth_date(text: str) -> Optional[date]:
    text = (text or "").strip()
    m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", text)
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    rec, is_new = gs_upsert_user_from_update(update)
    today = _today()

    # admin notify new
    if is_new:
        u = update.effective_user
        await notify_admins(
            context.application,
            f"🆕 Новый пользователь: id={u.id}, username=@{u.username or '-'} {u.first_name or ''} {u.last_name or ''}\n"
            f"Trial до: {(today + timedelta(days=TRIAL_DAYS)).strftime('%d.%m.%Y')}",
        )

    # autoblock if trial expired
    reason = maybe_autoblock(rec, today)
    if reason == "trial_expired":
        gs_set_plan_status(rec.telegram_user_id, status=STATUS_BLOCKED, plan=PLAN_BLOCKED)
        rec = gs_get_user(rec.telegram_user_id) or rec

    if rec.status != STATUS_ACTIVE:
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору.",
            reply_markup=MENU,
        )
        return ConversationHandler.END

    # need birth date
    if rec.birth_date is None:
        await update.message.reply_text(
            "Введите дату рождения в формате *ДД.ММ.ГГГГ*\nПример: 05.03.1994",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=MENU,
        )
        return BIRTHDATE_STATE

    # show forecast immediately
    full_ym = should_show_full_year_month(rec, today)
    if full_ym:
        gs_set_last_full_ym(rec.telegram_user_id, today.strftime("%Y-%m"))
    text = render_forecast(rec, today, full_year_month=full_ym)
    await update.message.reply_text(text, reply_markup=MENU)
    return ConversationHandler.END


async def set_birth_date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    rec, _ = gs_upsert_user_from_update(update)
    today = _today()

    if rec.status != STATUS_ACTIVE:
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору.",
            reply_markup=MENU,
        )
        return ConversationHandler.END

    bd = parse_birth_date(update.message.text)
    if not bd:
        await update.message.reply_text("Неверный формат. Введите так: 05.03.1994")
        return BIRTHDATE_STATE

    try:
        gs_set_birth_date(rec.telegram_user_id, bd)
    except Exception:
        log.exception("Failed to save birth_date to Google Sheets")
        await update.message.reply_text("❌ Не смог сохранить дату рождения. Проверь доступ к Google Sheets.")
        return BIRTHDATE_STATE

    rec2 = gs_get_user(rec.telegram_user_id)
    if rec2 is None or rec2.birth_date is None:
        await update.message.reply_text("❌ Не смог сохранить дату рождения. Проверь доступ к Google Sheets.")
        return BIRTHDATE_STATE

    full_ym = should_show_full_year_month(rec2, today)
    if full_ym:
        gs_set_last_full_ym(rec2.telegram_user_id, today.strftime("%Y-%m"))

    text = render_forecast(rec2, today, full_year_month=full_ym)
    await update.message.reply_text(text, reply_markup=MENU)
    return ConversationHandler.END


async def on_menu_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rec, _ = gs_upsert_user_from_update(update)
    today = _today()

    reason = maybe_autoblock(rec, today)
    if reason == "trial_expired":
        gs_set_plan_status(rec.telegram_user_id, status=STATUS_BLOCKED, plan=PLAN_BLOCKED)
        rec = gs_get_user(rec.telegram_user_id) or rec

    if rec.status != STATUS_ACTIVE:
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору.",
            reply_markup=MENU,
        )
        return

    if rec.birth_date is None:
        await update.message.reply_text(
            "Введите дату рождения в формате *ДД.ММ.ГГГГ*\nПример: 05.03.1994",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=MENU,
        )
        return

    full_ym = False
    if has_full_access(rec, today):
        full_ym = should_show_full_year_month(rec, today)
        if full_ym:
            gs_set_last_full_ym(rec.telegram_user_id, today.strftime("%Y-%m"))

    text = render_forecast(rec, today, full_year_month=full_ym)
    await update.message.reply_text(text, reply_markup=MENU)


async def on_menu_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rec, _ = gs_upsert_user_from_update(update)
    today = _today()

    reason = maybe_autoblock(rec, today)
    if reason == "trial_expired":
        gs_set_plan_status(rec.telegram_user_id, status=STATUS_BLOCKED, plan=PLAN_BLOCKED)
        rec = gs_get_user(rec.telegram_user_id) or rec

    if rec.status != STATUS_ACTIVE:
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору.",
            reply_markup=MENU,
        )
        return

    await update.message.reply_text(render_profile(rec, today), reply_markup=MENU)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Keep the bot alive on errors
    try:
        raise context.error
    except Exception as e:
        log.exception("Unhandled error: %s", e)


# =========================
# DAILY BROADCAST (Premium + active trial)
# =========================
async def daily_broadcast(app: Application) -> None:
    try:
        gs_init()
    except Exception:
        log.exception("daily_broadcast: gs_init failed")
        return

    today = _today()
    users = gs_all_users()
    for rec in users:
        # auto trial -> blocked
        reason = maybe_autoblock(rec, today)
        if reason == "trial_expired":
            gs_set_plan_status(rec.telegram_user_id, status=STATUS_BLOCKED, plan=PLAN_BLOCKED)
            continue

        if rec.status != STATUS_ACTIVE:
            continue
        if rec.birth_date is None:
            continue

        if not has_full_access(rec, today):
            continue

        full_ym = should_show_full_year_month(rec, today)
        if full_ym:
            gs_set_last_full_ym(rec.telegram_user_id, today.strftime("%Y-%m"))

        text = render_forecast(rec, today, full_year_month=full_ym)
        try:
            await app.bot.send_message(chat_id=rec.telegram_user_id, text=text, reply_markup=MENU)
        except Exception:
            log.exception("daily_broadcast: failed to send to %s", rec.telegram_user_id)


def schedule_jobs(app: Application) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=TZ)
    # 09:00 local time
    scheduler.add_job(lambda: asyncio.create_task(daily_broadcast(app)), "cron", hour=9, minute=0, id="daily_broadcast")
    scheduler.start()
    return scheduler


# =========================
# MAIN
# =========================
def main() -> None:
    # init sheets early to fail fast if misconfigured
    try:
        gs_init()
        log.info("Google Sheets connected OK")
    except Exception as e:
        log.warning("Google Sheets not ready at startup: %s", e)

    app = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            BIRTHDATE_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_birth_date_handler)],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    app.add_handler(MessageHandler(filters.Regex(r"^📅 Прогноз на сегодня$"), on_menu_today))
    app.add_handler(MessageHandler(filters.Regex(r"^👤 Профиль$"), on_menu_profile))

    app.add_error_handler(error_handler)

    # jobs
    schedule_jobs(app)

    log.info("Bot started")
    app.run_polling(
        close_loop=False,
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
