import base64

_gs_ok = False
_gs_client = None
_subs_ws = None
_gs_last_fail_at = None  # чтобы не спамить логами

def _safe_preview(s: str) -> str:
    s = s or ""
    s = s.strip()
    if not s:
        return "EMPTY"
    # показываем только первые 12 символов, без утечки
    return f"len={len(s)} head={repr(s[:12])}"

def _load_sa_info_from_env(raw: str) -> dict:
    """
    1) Если это JSON (начинается с '{') -> json.loads
    2) Иначе пробуем base64 -> decode -> json.loads
    """
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("GOOGLE_SA_JSON is empty after strip()")

    if raw.startswith("{"):
        return json.loads(raw)

    # base64 fallback
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        decoded = decoded.strip()
        if not decoded.startswith("{"):
            raise ValueError("base64 decoded but not JSON")
        return json.loads(decoded)
    except Exception as e:
        raise ValueError(f"GOOGLE_SA_JSON is neither JSON nor valid base64 JSON: {type(e).__name__}: {e}")

def gs_init_safe() -> bool:
    global _gs_ok, _gs_client, _subs_ws, _gs_last_fail_at

    if _gs_ok:
        return True

    if not GSHEET_ID:
        log.warning("Google Sheets disabled: GSHEET_ID not set")
        return False

    # анти-спам: если уже падали в последние 60 секунд — не пробуем снова
    now_dt = _now()
    if _gs_last_fail_at and (now_dt - _gs_last_fail_at).total_seconds() < 60:
        return False

    try:
        raw = os.getenv("GOOGLE_SA_JSON", "")
        log.info("GS env check: GSHEET_ID=%s GOOGLE_SA_JSON=%s", GSHEET_ID[:6] + "...", _safe_preview(raw))

        sa_info = _load_sa_info_from_env(raw)

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        _gs_client = gspread.authorize(creds)

        sh = _gs_client.open_by_key(GSHEET_ID)
        _subs_ws = sh.worksheet(SUBS_SHEET_NAME)

        header = _subs_ws.row_values(1)
        if header != SUBS_COLUMNS:
            _subs_ws.resize(rows=max(_subs_ws.row_count, 2), cols=len(SUBS_COLUMNS))
            _subs_ws.update("A1", [SUBS_COLUMNS])

        _gs_ok = True
        log.info("Google Sheets connected OK")
        return True

    except Exception as e:
        _gs_last_fail_at = now_dt
        log.warning("Google Sheets not ready: %s", e)
        return False
