import os
import json
import base64
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

TZ = ZoneInfo("Asia/Almaty")


def load_sa_info() -> dict:
    """
    Robust parser for GOOGLE_SA_JSON stored in env.
    Handles:
      - normal JSON
      - JSON with escaped newlines (\\n)
      - base64-encoded JSON (optional)
    """
    raw = os.environ.get("GOOGLE_SA_JSON", "")
    if not raw or not raw.strip():
        raise ValueError("GOOGLE_SA_JSON env is empty")

    raw = raw.strip()

    # Try base64 decode if it looks like base64 and not JSON-ish
    # (Optional convenience)
    if raw and raw[0] != "{":
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
            if decoded.strip().startswith("{"):
                return json.loads(decoded)
        except Exception:
            pass

    # Convert escaped newlines into real newlines (common Render issue)
    raw = raw.replace("\\n", "\n")

    return json.loads(raw)


def make_client() -> gspread.Client:
    info = load_sa_info()
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]  # read/write
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def main():
    print("=== Google Sheets DIAGNOSTIC ===")

    gs_id = (os.environ.get("GSHEET_ID") or "").strip()
    if not gs_id:
        print("FAIL: GSHEET_ID is empty in ENV")
        return

    try:
        info = load_sa_info()
        sa_email = info.get("client_email", "(missing client_email)")
        print(f"OK: GOOGLE_SA_JSON parsed. service_account_email={sa_email}")
    except Exception as e:
        print(f"FAIL: GOOGLE_SA_JSON parse error: {type(e).__name__}: {e}")
        return

    try:
        gc = make_client()
        print("OK: gspread client created")
    except Exception as e:
        print(f"FAIL: cannot create gspread client: {type(e).__name__}: {e}")
        return

    try:
        sh = gc.open_by_key(gs_id)
        print(f"OK: opened spreadsheet: title='{sh.title}'")
    except Exception as e:
        print(f"FAIL: cannot open spreadsheet by GSHEET_ID: {type(e).__name__}: {e}")
        print("Hint: check GSHEET_ID is the *ID* (not full URL) and the sheet is shared with service account as Editor.")
        return

    try:
        worksheets = sh.worksheets()
        ws_titles = [w.title for w in worksheets]
        print("Worksheets:", ", ".join(ws_titles) if ws_titles else "(none)")
    except Exception as e:
        print(f"FAIL: cannot list worksheets: {type(e).__name__}: {e}")
        return

    if "subscriptions" not in ws_titles:
        print("FAIL: worksheet 'subscriptions' not found")
        print("Hint: create a sheet tab named EXACTLY: subscriptions")
        return

    try:
        ws = sh.worksheet("subscriptions")
        print("OK: opened worksheet 'subscriptions'")
    except Exception as e:
        print(f"FAIL: cannot open worksheet 'subscriptions': {type(e).__name__}: {e}")
        return

    try:
        headers = ws.row_values(1)
        print("Row1 headers:", " | ".join(headers) if headers else "(empty row1)")
    except Exception as e:
        print(f"FAIL: cannot read header row: {type(e).__name__}: {e}")
        return

    # Minimal required columns (not enforced, but helpful)
    required = ["telegram_user_id", "status", "plan", "access_until"]
    missing = [c for c in required if c not in headers]
    if missing:
        print("WARN: missing recommended columns in row1:", ", ".join(missing))
        print("Recommended row1 headers:")
        print("telegram_user_id | status | plan | access_until | created_at | username | first_name | last_name")

    # Append a test row
    test_user_id = 999999999  # just a marker
    now_str = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

    row_to_append = [
        test_user_id,
        "active",
        "trial",
        "",  # access_until empty (ok)
        now_str,
        "diag",
        "diag",
        "diag",
    ]

    try:
        ws.append_row(row_to_append, value_input_option="USER_ENTERED")
        print("OK: appended test row to 'subscriptions'")
    except Exception as e:
        print(f"FAIL: cannot append row (write access problem): {type(e).__name__}: {e}")
        print("Hint: service account must be shared as Editor, and scopes must allow write.")
        return

    # Verify it's there (simple scan)
    try:
        records = ws.get_all_records()
        found = any(str(r.get("telegram_user_id", "")).strip() == str(test_user_id) for r in records)
        print("VERIFY: test row present:", found)
        if not found:
            print("WARN: row append reported OK but record not found. Check filters/headers or sheet caching.")
    except Exception as e:
        print(f"WARN: appended row but cannot verify via get_all_records: {type(e).__name__}: {e}")

    print("=== DIAG COMPLETE ===")
    print("If everything is OK here, your main bot code should also be able to append rows.")


if __name__ == "__main__":
    main()
