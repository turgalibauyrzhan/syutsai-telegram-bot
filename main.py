import os

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL")
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

print("BOT_TOKEN:", bool(BOT_TOKEN))
print("PUBLIC_URL:", bool(PUBLIC_URL))
print("SHEET_ID:", bool(SHEET_ID))
print("GOOGLE_CREDS_JSON:", bool(GOOGLE_CREDS_JSON))
