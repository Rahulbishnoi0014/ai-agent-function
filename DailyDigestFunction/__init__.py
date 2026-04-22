import azure.functions as func
import psycopg2
import httpx
import json
import os
import logging
import pytz
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google import genai
from google.genai import types

IST = pytz.timezone('Asia/Kolkata')

# ── DB CONNECTION ─────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

def get_all_connected_users():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, first_name, google_token
        FROM users
        WHERE google_token IS NOT NULL
    """)
    users = cur.fetchall()
    cur.close()
    conn.close()
    return users

# ── GMAIL ─────────────────────────────────────────────────────────────────
def get_recent_emails(token_json: str, max_results: int = 5):
    creds = Credentials.from_authorized_user_info(json.loads(token_json))
    service = build('gmail', 'v1', credentials=creds)
    results = service.users().messages().list(
        userId='me', maxResults=max_results, labelIds=['INBOX']
    ).execute()
    messages = results.get('messages', [])
    emails = []
    for msg in messages:
        full = service.users().messages().get(
            userId='me', id=msg['id'], format='metadata',
            metadataHeaders=['From', 'Subject', 'Date']
        ).execute()
        headers = {h['name']: h['value'] for h in full['payload']['headers']}
        emails.append({
            'from':    headers.get('From', 'Unknown'),
            'subject': headers.get('Subject', 'No subject'),
            'snippet': full.get('snippet', '')
        })
    return emails

# ── CALENDAR ──────────────────────────────────────────────────────────────
def get_todays_events(token_json: str):
    creds = Credentials.from_authorized_user_info(json.loads(token_json))
    service = build('calendar', 'v3', credentials=creds)
    now = datetime.now(IST)
    start = now.replace(hour=0,  minute=0,  second=0).isoformat()
    end   = now.replace(hour=23, minute=59, second=59).isoformat()
    events = service.events().list(
        calendarId='primary',
        timeMin=start, timeMax=end,
        singleEvents=True, orderBy='startTime'
    ).execute()
    return events.get('items', [])

# ── GEMINI SUMMARY ────────────────────────────────────────────────────────
def generate_digest(user_name: str, emails: list, events: list) -> str:
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    now = datetime.now(IST).strftime('%A, %d %B %Y')

    prompt = f"""You are a personal AI assistant sending a morning digest to {user_name}.
Today is {now}.

Emails received:
{json.dumps(emails, indent=2)}

Today's calendar events:
{json.dumps(events, indent=2)}

Write a warm, friendly morning digest message for Telegram.
Include:
- A good morning greeting with today's date
- Brief summary of important emails (max 3)
- Today's schedule from calendar
- A motivational closing line

Keep it concise and use emojis. Make it feel personal."""

    response = client.models.generate_content(
        model="gemini-1.5-flash",
        contents=[{"role": "user", "parts": [{"text": prompt}]}]
    )
    return response.text

# ── SEND TELEGRAM ─────────────────────────────────────────────────────────
async def send_telegram(user_id: int, message: str):
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    async with httpx.AsyncClient() as client:
        await client.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": user_id,
                "text": message,
                "parse_mode": "Markdown"
            }
        )

# ── MAIN FUNCTION ─────────────────────────────────────────────────────────
async def main(timer: func.TimerRequest) -> None:
    now_ist = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')
    logging.info(f"Daily digest triggered at {now_ist}")

    users = get_all_connected_users()
    logging.info(f"Found {len(users)} connected users")

    for user_id, first_name, token_json in users:
        try:
            logging.info(f"Processing digest for user {user_id} ({first_name})")

            emails = get_recent_emails(token_json, 5)
            events = get_todays_events(token_json)

            if not emails and not events:
                logging.info(f"No emails or events for {user_id} — skipping")
                continue

            digest = generate_digest(first_name or "there", emails, events)
            await send_telegram(user_id, digest)

            logging.info(f"Digest sent to {user_id} successfully")

        except Exception as e:
            logging.error(f"Failed for user {user_id}: {str(e)}")
            continue

    logging.info("Daily digest complete!")