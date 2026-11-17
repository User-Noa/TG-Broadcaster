"""
Production ASGI Server for TG-Broadcaster & Auto-Seen Service
Features:
- Telethon MTProto Client with auto-proxy detection (Cloud direct connect / Local SOCKS5).
- 24/7 Real-Time Auto-Seen + Periodic Sweep.
- Daily Random Daytime Broadcaster (Contacts/Channels with link, Groups without link).
- Anti-sleep Keep-Alive Mesh (4m ping).
- Fast, secure API with password protection (X-Locker-Token).
- Executive Unslop dark-graphite web UI.
"""

import os
import sys
import json
import random
import socket
import asyncio
import datetime
import logging
from typing import Optional, Dict, Any, List
from collections import deque

import aiohttp
import socks
from fastapi import FastAPI, Header, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl import types
from telethon.tl.functions.contacts import GetContactsRequest
from telethon.errors import (
    FloodWaitError,
    ChatWriteForbiddenError,
    UserBannedInChannelError,
    UserIsBlockedError,
    PeerFloodError,
)

from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Logging & Ring Buffer
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("serverize")

IRAN_TZ = ZoneInfo("Asia/Tehran")

LOG_BUFFER = deque(maxlen=200)

def log_event(msg: str, level: str = "INFO"):
    timestamp = datetime.datetime.now(IRAN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    entry = {"time": timestamp, "level": level, "message": msg}
    LOG_BUFFER.appendleft(entry)
    if level == "ERROR":
        logger.error(msg)
    elif level == "WARNING":
        logger.warning(msg)
    else:
        logger.info(msg)

# ---------------------------------------------------------------------------
# Configuration & Secrets
# ---------------------------------------------------------------------------
APP_PASSWORD = os.getenv("APP_PASSWORD", "Mohammad86$")
SESSION_STR = os.getenv("SESSION_STR", "")
API_ID = int(os.getenv("API_ID", "2040"))
API_HASH = os.getenv("API_HASH", "")
PARTNER_URLS = os.getenv("PARTNER_URLS", "").split(",") if os.getenv("PARTNER_URLS") else []

RANDOM_HOUR_START = int(os.getenv("RANDOM_HOUR_START", "10"))
RANDOM_HOUR_END = int(os.getenv("RANDOM_HOUR_END", "21"))
SWEEP_INTERVAL_SECONDS = int(os.getenv("SWEEP_INTERVAL_SECONDS", "300"))

# Auto-detect proxy: on Cloud (Render) direct connection works; on local check 127.0.0.1:10808
def get_proxy_config():
    proxy_env = os.getenv("SOCKS_PROXY")
    if proxy_env:
        host, port = proxy_env.split(":")
        return (socks.SOCKS5, host, int(port))
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.0)
        s.connect(('127.0.0.1', 10808))
        s.close()
        logger.info("[✓] Local SOCKS5 proxy detected on 127.0.0.1:10808")
        return (socks.SOCKS5, '127.0.0.1', 10808)
    except Exception:
        logger.info("[✓] No local proxy found, using Direct Cloud MTProto Connection")
        return None

# ---------------------------------------------------------------------------
# Global State
# ---------------------------------------------------------------------------
state = {
    "boot_time": datetime.datetime.now().isoformat(),
    "client_connected": False,
    "user_id": None,
    "user_name": None,
    "phone": None,
    "broadcast_enabled": True,
    "broadcast_running": False,
    "last_broadcast_time": None,
    "next_broadcast_time": None,
    "total_broadcasts": 0,
    "total_messages_sent": 0,
    "total_messages_seen": 0,
    "auto_seen_active": True,
    "total_targets": 0,
    "private_targets": 0,
    "group_targets": 0,
}

telegram_client: Optional[TelegramClient] = None
broadcast_lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# Telegram Business Logic
# ---------------------------------------------------------------------------
def message_has_links(msg):
    if msg.entities:
        for ent in msg.entities:
            if isinstance(ent, (types.MessageEntityUrl, types.MessageEntityTextUrl)):
                return True
    text = msg.text or ''
    for keyword in ['t.me/', 'http://', 'https://', 'www.', '@']:
        if keyword in text:
            return True
    return False

async def load_target_messages(client: TelegramClient):
    recent = await client.get_messages('me', limit=10)
    text_msgs = [m for m in recent if m.text and not m.media]
    if not text_msgs:
        raise ValueError("هیچ پیام متنی در Saved Messages یافت نشد.")
    return text_msgs[0]

async def collect_targets(client: TelegramClient):
    targets = []
    seen_ids = set()

    # 1. Active Dialogs First (Existing Direct Chats, Groups, Channels)
    dialogs = await client.get_dialogs()
    for d in dialogs:
        if getattr(d.entity, 'deactivated', False) or getattr(d.entity, 'migrated_to', None):
            continue
        if d.id in seen_ids:
            continue
        seen_ids.add(d.id)

        if d.is_user:
            if d.entity.is_self or d.entity.bot or d.id in (777000, 42777):
                continue
            targets.append((f"Chat: {d.title}", d.input_entity, 'private'))
        elif d.is_group:
            targets.append((f"Group: {d.title}", d.input_entity, 'group'))
        elif d.is_channel:
            if not d.entity.broadcast:
                targets.append((f"Supergroup: {d.title}", d.input_entity, 'group'))
            else:
                admin_rights = getattr(d.entity, 'admin_rights', None)
                creator = getattr(d.entity, 'creator', False)
                if creator or (admin_rights and admin_rights.post_messages):
                    targets.append((f"Channel: {d.title}", d.input_entity, 'private'))

    # 2. Remaining Contacts from address book
    try:
        contacts_res = await client(GetContactsRequest(hash=0))
        for u in contacts_res.users:
            if u.is_self or u.bot or getattr(u, 'deleted', False):
                continue
            if u.id in seen_ids:
                continue
            seen_ids.add(u.id)
            name = f"{u.first_name or ''} {u.last_name or ''}".strip()
            targets.append((f"Contact: {name or 'User'}", u, 'private'))
    except Exception as e:
        log_event(f"Notice: Failed to fetch contacts list: {e}", "WARNING")

    state["total_targets"] = len(targets)
    state["private_targets"] = sum(1 for t in targets if t[2] == 'private')
    state["group_targets"] = sum(1 for t in targets if t[2] == 'group')
    return targets

async def execute_broadcast():
    if not telegram_client or not telegram_client.is_connected():
        log_event("Cannot broadcast: Telegram client disconnected.", "ERROR")
        return {"success": False, "error": "Client disconnected"}

    if broadcast_lock.locked():
        log_event("Broadcast is already in progress, request ignored.", "WARNING")
        return {"success": False, "error": "Broadcast already running"}

    async with broadcast_lock:
        state["broadcast_running"] = True
        log_event("🚀 Starting Telegram broadcast cycle...")
        try:
            target_msg = await load_target_messages(telegram_client)
            targets = await collect_targets(telegram_client)
            
            KEYWORD = "کسانی که از این شخص خرید داشتند"
            success_count = 0
            fail_count = 0

            for idx, (label, entity, kind) in enumerate(targets, start=1):
                try:
                    # Check recent messages in this target to see if already sent
                    recent_msgs = []
                    try:
                        recent_msgs = await telegram_client.get_messages(entity, limit=5)
                    except Exception:
                        pass

                    matching_msgs = [m for m in recent_msgs if m.out and m.text and KEYWORD in m.text]

                    if matching_msgs:
                        for m in matching_msgs:
                            await telegram_client.edit_message(
                                entity,
                                m.id,
                                text=target_msg.text,
                                formatting_entities=target_msg.entities,
                                link_preview=False
                            )
                        success_count += 1
                    else:
                        await telegram_client.send_message(
                            entity,
                            message=target_msg.text,
                            formatting_entities=target_msg.entities,
                            link_preview=False
                        )
                        success_count += 1
                except FloodWaitError as e:
                    log_event(f"Flood wait {e.seconds}s on {label}", "WARNING")
                    await asyncio.sleep(e.seconds + 1)
                    try:
                        await telegram_client.send_message(
                            entity,
                            message=target_msg.text,
                            formatting_entities=target_msg.entities,
                            link_preview=False
                        )
                        success_count += 1
                    except Exception:
                        fail_count += 1
                except (ChatWriteForbiddenError, UserBannedInChannelError, UserIsBlockedError) as e:
                    fail_count += 1
                except PeerFloodError:
                    log_event(f"PeerFloodError: skipped non-mutual contact {label}", "WARNING")
                    fail_count += 1
                except Exception as e:
                    fail_count += 1

                await asyncio.sleep(random.uniform(4.0, 7.0))

            state["total_broadcasts"] += 1
            state["total_messages_sent"] += success_count
            state["last_broadcast_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_event(f"✓ Broadcast completed: {success_count} succeeded, {fail_count} skipped/failed.")
            return {"success": True, "sent": success_count, "failed": fail_count}
        except Exception as e:
            log_event(f"Broadcast failed with unexpected error: {e}", "ERROR")
            return {"success": False, "error": str(e)}
        finally:
            state["broadcast_running"] = False

# --- Auto-Seen Handlers ---

async def mark_all_dialogs_read(verbose: bool = False):
    if not telegram_client or not telegram_client.is_connected():
        return 0
    try:
        dialogs = await telegram_client.get_dialogs()
        unreads = [d for d in dialogs if (d.unread_count > 0 or d.unread_mentions_count > 0)]
        if not unreads:
            if verbose:
                log_event("Auto-Seen: All chats are read.")
            return 0

        sem = asyncio.Semaphore(5)
        async def mark_one(d):
            async with sem:
                try:
                    await telegram_client.send_read_acknowledge(d.entity, clear_mentions=True, clear_reactions=True)
                    return True
                except Exception:
                    return False

        results = await asyncio.gather(*(mark_one(d) for d in unreads))
        cleared = sum(1 for r in results if r)
        state["total_messages_seen"] += cleared
        if verbose or cleared > 0:
            log_event(f"👀 Auto-Seen sweep: Cleared {cleared}/{len(unreads)} unread dialog(s).")
        return cleared
    except Exception as e:
        log_event(f"Auto-Seen sweep error: {e}", "WARNING")
        return 0

def register_realtime_auto_seen(client: TelegramClient):
    @client.on(events.NewMessage)
    async def on_new_msg(event):
        try:
            await asyncio.sleep(random.uniform(0.3, 0.7))
            await event.message.mark_read()
            state["total_messages_seen"] += 1
        except Exception:
            pass
    log_event("👀 Real-Time Auto-Seen registered and listening.")

# --- Background Workers ---

def calculate_next_random_time(force_tomorrow: bool = True):
    now = datetime.datetime.now(IRAN_TZ)
    if force_tomorrow or now.hour >= RANDOM_HOUR_END:
        target_date = now.date() + datetime.timedelta(days=1)
        rand_hour = random.randint(RANDOM_HOUR_START, RANDOM_HOUR_END)
    else:
        target_date = now.date()
        min_hour = max(RANDOM_HOUR_START, now.hour + 1)
        rand_hour = random.randint(min_hour, RANDOM_HOUR_END)

    rand_minute = random.randint(0, 59)
    rand_second = random.randint(0, 59)
    return datetime.datetime(
        target_date.year, target_date.month, target_date.day,
        rand_hour, rand_minute, rand_second,
        tzinfo=IRAN_TZ
    )

async def auto_seen_sweep_loop():
    while True:
        await asyncio.sleep(SWEEP_INTERVAL_SECONDS)
        if state["auto_seen_active"]:
            await mark_all_dialogs_read(verbose=False)

async def broadcast_scheduler_loop():
    await asyncio.sleep(15)  # Initial settle delay
    while True:
        next_run = calculate_next_random_time(force_tomorrow=False)
        state["next_broadcast_time"] = next_run.strftime("%Y-%m-%d %H:%M:%S (Iran Time)")
        log_event(f"🎲 Next random broadcast scheduled for: {state['next_broadcast_time']}")

        while datetime.datetime.now(IRAN_TZ) < next_run:
            await asyncio.sleep(30)

        if state["broadcast_enabled"]:
            log_event(f"⏰ Daily random broadcast triggered at {datetime.datetime.now(IRAN_TZ).strftime('%H:%M:%S')}")
            await execute_broadcast()
        else:
            log_event("Broadcast schedule arrived but broadcaster is currently disabled by user.", "WARNING")

        # Sleep a bit before scheduling tomorrow's run
        await asyncio.sleep(60)

async def keep_alive_mesh_loop():
    """Mutual 24/7 keep-alive mesh across Render services to prevent 15-minute idle spin-down."""
    targets = [
        "https://tg-broadcaster-k8ur.onrender.com/health",
        "https://tg-locker.onrender.com/health",
    ]
    custom_url = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("PING_URL")
    if custom_url:
        t = f"{custom_url.rstrip('/')}/health"
        if t not in targets:
            targets.append(t)
    for p in PARTNER_URLS:
        p_clean = p.strip()
        if p_clean:
            url_to_add = f"{p_clean.rstrip('/')}/health" if not p_clean.endswith("/health") else p_clean
            if url_to_add not in targets:
                targets.append(url_to_add)

    log_event(f"⏰ 24/7 Keep-Alive mesh active for: {targets}")
    await asyncio.sleep(15)

    while True:
        await asyncio.sleep(240)
        async with aiohttp.ClientSession() as session:
            for target in targets:
                try:
                    async with session.get(target, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        logger.info(f"🔔 Keep-alive ping -> {target} [HTTP {resp.status}]")
                except Exception as e:
                    logger.debug(f"Keep-alive ping error ({target}): {e}")

# ---------------------------------------------------------------------------
# FastAPI App & Lifespan
# ---------------------------------------------------------------------------
app = FastAPI(title="TG-Broadcaster Cloud Engine")

os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.on_event("startup")
async def on_startup():
    global telegram_client
    log_event("Initializing TG-Broadcaster ASGI Server...")

    proxy_cfg = get_proxy_config()
    telegram_client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH, proxy=proxy_cfg)
    try:
        await telegram_client.connect()
        if await telegram_client.is_user_authorized():
            me = await telegram_client.get_me()
            state["client_connected"] = True
            state["user_id"] = me.id
            state["user_name"] = f"{me.first_name or ''} {me.last_name or ''}".strip()
            state["phone"] = getattr(me, 'phone', 'N/A')
            log_event(f"✓ Telegram Authorized as: {state['user_name']} (+{state['phone']})")

            # Setup Auto-Seen
            register_realtime_auto_seen(telegram_client)
            await mark_all_dialogs_read(verbose=True)

            # Initial target count
            await collect_targets(telegram_client)
        else:
            log_event("ERROR: Telegram Session is not authorized!", "ERROR")
    except Exception as e:
        log_event(f"Telegram connection error: {e}", "ERROR")

    # Start Background Workers
    asyncio.create_task(auto_seen_sweep_loop())
    asyncio.create_task(broadcast_scheduler_loop())
    asyncio.create_task(keep_alive_mesh_loop())

@app.on_event("shutdown")
async def on_shutdown():
    global telegram_client
    if telegram_client and telegram_client.is_connected():
        await telegram_client.disconnect()
        log_event("Telegram client disconnected cleanly.")

# ---------------------------------------------------------------------------
# Auth Middleware / Dependency
# ---------------------------------------------------------------------------
def require_auth(x_locker_token: Optional[str] = Header(None)):
    if not x_locker_token or x_locker_token != APP_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid security token.")

# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    index_path = os.path.join("static", "index.html")
    if os.path.isfile(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h2>TG-Broadcaster Engine Ready (static/index.html missing)</h2>")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "client_connected": state["client_connected"],
        "broadcast_running": state["broadcast_running"],
        "boot_time": state["boot_time"]
    }

@app.post("/api/auth/login")
async def login(request: Request):
    try:
        body = await request.json()
        pwd = body.get("password", "")
    except Exception:
        pwd = ""
    if pwd == APP_PASSWORD:
        return {"status": "ok", "token": APP_PASSWORD}
    raise HTTPException(status_code=401, detail="Invalid password.")

@app.get("/api/auth/status")
async def auth_status(x_locker_token: Optional[str] = Header(None)):
    is_valid = (x_locker_token == APP_PASSWORD)
    return {"authenticated": is_valid}

@app.get("/api/status")
async def get_status(x_locker_token: Optional[str] = Header(None)):
    require_auth(x_locker_token)
    return {
        "account": {
            "name": state["user_name"],
            "id": state["user_id"],
            "phone": state["phone"],
            "connected": state["client_connected"]
        },
        "metrics": {
            "broadcast_enabled": state["broadcast_enabled"],
            "broadcast_running": state["broadcast_running"],
            "total_broadcasts": state["total_broadcasts"],
            "total_sent": state["total_messages_sent"],
            "total_seen": state["total_messages_seen"],
            "last_broadcast": state["last_broadcast_time"],
            "next_broadcast": state["next_broadcast_time"],
            "targets_count": state["total_targets"],
            "private_count": state["private_targets"],
            "group_count": state["group_targets"]
        }
    }

@app.post("/api/broadcast/trigger")
async def trigger_broadcast(background_tasks: BackgroundTasks, x_locker_token: Optional[str] = Header(None)):
    require_auth(x_locker_token)
    if state["broadcast_running"]:
        return JSONResponse(status_code=400, content={"status": "error", "message": "Broadcast already running."})
    
    background_tasks.add_task(execute_broadcast)
    return {"status": "ok", "message": "Broadcast cycle started in background."}

@app.post("/api/broadcast/toggle")
async def toggle_broadcast(request: Request, x_locker_token: Optional[str] = Header(None)):
    require_auth(x_locker_token)
    body = await request.json()
    enabled = bool(body.get("enabled", True))
    state["broadcast_enabled"] = enabled
    log_event(f"Broadcaster schedule toggled: {'ENABLED' if enabled else 'DISABLED'}")
    return {"status": "ok", "broadcast_enabled": state["broadcast_enabled"]}

@app.post("/api/seen/sweep")
async def trigger_sweep(background_tasks: BackgroundTasks, x_locker_token: Optional[str] = Header(None)):
    require_auth(x_locker_token)
    background_tasks.add_task(mark_all_dialogs_read, True)
    return {"status": "ok", "message": "Full seen sweep triggered."}

@app.get("/api/logs")
async def get_logs(x_locker_token: Optional[str] = Header(None)):
    require_auth(x_locker_token)
    return {"logs": list(LOG_BUFFER)}
