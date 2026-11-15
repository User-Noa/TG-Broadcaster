import argparse
import asyncio
import datetime
import random
import socket
import sys
import socks
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

session_str = os.getenv(
    "SESSION_STR",
    "1BJWap1wBuz4d1kM7_38ZkKTif6Fc36eJs3SOlnuZ7y5UrBTbRQv8hcPa_gbOlBzzCsggVsRqaB7kk7S8MTMPqR6XJzcbJ6EF2jODP-kTcXWHJmtfvFqE9sf_1tZR6KGgxTCPS7NEVOpIWOpym1JzhJFdTq1buBPnBD5eEt2LwktmiBDCxOAeVHHC4e2Ng2k9jr9MCNCaYAEVu4fdb7k6IKfEAWxwE71alqK2UfOLfot8ZA2UfptnlwzfZM7_HSystrdV8k4l2WLztBiQKJhYt5pofzQ1cviEBgrF6Yy-0YHaM3Zvc8SfFVR1RhOYWCKfyZ-opC_w29-z_G8fXnsL5I1LXg1GP3w="
)
api_id = int(os.getenv("API_ID", "2040"))
api_hash = os.getenv("API_HASH", "b18441a1b60875e30523c4b28f5319e6")
proxy = (socks.SOCKS5, '127.0.0.1', 10808)

# Daily Schedule Settings: Random time window each day (e.g. between 10:00 and 21:00)
RANDOM_HOUR_START = 10  # 10:00 AM
RANDOM_HOUR_END = 21    # 09:00 PM

# Auto-Seen Settings: Interval (in seconds) to sweep all dialogs and clear any unread messages
SWEEP_INTERVAL_SECONDS = 300  # Sweep every 5 minutes

def check_proxy():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.5)
    try:
        s.connect(('127.0.0.1', 10808))
        s.close()
        print("[✓] SOCKS5 proxy is ACTIVE and VERIFIED on 127.0.0.1:10808")
        return True
    except Exception as e:
        print(f"[!] SOCKS5 proxy error (127.0.0.1:10808): {e}")
        return False

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

async def load_target_messages(client):
    recent = await client.get_messages('me', limit=10)
    text_msgs = [m for m in recent if m.text and not m.media]
    
    if len(text_msgs) < 2:
        raise ValueError(f"Found only {len(text_msgs)} text messages in Saved Messages; expected at least 2.")

    msg_with_link = next((m for m in text_msgs if message_has_links(m)), None)
    msg_no_link = next((m for m in text_msgs if not message_has_links(m)), None)

    if not msg_with_link or not msg_no_link:
        print("[!] Notice: Using the two most recent text messages.")
        msg_no_link, msg_with_link = text_msgs[0], text_msgs[1]

    return msg_with_link, msg_no_link

async def collect_targets(client):
    targets = []
    seen_ids = set()

    # 1. Contacts from address book
    try:
        contacts_res = await client(GetContactsRequest(hash=0))
        for u in contacts_res.users:
            if u.is_self or u.bot or getattr(u, 'deleted', False):
                continue
            seen_ids.add(u.id)
            name = f"{u.first_name or ''} {u.last_name or ''}".strip()
            targets.append((f"Contact: {name or 'User'}", u, 'private'))
    except Exception as e:
        print(f"Notice: Failed to fetch contacts list ({e}), continuing with dialogs.")

    # 2. Dialogs (Direct Chats, Groups, Channels)
    dialogs = await client.get_dialogs()
    for d in dialogs:
        # Skip deactivated or migrated chats (completely avoids PeerIdInvalidError)
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

    return targets

# --- AUTO-SEEN (سین خودکار کامل اکانت) ---

async def mark_all_dialogs_read(client, verbose=True):
    """Scans all dialogs and marks unread messages, mentions, and reactions as READ."""
    try:
        dialogs = await client.get_dialogs()
        unreads = [d for d in dialogs if (d.unread_count > 0 or d.unread_mentions_count > 0)]
        if not unreads:
            if verbose:
                print("[👀] Auto-Seen: All chats are currently read (0 unread).")
            return 0

        if verbose:
            print(f"[👀] Auto-Seen: Found {len(unreads)} dialog(s) with unread messages. Marking as READ...")

        sem = asyncio.Semaphore(5)

        async def mark_single(d):
            async with sem:
                try:
                    await client.send_read_acknowledge(d.entity, clear_mentions=True, clear_reactions=True)
                    return True
                except Exception:
                    return False

        results = await asyncio.gather(*(mark_single(d) for d in unreads))
        cleared = sum(1 for r in results if r)

        if verbose:
            print(f"[👀] Auto-Seen: Successfully marked {cleared}/{len(unreads)} dialog(s) as READ.")
        return cleared
    except Exception as e:
        if verbose:
            print(f"[!] Auto-Seen error: {e}")
        return 0

def register_realtime_auto_read(client):
    """Listens for all incoming messages in real-time and marks them as read immediately."""
    @client.on(events.NewMessage)
    async def on_new_message(event):
        try:
            # Human-like subtle delay (0.3s - 0.7s) before marking read
            await asyncio.sleep(random.uniform(0.3, 0.7))
            await event.message.mark_read()
        except Exception:
            pass

    print("[👀] Real-Time Auto-Seen is ACTIVE (All incoming messages in all chats are read instantly).")

async def auto_seen_sweep_worker(client):
    """Periodically sweeps all dialogs to ensure zero unread messages across the entire account."""
    while True:
        await asyncio.sleep(SWEEP_INTERVAL_SECONDS)
        await mark_all_dialogs_read(client, verbose=False)

# --- BROADCAST EXECUTION ---

async def run_broadcast(client):
    try:
        # Step 1: Detect and load link / no-link messages
        msg_with_link, msg_no_link = await load_target_messages(client)
        print(f"\n[✓] Loaded Text Messages from Saved Messages:")
        print(f"  • Link Message (ID {msg_with_link.id}, {len(msg_with_link.text)} chars) -> For Contacts, Direct Chats & Channels")
        print(f"  • No-Link Message (ID {msg_no_link.id}, {len(msg_no_link.text)} chars) -> For Groups & Supergroups")

        # Step 2: Collect valid targets
        targets = await collect_targets(client)
        private_count = sum(1 for t in targets if t[2] == 'private')
        group_count = sum(1 for t in targets if t[2] == 'group')
        print(f"\nTotal targets to send: {len(targets)} ({private_count} Private/Channels, {group_count} Groups)")

        success = 0
        failed = 0

        # Step 3: Broadcast with intelligent target routing
        for idx, (label, entity, kind) in enumerate(targets, start=1):
            target_msg = msg_no_link if kind == 'group' else msg_with_link
            link_preview = (kind != 'group')

            try:
                print(f"[{idx}/{len(targets)}] Sending to {label} ({kind})...", end=" ", flush=True)
                await client.send_message(
                    entity,
                    message=target_msg.text,
                    formatting_entities=target_msg.entities,
                    link_preview=link_preview
                )
                print("OK")
                success += 1
            except FloodWaitError as e:
                print(f"FLOOD WAIT ({e.seconds}s). Waiting...")
                await asyncio.sleep(e.seconds + 1)
                try:
                    await client.send_message(
                        entity,
                        message=target_msg.text,
                        formatting_entities=target_msg.entities,
                        link_preview=link_preview
                    )
                    print("OK (after wait)")
                    success += 1
                except Exception as e2:
                    print(f"FAILED: {e2}")
                    failed += 1
            except (ChatWriteForbiddenError, UserBannedInChannelError, UserIsBlockedError) as e:
                print(f"SKIPPED ({type(e).__name__})")
                failed += 1
            except PeerFloodError:
                print("\n[!] PEER FLOOD ERROR: Telegram stopped further messages for this session.")
                break
            except Exception as e:
                print(f"FAILED: {type(e).__name__}: {e}")
                failed += 1

            # Safe human delay (4.0 to 7.0 seconds)
            await asyncio.sleep(random.uniform(4.0, 7.0))

        print(f"\n[✓] Broadcast completed! Success: {success}, Failed/Skipped: {failed}")
        return True
    except Exception as e:
        print(f"[!] Broadcast execution error: {e}")
        return False

# --- SCHEDULING ---

def get_next_random_run_time(start_hour=RANDOM_HOUR_START, end_hour=RANDOM_HOUR_END, force_tomorrow=True):
    now = datetime.datetime.now()
    if force_tomorrow:
        target_date = now.date() + datetime.timedelta(days=1)
        rand_hour = random.randint(start_hour, end_hour)
    else:
        if now.hour < end_hour:
            target_date = now.date()
            min_hour = max(start_hour, now.hour + 1)
            rand_hour = random.randint(min_hour, end_hour)
        else:
            target_date = now.date() + datetime.timedelta(days=1)
            rand_hour = random.randint(start_hour, end_hour)

    rand_minute = random.randint(0, 59)
    rand_second = random.randint(0, 59)
    target = datetime.datetime(
        target_date.year, target_date.month, target_date.day,
        rand_hour, rand_minute, rand_second
    )
    return target

async def wait_until(target_time):
    while True:
        now = datetime.datetime.now()
        remaining = (target_time - now).total_seconds()
        if remaining <= 0:
            break
        sleep_chunk = min(remaining, 60.0)
        await asyncio.sleep(sleep_chunk)

async def scheduler_worker(client, schedule_only):
    if not schedule_only:
        print("=== Starting Initial Broadcast ===")
        await run_broadcast(client)

    while True:
        next_run = get_next_random_run_time(RANDOM_HOUR_START, RANDOM_HOUR_END, force_tomorrow=True)
        now = datetime.datetime.now()
        diff = next_run - now
        hours, remainder = divmod(int(diff.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"\n[🎲] Random schedule selected for next broadcast:")
        print(f"  • Date & Time: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  • Waiting duration: {hours}h {minutes}m {seconds}s")
        print("[⏳] Bot is in standby mode. Auto-Seen is active in background 24/7...")

        await wait_until(next_run)

        print(f"\n=== Daily Broadcast Triggered: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        await run_broadcast(client)

# --- MAIN ---

async def main():
    parser = argparse.ArgumentParser(description="Daily Telegram Broadcaster & Full Auto-Seen")
    parser.add_argument("--once", action="store_true", help="Send once immediately and exit")
    parser.add_argument("--schedule-only", action="store_true", help="Do not send now, activate auto-seen and wait for next random broadcast")
    parser.add_argument("--seen-only", action="store_true", help="Run only the 24/7 auto-seen without sending broadcasts")
    args = parser.parse_args()

    if not check_proxy():
        print("[!] Execution aborted: SOCKS5 proxy is offline.")
        return

    client = TelegramClient(StringSession(session_str), api_id, api_hash, proxy=proxy)
    await client.connect()

    if not await client.is_user_authorized():
        print("ERROR: Unauthorized session.")
        await client.disconnect()
        return

    try:
        if args.once:
            print("=== Running Single Broadcast ===")
            await run_broadcast(client)
            return

        # 1. Activate Full Auto-Seen
        print("\n--- Initializing Full Auto-Seen ---")
        register_realtime_auto_read(client)
        await mark_all_dialogs_read(client, verbose=True)

        if args.seen_only:
            print("\n[👀] Running in Auto-Seen ONLY mode. Press Ctrl+C to stop.")
            sweep_task = asyncio.create_task(auto_seen_sweep_worker(client))
            await client.run_until_disconnected()
            return

        # 2. Run both Auto-Seen Sweep and Broadcast Scheduler concurrently
        sweep_task = asyncio.create_task(auto_seen_sweep_worker(client))
        scheduler_task = asyncio.create_task(scheduler_worker(client, args.schedule_only))

        await asyncio.gather(sweep_task, scheduler_task)

    finally:
        await client.disconnect()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Program stopped by user.")
