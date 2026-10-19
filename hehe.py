"""
Personal Telegram CHANNEL forwarder — bot-token version, configurable
entirely by chatting with the bot (no editing config in the file).

How this works:
  1. You create a bot via @BotFather and get a BOT_TOKEN.
  2. You add that bot as an ADMIN (with "post messages" permission) to
     both your source channel and your target channel.
  3. You message the bot directly (in its private chat) with commands
     to set source/target channels, start/stop forwarding, etc.
  4. Settings are saved to a local JSON file so they survive restarts.

The bot token alone only needs channels where it's an admin, and never
asks anyone for a phone number or code. Optionally, YOU (and only the
OWNER_ID you configure) can also run /login to authenticate your own
account in this same chat — that unlocks history backfill (/fwall,
/fwallrecent), which a bot token can't do. That login flow only ever
runs for OWNER_ID; it is not something other users of the bot can
trigger. The resulting session is stored locally in forwarder_state.json
and never sent anywhere else — treat that file like a password.

Setup:
    pip install telethon

    1. @BotFather -> /newbot -> get your BOT_TOKEN
    2. Get API_ID / API_HASH from https://my.telegram.org
    3. Find your own numeric Telegram id (e.g. message @userinfobot)
       and set OWNER_ID below — only this id can control the bot.
    4. Fill in API_ID / API_HASH / BOT_TOKEN / OWNER_ID.
    5. Run: python forwarder_bot.py
    6. Open a private chat with your bot and send /menu

Commands (only usable by OWNER_ID, in the bot's private chat):
    /menu                  Show this command list
    /setsource <channel>   Add a source channel (e.g. @mychannel)
    /settarget <channel>   Add a target channel
    /rmsource <channel>    Remove a source channel
    /rmtarget <channel>    Remove a target channel
    /status                Show current sources/targets and state
    /startfwd              Turn live forwarding ON
    /stopfwd               Turn live forwarding OFF
    /clear                 Wipe all sources/targets and stop forwarding
    /login                 Log in to YOUR OWN account (for history backfill)
    /loginstatus           Check if you're logged in
    /logout                Forget your logged-in session
    /fwall                 Forward ALL history, source[0] -> target[0]
    /fwallrecent <n>       Forward last n messages, source[0] -> target[0]
<channel> can be @username, a numeric id, or a t.me link.
"""

import asyncio
import os
import json
import random
import logging
import time

from telethon import TelegramClient, events, functions, utils
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.sessions import StringSession

logging.basicConfig(
    format="[%(levelname)s/%(asctime)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ─── CONFIG (edit these once) ─────────────────────────────────────────
API_ID = int(os.environ.get("TG_API_ID", ))
API_HASH = os.environ.get("TG_API_HASH", )
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", )

# Your own numeric Telegram user id. Only this user can control the
# bot's commands. Find yours via @userinfobot.
OWNER_ID = int(os.environ.get("TG_OWNER_ID", "8396796489"))
STATE_FILE = "forwarder_state.json"
# ────────────────────────────────────────────────────────────────────

bot = TelegramClient("forwarder_bot_session", API_ID, API_HASH)

# In-memory state, mirrored to STATE_FILE
state = {
    "sources": [],         # list of resolved numeric chat ids
    "targets": [],         # list of resolved numeric chat ids
    "source_labels": {},   # id -> original text you typed, for display
    "target_labels": {},
    "active": False,
    "user_session": None,   # your own account's session string, once logged in
}

# Holds the in-progress login client + step while you're going through
# /login. Only ever used for OWNER_ID — never for anyone else.
login_flow = {"step": None, "client": None, "phone": None, "phone_code_hash": None}

# The authenticated client for YOUR account, once /login succeeds.
# Only used for things a bot token can't do, like reading old history.
user_client = None


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
            state.update(data)
        except Exception as exc:
            log.error("Could not load state file: %s", exc)


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as exc:
        log.error("Could not save state file: %s", exc)


def owner_only(func):
    async def wrapper(event):
        if event.sender_id != OWNER_ID:
            return  # silently ignore anyone else
        return await func(event)
    return wrapper


async def resolve_one(arg):
    arg = arg.strip()
    if "t.me/" in arg:
        arg = "@" + arg.split("t.me/")[-1].split("?")[0].strip("/")
    client = user_client if user_client is not None else bot
    try:
        if arg.lstrip("-").isdigit():
            ent = await client.get_entity(int(arg))
        else:
            ent = await client.get_entity(arg)
        # IMPORTANT: use the marked peer id (e.g. -100xxxxxxxxxx for
        # channels), the same form event.chat_id reports — not the raw
        # ent.id — or live-message matching silently never fires.
        return utils.get_peer_id(ent)
    except Exception as exc:
        log.error("Resolve failed for %r: %s", arg, exc)
        return None


def _explain_forward_error(exc):
    name = type(exc).__name__
    if "CHAT_WRITE_FORBIDDEN" in name or "CHAT_WRITE_FORBIDDEN" in str(exc):
        return "You don't have permission to post in the target — it needs admin/post rights there."
    if "CHAT_FORWARDS_RESTRICTED" in str(exc):
        return "The source channel has 'restrict saving content' enabled — forwarding from it is blocked for everyone except admins there."
    if "CHANNEL_PRIVATE" in str(exc):
        return "Can't access that channel — you may have been removed, or it's private and you're not actually a member."
    return str(exc)


async def forward_one(client, src_id, tgt_id, msg_id, retries=3):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            await client(functions.messages.ForwardMessagesRequest(
                from_peer=src_id,
                id=[msg_id],
                to_peer=tgt_id,
                random_id=[random.randint(1, 10**18)],  # must be a list, one per id
            ))
            return True
        except FloodWaitError as fw:
            log.warning("Flood wait %ss", fw.seconds)
            await asyncio.sleep(fw.seconds + 1)
        except Exception as exc:
            last_exc = exc
            log.error("Forward failed (attempt %s): %s", attempt + 1, _explain_forward_error(exc))
            await asyncio.sleep(1)
    if last_exc is not None:
        forward_one.last_error = _explain_forward_error(last_exc)
    return False


forward_one.last_error = None
_error_cooldown = {}  # target_id -> last time we notified about a failure there


async def user_live_forward_callback(event):
    """Live-forward engine that runs on YOUR logged-in account, so it
    sees every chat you're actually a member of — not just channels
    where the bot happens to be admin."""
    if not state["active"]:
        return
    if event.chat_id not in state["sources"]:
        return
    for tgt in state["targets"]:
        ok = await forward_one(user_client, event.chat_id, tgt, event.message.id)
        if ok:
            log.info("[account] Forwarded msg %s: %s -> %s", event.message.id, event.chat_id, tgt)
        else:
            log.error("[account] Could not forward to %s: %s", tgt, forward_one.last_error)
            last_notified = _error_cooldown.get(tgt, 0)
            if time.time() - last_notified > 300:  # at most once per 5 min per target
                _error_cooldown[tgt] = time.time()
                try:
                    await bot.send_message(
                        OWNER_ID,
                        f"⚠️ Couldn't forward a message to `{tgt}`:\n{forward_one.last_error}"
                    )
                except Exception:
                    pass


def register_user_forward_handler():
    """Call this once user_client is set, to start live forwarding
    through your own account instead of the bot."""
    user_client.add_event_handler(user_live_forward_callback, events.NewMessage())


async def migrate_ids():
    """Re-resolve any channels saved under the old, broken raw-id
    format (from before the marked-id fix) using the original text
    you typed, which was kept in *_labels. Safe to run every startup —
    it's a no-op once everything is already correct."""
    changed = False

    fixed_sources, fixed_source_labels = [], {}
    for label in state["source_labels"].values():
        cid = await resolve_one(label)
        if cid:
            fixed_sources.append(cid)
            fixed_source_labels[str(cid)] = label
    if set(fixed_sources) != set(state["sources"]):
        changed = True
    state["sources"] = list(dict.fromkeys(fixed_sources))
    state["source_labels"] = fixed_source_labels

    fixed_targets, fixed_target_labels = [], {}
    for label in state["target_labels"].values():
        cid = await resolve_one(label)
        if cid:
            fixed_targets.append(cid)
            fixed_target_labels[str(cid)] = label
    if set(fixed_targets) != set(state["targets"]):
        changed = True
    state["targets"] = list(dict.fromkeys(fixed_targets))
    state["target_labels"] = fixed_target_labels

    if changed:
        save_state()
        log.info("Migrated saved channel ids to the corrected format.")


MENU_TEXT = (
    "**Forwarder bot commands:**\n\n"
    "`/setsource <channel>` — add a source channel\n"
    "`/settarget <channel>` — add a target channel\n"
    "`/rmsource <channel>` — remove a source channel\n"
    "`/rmtarget <channel>` — remove a target channel\n"
    "`/status` — show current config\n"
    "`/startfwd` — turn live forwarding ON\n"
    "`/stopfwd` — turn live forwarding OFF\n"
    "`/clear` — wipe everything\n"
    "`/fixids` — re-check/fix saved channel ids\n\n"
    "**Your own account login** (recommended — unlocks history backfill,\n"
    "and only needs you to be a MEMBER of the source channel, not admin):\n"
    "`/login` — log in to your own Telegram account\n"
    "`/loginstatus` — check if you're logged in\n"
    "`/logout` — forget your logged-in session\n"
    "`/fwall` — forward ALL history, source[0] → target[0]\n"
    "`/fwallrecent <n>` — forward last n messages, source[0] → target[0]\n\n"
    "`<channel>` = @username, numeric id, or t.me link.\n"
    "Reading the source only needs you to be a member. Posting to the "
    "target needs admin/post rights there if it's a broadcast channel "
    "(a group just needs regular membership) — that's a Telegram rule, "
    "not something this bot can get around."
)


async def backfill(src_id, tgt_id, origin_chat, limit=None):
    assert user_client is not None
    ids = []
    async for msg in user_client.iter_messages(src_id, limit=limit):
        ids.append(msg.id)
    ids.sort()
    total = len(ids)
    if total == 0:
        return await bot.send_message(origin_chat, "⚠️ No messages found.")

    await bot.send_message(origin_chat, f"⏳ Forwarding `{total}` messages...")
    sent, failed = 0, 0
    for i, mid in enumerate(ids, 1):
        ok = await forward_one(user_client, src_id, tgt_id, mid)
        sent += ok
        failed += not ok
        if i % 50 == 0:
            await bot.send_message(origin_chat, f"📊 `{i}/{total}` ✅{sent} ❌{failed}")
        await asyncio.sleep(0.4)

    summary = f"✅ Done. Forwarded `{sent}`, failed `{failed}`."
    if failed and forward_one.last_error:
        summary += f"\n\nLast failure reason: {forward_one.last_error}"
    await bot.send_message(origin_chat, summary)


async def main():
    if not API_ID or not API_HASH or not BOT_TOKEN:
        raise SystemExit("Set TG_API_ID, TG_API_HASH and TG_BOT_TOKEN before running.")
    if not OWNER_ID:
        raise SystemExit("Set TG_OWNER_ID to your own numeric Telegram id before running.")

    global user_client

    load_state()
    await bot.start(bot_token=BOT_TOKEN)
    me = await bot.get_me()
    log.info("Logged in as bot @%s", me.username)

    # Reconnect your own account automatically if already logged in
    # from a previous run.
    if state.get("user_session"):
        try:
            user_client = TelegramClient(StringSession(state["user_session"]), API_ID, API_HASH)
            await user_client.connect()
            if await user_client.is_user_authorized():
                log.info("Reconnected your saved account session.")
                register_user_forward_handler()
            else:
                log.warning("Saved session is no longer valid; use /login again.")
                user_client = None
        except Exception as exc:
            log.error("Could not reconnect saved session: %s", exc)
            user_client = None

    await migrate_ids()

    # ── DEBUG: logs every incoming message before any filtering, so
    #    you can see in the console whether the bot is receiving
    #    anything at all, and whether sender_id matches your OWNER_ID.
    @bot.on(events.NewMessage())
    async def debug_log(event):
        log.info(
            "Incoming: chat_id=%s sender_id=%s text=%r (OWNER_ID=%s match=%s)",
            event.chat_id, event.sender_id, event.raw_text,
            OWNER_ID, event.sender_id == OWNER_ID,
        )

    @bot.on(events.NewMessage(pattern="/(start|menu)"))
    @owner_only
    async def menu_handler(event):
        await event.reply(MENU_TEXT)

    @bot.on(events.NewMessage(pattern=r"/setsource (.+)"))
    @owner_only
    async def setsource_handler(event):
        arg = event.pattern_match.group(1)
        cid = await resolve_one(arg)
        if not cid:
            return await event.reply("❌ Couldn't resolve that. Is the bot an admin there?")
        if cid not in state["sources"]:
            state["sources"].append(cid)
        state["source_labels"][str(cid)] = arg
        save_state()
        await event.reply(f"✅ Source added: `{arg}` (`{cid}`)")

    @bot.on(events.NewMessage(pattern=r"/settarget (.+)"))
    @owner_only
    async def settarget_handler(event):
        arg = event.pattern_match.group(1)
        cid = await resolve_one(arg)
        if not cid:
            return await event.reply("❌ Couldn't resolve that. Is the bot an admin there?")
        if cid not in state["targets"]:
            state["targets"].append(cid)
        state["target_labels"][str(cid)] = arg
        save_state()
        await event.reply(f"✅ Target added: `{arg}` (`{cid}`)")

    @bot.on(events.NewMessage(pattern=r"/rmsource (.+)"))
    @owner_only
    async def rmsource_handler(event):
        arg = event.pattern_match.group(1)
        cid = await resolve_one(arg)
        if cid in state["sources"]:
            state["sources"].remove(cid)
            state["source_labels"].pop(str(cid), None)
            save_state()
            return await event.reply(f"🗑️ Removed source `{arg}`")
        await event.reply("Not found in sources.")

    @bot.on(events.NewMessage(pattern=r"/rmtarget (.+)"))
    @owner_only
    async def rmtarget_handler(event):
        arg = event.pattern_match.group(1)
        cid = await resolve_one(arg)
        if cid in state["targets"]:
            state["targets"].remove(cid)
            state["target_labels"].pop(str(cid), None)
            save_state()
            return await event.reply(f"🗑️ Removed target `{arg}`")
        await event.reply("Not found in targets.")

    @bot.on(events.NewMessage(pattern="/fixids"))
    @owner_only
    async def fixids_handler(event):
        await migrate_ids()
        await event.reply("🔧 Re-checked and fixed saved channel ids if needed. Run /status to see current config.")

    @bot.on(events.NewMessage(pattern="/status"))
    @owner_only
    async def status_handler(event):
        src = "\n".join(f"• {state['source_labels'].get(str(i), i)}" for i in state["sources"]) or "none"
        tgt = "\n".join(f"• {state['target_labels'].get(str(i), i)}" for i in state["targets"]) or "none"
        await event.reply(
            f"**Forwarding:** `{'ON' if state['active'] else 'OFF'}`\n\n"
            f"**Sources:**\n{src}\n\n**Targets:**\n{tgt}"
        )

    @bot.on(events.NewMessage(pattern="/startfwd"))
    @owner_only
    async def startfwd_handler(event):
        if not state["sources"] or not state["targets"]:
            return await event.reply("⚠️ Add at least one source and one target first.")
        state["active"] = True
        save_state()
        await event.reply("🚀 Live forwarding is now ON.")

    @bot.on(events.NewMessage(pattern="/stopfwd"))
    @owner_only
    async def stopfwd_handler(event):
        state["active"] = False
        save_state()
        await event.reply("🛑 Live forwarding is now OFF.")

    @bot.on(events.NewMessage(pattern="/clear"))
    @owner_only
    async def clear_handler(event):
        state["sources"].clear()
        state["targets"].clear()
        state["source_labels"].clear()
        state["target_labels"].clear()
        state["active"] = False
        save_state()
        await event.reply("🧹 Cleared all config.")

    # ── Your own account login, step by step, in this chat ──
    @bot.on(events.NewMessage(pattern="/login"))
    @owner_only
    async def login_handler(event):
        if user_client is not None:
            return await event.reply("You're already logged in. Use `/logout` first to re-login.")
        if login_flow["step"] is not None:
            return await event.reply("A login is already in progress. Send the next value, or wait.")
        login_flow["step"] = "NUMBER"
        await event.reply(
            "🔐 Logging in to **your own account**.\n"
            "This runs only for you and stays local to this bot's server.\n\n"
            "Send your phone number, with country code (e.g. `+15551234567`):"
        )

    @bot.on(events.NewMessage(pattern="/logout"))
    @owner_only
    async def logout_handler(event):
        global user_client
        if user_client is not None:
            await user_client.disconnect()
            user_client = None
        state["user_session"] = None
        save_state()
        await event.reply("🔒 Logged out and forgot the saved session.")

    @bot.on(events.NewMessage(pattern="/loginstatus"))
    @owner_only
    async def loginstatus_handler(event):
        logged_in = user_client is not None
        await event.reply(f"Account login: `{'ACTIVE' if logged_in else 'NOT LOGGED IN'}`")

    @bot.on(events.NewMessage(pattern="/fwall"))
    @owner_only
    async def fwall_handler(event):
        if user_client is None:
            return await event.reply("⚠️ Not logged in. Use `/login` first.")
        if not state["sources"] or not state["targets"]:
            return await event.reply("⚠️ Set a source and target first.")
        await event.reply("⏳ Starting full history forward...")
        asyncio.create_task(backfill(
            state["sources"][0], state["targets"][0], event.chat_id, limit=None
        ))

    @bot.on(events.NewMessage(pattern=r"/fwallrecent(?:\s+(\d+))?"))
    @owner_only
    async def fwallrecent_handler(event):
        if user_client is None:
            return await event.reply("⚠️ Not logged in. Use `/login` first.")
        if not state["sources"] or not state["targets"]:
            return await event.reply("⚠️ Set a source and target first.")
        n = int(event.pattern_match.group(1)) if event.pattern_match.group(1) else 100
        await event.reply(f"⏳ Forwarding last `{n}` messages...")
        asyncio.create_task(backfill(
            state["sources"][0], state["targets"][0], event.chat_id, limit=n
        ))

    # Handles the free-text steps of /login (phone -> code -> 2FA).
    # Only ever acts for OWNER_ID, and only while a login is in progress.
    @bot.on(events.NewMessage())
    async def login_flow_handler(event):
        global user_client
        if event.sender_id != OWNER_ID:
            return
        if login_flow["step"] is None:
            return
        text = (event.raw_text or "").strip()
        if text.startswith("/"):
            return  # let command handlers deal with it

        step = login_flow["step"]

        if step == "NUMBER":
            await event.reply("⏳ Connecting...")
            client = TelegramClient(StringSession(), API_ID, API_HASH)
            await client.connect()
            try:
                sent = await client.send_code_request(text)
                login_flow.update(step="OTP", client=client, phone=text,
                                   phone_code_hash=sent.phone_code_hash)
                await event.reply("📩 Enter the code Telegram just sent you:")
            except Exception as exc:
                await event.reply(f"❌ `{exc}`")
                login_flow.update(step=None, client=None, phone=None, phone_code_hash=None)

        elif step == "OTP":
            client = login_flow["client"]
            try:
                await client.sign_in(
                    phone=login_flow["phone"], code=text,
                    phone_code_hash=login_flow["phone_code_hash"],
                )
                session_str = client.session.save()
                state["user_session"] = session_str
                save_state()
                user_client = client
                register_user_forward_handler()
                await event.reply(
                    "✅ Logged in! Live forwarding now runs through your account.\n"
                    "You can also use `/fwall` and `/fwallrecent`."
                )
                login_flow.update(step=None, client=None, phone=None, phone_code_hash=None)
            except SessionPasswordNeededError:
                login_flow["step"] = "PASSWORD"
                await event.reply("🔒 Your account has 2FA. Enter your password:")
            except Exception as exc:
                await event.reply(f"❌ `{exc}`")
                login_flow.update(step=None, client=None, phone=None, phone_code_hash=None)

        elif step == "PASSWORD":
            client = login_flow["client"]
            try:
                await client.sign_in(password=text)
                session_str = client.session.save()
                state["user_session"] = session_str
                save_state()
                user_client = client
                register_user_forward_handler()
                await event.reply(
                    "✅ Logged in! Live forwarding now runs through your account.\n"
                    "You can also use `/fwall` and `/fwallrecent`."
                )
            except Exception as exc:
                await event.reply(f"❌ `{exc}`")
            finally:
                login_flow.update(step=None, client=None, phone=None, phone_code_hash=None)

    # ── Fallback forwarding engine using the BOT token itself — only
    #    used while you have NOT logged in with /login. Once you log
    #    in, user_live_forward_callback (on your own account) takes
    #    over and this one steps aside, since your account sees every
    #    chat you're in rather than only channels the bot is admin in.
    @bot.on(events.NewMessage())
    async def live_forward(event):
        if user_client is not None:
            return  # your account handles it now
        if not state["active"]:
            return
        if event.chat_id not in state["sources"]:
            return
        for tgt in state["targets"]:
            ok = await forward_one(bot, event.chat_id, tgt, event.message.id)
            if ok:
                log.info("[bot] Forwarded msg %s: %s -> %s", event.message.id, event.chat_id, tgt)

    log.info("Bot is running. Message it privately with /menu to configure.")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
