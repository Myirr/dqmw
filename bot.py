import logging
import logging.config

# Get logging configurations
logging.config.fileConfig('logging.conf')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("imdbpy").setLevel(logging.ERROR)

import os
import sys
import asyncio
import logging
import subprocess
from aiocache import caches
from pyrogram import types, Client
from utils import temp, set_commands
from database.users_chats_db import db
from database.ia_filterdb import initialize_db_connections
from pyrogram.errors import ChannelPrivate, MessageDeleteForbidden
from typing import Union, Optional, AsyncGenerator
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from datetime import datetime, timedelta
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from database.postgres import pgDb
from plugins.db_migration import hourly_backup
from info import SESSION, API_ID, API_HASH, BOT_TOKEN, LOG_STR, LOG_CHANNEL, PORT, ADMINS
from Script import script  
import pytz
from aiohttp import web
from plugins import web_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
apscheduler_logger = logging.getLogger("apscheduler")
apscheduler_logger.setLevel(logging.DEBUG)

async def test_redis_connection():
    try:
        cache = caches.get('default')
        await cache.set("health_check", "ok", ttl=60)
        value = await cache.get("health_check")
        if value == "ok":
            logging.info("✅ Redis connection test passed.")
        else:
            logging.warning("⚠️ Redis connection test failed: unexpected value.")
    except Exception as e:
        logging.error(f"❌ Redis connection failed: {e}")
        raise

async def auto_restart():
    logging.info("Executing auto_restart function...")
    try:
        os.execl(sys.executable, sys.executable, *sys.argv)
    except Exception as e:
        logging.error(f"Error during auto_restart: {e}")

async def delete_message(client: Client, chat_id: int, message_id: int):
    try:
        await client.delete_messages(chat_id=chat_id, message_ids=message_id)
        logging.info(f"Auto-deleted message {message_id} from {chat_id}")
    except MessageDeleteForbidden:
        logging.error(f"Cannot delete message {message_id} from {chat_id}. No delete permission.")
    except asyncio.CancelledError:
        logging.info(f"Deletion of message {message_id} from {chat_id} was cancelled.")
    except Exception as e:
        logging.error(f"Error deleting message {message_id} from {chat_id}: {e}")
        
class Bot(Client):
    main_bot_instance = None

    def __init__(self, name="DQAutofilter", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, plugins={"root": "plugins"}, workdir=None):
        if workdir is None:
            workdir = "sessions"
        if not os.path.exists(workdir):
            os.makedirs(workdir, exist_ok=True)

        super().__init__(
            name=name,
            api_id=api_id,
            api_hash=api_hash,
            bot_token=bot_token,
            plugins=plugins or dict(root="plugins"),
            workdir=workdir
        )

        self.scheduler = AsyncIOScheduler()
        self.is_main_bot = (bot_token == BOT_TOKEN)
        self.id = None
        self.name = None
        self.username = None
        self.mention = None

        if self.is_main_bot:
            Bot.main_bot_instance = self

    async def start(self):
        try:
            if self.is_main_bot:
                logging.info("Initializing main bot databases and settings...")
                media_db_status = await initialize_db_connections()
                b_users, b_chats = await db.get_banned()
                temp.BANNED_USERS = b_users
                temp.BANNED_CHATS = b_chats
                logging.info("Main bot initialization completed.")

            await super().start()

            if self.is_main_bot:
                await set_commands(self)

            me = await self.get_me()
            self.id = me.id
            self.name = me.full_name
            self.username = me.username
            self.mention = me.mention

            if self.is_main_bot:
                temp.U_NAME = me.username
                self.scheduler.start()
                logging.info("Main bot scheduler started successfully.")

                self.scheduler.add_job(
                    auto_restart,
                    IntervalTrigger(hours=24),
                    name="Auto Restart"
                )
                logging.info("Auto restart job scheduled every day")

                self.scheduler.add_job(
                    func=hourly_backup,
                    trigger="interval",
                    hours=23,
                    args=[self],
                    id='periodic_database_backup',
                    name='Periodic Database Backup',
                    replace_existing=True
                )
                logging.info("Periodic database backup job scheduled every 23 hours.")

            start_text = f"{me.full_name} 𝖲𝗍𝖺𝗋𝗍𝖾𝖽..."
            logging.info(start_text)

            if self.is_main_bot:
                for admin_id in ADMINS:
                    try:
                        await self.send_message(admin_id, start_text)
                    except Exception as e:
                        logging.warning(f"Failed to send start message to admin {admin_id}: {e}")

        except Exception as e:
            logging.error(f"Error during bot startup: {e}")

            if self.is_main_bot:
                try:
                    admin_id = ADMINS[0] if ADMINS else None
                    if admin_id:
                        await self.send_message(admin_id, f"Error: {e}")

                except Exception as send_error:
                    logging.error(f"Failed to send error message: {send_error}")

            if self.is_main_bot:
                await asyncio.sleep(60)
                os.execl(sys.executable, sys.executable, "bot.py")
                
    async def iter_messages(self, chat_id: Union[int, str], limit: int, offset: int = 0) -> Optional[AsyncGenerator["types.Message", None]]:
        current = offset
        while current < limit:
            new_diff = min(200, limit - current)
            try:
                messages = await self.get_messages(chat_id, list(range(current, current + new_diff)))
            except ChannelPrivate:
                logging.warning(f"Cannot access chat {chat_id}. It may be private.")
                break
            for message in messages:
                yield message
                current += 1
            await asyncio.sleep(0.1)
            
    async def stop(self, *args):
        await super().stop()
        logging.info("Bot stopped. Bye.")

    async def iter_messages(
        self,
        chat_id: Union[int, str],
        limit: int,
        offset: int = 0,
    ) -> Optional[AsyncGenerator["types.Message", None]]:
        """Iterate through a chat sequentially.
        This convenience method does the same as repeatedly calling :meth:`~pyrogram.Client.get_messages` in a loop, thus saving
        you from the hassle of setting up boilerplate code. It is useful for getting the whole chat messages with a
        single call.
        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.
                For your personal cloud (Saved Messages) you can simply use "me" or "self".
                For a contact that exists in your Telegram address book you can use his phone number (str).
                
            limit (``int``):
                Identifier of the last message to be returned.
                
            offset (``int``, *optional*):
                Identifier of the first message to be returned.
                Defaults to 0.
        Returns:
            ``Generator``: A generator yielding :obj:`~pyrogram.types.Message` objects.
        Example:
            .. code-block:: python
                for message in app.iter_messages("pyrogram", 1, 15000):
                    print(message.text)
        """
        current = offset
        while True:
            new_diff = min(200, limit - current)
            if new_diff <= 0:
                return
            messages = await self.get_messages(chat_id, list(range(current, current+new_diff+1)))
            for message in messages:
                yield message
                current += 1
                
async def pgDBinit():
    await pgDb.connect()
    tables_to_check = [
        "files_backup",
        "users",
        "groups"
    ]
    
    for table in tables_to_check:
        try:
            total = await pgDb.total_rows(table)
            logging.info(f"✅ PostgreSQL: {total} records in '{table}'")
        except Exception as e:
            logging.warning(f"⚠️ Could not get record count from '{table}': {e}")

async def startup():
    try:
        await pgDBinit()
        await test_redis_connection()
        logging.info("Starting multi-client bot system...")
        await main()
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Error in main: {e}")
        os.execl(sys.executable, sys.executable, "bot.py")

async def main():
    try:
        main_bot = Bot()
        await main_bot.start()
        logging.info("Main bot started successfully")
        await asyncio.Event().wait()
    except Exception as e:
        logging.error(f"Critical error in main function: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(startup())
    except Exception as e:
        logging.error(f"Critical error during startup: {e}")
        sys.exit(1)
