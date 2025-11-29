import time
import datetime
import asyncio
import logging
from info import ADMINS
from pyrogram import Client, filters
from database.users_chats_db import db
from database.postgres import pgDb
from pyrogram.errors import InputUserDeactivated, FloodWait, UserIsBlocked, PeerIdInvalid

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS))
async def send_broadcast(client, message):
    if not message.reply_to_message:
        return await message.reply_text(
            "⚠️ **Please reply to a message to broadcast.**", quote=True
        )
    
    total_users = await pgDb.total_users_count()
    done = 0
    failed = 0
    success = 0
    skip = 0

    if len(message.command) != 1:
        try:
            skip = int(message.text.split(' ', 1)[1])
            done += skip
        except ValueError:
            return await message.reply_text("⚠️ **Invalid skip value.**", quote=True)

    sts = await message.reply_text(
        "📡 **Broadcast started... Please wait for updates.**", quote=True
    )
    start_time = time.time()

    async for user in pgDb.get_all_users_iterator(skip=skip):
        if user.get('is_banned', False):
            done += 1
            failed += 1
            continue
            
        out = await broadcast_messages(user_id=int(user['id']), message=message.reply_to_message)            
        if out:
            success += 1
        else:
            failed += 1
        done += 1

        if done % 50 == 0:
            try:
                elapsed = datetime.timedelta(seconds=int(time.time() - start_time))
                await sts.edit(
                    f"📊 **Broadcast Progress:**\n\n"
                    f"👥 **Total Users:** `{total_users}`\n"
                    f"✅ **Completed:** `{done}`\n"
                    f"✔️ **Success:** `{success}`\n"
                    f"❌ **Failed:** `{failed}`\n"
                    f"⏱ **Elapsed Time:** `{elapsed}`"
                )
            except Exception as e:
                logging.error(f"Error updating status message: {e}")

    completed_in = datetime.timedelta(seconds=int(time.time() - start_time))
    await sts.delete()

    await message.reply_text(
        f"🎉 **Broadcast Completed!**\n\n"
        f"📅 **Time Taken:** `{completed_in}`\n"
        f"👥 **Total Users:** `{total_users}`\n"
        f"✅ **Completed:** `{done}`\n"
        f"✔️ **Success:** `{success}`\n"
        f"❌ **Failed:** `{failed}`",
        quote=True
    )

async def broadcast_messages(user_id, message):
    try:
        await message.copy(chat_id=user_id)
        return True
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        await message.copy(chat_id=user_id)
        return True
    except InputUserDeactivated:
        await db.delete_user(user_id)
        await pgDb.delete_user_by_id(user_id)
        logging.info(f"User {user_id} removed from database (deactivated account).")
        return False
    except UserIsBlocked:
        await db.delete_user(user_id)
        await pgDb.delete_user_by_id(user_id)
        logging.info(f"User {user_id} blocked the bot.")
        return False
    except PeerIdInvalid:
        await db.delete_user(user_id)
        await pgDb.delete_user_by_id(user_id)
        logging.info(f"User {user_id} removed from database (invalid ID).")
        return False
    except Exception as e:
        logging.error(f"Unexpected error with user {user_id}: {e}")
        return False
