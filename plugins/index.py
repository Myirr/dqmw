import re
import asyncio
import time
from utils import temp
from info import ADMINS
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait
from pyrogram.errors.exceptions.bad_request_400 import (
    ChannelInvalid,
    ChatAdminRequired,
    UsernameInvalid,
    UsernameNotModified,
)
from database.ia_filterdb import save_file
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

lock = asyncio.Lock()

INITIAL_BATCH_SIZE = 25
MIN_BATCH_SIZE = 5
MAX_BATCH_SIZE = 50
MAX_REQUESTS_PER_MINUTE = 50
REQUEST_INTERVAL = 60 / MAX_REQUESTS_PER_MINUTE
last_request_time = 0
consecutive_errors = 0
error_backoff = 1.0
MAX_RETRIES = 5
RETRY_DELAY = 5
CONNECTION_TIMEOUT = 20

@Client.on_callback_query(filters.regex(r"^index"))
async def index_files(bot, query):
    try:
        if query.data.startswith("index_cancel"):
            temp.CANCEL = True
            return await query.answer("Cancelling Indexing")

        _, action, chat, lst_msg_id, from_user = query.data.split("#")

        if action == "reject":
            await query.message.delete()
            await bot.send_message(
                int(from_user),
                f"Your submission for indexing {chat} has been declined by our moderators.",
                reply_to_message_id=int(lst_msg_id),
            )
            return

        if lock.locked():
            return await query.answer(
                "Wait until the previous process is complete.", show_alert=True
            )

        await query.answer("Processing...⏳", show_alert=True)

        if int(from_user) not in ADMINS:
            await bot.send_message(
                int(from_user),
                f"Your submission for indexing {chat} has been accepted by our moderators and will be added soon.",
                reply_to_message_id=int(lst_msg_id),
            )

        await query.message.edit(
            "Starting Indexing",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Cancel", callback_data="index_cancel")]]
            ),
        )

        try:
            chat = int(chat)
        except ValueError:
            pass

        await index_files_to_db(int(lst_msg_id), chat, query.message, bot)
    except Exception as e:
        logger.exception("Error in index_files: %s", e)

@Client.on_message(
    (
        filters.forwarded
        | (
            filters.regex(
                r"(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$"
            )
        )
        & filters.text
    )
    & (filters.private | filters.group)
    & filters.incoming
    & filters.user(ADMINS)
)
async def send_for_index(bot, message):
    """Handles messages to initiate indexing in both private and group chats."""
    try:
        chat_id = None
        last_msg_id = None

        if message.text:
            regex = re.compile(
                r"(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$"
            )
            match = regex.match(message.text)
            if not match:
                return await message.reply("Invalid link")

            chat_id = match.group(4)
            last_msg_id = int(match.group(5))

            if chat_id.isnumeric():
                chat_id = int("-100" + chat_id)

        elif message.forward_from_chat and message.forward_from_chat.type == enums.ChatType.CHANNEL:
            last_msg_id = message.forward_from_message_id
            chat_id = message.forward_from_chat.username or message.forward_from_chat.id

        if not chat_id or not last_msg_id:
            return

        try:
            await bot.get_chat(chat_id)
        except ChannelInvalid:
            return await message.reply(
                "This may be a private channel/group. Make me an admin there to index the files."
            )
        except (UsernameInvalid, UsernameNotModified):
            return await message.reply("Invalid link specified.")
        except Exception as e:
            logger.exception("Error in validating chat: %s", e)
            return await message.reply(f"Error: {e}")

        try:
            message_data = await bot.get_messages(chat_id, last_msg_id)
        except Exception:
            return await message.reply(
                "Make sure I am an admin in the channel, especially if the channel is private."
            )

        if message_data.empty:
            return await message.reply(
                "This might be a group, and I am not an admin of the group."
            )

        await message.reply("Starting indexing process...")
        
        progress_message = await message.reply(
            "Indexing in progress...",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Cancel", callback_data="index_cancel")]]
            )
        )
        
        await index_files_to_db(last_msg_id, chat_id, progress_message, bot)

    except Exception as e:
        logger.exception("Error in send_for_index: %s", e)

@Client.on_message(filters.command("index") & filters.group & filters.user(ADMINS))
async def group_index_command(bot, message):
    try:
        if len(message.command) < 2:
            return await message.reply(
                "Usage: /index <channel_link>\n"
                "Example: /index https://t.me/channel_name/123"
            )
        
        link = message.command[1]
        
        chat_member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        if chat_member.status not in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]:
            if message.from_user.id not in ADMINS:
                return await message.reply("You need to be an admin in this group to use this command.")
        
        regex = re.compile(
            r"(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$"
        )
        match = regex.match(link)
        if not match:
            return await message.reply("Invalid link format. Please provide a valid Telegram channel/group link.")

        chat_id = match.group(4)
        last_msg_id = int(match.group(5))

        if chat_id.isnumeric():
            chat_id = int("-100" + chat_id)

        try:
            await bot.get_chat(chat_id)
        except ChannelInvalid:
            return await message.reply(
                "This may be a private channel/group. Make me an admin there to index the files."
            )
        except (UsernameInvalid, UsernameNotModified):
            return await message.reply("Invalid link specified.")
        except Exception as e:
            logger.exception("Error in validating chat: %s", e)
            return await message.reply(f"Error: {e}")

        try:
            message_data = await bot.get_messages(chat_id, last_msg_id)
        except Exception:
            return await message.reply(
                "Make sure I am an admin in the target channel/group."
            )

        if message_data.empty:
            return await message.reply(
                "Could not access the specified message. Make sure I am an admin in the target channel/group."
            )

        await message.reply("Starting indexing process...")
        
        progress_message = await message.reply(
            "Indexing in progress...",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Cancel", callback_data="index_cancel")]]
            )
        )
        
        await index_files_to_db(last_msg_id, chat_id, progress_message, bot)

    except Exception as e:
        logger.exception("Error in group_index_command: %s", e)
        await message.reply(f"An error occurred: {str(e)}")

@Client.on_message(filters.command("setskip") & filters.user(ADMINS))
async def set_skip_number(bot, message):
    try:
        if " " in message.text:
            _, skip = message.text.split(" ")
            temp.CURRENT = int(skip)
            await message.reply(f"Successfully set SKIP number as {temp.CURRENT}")
        else:
            await message.reply("Provide a skip number.")
    except ValueError:
        await message.reply("Skip number must be an integer.")
    except Exception as e:
        logger.exception("Error in set_skip_number: %s", e)

@Client.on_message(filters.command("status") & (filters.private | filters.group) & filters.user(ADMINS))
async def indexing_status(bot, message):
    try:
        if lock.locked():
            await message.reply(
                f"🔄 Indexing is currently in progress.\n"
                f"📊 Current position: {temp.CURRENT}\n"
                f"⏹️ Cancel status: {'Yes' if temp.CANCEL else 'No'}"
            )
        else:
            await message.reply("✅ No indexing process is currently running.")
    except Exception as e:
        logger.exception("Error in indexing_status: %s", e)

async def robust_api_call(bot, chat_id, message_ids, max_retries=MAX_RETRIES):
    global consecutive_errors, error_backoff
    
    for attempt in range(max_retries):
        try:
            await asyncio.sleep(REQUEST_INTERVAL * error_backoff)
            
            messages = await asyncio.wait_for(
                bot.get_messages(chat_id=chat_id, message_ids=message_ids),
                timeout=CONNECTION_TIMEOUT
            )
            
            consecutive_errors = 0
            error_backoff = max(1.0, error_backoff * 0.8)
            
            return messages, None
            
        except (OSError, ConnectionError, BrokenPipeError) as e:
            consecutive_errors += 1
            error_backoff = min(5.0, error_backoff * 1.5)
            
            retry_delay = RETRY_DELAY * (2 ** attempt)
            logger.warning(
                f"Connection error on attempt {attempt + 1}/{max_retries}: {e}. "
                f"Retrying in {retry_delay:.1f}s..."
            )
            
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
            else:
                return None, f"Connection failed after {max_retries} attempts: {e}"
                
        except asyncio.TimeoutError:
            consecutive_errors += 1
            error_backoff = min(5.0, error_backoff * 1.2)
            
            logger.warning(
                f"Timeout on attempt {attempt + 1}/{max_retries}. "
                f"Retrying in {RETRY_DELAY * (attempt + 1):.1f}s..."
            )
            
            if attempt < max_retries - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            else:
                return None, f"Request timed out after {max_retries} attempts"
                
        except FloodWait as e:
            wait_time = getattr(e, 'value', getattr(e, 'x', 30))
            logger.warning(f"FloodWait: Waiting for {wait_time} seconds")
            await asyncio.sleep(wait_time)
            
            if attempt < max_retries - 1:
                continue
            else:
                return None, f"FloodWait persisted after {max_retries} attempts"
                
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            else:
                return None, f"Unexpected error: {e}"
    
    return None, "Max retries exceeded"

async def index_files_to_db(lst_msg_id, chat, msg, bot):
    total_files = 0
    duplicate = 0
    errors = 0
    deleted = 0
    no_media = 0
    unsupported = 0
    
    start_time = time.time()
    status_update_time = time.time()
    
    global consecutive_errors, error_backoff
    
    current_batch_size = INITIAL_BATCH_SIZE
    
    async with lock:
        try:
            current = temp.CURRENT
            temp.CANCEL = False
            
            processed_speed_history = []
            
            while current < lst_msg_id and not temp.CANCEL:
                try:
                    remaining = lst_msg_id - current
                    
                    if consecutive_errors > 3:
                        current_batch_size = max(MIN_BATCH_SIZE, current_batch_size // 2)
                    elif consecutive_errors == 0 and current_batch_size < MAX_BATCH_SIZE:
                        current_batch_size = min(MAX_BATCH_SIZE, current_batch_size + 5)
                    
                    actual_batch_size = min(current_batch_size, remaining)
                    message_ids = list(range(
                        max(current + 1, 1),
                        current + actual_batch_size + 1
                    ))
                    
                    if not message_ids:
                        break
                    
                    batch_start_time = time.time()
                    
                    messages, error = await robust_api_call(bot, chat, message_ids)
                    
                    if error:
                        logger.error(f"API call failed: {error}")
                        errors += len(message_ids)
                        current += actual_batch_size
                        
                        await asyncio.sleep(5)
                        continue
                    
                    tasks = []
                    for message in messages:
                        if message and not message.empty and message.media:
                            if message.media in [
                                enums.MessageMediaType.VIDEO,
                                enums.MessageMediaType.AUDIO,
                                enums.MessageMediaType.DOCUMENT,
                            ]:
                                media = getattr(message, message.media.value, None)
                                if media:
                                    media.file_type = message.media.value
                                    media.caption = message.caption
                                    tasks.append(save_file(media))
                                else:
                                    unsupported += 1
                            else:
                                unsupported += 1
                        elif message and not message.empty and not message.media:
                            no_media += 1
                        else:
                            deleted += 1
                    
                    for i in range(0, len(tasks), 3):
                        batch_tasks = tasks[i:i+3]
                        if batch_tasks:
                            try:
                                results = await asyncio.wait_for(
                                    asyncio.gather(*batch_tasks, return_exceptions=True),
                                    timeout=30
                                )
                                
                                for result in results:
                                    if isinstance(result, Exception):
                                        errors += 1
                                        logger.error(f"Error saving file: {result}")
                                    elif isinstance(result, tuple):
                                        saved, status = result
                                        if saved:
                                            total_files += 1
                                        elif status == 0:
                                            duplicate += 1
                                        else:
                                            errors += 1
                            except asyncio.TimeoutError:
                                logger.warning("File save batch timed out")
                                errors += len(batch_tasks)
                            
                            await asyncio.sleep(0.1)
                    
                    batch_time = time.time() - batch_start_time
                    batch_speed = actual_batch_size / batch_time if batch_time > 0 else 0
                    processed_speed_history.append(batch_speed)
                    
                    if len(processed_speed_history) > 10:
                        processed_speed_history.pop(0)
                    
                    current_time = time.time()
                    if current_time - status_update_time >= 15:
                        try:
                            elapsed = current_time - start_time
                            speed = current / elapsed if elapsed > 0 else 0
                            
                            avg_recent_speed = sum(processed_speed_history) / len(processed_speed_history) if processed_speed_history else speed
                            remaining_msgs = lst_msg_id - current
                            eta_seconds = remaining_msgs / avg_recent_speed if avg_recent_speed > 0 else 0
                            
                            eta_hours, remainder = divmod(int(eta_seconds), 3600)
                            eta_minutes, eta_seconds = divmod(remainder, 60)
                            eta_str = f"{eta_hours}h {eta_minutes}m {eta_seconds}s" if eta_hours > 0 else f"{eta_minutes}m {eta_seconds}s"
                            
                            progress_pct = (current / lst_msg_id) * 100 if lst_msg_id > 0 else 0
                            
                            status_text = (
                                f"📊 Progress: {progress_pct:.1f}% ({current}/{lst_msg_id})\n"
                                f"⏱️ Elapsed: {int(elapsed//3600)}h {int((elapsed%3600)//60)}m {int(elapsed%60)}s\n"
                                f"🕒 ETA: {eta_str}\n"
                                f"⚡ Speed: {int(avg_recent_speed)} msg/sec\n"
                                f"🔄 Batch size: {current_batch_size}\n\n"
                                f"📥 Saved: {total_files}\n"
                                f"🔁 Duplicates: {duplicate}\n"
                                f"🗑️ Deleted: {deleted}\n"
                                f"📭 Non-media: {no_media + unsupported}\n"
                                f"⚠️ Errors: {errors}\n"
                                f"🌐 Connection errors: {consecutive_errors}"
                            )
                            
                            await msg.edit_text(
                                status_text,
                                reply_markup=InlineKeyboardMarkup(
                                    [[InlineKeyboardButton("Cancel", callback_data="index_cancel")]]
                                )
                            )
                            status_update_time = current_time
                            
                        except FloodWait as e:
                            wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                            logger.warning(f"FloodWait in status update: {wait_time} seconds")
                            await asyncio.sleep(wait_time)
                        except Exception as e:
                            logger.error(f"Error updating status: {e}")
                    
                    current += actual_batch_size
                    
                    base_delay = 1.0
                    if consecutive_errors > 0:
                        base_delay *= (1 + consecutive_errors * 0.5)
                    
                    await asyncio.sleep(base_delay)

                except Exception as e:
                    logger.error(f"Error processing batch: {e}")
                    errors += actual_batch_size if 'actual_batch_size' in locals() else current_batch_size
                    current += actual_batch_size if 'actual_batch_size' in locals() else current_batch_size
                    consecutive_errors += 1
                    await asyncio.sleep(3)
                    continue

            total_time = time.time() - start_time
            avg_speed = lst_msg_id / total_time if total_time > 0 else 0
            
            total_hours, remainder = divmod(int(total_time), 3600)
            total_minutes, total_seconds = divmod(remainder, 60)
            total_time_str = f"{total_hours}h {total_minutes}m {total_seconds}s" if total_hours > 0 else f"{total_minutes}m {total_seconds}s"
            
            final_status = (
                f"✅ Indexing completed!\n\n"
                f"📊 Total messages: {lst_msg_id}\n"
                f"⏱️ Total time: {total_time_str}\n"
                f"⚡ Average speed: {int(avg_speed)} msg/sec\n\n"
                f"📁 Successfully saved: {total_files}\n"
                f"🔁 Duplicates skipped: {duplicate}\n"
                f"🗑️ Deleted messages: {deleted}\n"
                f"📭 Non-media messages: {no_media + unsupported}\n"
                f"⚠️ Errors occurred: {errors}\n\n"
                f"💾 Success rate: {(total_files / max(1, lst_msg_id) * 100):.1f}%"
            )
            
            retry_count = 3
            while retry_count > 0:
                try:
                    await msg.edit_text(final_status)
                    break
                except FloodWait as e:
                    wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                    logger.warning(f"FloodWait in final update: {wait_time} seconds")
                    await asyncio.sleep(wait_time)
                    retry_count -= 1
                except Exception as e:
                    logger.error(f"Error in final status update: {e}")
                    retry_count -= 1
                    if retry_count > 0:
                        await asyncio.sleep(5)
                    
        except Exception as e:
            logger.exception(f"Critical error in index_files_to_db: {e}")
            try:
                await msg.edit_text(f"❌ Indexing failed with critical error: {str(e)[:100]}")
            except:
                pass
