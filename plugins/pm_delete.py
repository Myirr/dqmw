from pyrogram import Client, filters
from bot import Bot
from info import ADMINS
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import re
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

scheduler = AsyncIOScheduler()
if not scheduler.running:
    scheduler.start()

URL_PATTERN = re.compile(r'https?://\S+|t\.me/\S+|telegram\.me/\S+')

async def delete_warning_message(client, chat_id, message_id):
    try:
        await client.delete_messages(chat_id=chat_id, message_ids=message_id)
        logger.info(f"Successfully deleted warning message {message_id} from chat {chat_id}")
    except Exception as e:
        logger.error(f"Failed to delete warning message {message_id}: {e}")

@Client.on_message(
    filters.private & 
    ~filters.me &
    (
        (filters.document | filters.photo | filters.video | filters.audio | 
        filters.animation | filters.voice | filters.sticker) |
        (filters.text & filters.regex(URL_PATTERN))
    )
)
async def delete_media_and_links(client, message):
    try:
        user_id = message.from_user.id

        if message.via_bot:
            if message.via_bot.id == client.me.id:
                logger.info(f"Allowed inline search result from bot itself: {user_id}")
                return
            else:
                logger.info(f"Deleting inline search result from another bot: {message.via_bot.id}")
        
        if user_id in ADMINS:
            message_type = "media" if message.media else "link"
            logger.info(f"Allowed {message_type} message from admin {user_id}")
            return
        
        message_type = "media" if message.media else "link"
        
        try:
            await message.delete()
            logger.info(f"Deleted {message_type} message from user {user_id}")
        except Exception as e:
            logger.error(f"Failed to delete {message_type}: {e}")
            return
        
        try:
            warning_text = "**🚫 𝖬𝖾𝖽𝗂𝖺 𝖿𝗂𝗅𝖾𝗌 𝖺𝗋𝖾 𝗇𝗈𝗍 𝖺𝗅𝗅𝗈𝗐𝖾𝖽 𝖺𝗇𝖽 𝗁𝖺𝗏𝖾 𝖻𝖾𝖾𝗇 𝖽𝖾𝗅𝖾𝗍𝖾𝖽.**" if message_type == "media" else "**🚫 𝖤𝗑𝗍𝖾𝗋𝗇𝖺𝗅 𝗅𝗂𝗇𝗄𝗌 𝖺𝗋𝖾 𝗇𝗈𝗍 𝖺𝗅𝗅𝗈𝗐𝖾𝖽 𝖺𝗇𝖽 𝗁𝖺𝗏𝖾 𝖻𝖾𝖾𝗇 𝖽𝖾𝗅𝖾𝗍𝖾𝖽.**"
            
            warning_msg = await client.send_message(
                chat_id=message.chat.id,
                text=warning_text
            )
            
            delete_time = datetime.now() + timedelta(seconds=10)

            async def job_wrapper():
                await delete_warning_message(client, warning_msg.chat.id, warning_msg.id)
            
            scheduler.add_job(
                job_wrapper,
                'date',
                run_date=delete_time
            )
            
        except Exception as e:
            logger.error(f"Could not send warning: {e}")
    
    except Exception as e:
        logger.error(f"Error in delete_media_and_links handler: {e}")
