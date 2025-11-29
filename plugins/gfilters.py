import io
from pyrogram import filters, Client, enums
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from database.filters_mdb import add_filter, get_filters, delete_filter, count_filters
from utils import get_file_id, parser, split_quotes
from info import ADMINS
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@Client.on_message(filters.command(['filter', 'add']) & filters.user(ADMINS) & filters.private)
async def addfilter(client, message):
    args = message.text.html.split(None, 1)
    if len(args) < 2: return await message.reply_text("Command Incomplete :(", quote=True)

    extracted = split_quotes(args[1])
    text = extracted[0].lower()

    if not message.reply_to_message and len(extracted) < 2:
        return await message.reply_text("Add some content to save your filter!", quote=True)

    if (len(extracted) >= 2) and not message.reply_to_message:
        reply_text, btn, alert = parser(extracted[1], text)
        fileid = None
        if not reply_text:
            return await message.reply_text("You cannot have buttons alone, give some text to go with it!", quote=True)

    elif message.reply_to_message and message.reply_to_message.reply_markup:
        try:
            rm = message.reply_to_message.reply_markup
            btn = rm.inline_keyboard
            msg = get_file_id(message.reply_to_message)
            if msg:
                fileid = msg.file_id
                reply_text = message.reply_to_message.caption.html
            else:
                reply_text = message.reply_to_message.text.html
                fileid = None
            alert = None
        except:
            reply_text = ""
            btn = "[]" 
            fileid = None
            alert = None

    elif message.reply_to_message and message.reply_to_message.media:
        try:
            msg = get_file_id(message.reply_to_message)
            fileid = msg.file_id if msg else None
            reply_text, btn, alert = parser(extracted[1], text) if message.reply_to_message.sticker else parser(message.reply_to_message.caption.html, text)
        except:
            reply_text = ""
            btn = "[]"
            alert = None
    elif message.reply_to_message and message.reply_to_message.text:
        try:
            fileid = None
            reply_text, btn, alert = parser(message.reply_to_message.text.html, text)
        except:
            reply_text = ""
            btn = "[]"
            alert = None
    else:
        return
    await add_filter(text, reply_text, btn, fileid, alert)
    await message.reply_text(f"Filter for  `{text}`  added", quote=True, parse_mode=enums.ParseMode.MARKDOWN)


@Client.on_message(filters.command(['viewfilters', 'filters']) & filters.user(ADMINS) & filters.private)
async def get_all(client, message):
    texts = await get_filters()
    count = await count_filters()
    if count:
        filterlist = f"Total number of filters: {count}\n\n"
        for num, text in enumerate(texts, 1):
            filterlist += f"{num}. `{text}`\n"
        if len(filterlist) > 4096:
            with io.BytesIO(str.encode(filterlist.replace("`", ""))) as keyword_file:
                keyword_file.name = "keywords.txt"
                await message.reply_document(document=keyword_file, quote=True)
            return
    else:
        filterlist = f"There are no active filters"
    await message.reply_text(text=filterlist, quote=True, parse_mode=enums.ParseMode.MARKDOWN)
        

@Client.on_message(filters.command('del') & filters.user(ADMINS) & filters.private)
async def deletefilter(client, message):
    try:
        cmd, text = message.text.split(" ", 1)
    except:
        return await message.reply_text(
            "<i>Mention the filtername which you wanna delete!</i>\n\n<code>/del filtername</code>\n\nUse /viewfilters to view all available filters",
            quote=True
        )
    query = text.strip().lower()
    await delete_filter(message, query)
        

@Client.on_message(filters.command('delall') & filters.user(ADMINS) & filters.private)
async def delallconfirm(client, message):
    await message.reply_text(
        f"This will delete all filters.\nDo you want to continue??",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(text="YES",callback_data="delallconfirm")],
            [InlineKeyboardButton(text="CANCEL",callback_data="delallcancel")]
        ]),
        quote=True
    )
