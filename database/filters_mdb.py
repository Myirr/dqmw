import pymongo, logging
from pyrogram import enums
from info import FILTER_DB, DATABASE_NAME
from database.postgres import pgDb

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


mydb = pymongo.MongoClient(FILTER_DB)[DATABASE_NAME]
grp_id = "globalfilter"

async def add_filter(text, reply_text, btn, file, alert):
    mydb[str(grp_id)].update_one({'text': str(text)}, {"$set": {
        'text': str(text), 'reply': str(reply_text), 'btn': str(btn), 'file': str(file), 'alert': str(alert)
    }}, upsert=True)
    await pgDb.add_filter(str(text), str(reply_text), str(btn), str(file), str(alert))

async def find_filter(name):
    file = mydb[str(grp_id)].find_one({"text": name})
    if file:
        return file['reply'], file['btn'], file.get('alert'), file['file']
    return None, None, None, None

async def get_filters():
    return [file['text'] for file in mydb[str(grp_id)].find()]

async def delete_filter(message, text):
    if mydb[str(grp_id)].count_documents({'text': text}) == 1:
        mydb[str(grp_id)].delete_one({'text': text})
        await pgDb.delete_filter(text)
        await message.reply_text(f"'`{text}`' deleted. I'll not respond to that filter anymore.", quote=True, parse_mode=enums.ParseMode.MARKDOWN)
    else:
        await message.reply_text("Couldn't find that filter!", quote=True)

async def del_all(message):
    if str(grp_id) in mydb.list_collection_names():
        mydb[str(grp_id)].drop()
        await pgDb.delete_all_filters()
        await message.edit_text(f"All filters have been removed")
    else:
        await message.edit_text(f"Nothing to remove")

async def count_filters():
    return mydb[str(grp_id)].count_documents({}) or False

async def filter_stats():
    collections = [col for col in mydb.list_collection_names() if col != "CONNECTION"]
    totalcount = sum(mydb[col].count_documents({}) for col in collections)
    return len(collections), totalcount
