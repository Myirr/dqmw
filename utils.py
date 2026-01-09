import logging
from pyrogram.errors import InputUserDeactivated, UserNotParticipant, FloodWait, UserIsBlocked, PeerIdInvalid
from info import AUTH_CHANNEL, LONG_IMDB_DESCRIPTION, MAX_LIST_ELM
from imdb import Cinemagoer 
import asyncio
from pyrogram.types import Message, InlineKeyboardButton
from pyrogram import enums
from typing import Union
import random 
import re
import os
from datetime import datetime
from typing import List
from database.users_chats_db import db
from bs4 import BeautifulSoup
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BTN_URL_REGEX = re.compile(
    r"(\[([^\[]+?)\]\((buttonurl|buttonalert):(?:/{0,2})(.+?)(:same)?\))"
)

imdb = Cinemagoer() 

BANNED = {}
SMART_OPEN = '“'
SMART_CLOSE = '”'
START_CHAR = ('\'', '"', SMART_OPEN)

# temp db for banned 
class temp(object):
    BANNED_USERS = []
    BANNED_CHATS = []
    ME = None
    CURRENT=int(os.environ.get("SKIP", 2))
    CANCEL = False
    MELCOW = {}
    U_NAME = None
    B_NAME = None
    SETTINGS = {}

async def is_subscribed(bot, query):
    try:
        user = await bot.get_chat_member(AUTH_CHANNEL, query.from_user.id)
    except UserNotParticipant:
        pass
    except Exception as e:
        logger.exception(e)
    else:
        if user.status != 'kicked':
            return True

    return False

import re
import aiohttp
import difflib
from urllib.parse import quote_plus

from info import TMDB_API_KEY  # make sure it's in info.py

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG  = "https://image.tmdb.org/t/p/original"

def list_to_str(x):
    if not x:
        return "N/A"
    if isinstance(x, str):
        return x
    try:
        return ", ".join([str(i) for i in x if i])
    except Exception:
        return str(x)

def _extract_year(query: str, file: str | None = None):
    # only extract year (no other title changes)
    y = re.findall(r"[1-2]\d{3}$", query or "", flags=re.IGNORECASE)
    if y:
        year = y[0]
        title = (query.replace(year, "")).strip()
        return title, int(year)
    if file:
        y2 = re.findall(r"[1-2]\d{3}", file or "", flags=re.IGNORECASE)
        if y2:
            return query.strip(), int(y2[0])
    return query.strip(), None

async def _tmdb_get(session: aiohttp.ClientSession, url: str):
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
        if r.status != 200:
            return None
        return await r.json()

async def _tmdb_search_multi(session, title: str, year: int | None):
    url = f"{TMDB_BASE}/search/multi?api_key={TMDB_API_KEY}&query={quote_plus(title)}&include_adult=false"
    data = await _tmdb_get(session, url)
    if not data:
        return []

    results = data.get("results") or []
    # keep only tv/movie
    results = [x for x in results if x.get("media_type") in ("tv", "movie")]

    # if year is present, prefer matching year but don’t hard-filter
    if year:
        def _year_of(item):
            d = item.get("first_air_date") if item.get("media_type") == "tv" else item.get("release_date")
            if d and len(d) >= 4 and d[:4].isdigit():
                return int(d[:4])
            return None

        scored = []
        for it in results:
            y = _year_of(it)
            bonus = 0.0
            if y == year:
                bonus = 0.25
            elif y is not None and abs(y - year) == 1:
                bonus = 0.10
            sc = difflib.SequenceMatcher(None, title.lower(), (it.get("name") or it.get("title") or "").lower()).ratio()
            scored.append((sc + bonus, it))
        scored.sort(key=lambda t: t[0], reverse=True)
        return [x for _, x in scored]

    # otherwise sort by best title similarity
    results.sort(
        key=lambda it: difflib.SequenceMatcher(
            None, title.lower(), (it.get("name") or it.get("title") or "").lower()
        ).ratio(),
        reverse=True
    )
    return results

async def _tmdb_details(session, media_type: str, tmdb_id: int):
    # details + credits + external ids
    url = f"{TMDB_BASE}/{media_type}/{tmdb_id}?api_key={TMDB_API_KEY}&append_to_response=credits,external_ids"
    return await _tmdb_get(session, url)

def _pick_people(credits: dict, key: str, limit: int = 10):
    if not credits:
        return "N/A"
    arr = credits.get(key) or []
    names = [p.get("name") for p in arr if p.get("name")]
    return list_to_str(names[:limit]) if names else "N/A"

def _pick_jobs(credits: dict, jobs: set[str], limit: int = 10):
    if not credits:
        return "N/A"
    crew = credits.get("crew") or []
    names = []
    for c in crew:
        if c.get("job") in jobs and c.get("name"):
            names.append(c["name"])
    # unique preserve order
    uniq = []
    for n in names:
        if n not in uniq:
            uniq.append(n)
    return list_to_str(uniq[:limit]) if uniq else "N/A"

async def get_poster(query, bulk=False, id=False, file=None):
    """
    TMDB-ONLY replacement for your Cinemagoer get_poster().
    Returns dict with same keys your IMDB_TEMPLATE expects (missing -> 'N/A').
    """
    # keep your signature compatible
    q = (query or "").strip()
    if not q:
        return None

    title, year = _extract_year(q.lower() if not id else q, file=file)

    async with aiohttp.ClientSession() as session:
        # If id=True, treat query as TMDB id is NOT supported in your flow,
        # so we still do search. (You can extend to support tmdb ids if you want.)
        results = await _tmdb_search_multi(session, title, year)
        if not results:
            return None
        if bulk:
            return results

        best = results[0]
        media_type = best["media_type"]  # "tv" or "movie"
        tmdb_id = best["id"]

        details = await _tmdb_details(session, media_type, tmdb_id)
        if not details:
            return None

        # common fields
        if media_type == "tv":
            name = details.get("name") or best.get("name") or title
            date = details.get("first_air_date") or best.get("first_air_date") or "N/A"
            kind = "tv series"
            seasons = details.get("number_of_seasons") or "N/A"
            runtime = details.get("episode_run_time") or []
            runtime = list_to_str(runtime) if runtime else "N/A"
        else:
            name = details.get("title") or best.get("title") or title
            date = details.get("release_date") or best.get("release_date") or "N/A"
            kind = "movie"
            seasons = "N/A"
            runtime = details.get("runtime")
            runtime = str(runtime) if runtime else "N/A"

        # external ids
        ext = details.get("external_ids") or {}
        imdb_id = ext.get("imdb_id")
        imdb_id = imdb_id if imdb_id else "N/A"

        # poster
        poster_path = details.get("poster_path") or best.get("poster_path")
        poster = f"{TMDB_IMG}{poster_path}" if poster_path else None

        # overview/plot
        plot = details.get("overview") or "N/A"
        if plot != "N/A" and len(plot) > 800:
            plot = plot[:800] + "..."

        # rating/votes
        rating = details.get("vote_average")
        votes = details.get("vote_count")

        # genres
        genres = details.get("genres") or []
        genres = list_to_str([g.get("name") for g in genres if g.get("name")]) if genres else "N/A"

        # credits
        credits = details.get("credits") or {}
        cast = _pick_people(credits, "cast", limit=12)
        director = _pick_jobs(credits, {"Director"}, limit=5)
        writer = _pick_jobs(credits, {"Writer", "Screenplay", "Story", "Creator"}, limit=8)
        producer = _pick_jobs(credits, {"Producer", "Executive Producer"}, limit=8)
        composer = _pick_jobs(credits, {"Original Music Composer", "Composer"}, limit=5)
        cinematographer = _pick_jobs(credits, {"Director of Photography"}, limit=5)

        # TMDB urls
        url = f"https://www.themoviedb.org/{media_type}/{tmdb_id}"

        # Fill keys your template expects (unknown ones -> N/A)
        return {
            "title": name,
            "votes": votes if votes is not None else "N/A",
            "aka": "N/A",
            "seasons": seasons,
            "box_office": "N/A",
            "localized_title": "N/A",
            "kind": kind,
            "imdb_id": imdb_id if imdb_id == "N/A" else imdb_id,  # already "tt...."
            "cast": cast,
            "runtime": runtime,
            "countries": "N/A",
            "certificates": "N/A",
            "languages": "N/A",
            "director": director,
            "writer": writer,
            "producer": producer,
            "composer": composer,
            "cinematographer": cinematographer,
            "music_team": "N/A",
            "distributors": "N/A",
            "release_date": date,
            "year": (str(year) if year else (date[:4] if isinstance(date, str) and len(date) >= 4 else "N/A")),
            "genres": genres,
            "poster": poster,  # this is what you use in reply_photo
            "plot": plot,
            "rating": str(rating) if rating is not None else "N/A",
            "url": url
}
        
async def broadcast_messages(user_id, message):
    try:
        await message.copy(chat_id=user_id)
        return True, "Success"
    except FloodWait as e:
        await asyncio.sleep(e.x)
        return await broadcast_messages(user_id, message)
    except InputUserDeactivated:
        await db.delete_user(int(user_id))
        logging.info(f"{user_id}-Removed from Database, since deleted account.")
        return False, "Deleted"
    except UserIsBlocked:
        logging.info(f"{user_id} -Blocked the bot.")
        return False, "Blocked"
    except PeerIdInvalid:
        await db.delete_user(int(user_id))
        logging.info(f"{user_id} - PeerIdInvalid")
        return False, "Error"
    except Exception as e:
        return False, "Error"

async def search_gagala(text):
    agent_list = [
        #'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
        #'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
        #'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0',
        #'Opera/9.80 (Macintosh; Intel Mac OS X; U; en) Presto/2.2.15 Version/10.00',
        #'Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1',
        #'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59'
    ]
    text = text.replace(" ", '+')
    url = f'https://www.google.com/search?q={text}'
    retries = 1
    success = False
    while not success:
        try:
            usr_agent = {
                'User-Agent': random.choice(agent_list)
                }
            response = requests.get(url, headers=usr_agent)
            logging.info(f"Used agent = {usr_agent['User-Agent']}")
            success = True
        except Exception as e:
            wait = retries * 10
            logging.info(f"Error: {e}\n\nWait for {wait} seconds to retry !")
            asyncio.sleep(wait)
            retries += 1
    soup = BeautifulSoup(response.text, 'html.parser')
    titles = soup.find_all( 'h3' )
    return [title.getText() for title in titles]


async def get_settings(group_id):
    settings = temp.SETTINGS.get(group_id)
    if not settings:
        settings = await db.get_settings(group_id)
        temp.SETTINGS[group_id] = settings
    return settings
    
async def save_group_settings(group_id, key, value):
    current = await get_settings(group_id)
    current[key] = value
    temp.SETTINGS[group_id] = current
    await db.update_settings(group_id, current)
    
def get_size(size):
    """Get size in readable format"""

    units = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB"]
    size = float(size)
    i = 0
    while size >= 1024.0 and i < len(units):
        i += 1
        size /= 1024.0
    return "%.2f %s" % (size, units[i])

def split_list(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]  

def get_file_id(msg: Message):
    if msg.media:
        for message_type in (
            "photo",
            "animation",
            "audio",
            "document",
            "video",
            "video_note",
            "voice",
            "sticker"
        ):
            obj = getattr(msg, message_type)
            if obj:
                setattr(obj, "message_type", message_type)
                return obj

def extract_user(message: Message) -> Union[int, str]:
    """extracts the user from a message"""
    # https://github.com/SpEcHiDe/PyroGramBot/blob/f30e2cca12002121bad1982f68cd0ff9814ce027/pyrobot/helper_functions/extract_user.py#L7
    user_id = None
    user_first_name = None
    if message.reply_to_message:
        user_id = message.reply_to_message.from_user.id
        user_first_name = message.reply_to_message.from_user.first_name

    elif len(message.command) > 1:
        if (
            len(message.entities) > 1 and
            message.entities[1].type == enums.MessageEntityType.TEXT_MENTION
        ):
           
            required_entity = message.entities[1]
            user_id = required_entity.user.id
            user_first_name = required_entity.user.first_name
        else:
            user_id = message.command[1]
            # don't want to make a request -_-
            user_first_name = user_id
        try:
            user_id = int(user_id)
        except ValueError:
            pass
    else:
        user_id = message.from_user.id
        user_first_name = message.from_user.first_name
    return (user_id, user_first_name)

def list_to_str(k):
    if not k:
        return "N/A"
    elif len(k) == 1:
        return str(k[0])
    elif MAX_LIST_ELM:
        k = k[:int(MAX_LIST_ELM)]
        return ' '.join(f'{elem}, ' for elem in k)
    else:
        return ' '.join(f'{elem}, ' for elem in k)

def last_online(from_user):
    time = ""
    if from_user.is_bot:
        time += "🤖 Bot :("
    elif from_user.status == enums.UserStatus.RECENTLY:
        time += "Recently"
    elif from_user.status == enums.UserStatus.LAST_WEEK:
        time += "Within the last week"
    elif from_user.status == enums.UserStatus.LAST_MONTH:
        time += "Within the last month"
    elif from_user.status == enums.UserStatus.LONG_AGO:
        time += "A long time ago :("
    elif from_user.status == enums.UserStatus.ONLINE:
        time += "Currently Online"
    elif from_user.status == enums.UserStatus.OFFLINE:
        time += from_user.last_online_date.strftime("%a, %d %b %Y, %H:%M:%S")
    return time


def split_quotes(text: str) -> List:
    if not any(text.startswith(char) for char in START_CHAR):
        return text.split(None, 1)
    counter = 1  # ignore first char -> is some kind of quote
    while counter < len(text):
        if text[counter] == "\\":
            counter += 1
        elif text[counter] == text[0] or (text[0] == SMART_OPEN and text[counter] == SMART_CLOSE):
            break
        counter += 1
    else:
        return text.split(None, 1)

    # 1 to avoid starting quote, and counter is exclusive so avoids ending
    key = remove_escapes(text[1:counter].strip())
    # index will be in range, or `else` would have been executed and returned
    rest = text[counter + 1:].strip()
    if not key:
        key = text[0] + text[0]
    return list(filter(None, [key, rest]))

def parser(text, keyword):
    if "buttonalert" in text:
        text = (text.replace("\n", "\\n").replace("\t", "\\t"))
    buttons = []
    note_data = ""
    prev = 0
    i = 0
    alerts = []
    for match in BTN_URL_REGEX.finditer(text):
        # Check if btnurl is escaped
        n_escapes = 0
        to_check = match.start(1) - 1
        while to_check > 0 and text[to_check] == "\\":
            n_escapes += 1
            to_check -= 1

        # if even, not escaped -> create button
        if n_escapes % 2 == 0:
            note_data += text[prev:match.start(1)]
            prev = match.end(1)
            if match.group(3) == "buttonalert":
                # create a thruple with button label, url, and newline status
                if bool(match.group(5)) and buttons:
                    buttons[-1].append(InlineKeyboardButton(
                        text=match.group(2),
                        callback_data=f"alertmessage:{i}:{keyword}"
                    ))
                else:
                    buttons.append([InlineKeyboardButton(
                        text=match.group(2),
                        callback_data=f"alertmessage:{i}:{keyword}"
                    )])
                i += 1
                alerts.append(match.group(4))
            elif bool(match.group(5)) and buttons:
                buttons[-1].append(InlineKeyboardButton(
                    text=match.group(2),
                    url=match.group(4).replace(" ", "")
                ))
            else:
                buttons.append([InlineKeyboardButton(
                    text=match.group(2),
                    url=match.group(4).replace(" ", "")
                )])

        else:
            note_data += text[prev:to_check]
            prev = match.start(1) - 1
    else:
        note_data += text[prev:]

    try:
        return note_data, buttons, alerts
    except:
        return note_data, buttons, None

def remove_escapes(text: str) -> str:
    res = ""
    is_escaped = False
    for counter in range(len(text)):
        if is_escaped:
            res += text[counter]
            is_escaped = False
        elif text[counter] == "\\":
            is_escaped = True
        else:
            res += text[counter]
    return res


def humanbytes(size):
    if not size:
        return ""
    power = 2**10
    n = 0
    Dic_powerN = {0: ' ', 1: 'Ki', 2: 'Mi', 3: 'Gi', 4: 'Ti'}
    while size > power:
        size /= power
        n += 1
    return str(round(size, 2)) + " " + Dic_powerN[n] + 'B'
