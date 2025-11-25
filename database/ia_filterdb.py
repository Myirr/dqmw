import logging
import re
import asyncio
import math
import base64
from datetime import datetime
from struct import pack
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError, ConnectionFailure, ServerSelectionTimeoutError
from pyrogram.file_id import FileId
from info import DATABASE_URI, DATABASE_NAME, COLLECTION_NAME, ADMINS, FILTER_DB, FILE_URI1, FILE_URI2, FILE_URI3, FILE_URI4
from aiocache import Cache
from aiocache.decorators import cached
from functools import lru_cache
import heapq
from asyncio import TimeoutError, wait_for
from database.postgres import pgDb

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

cache = Cache(Cache.MEMORY)
file_details_cache = {}

uris = [FILE_URI1, FILE_URI2, FILE_URI3, FILE_URI4]
instances = []
instance1 = instance2 = instance3 = instance4 = None
db_initialized = False

async def initialize_db_connections():
    global instances, instance1, instance2, instance3, instance4, db_initialized
    
    if db_initialized:
        return True
        
    instances = []
    
    for uri in uris:
        try:
            client = AsyncIOMotorClient(
                uri, 
                serverSelectionTimeoutMS=5000,
                maxPoolSize=50,
                minPoolSize=10,
                maxIdleTimeMS=30000
            )
            await client.server_info()
            db = client[DATABASE_NAME]
            instances.append(db[COLLECTION_NAME])
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to media database at {uri}: {e}")
            instances.append(None)
        except Exception as e:
            logger.error(f"Unexpected error connecting to media database {uri}: {e}")
            instances.append(None)
    
    instance_count = len(instances)
    instance1 = instances[0] if instance_count > 0 else None
    instance2 = instances[1] if instance_count > 1 else None
    instance3 = instances[2] if instance_count > 2 else None
    instance4 = instances[3] if instance_count > 3 else None
    
    db_initialized = any(instance is not None for instance in instances)
    if not db_initialized:
        logger.error("Failed to initialize any media database connections")
    
    return db_initialized

async def save_file(media):
    if not pgDb.pool:
        await pgDb.connect()

    if not db_initialized:
        await initialize_db_connections()

    if not any(instance is not None for instance in instances):
        logger.error("All database instances are None. Aborting save operation.")
        return False, 0

    if not hasattr(media, 'file_id') or not media.file_id:
        logger.warning("Media missing file_id attribute")
        return False, 2

    file_id, _ = unpack_new_file_id(media.file_id)

    file = {
        'file_id': file_id,
        'file_name': re.sub(r'[\[\]\(\)@_\-\.\+]', ' ', str(getattr(media, 'file_name', ''))).strip(),
        'file_size': getattr(media, 'file_size', 0),
        'caption': media.caption.html if hasattr(media, 'caption') and media.caption else None,
        'created_at': datetime.now(),
        'backup': False
    }
    
    file['file_name'] = ' '.join(file['file_name'].split())

    valid_instances = [i for i in instances if i is not None]

    await pgDb.append_file(
        filename=file['file_name'],
        fileid=file['file_id'],
        filesize=file['file_size'],
        caption=file['caption']
    )

    async def check_duplicate(instance):
        try:
            return await instance.find_one({'file_id': file_id})
        except Exception:
            return None

    results = await asyncio.gather(*[check_duplicate(i) for i in valid_instances], return_exceptions=True)
    if any(r for r in results if r and not isinstance(r, Exception)):
        return False, 0

    for instance in valid_instances:
        try:
            stats = await instance.database.command('dbstats')
            if stats.get('dataSize', 0) > 480 * 1024 * 1024:
                continue

            try:
                result = await instance.insert_one(file)
                if result.inserted_id:
                    return True, 1
            except DuplicateKeyError:
                return False, 0
            except Exception:
                pass
        except Exception:
            pass
    
    return False, 2

def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0

    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])

    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref
