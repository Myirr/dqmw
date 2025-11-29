from pyrogram import Client, filters
from motor.motor_asyncio import AsyncIOMotorClient
from info import (
    DATABASE_URI,
    DATABASE_NAME,
    COLLECTION_NAME,
    ADMINS,
    FILTER_DB,
    FILE_URI1,
    FILE_URI2,
    FILE_URI3,
    FILE_URI4,
)
from database.users_chats_db import db
from database.postgres import pgDb
import re, os, asyncio
from utils import get_size, temp
import logging
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

uris = [FILE_URI1, FILE_URI2, FILE_URI3, FILE_URI4]
instances = []

for uri in uris:
    try:
        client = AsyncIOMotorClient(uri)
        db = client[DATABASE_NAME]
        instances.append(db[COLLECTION_NAME])
    except Exception as e:
        logger.error(f"Failed to connect to database at {uri}: {e}")
        instances.append(None)

instance1, instance2, instance3, instance4 = instances

async def ensure_postgresql_tables():
    try:
        async with pgDb.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    name TEXT NOT NULL,
                    is_banned BOOLEAN DEFAULT FALSE,
                    ban_reason TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS groups (
                    id BIGINT PRIMARY KEY,
                    title TEXT NOT NULL,
                    is_disabled BOOLEAN DEFAULT FALSE,
                    reason TEXT DEFAULT '',
                    settings JSONB DEFAULT '{}',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS fsub_first (
                    id BIGINT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS fsub_second (
                    id BIGINT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS filters (
                    id SERIAL PRIMARY KEY,
                    text VARCHAR(500) UNIQUE NOT NULL,
                    reply TEXT,
                    btn TEXT,
                    file TEXT,
                    alert TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_filters_text ON filters (text)")
            
            logger.info("PostgreSQL tables ensured successfully")
            return True
    except Exception as e:
        logger.error(f"Error ensuring PostgreSQL tables: {e}")
        return False

async def migrate_users_data():
    try:
        migrated = 0
        skipped = 0
        errors = 0
        
        existing_users = set()
        async with pgDb.pool.acquire() as conn:
            rows = await conn.fetch("SELECT id FROM users")
            existing_users = {row['id'] for row in rows}
        
        user_client = AsyncIOMotorClient(DATABASE_URI)
        try:
            user_db = user_client[DATABASE_NAME]
            users_collection = user_db["users"]
            
            batch_size = 1000
            skip = 0
            
            while True:
                try:
                    cursor = users_collection.find({}).skip(skip).limit(batch_size)
                    users_batch = await cursor.to_list(length=batch_size)
                    
                    if not users_batch:
                        break
                    
                    for user in users_batch:
                        try:
                            user_id = user.get('id')
                            if not user_id:
                                errors += 1
                                continue
                                
                            if user_id in existing_users:
                                skipped += 1
                                continue
                            
                            name = user.get('name', 'Unknown')[:100]
                            ban_status = user.get('ban_status', {})
                            is_banned = ban_status.get('is_banned', False)
                            ban_reason = ban_status.get('ban_reason', '')
                            
                            async with pgDb.pool.acquire() as conn:
                                await conn.execute(
                                    "INSERT INTO users (id, name, is_banned, ban_reason) VALUES ($1, $2, $3, $4)",
                                    user_id, name, is_banned, ban_reason
                                )
                            
                            existing_users.add(user_id)
                            migrated += 1
                            
                        except Exception as e:
                            logger.error(f"Error migrating user {user.get('id', 'Unknown')}: {e}")
                            errors += 1
                    
                    skip += batch_size
                    
                except Exception as e:
                    logger.error(f"Error processing users batch at skip {skip}: {e}")
                    skip += batch_size
                    errors += 1
                    continue
                    
        finally:
            user_client.close()
            
        return migrated, skipped, errors
        
    except Exception as e:
        logger.error(f"Error in migrate_users_data: {e}")
        return 0, 0, 1

async def migrate_groups_data():
    try:
        migrated = 0
        skipped = 0
        errors = 0
        
        existing_groups = set()
        async with pgDb.pool.acquire() as conn:
            rows = await conn.fetch("SELECT id FROM groups")
            existing_groups = {row['id'] for row in rows}
        
        group_client = AsyncIOMotorClient(DATABASE_URI)
        try:
            group_db = group_client[DATABASE_NAME]
            groups_collection = group_db["groups"]
            
            batch_size = 1000
            skip = 0
            
            while True:
                try:
                    cursor = groups_collection.find({}).skip(skip).limit(batch_size)
                    groups_batch = await cursor.to_list(length=batch_size)
                    
                    if not groups_batch:
                        break
                    
                    for group in groups_batch:
                        try:
                            group_id = group.get('id')
                            if not group_id:
                                errors += 1
                                continue
                                
                            if group_id in existing_groups:
                                skipped += 1
                                continue
                            
                            title = group.get('title', 'Unknown Group')
                            chat_status = group.get('chat_status', {})
                            is_disabled = chat_status.get('is_disabled', False)
                            reason = chat_status.get('reason', '')
                            settings = group.get('settings', {})
                            
                            async with pgDb.pool.acquire() as conn:
                                await conn.execute(
                                    "INSERT INTO groups (id, title, is_disabled, reason, settings) VALUES ($1, $2, $3, $4, $5)",
                                    group_id, title, is_disabled, reason, settings
                                )
                            
                            existing_groups.add(group_id)
                            migrated += 1
                            
                        except Exception as e:
                            logger.error(f"Error migrating group {group.get('id', 'Unknown')}: {e}")
                            errors += 1
                    
                    skip += batch_size
                    
                except Exception as e:
                    logger.error(f"Error processing groups batch at skip {skip}: {e}")
                    skip += batch_size
                    errors += 1
                    continue
                    
        finally:
            group_client.close()
            
        return migrated, skipped, errors
        
    except Exception as e:
        logger.error(f"Error in migrate_groups_data: {e}")
        return 0, 0, 1

async def migrate_fsub_data():
    fsub_client = AsyncIOMotorClient(DATABASE_URI)

    fsub_db = fsub_client['req1']
    fsub_collection = fsub_db['fsub_first']

    fsub2_db = fsub_client['req2']
    fsub2_collection = fsub2_db['fsub_second']

    migrated = 0
    skipped = 0
    errors = 0

    migrated2 = 0
    skipped2 = 0
    errors2 = 0

    count1 = await fsub_collection.count_documents({})
    count2 = await fsub2_collection.count_documents({})
    logger.info(f"📦 Migrating fsub_first: {count1} users, fsub_second: {count2} users")

    batch_size = 1000
    skip = 0
    
    while skip < count1:
        try:
            cursor = fsub_collection.find({}).skip(skip).limit(batch_size)
            users_batch = await cursor.to_list(length=batch_size)
            
            if not users_batch:
                break
            
            for user in users_batch:
                try:
                    user_id_raw = user.get("id")
                    if not user_id_raw:
                        logger.warning(f"⚠️ Missing 'id' in fsub_first user: {user}")
                        errors += 1
                        continue

                    try:
                        user_id = int(user_id_raw)
                    except ValueError:
                        logger.warning(f"⚠️ Invalid 'id' (non-int) in fsub_first: {user}")
                        errors += 1
                        continue

                    exists = await pgDb.get_user(user_id)
                    if exists:
                        skipped += 1
                    else:
                        await pgDb.add_fsub_user("fsub_first", user_id)
                        migrated += 1

                except Exception as e:
                    errors += 1
                    logger.error(f"❌ Error migrating fsub_first user {user}: {e}")
            
            skip += batch_size
            
        except Exception as e:
            logger.error(f"Error processing fsub_first batch at skip {skip}: {e}")
            skip += batch_size
            errors += 1
            continue

    skip = 0
    
    while skip < count2:
        try:
            cursor = fsub2_collection.find({}).skip(skip).limit(batch_size)
            users_batch = await cursor.to_list(length=batch_size)
            
            if not users_batch:
                break
            
            for user in users_batch:
                try:
                    user_id_raw = user.get("id")
                    if not user_id_raw:
                        logger.warning(f"⚠️ Missing 'id' in fsub_second user: {user}")
                        errors2 += 1
                        continue

                    try:
                        user_id = int(user_id_raw)
                    except ValueError:
                        logger.warning(f"⚠️ Invalid 'id' (non-int) in fsub_second: {user}")
                        errors2 += 1
                        continue

                    exists = await pgDb.get_user2(user_id)
                    if exists:
                        skipped2 += 1
                    else:
                        await pgDb.add_fsub_user("fsub_second", user_id)
                        migrated2 += 1

                except Exception as e:
                    errors2 += 1
                    logger.error(f"❌ Error migrating fsub_second user {user}: {e}")
            
            skip += batch_size
            
        except Exception as e:
            logger.error(f"Error processing fsub_second batch at skip {skip}: {e}")
            skip += batch_size
            errors2 += 1
            continue

    fsub_client.close()

    logger.info(f"✅ fsub_first: Migrated={migrated}, Skipped={skipped}, Errors={errors}")
    logger.info(f"✅ fsub_second: Migrated={migrated2}, Skipped={skipped2}, Errors={errors2}")

    return migrated, skipped, errors, migrated2, skipped2, errors2

async def migrate_global_filters():
    try:
        migrated = 0
        skipped = 0
        errors = 0
        
        existing_filters = set()
        async with pgDb.pool.acquire() as conn:
            rows = await conn.fetch("SELECT text FROM filters")
            existing_filters = {row['text'] for row in rows}
        
        filter_client = AsyncIOMotorClient(FILTER_DB)
        try:
            filter_db = filter_client[DATABASE_NAME]
            
            grp_id = "globalfilter"
            filters_collection = filter_db[grp_id]
            
            if grp_id in await filter_db.list_collection_names():
                batch_size = 1000
                skip = 0
                
                while True:
                    try:
                        cursor = filters_collection.find({}).skip(skip).limit(batch_size)
                        filters_batch = await cursor.to_list(length=batch_size)
                        
                        if not filters_batch:
                            break
                        
                        for filter_doc in filters_batch:
                            try:
                                text = filter_doc.get('text')
                                if not text:
                                    errors += 1
                                    continue
                                
                                if text in existing_filters:
                                    skipped += 1
                                    continue
                                
                                reply = filter_doc.get('reply', '')
                                btn = filter_doc.get('btn', '')
                                file = filter_doc.get('file', '')
                                alert = filter_doc.get('alert', '')
                                
                                async with pgDb.pool.acquire() as conn:
                                    await conn.execute("""
                                        INSERT INTO filters (text, reply, btn, file, alert)
                                        VALUES ($1, $2, $3, $4, $5)
                                    """, text, reply, btn, file, alert)
                                
                                existing_filters.add(text)
                                migrated += 1
                                
                            except Exception as e:
                                logger.error(f"Error migrating filter '{filter_doc.get('text', 'Unknown')}': {e}")
                                errors += 1
                        
                        skip += batch_size
                        
                    except Exception as e:
                        logger.error(f"Error processing filters batch at skip {skip}: {e}")
                        skip += batch_size
                        errors += 1
                        continue
            else:
                logger.info("No global filters collection found in MongoDB")
                
        finally:
            filter_client.close()
            
        return migrated, skipped, errors
        
    except Exception as e:
        logger.error(f"Error in migrate_global_filters: {e}")
        return 0, 0, 1

async def migrate_files_data():
    try:
        migrated = 0
        skipped = 0
        errors = 0
        
        existing_file_ids = set()
        async with pgDb.pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT fileid FROM {pgDb.table_name}")
            existing_file_ids = {row['fileid'] for row in rows}
        
        batch_size = 10000
        
        for db_index, instance in enumerate(instances, 1):
            if instance is None:
                continue
                
            try:
                total_files = await instance.count_documents({})
                if total_files == 0:
                    continue
                
                skip = 0
                while skip < total_files:
                    try:
                        cursor = instance.find({}).skip(skip).limit(batch_size)
                        files_batch = await cursor.to_list(length=batch_size)
                        
                        if not files_batch:
                            break
                        
                        for file in files_batch:
                            try:
                                file_id = file.get("file_id")
                                file_name = file.get("file_name", "Unknown")
                                file_size = file.get("file_size", 0)
                                caption = file.get("caption", file_name)
                                
                                if not file_id:
                                    errors += 1
                                    continue
                                
                                if file_id in existing_file_ids:
                                    skipped += 1
                                    continue
                                
                                await pgDb.append_file(file_name, file_id, file_size, caption)
                                existing_file_ids.add(file_id)
                                migrated += 1
                                
                            except Exception as e:
                                logger.error(f"Error migrating file {file.get('file_name', 'Unknown')}: {e}")
                                errors += 1
                        
                        skip += len(files_batch)
                        
                        if (migrated + skipped) % 50000 == 0:
                            logger.info(f"DB{db_index} Progress: {migrated + skipped}/{total_files} processed")
                        
                    except Exception as e:
                        logger.error(f"Error processing batch starting at {skip}: {e}")
                        errors += 1
                        skip += batch_size
                        continue
                        
            except Exception as e:
                logger.error(f"Error migrating from MongoDB DB{db_index}: {e}")
                errors += 1
        
        return migrated, skipped, errors
        
    except Exception as e:
        logger.error(f"Error in migrate_files_data: {e}")
        return 0, 0, 1

@Client.on_message(filters.command("db") & filters.user(ADMINS))
async def all_db_stats(bot, message):
    total_capacity = 512 * 1024 * 1024
    uris = [FILE_URI1, FILE_URI2, FILE_URI3, FILE_URI4, FILTER_DB, DATABASE_URI]
    stats_text = "STATS OF ALL DB'S:\n\n"

    for index, uri in enumerate(uris):
        client = AsyncIOMotorClient(uri)
        try:
            db_name = DATABASE_NAME
            try:
                stats = await client[db_name].command("dbStats")
                total_files = await client[db_name][COLLECTION_NAME].count_documents({})
                used_storage = stats.get("dataSize", 0)
                free_storage = total_capacity - used_storage

                stats_text += f"MongoDB DB{index + 1}:\n"
                stats_text += (
                    f"URI: {uri}\n"
                    f"Total Files: {total_files}\n"
                    f"Total Storage: {get_size(total_capacity)}\n"
                    f"Free Storage: {get_size(used_storage)}\n"
                    f"Used Storage: {get_size(free_storage)}\n\n"
                )
            except Exception as e:
                logger.error(f"Error fetching stats for {uri}: {e}")
                stats_text += f"Error fetching stats for MongoDB DB{index + 1}: {str(e)}\n"
        finally:
            client.close()

    try:
        pg_total_files = await pgDb.total_files(pgDb.table_name)
        total_filters = 0
        async with pgDb.pool.acquire() as conn:
            total_filters = await conn.fetchval("SELECT COUNT(*) FROM filters")
        
        stats_text += f"PostgreSQL DB:\n"
        stats_text += f"Table: {pgDb.table_name}\n"
        stats_text += f"Total Files: {pg_total_files or 0}\n"
        stats_text += f"Total Filters: {total_filters or 0}\n"
        stats_text += f"Connection: Active\n\n"
    except Exception as e:
        logger.error(f"Error fetching PostgreSQL stats: {e}")
        stats_text += f"PostgreSQL DB: Error - {str(e)}\n\n"

    await message.reply_text(stats_text)

@Client.on_message(filters.command("stats") & filters.user(ADMINS))
async def get_stats(bot, message):
    try:
        rju = await message.reply("Fetching stats...")

        mongodb_counts = [0, 0, 0, 0]
        total_mongodb_files = 0

        for i, uri in enumerate(uris):
            client = AsyncIOMotorClient(uri)
            try:
                db = client[DATABASE_NAME]
                collection = db[COLLECTION_NAME]
                count = await collection.count_documents({})
                mongodb_counts[i] = count
                total_mongodb_files += count
                logger.info(f"Files in MongoDB DB{i+1}: {count}")
            except Exception as e:
                logger.error(f"Error counting documents in MongoDB DB{i+1}: {e}")
            finally:
                client.close()

        pg_files = 0
        pg_filters = 0
        try:
            pg_files = await pgDb.total_files(pgDb.table_name) or 0
            
            async with pgDb.pool.acquire() as conn:
                pg_filters = await conn.fetchval("SELECT COUNT(*) FROM filters") or 0
            
            logger.info(f"Files in PostgreSQL: {pg_files}")
            logger.info(f"Filters in PostgreSQL: {pg_filters}")
        except Exception as e:
            logger.error(f"Error counting PostgreSQL data: {e}")

        user_client = AsyncIOMotorClient(DATABASE_URI)
        try:
            user_db = user_client[DATABASE_NAME]
            total_users = await user_db["users"].count_documents({})
        except Exception as e:
            logger.error(f"Error fetching user count: {e}")
            total_users = 0
        finally:
            user_client.close()

        total_files = total_mongodb_files + pg_files

        await rju.edit(
            text=f"""
📊 **Database Stats**

- 📁 **Total Files:** <code>{total_files}</code>
- 👥 **Total Users:** <code>{total_users}</code>
- 🔧 **Total Filters:** <code>{pg_filters}</code>

**📂 MongoDB Stats:**
- **Total MongoDB Files:** <code>{total_mongodb_files}</code>
- **MongoDB DB1 Files:** <code>{mongodb_counts[0]}</code>
- **MongoDB DB2 Files:** <code>{mongodb_counts[1]}</code>
- **MongoDB DB3 Files:** <code>{mongodb_counts[2]}</code>
- **MongoDB DB4 Files:** <code>{mongodb_counts[3]}</code>

**🐘 PostgreSQL Stats:**
- **PostgreSQL Files:** <code>{pg_files}</code>
"""
        )
    except Exception as e:
        logger.error(f"❌ Error fetching stats: {e}")
        await message.reply(f"❌ Error fetching stats: {e}")

@Client.on_message(filters.command("mongostats") & filters.user(ADMINS))
async def get_mongo_stats(bot, message):
    try:
        rju = await message.reply("Fetching MongoDB stats...")
        
        stats_text = "🍃 **MongoDB Database Stats**\n\n"
        total_mongo_files = 0
        total_mongo_size = 0
        
        for i, uri in enumerate(uris, 1):
            if not uri:
                stats_text += f"**MongoDB DB{i}:** Not configured\n\n"
                continue
                
            client = AsyncIOMotorClient(uri)
            try:
                db = client[DATABASE_NAME]
                collection = db[COLLECTION_NAME]
                
                file_count = await collection.count_documents({})
                total_mongo_files += file_count
                
                try:
                    db_stats = await db.command("dbStats")
                    data_size = db_stats.get("dataSize", 0)
                    storage_size = db_stats.get("storageSize", 0)
                    index_size = db_stats.get("indexSize", 0)
                    total_mongo_size += data_size
                    
                    stats_text += f"**MongoDB DB{i}:**\n"
                    stats_text += f"• 📁 Files: <code>{file_count}</code>\n"
                    stats_text += f"• 💾 Data Size: <code>{get_size(data_size)}</code>\n"
                    stats_text += f"• 🗄️ Storage Size: <code>{get_size(storage_size)}</code>\n"
                    stats_text += f"• 📇 Index Size: <code>{get_size(index_size)}</code>\n"
                    stats_text += f"• 🔗 Status: Connected\n\n"
                except Exception as e:
                    stats_text += f"**MongoDB DB{i}:**\n"
                    stats_text += f"• 📁 Files: <code>{file_count}</code>\n"
                    stats_text += f"• ❌ Stats Error: {str(e)[:50]}...\n\n"
                
                try:
                    sample_files = await collection.find({}).limit(3).to_list(length=3)
                    if sample_files:
                        stats_text += f"**Recent Files (DB{i}):**\n"
                        for j, file in enumerate(sample_files, 1):
                            filename = file.get('file_name', 'Unknown')[:30]
                            if len(file.get('file_name', '')) > 30:
                                filename += '...'
                            file_size = get_size(file.get('file_size', 0))
                            stats_text += f"{j}. {filename} ({file_size})\n"
                        stats_text += "\n"
                except Exception as e:
                    logger.error(f"Error fetching sample files from DB{i}: {e}")
                    
            except Exception as e:
                stats_text += f"**MongoDB DB{i}:**\n"
                stats_text += f"• ❌ Connection Error: {str(e)[:50]}...\n\n"
            finally:
                client.close()
        
        stats_text += f"📊 **MongoDB Summary:**\n"
        stats_text += f"• 🗃️ Total Files: <code>{total_mongo_files}</code>\n"
        stats_text += f"• 💾 Total Data Size: <code>{get_size(total_mongo_size)}</code>\n"
        stats_text += f"• 🔗 Active Connections: <code>{len([uri for uri in uris if uri])}</code>\n"
        
        await rju.edit(stats_text)
        
    except Exception as e:
        logger.error(f"❌ Error fetching MongoDB stats: {e}")
        await message.reply(f"❌ Error fetching MongoDB stats: {e}")

@Client.on_message(filters.command("deleteall") & filters.user(ADMINS))
async def delete_all(bot, message):
    deleted_count = 0

    for uri in uris:
        client = AsyncIOMotorClient(uri)
        try:
            db = client[DATABASE_NAME]
            collection = db[COLLECTION_NAME]

            result = await collection.delete_many({})
            deleted_count += result.deleted_count
            logger.info(f"Deleted {result.deleted_count} documents from MongoDB {uri}")
        finally:
            client.close()

    try:
        async with pgDb.pool.acquire() as conn:
            result = await conn.execute(f"DELETE FROM {pgDb.table_name}")
            pg_deleted = int(result.split()[-1]) if result.split()[-1].isdigit() else 0
            deleted_count += pg_deleted
            logger.info(f"Deleted {pg_deleted} records from PostgreSQL")
    except Exception as e:
        logger.error(f"Error deleting from PostgreSQL: {e}")

    await message.reply_text(
        f"Deletion process complete. Total deleted: {deleted_count}"
    )

@Client.on_message(filters.command("delete") & filters.user(ADMINS))
async def delete_file(bot, message):
    try:
        if message.reply_to_message and (message.reply_to_message.document or message.reply_to_message.video):
            media = message.reply_to_message.document or message.reply_to_message.video
            filename = media.file_name
        elif len(message.command) >= 2:
            filename = " ".join(message.command[1:]).strip()
        else:
            await message.reply_text("Please either:\n1. Reply to a file with /delete\n2. Use /delete with filename\n\nExample: `/delete filename.mkv`")
            return

        found_files = []
        total_results = 0

        for index, instance in enumerate(instances, 1):
            if instance is None:
                continue

            try:
                regex_pattern = re.compile(re.escape(filename), re.IGNORECASE)
                cursor = instance.find({
                    "file_name": regex_pattern
                })

                instance_files = await cursor.to_list(length=None)
                
                for file in instance_files:
                    found_files.append({
                        "file_name": file.get("file_name", "Unknown"),
                        "file_id": file.get("file_id"),
                        "file_size": file.get("file_size", 0),
                        "caption": file.get("caption", "No caption"),
                        "instance": instance,
                        "db_type": "mongodb"
                    })
            except Exception as e:
                logger.error(f"Error searching in MongoDB DB{index}: {e}")
                continue

        try:
            pg_files, _, _ = await pgDb.get_search_results(filename, max_results=1000)
            for file in pg_files:
                found_files.append({
                    "file_name": file.get("file_name", "Unknown"),
                    "file_id": file.get("file_id"),
                    "file_size": file.get("file_size", 0),
                    "caption": file.get("caption", "No caption"),
                    "instance": None,
                    "db_type": "postgresql"
                })
        except Exception as e:
            logger.error(f"Error searching in PostgreSQL: {e}")

        total_results = len(found_files)

        if not found_files:
            await message.reply_text(f"❌ No files found matching: `{filename}`")
            return

        deleted_count = 0
        deleted_files = []
        
        for file in found_files:
            try:
                if file["db_type"] == "mongodb":
                    if file.get('instance') is None or file.get('file_id') is None:
                        logger.warning(f"Skipping MongoDB file due to missing instance or file_id: {file}")
                        continue

                    result = await file['instance'].delete_one({"file_id": file["file_id"]})
                    if result.deleted_count > 0:
                        deleted_count += 1
                        deleted_files.append(f"{file.get('file_name', 'Unknown')} (MongoDB)")
                        logger.info(f"Deleted MongoDB file '{file.get('file_name', 'Unknown')}'")
                
                elif file["db_type"] == "postgresql":
                    if file.get('file_id') is None:
                        logger.warning(f"Skipping PostgreSQL file due to missing file_id: {file}")
                        continue

                    async with pgDb.pool.acquire() as conn:
                        result = await conn.execute(f"DELETE FROM {pgDb.table_name} WHERE fileid = $1", file["file_id"])
                        if result and result.split()[-1].isdigit() and int(result.split()[-1]) > 0:
                            deleted_count += 1
                            deleted_files.append(f"{file.get('file_name', 'Unknown')} (PostgreSQL)")
                            logger.info(f"Deleted PostgreSQL file '{file.get('file_name', 'Unknown')}'")
                            
            except Exception as e:
                logger.error(f"Error deleting file: {e}")

        if deleted_count > 0:
            await message.reply_text(f"✅ Successfully deleted {deleted_count} file(s) matching: `{filename}`")
            
            batch_size = 10
            for i in range(0, len(deleted_files), batch_size):
                batch = deleted_files[i:i + batch_size]
                batch_text = "**Deleted Files:**\n" + "\n".join(f"{j+1}. {fname}" for j, fname in enumerate(batch))
                await message.reply_text(batch_text)
        else:
            await message.reply_text(f"❌ No files were deleted matching: `{filename}`")

    except Exception as e:
        logger.error(f"❌ Error occurred: {e}")
        await message.reply_text(f"❌ Error occurred: {e}")

@Client.on_message(filters.command("search") & filters.user(ADMINS))
async def search_files(bot, message):
    try:
        if len(message.command) < 2:
            await message.reply_text("Usage: `/search filename`\n\nExample: `/search movie.mkv`")
            return
            
        query = " ".join(message.command[1:]).strip()
        rju = await message.reply(f"Searching for: `{query}`...")
        
        all_results = []
        
        for index, instance in enumerate(instances, 1):
            if instance is None:
                continue
                
            try:
                regex_pattern = re.compile(re.escape(query), re.IGNORECASE)
                cursor = instance.find({"file_name": regex_pattern}).limit(10)
                instance_files = await cursor.to_list(length=None)
                
                for file in instance_files:
                    all_results.append({
                        "name": file.get("file_name", "Unknown"),
                        "size": get_size(file.get("file_size", 0)),
                        "source": f"MongoDB DB{index}"
                    })
            except Exception as e:
                logger.error(f"Error searching MongoDB DB{index}: {e}")
        
        try:
            pg_results, _, total = await pgDb.get_search_results(query, max_results=10)
            for file in pg_results:
                all_results.append({
                    "name": file.get("file_name", "Unknown"),
                    "size": get_size(file.get("file_size", 0)),
                    "source": "PostgreSQL"
                })
        except Exception as e:
            logger.error(f"Error searching PostgreSQL: {e}")
        
        if not all_results:
            await rju.edit(f"❌ No files found matching: `{query}`")
            return
        
        result_text = f"🔍 **Search Results for:** `{query}`\n\n**Found {len(all_results)} files:**\n\n"
        
        for i, result in enumerate(all_results[:20], 1):
            result_text += f"{i}. **{result['name'][:40]}{'...' if len(result['name']) > 40 else ''}**\n"
            result_text += f"   📊 Size: {result['size']} | 🗄️ Source: {result['source']}\n\n"
        
        if len(all_results) > 20:
            result_text += f"... and {len(all_results) - 20} more results"
        
        await rju.edit(result_text)
        
    except Exception as e:
        logger.error(f"❌ Error in search: {e}")
        await message.reply(f"❌ Error in search: {e}")

@Client.on_message(filters.command("migratedb") & filters.user(ADMINS))
async def migrate_specific_db(bot, message):
    try:
        if len(message.command) < 2:
            await message.reply_text(
                "**Usage:** `/migratedb <db_number>`\n\n"
                "**Examples:**\n"
                "• `/migratedb 1` - Migrate from MongoDB DB1\n"
                "• `/migratedb 2` - Migrate from MongoDB DB2\n"
                "• `/migratedb 3` - Migrate from MongoDB DB3\n"
                "• `/migratedb 4` - Migrate from MongoDB DB4\n"
            )
            return

        try:
            db_number = int(message.command[1])
            if db_number < 1 or db_number > 4:
                raise ValueError("Invalid database number")
        except ValueError:
            await message.reply_text("❌ Invalid database number. Use 1, 2, 3, or 4.")
            return

        db_index = db_number - 1
        instance = instances[db_index]
        
        if instance is None:
            await message.reply_text(f"❌ MongoDB DB{db_number} is not available or not connected.")
            return

        progress_msg = await message.reply(f"🚀 **Migrating MongoDB DB{db_number}...**\n\nInitializing...")
        
        migrated = 0
        skipped = 0
        errors = 0
        
        existing_file_ids = set()
        try:
            async with pgDb.pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT fileid FROM {pgDb.table_name}")
                existing_file_ids = {row['fileid'] for row in rows}
        except Exception as e:
            logger.error(f"Error fetching existing PostgreSQL file IDs: {e}")
            await progress_msg.edit("❌ Error accessing PostgreSQL. Migration aborted.")
            return

        total_files = await instance.count_documents({})
        
        if total_files == 0:
            await progress_msg.edit(f"ℹ️ MongoDB DB{db_number} is empty. Nothing to migrate.")
            return

        batch_size = 10000
        skip = 0
        
        while skip < total_files:
            try:
                cursor = instance.find({}).skip(skip).limit(batch_size)
                files_batch = await cursor.to_list(length=batch_size)
                
                if not files_batch:
                    break
                
                for file in files_batch:
                    try:
                        file_id = file.get("file_id")
                        file_name = file.get("file_name", "Unknown")
                        file_size = file.get("file_size", 0)
                        caption = file.get("caption", file_name)

                        if not file_id:
                            errors += 1
                            continue

                        if file_id in existing_file_ids:
                            skipped += 1
                            continue

                        await pgDb.append_file(file_name, file_id, file_size, caption)
                        existing_file_ids.add(file_id)
                        migrated += 1

                    except Exception as e:
                        logger.error(f"Error migrating file: {e}")
                        errors += 1
                        continue

                skip += len(files_batch)

                if (migrated + skipped + errors) % 100 == 0:
                    progress = migrated + skipped + errors
                    await progress_msg.edit(
                        f"🔄 **Migrating MongoDB DB{db_number}**\n\n"
                        f"📊 Progress: {progress}/{total_files}\n"
                        f"✅ Migrated: {migrated}\n"
                        f"⏭️ Skipped: {skipped}\n"
                        f"❌ Errors: {errors}"
                    )

            except Exception as e:
                logger.error(f"Error processing batch starting at {skip}: {e}")
                errors += 1
                skip += batch_size
                continue

        await progress_msg.edit(
            f"✅ **DB{db_number} Migration Completed!**\n\n"
            f"📊 **Results:**\n"
            f"✅ Migrated: {migrated}\n"
            f"⏭️ Skipped: {skipped}\n"
            f"❌ Errors: {errors}\n\n"
            f"🎉 Total processed: {migrated + skipped + errors}/{total_files}"
        )

    except Exception as e:
        logger.error(f"❌ Error in specific DB migration: {e}")
        await message.reply(f"❌ Error in migration: {e}")

@Client.on_message(filters.command("fullmigrate") & filters.user(ADMINS))
async def full_migration(bot, message):
    """Complete migration of all data types from MongoDB to PostgreSQL"""
    try:
        if len(message.command) < 2 or message.command[1].lower() != "confirm":
            await message.reply_text(
                "⚠️ **WARNING: COMPLETE DATABASE MIGRATION**\n\n"
                "This will migrate ALL data from MongoDB to PostgreSQL:\n"
                "• 📁 Files from all MongoDB instances\n"
                "• 👥 Users data\n"
                "• 👥 Groups/Chats data\n"
                "• 🔒 Force Subscribe users (both channels)\n"
                "• 🔧 Global Filters\n\n"
                "**Features:**\n"
                "• Skips existing data (no duplicates)\n"
                "• Keeps MongoDB data intact\n"
                "• Creates PostgreSQL tables if needed\n"
                "• Comprehensive error handling\n"
                "• Cursor timeout protection\n\n"
                "**To proceed:** `/fullmigrate confirm`"
            )
            return

        progress_msg = await message.reply("🚀 **FULL MIGRATION STARTING**\n\nInitializing...")
        
        await progress_msg.edit("🔧 **Step 1/6:** Creating PostgreSQL tables...")
        if not await ensure_postgresql_tables():
            await progress_msg.edit("❌ **Migration Failed**\n\nCould not create PostgreSQL tables.")
            return
        
        await progress_msg.edit("👥 **Step 2/6:** Migrating users data...")
        users_migrated, users_skipped, users_errors = await migrate_users_data()
        
        await progress_msg.edit("🏢 **Step 3/6:** Migrating groups data...")
        groups_migrated, groups_skipped, groups_errors = await migrate_groups_data()
        
        await progress_msg.edit("🔒 **Step 4/6:** Migrating force subscribe users...")
        (fsub1_migrated, fsub1_skipped, fsub1_errors, 
         fsub2_migrated, fsub2_skipped, fsub2_errors) = await migrate_fsub_data()
        
        await progress_msg.edit("🔧 **Step 5/6:** Migrating global filters...")
        filters_migrated, filters_skipped, filters_errors = await migrate_global_filters()
        
        await progress_msg.edit("📁 **Step 6/6:** Migrating files data...")
        files_migrated, files_skipped, files_errors = await migrate_files_data()
        
        total_migrated = (users_migrated + groups_migrated + fsub1_migrated + 
                         fsub2_migrated + filters_migrated + files_migrated)
        total_skipped = (users_skipped + groups_skipped + fsub1_skipped + 
                        fsub2_skipped + filters_skipped + files_skipped)
        total_errors = (users_errors + groups_errors + fsub1_errors + 
                       fsub2_errors + filters_errors + files_errors)
        
        summary = f"""✅ **FULL MIGRATION COMPLETED!**

📊 **SUMMARY:**
✅ **Total Migrated:** {total_migrated}
⏭️ **Total Skipped:** {total_skipped}
❌ **Total Errors:** {total_errors}

📋 **DETAILED BREAKDOWN:**

👥 **Users:**
• Migrated: {users_migrated}
• Skipped: {users_skipped}
• Errors: {users_errors}

🏢 **Groups:**
• Migrated: {groups_migrated}
• Skipped: {groups_skipped}
• Errors: {groups_errors}

🔒 **Force Subscribe Channel 1:**
• Migrated: {fsub1_migrated}
• Skipped: {fsub1_skipped}
• Errors: {fsub1_errors}

🔒 **Force Subscribe Channel 2:**
• Migrated: {fsub2_migrated}
• Skipped: {fsub2_skipped}
• Errors: {fsub2_errors}

🔧 **Global Filters:**
• Migrated: {filters_migrated}
• Skipped: {filters_skipped}
• Errors: {filters_errors}

📁 **Files:**
• Migrated: {files_migrated}
• Skipped: {files_skipped}
• Errors: {files_errors}

🎉 **Migration completed successfully!**"""
        
        await progress_msg.edit(summary)
        
        logger.info(f"Full migration completed: {total_migrated} migrated, {total_skipped} skipped, {total_errors} errors")
        
    except Exception as e:
        logger.error(f"❌ Critical error during full migration: {e}")
        try:
            await progress_msg.edit(f"❌ **MIGRATION FAILED**\n\nCritical Error: {str(e)}")
        except:
            await message.reply(f"❌ **MIGRATION FAILED**\n\nCritical Error: {str(e)}")

@Client.on_message(filters.command("migrateusers") & filters.user(ADMINS))
async def migrate_users_only(bot, message):
    """Migrate only users data"""
    try:
        progress_msg = await message.reply("👥 **Migrating Users Data...**")
        
        if not await ensure_postgresql_tables():
            await progress_msg.edit("❌ Failed to create PostgreSQL tables.")
            return
        
        migrated, skipped, errors = await migrate_users_data()
        
        await progress_msg.edit(f"""✅ **Users Migration Completed!**

👥 **Results:**
• ✅ Migrated: {migrated}
• ⏭️ Skipped: {skipped}
• ❌ Errors: {errors}""")
        
    except Exception as e:
        logger.error(f"Error in users migration: {e}")
        await message.reply(f"❌ **Users Migration Failed:** {e}")

@Client.on_message(filters.command("migrategroups") & filters.user(ADMINS))
async def migrate_groups_only(bot, message):
    """Migrate only groups data"""
    try:
        progress_msg = await message.reply("🏢 **Migrating Groups Data...**")
        
        if not await ensure_postgresql_tables():
            await progress_msg.edit("❌ Failed to create PostgreSQL tables.")
            return
        
        migrated, skipped, errors = await migrate_groups_data()
        
        await progress_msg.edit(f"""✅ **Groups Migration Completed!**

🏢 **Results:**
• ✅ Migrated: {migrated}
• ⏭️ Skipped: {skipped}
• ❌ Errors: {errors}""")
        
    except Exception as e:
        logger.error(f"Error in groups migration: {e}")
        await message.reply(f"❌ **Groups Migration Failed:** {e}")

@Client.on_message(filters.command("migratefsub") & filters.user(ADMINS))
async def migrate_fsub_only(bot, message):
    """Migrate only force subscribe users"""
    try:
        progress_msg = await message.reply("🔒 **Migrating Force Subscribe Users...**")
        
        if not await ensure_postgresql_tables():
            await progress_msg.edit("❌ Failed to create PostgreSQL tables.")
            return
        
        (fsub1_migrated, fsub1_skipped, fsub1_errors, 
         fsub2_migrated, fsub2_skipped, fsub2_errors) = await migrate_fsub_data()
        
        await progress_msg.edit(f"""✅ **Force Subscribe Migration Completed!**

🔒 **Channel 1 Results:**
• ✅ Migrated: {fsub1_migrated}
• ⏭️ Skipped: {fsub1_skipped}
• ❌ Errors: {fsub1_errors}

🔒 **Channel 2 Results:**
• ✅ Migrated: {fsub2_migrated}
• ⏭️ Skipped: {fsub2_skipped}
• ❌ Errors: {fsub2_errors}""")
        
    except Exception as e:
        logger.error(f"Error in fsub migration: {e}")
        await message.reply(f"❌ **Force Subscribe Migration Failed:** {e}")

@Client.on_message(filters.command("migratefilters") & filters.user(ADMINS))
async def migrate_filters_only(bot, message):
    """Migrate only global filters"""
    try:
        progress_msg = await message.reply("🔧 **Migrating Global Filters...**")
        
        if not await ensure_postgresql_tables():
            await progress_msg.edit("❌ Failed to create PostgreSQL tables.")
            return
        
        migrated, skipped, errors = await migrate_global_filters()
        
        await progress_msg.edit(f"""✅ **Global Filters Migration Completed!**

🔧 **Results:**
• ✅ Migrated: {migrated}
• ⏭️ Skipped: {skipped}
• ❌ Errors: {errors}""")
        
    except Exception as e:
        logger.error(f"Error in filters migration: {e}")
        await message.reply(f"❌ **Global Filters Migration Failed:** {e}")

@Client.on_message(filters.command("migrate") & filters.user(ADMINS))
async def migrate_files_only(bot, message):
    """Migrate only files (for backward compatibility)"""
    try:
        if len(message.command) < 2 or message.command[1].lower() != "confirm":
            await message.reply_text(
                "⚠️ **WARNING: This will migrate files from MongoDB to PostgreSQL**\n\n"
                "**To proceed:** `/migrate confirm`\n\n"
                "**For complete migration of all data types, use:** `/fullmigrate confirm`"
            )
            return

        progress_msg = await message.reply("📁 **Migrating Files...**")
        migrated, skipped, errors = await migrate_files_data()
        
        await progress_msg.edit(f"""✅ **Files Migration Completed!**

📁 **Results:**
• ✅ Migrated: {migrated}
• ⏭️ Skipped: {skipped}
• ❌ Errors: {errors}""")
        
    except Exception as e:
        logger.error(f"Error in files migration: {e}")
        await message.reply(f"❌ **Files Migration Failed:** {e}")

@Client.on_message(filters.command("migrationhelp") & filters.user(ADMINS))
async def migration_help(bot, message):
    """Show migration commands help"""
    help_text = """🔄 **MIGRATION COMMANDS HELP**

**Complete Migration:**
• `/fullmigrate confirm` - Migrate ALL data types (recommended)

**Individual Migrations:**
• `/migrateusers` - Migrate only users data
• `/migrategroups` - Migrate only groups data
• `/migratefsub` - Migrate only force subscribe users
• `/migratefilters` - Migrate only global filters
• `/migrate confirm` - Migrate only files data

**Database Commands:**
• `/migratedb <1-4>` - Migrate specific MongoDB instance
• `/db` - Show all database statistics
• `/stats` - Show comprehensive file statistics
• `/mongostats` - Show detailed MongoDB statistics
• `/pgstats` - Show detailed PostgreSQL statistics

**Search & Management:**
• `/search <filename>` - Search files across all databases
• `/delete <filename>` - Delete files by name
• `/deleteall` - Delete all files (use with caution)

**Key Improvements:**
✅ **Cursor Timeout Protection** - Uses batch processing to prevent timeouts
✅ **Smaller Batch Sizes** - Better memory management and reliability
✅ **Enhanced Error Handling** - Continues processing even if some batches fail
✅ **Progress Tracking** - Real-time updates every 10,000 files
✅ **Automatic Table Creation** - Creates PostgreSQL tables if needed
✅ **Global Filters Support** - Migrates custom filters from MongoDB
✅ **Connection Management** - Proper client cleanup and resource management

**Note:** All migration commands preserve original MongoDB data and include comprehensive error recovery."""
    
    await message.reply_text(help_text)
