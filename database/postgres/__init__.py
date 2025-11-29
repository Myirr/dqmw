import os
import asyncpg
import logging
import asyncio
from typing import Optional, List, Tuple, Dict
from info import pgHost, pgDbname, pgPassword, pgPort, pgUsername
from aiocache import cached, Cache
from aiocache.serializers import PickleSerializer
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

REDIS_CONFIG = {
    'endpoint': pgHost,
    'port': int(os.getenv("REDIS_PORT", 6379)),
    'password': os.getenv("REDIS_PASSWORD", "root"),
    'namespace': os.getenv("REDIS_NAMESPACE", "x1redis")
}

class PostgreSQLManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=50)
        await self.create_all_tables()
        logger.info("✅ PostgreSQL connected and tables initialized")

    async def create_all_tables(self):
        table_queries = [
            """
            CREATE TABLE IF NOT EXISTS files_backup (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(500) NOT NULL,
                filesize BIGINT DEFAULT 0,
                fileid VARCHAR(200) UNIQUE NOT NULL,
                caption TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_fileid UNIQUE (fileid)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                is_banned BOOLEAN DEFAULT FALSE,
                ban_reason TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS groups (
                id BIGINT PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                is_disabled BOOLEAN DEFAULT FALSE,
                reason TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS group_settings (
                group_id BIGINT PRIMARY KEY REFERENCES groups(id) ON DELETE CASCADE,
                button_enabled BOOLEAN DEFAULT TRUE,
                botpm_enabled BOOLEAN DEFAULT TRUE,
                file_secure BOOLEAN DEFAULT FALSE,
                spell_check BOOLEAN DEFAULT TRUE,
                welcome_enabled BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fsub_first (
                id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fsub_second (
                id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS filters (
                id SERIAL PRIMARY KEY,
                text VARCHAR(500) UNIQUE NOT NULL,
                reply TEXT,
                btn TEXT,
                file TEXT,
                alert TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS subscription_settings (
                key VARCHAR(50) PRIMARY KEY,
                channel_id BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]
        index_queries = [
            "CREATE EXTENSION IF NOT EXISTS pg_trgm",
            "CREATE INDEX IF NOT EXISTS idx_files_filename_trgm ON files_backup USING GIN (LOWER(filename) gin_trgm_ops)",
            "CREATE INDEX IF NOT EXISTS idx_files_caption_trgm ON files_backup USING GIN (LOWER(caption) gin_trgm_ops)",
            "CREATE INDEX IF NOT EXISTS idx_files_created_at ON files_backup (created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_users_is_banned ON users (is_banned)",
            "CREATE INDEX IF NOT EXISTS idx_groups_is_disabled ON groups (is_disabled)",
            "CREATE INDEX IF NOT EXISTS idx_filters_text_trgm ON filters USING GIN (LOWER(text) gin_trgm_ops)"
        ]
        async with self.pool.acquire() as conn:
            for q in table_queries + index_queries:
                await conn.execute(q)

class EnhancedPostgreSQL:
    def __init__(self):
        self.dsn = f"postgresql://{pgUsername}:{pgPassword}@{pgHost}:{pgPort}/{pgDbname}"
        self.pool = None
        self.table_name = "files_backup"

    async def connect(self):
        manager = PostgreSQLManager(self.dsn)
        await manager.connect()
        self.pool = manager.pool

    async def execute_fetchval(self, query: str, *args):
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchval(query, *args)
        except Exception as e:
            logger.error(f"❌ Error in fetchval: {e}")
            return None

    async def execute_fetchrow(self, query: str, *args):
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchrow(query, *args)
        except Exception as e:
            logger.error(f"❌ Error in fetchrow: {e}")
            return None

    async def execute_fetch(self, query: str, *args):
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetch(query, *args)
        except Exception as e:
            logger.error(f"❌ Error in fetch: {e}")
            return []

    async def execute(self, query: str, *args):
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, *args)
        except Exception as e:
            logger.error(f"❌ Error in execute: {e}")

    async def total_files(self, table_name: Optional[str] = None) -> int:
        table = table_name or self.table_name
        return await self.total_rows(table)
    
    async def total_rows(self, table_name: str) -> int:
        return await self.execute_fetchval(f"SELECT COUNT(*) FROM {table_name}") or 0

    async def append_file(self, filename: str, fileid: str, filesize: int, caption: Optional[str]):
        await self.execute("""
            INSERT INTO files_backup (filename, filesize, fileid, caption)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (fileid) DO NOTHING
        """, filename, filesize, fileid, caption or filename)

    async def get_file_details(self, fileid: str) -> Optional[dict]:
        row = await self.execute_fetchrow("""
            SELECT fileid AS file_id, filename AS file_name, filesize AS file_size, caption, created_at
            FROM files_backup WHERE fileid = $1
        """, fileid)
        return dict(row) if row else None

    async def get_all_users(self, skip: int = 0, limit: Optional[int] = None):
        query = """
            SELECT id, name, is_banned, ban_reason, created_at, updated_at 
            FROM users 
            ORDER BY created_at DESC
        """
        params = []
    
        if limit:
            query += f" LIMIT ${len(params) + 1}"
            params.append(limit)
    
        if skip > 0:
            query += f" OFFSET ${len(params) + 1}"
            params.append(skip)
    
        rows = await self.execute_fetch(query, *params)
        return [dict(row) for row in rows]
    
    async def total_users_count(self) -> int:
        return await self.execute_fetchval("SELECT COUNT(*) FROM users") or 0

    async def get_all_users_iterator(self, skip: int = 0, batch_size: int = 100):
        offset = skip
        while True:
            users = await self.get_all_users(skip=offset, limit=batch_size)
            if not users:
                break
            for user in users:
                yield user
            offset += len(users)
            if len(users) < batch_size:
                break

    async def delete_user_by_id(self, user_id: int):
        await self.execute("DELETE FROM users WHERE id = $1", user_id)

    @cached(ttl=180, cache=Cache.REDIS, **REDIS_CONFIG, serializer=PickleSerializer())
    async def is_user_exist(self, user_id: int) -> bool:
        return await self.execute_fetchval("SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", user_id) or False

    @cached(ttl=180, cache=Cache.REDIS, **REDIS_CONFIG, serializer=PickleSerializer())
    async def get_user(self, user_id: int) -> Optional[dict]:
        if await self.execute_fetchval("SELECT EXISTS(SELECT 1 FROM fsub_first WHERE id = $1)", user_id):
            return {"id": user_id}
        return None

    @cached(ttl=180, cache=Cache.REDIS, **REDIS_CONFIG, serializer=PickleSerializer())
    async def get_user2(self, user_id: int) -> Optional[dict]:
        if await self.execute_fetchval("SELECT EXISTS(SELECT 1 FROM fsub_second WHERE id = $1)", user_id):
            return {"id": user_id}
        return None

    async def add_fsub_user(self, table: str, user_id: int):
        if table in ["fsub_first", "fsub_second"]:
            await self.execute(f"INSERT INTO {table} (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", user_id)

    async def add_filter(self, text: str, reply: str, btn: str, file: str, alert: str):
        await self.execute("""
            INSERT INTO filters (text, reply, btn, file, alert)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (text) DO UPDATE SET
                reply = EXCLUDED.reply,
                btn = EXCLUDED.btn,
                file = EXCLUDED.file,
                alert = EXCLUDED.alert,
                updated_at = CURRENT_TIMESTAMP
        """, text, reply, btn, file, alert)

    async def delete_filter(self, text: str):
        await self.execute("DELETE FROM filters WHERE text = $1", text)

    async def delete_all_filters(self):
        await self.execute("DELETE FROM filters")

    @cached(ttl=300, cache=Cache.REDIS, **REDIS_CONFIG, serializer=PickleSerializer())
    async def get_filters(self) -> List[str]:
        rows = await self.execute_fetch("SELECT text FROM filters ORDER BY created_at DESC")
        return [row["text"] for row in rows]

    @cached(ttl=300, cache=Cache.REDIS, **REDIS_CONFIG, serializer=PickleSerializer())
    async def find_filter(self, name: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        row = await self.execute_fetchrow("SELECT reply, btn, alert, file FROM filters WHERE text = $1", name)
        if row:
            return row["reply"], row["btn"], row["alert"], row["file"]
        return None, None, None, None

    @cached(ttl=300, cache=Cache.REDIS, **REDIS_CONFIG, serializer=PickleSerializer())
    async def get_search_results(self, query: str, max_results: int = 10, offset: int = 0):
        
        words = query.lower().split()
        like_clauses = []
        params = []
        param_index = 1

        for word in words:
            like_clauses.append(f"(LOWER(filename) ILIKE ${param_index} OR LOWER(caption) ILIKE ${param_index})")
            params.append(f"%{word}%")
            param_index += 1

        conditions = " AND ".join(like_clauses)

        max_allowed = 210 - offset
        max_results = min(max_results, max_allowed)

        total_query = f"""
           SELECT COUNT(*) FROM (
             SELECT 1 FROM {self.table_name}
             WHERE {conditions}
             LIMIT 211
           ) AS limited_count
           """

        data_query = f"""
            SELECT fileid AS file_id, filename AS file_name, filesize AS file_size, caption, created_at,
            CASE
                WHEN LOWER(filename) = ${param_index} THEN 3
                WHEN LOWER(caption) = ${param_index} THEN 2
                WHEN LOWER(filename) ILIKE '%' || ${param_index} || '%' THEN 1
                ELSE 0
            END AS rank
            FROM {self.table_name}
            WHERE {conditions}
            ORDER BY rank DESC, created_at DESC
            LIMIT ${param_index + 1} OFFSET ${param_index + 2}
        """

        params_for_data = params + [query.lower(), max_results, offset]

        total_task = asyncio.create_task(self.execute_fetchval(total_query, *params))
        rows_task = asyncio.create_task(self.execute_fetch(data_query, *params_for_data))
        total, rows = await asyncio.gather(total_task, rows_task)

        total = min(total, 210)
        files = [dict(r) for r in rows]
        next_offset = offset + len(files) if offset + len(files) < total else ''
        return files, next_offset, total

pgDb = EnhancedPostgreSQL()
      
