import os
import sqlite3
import asyncio
import logging
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.types import Document
from info import ADMINS, DB_CHANNELS
import tempfile
import zipfile
import json
from database.postgres import pgDb
import gzip
import pickle

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DataExportImport:
    def __init__(self):
        self.temp_dir = tempfile.gettempdir()
        self.batch_size = 5000
        
    async def create_sqlite_backup(self, backup_name: str = None, compress: bool = True):
        try:
            if not pgDb.pool:
                await pgDb.connect()
            
            if not backup_name:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_name = f"bot_backup_{timestamp}"
            
            sqlite_path = os.path.join(self.temp_dir, f"{backup_name}.db")
            
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.cursor()
            
            await self._optimize_sqlite_settings(cursor)
            
            await self._create_sqlite_tables(cursor)
            
            tables_exported = {}
            
            files_count = await self._export_files_optimized(cursor)
            tables_exported['files'] = files_count
            
            users_count = await self._export_users_optimized(cursor)
            tables_exported['users'] = users_count
            
            groups_count = await self._export_groups_optimized(cursor)
            tables_exported['groups'] = groups_count
            
            fsub1_count = await self._export_fsub_first_optimized(cursor)
            tables_exported['fsub_first'] = fsub1_count
            
            fsub2_count = await self._export_fsub_second_optimized(cursor)
            tables_exported['fsub_second'] = fsub2_count
            
            filters_count = await self._export_filters_optimized(cursor)
            tables_exported['filters'] = filters_count
            
            metadata = {
                'export_date': datetime.now().isoformat(),
                'tables': tables_exported,
                'total_records': sum(tables_exported.values()),
                'version': '2.0',
                'compressed': compress
            }
            
            cursor.execute("""
                CREATE TABLE _metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            cursor.execute("INSERT INTO _metadata (key, value) VALUES (?, ?)", 
                         ('metadata', json.dumps(metadata)))
            
            conn.commit()
            
            await self._optimize_database(cursor)
            
            conn.close()
            
            original_size = os.path.getsize(sqlite_path)
            final_path = sqlite_path
            
            if compress and original_size > 50 * 1024 * 1024:
                compressed_path = f"{sqlite_path}.gz"
                with open(sqlite_path, 'rb') as f_in:
                    with gzip.open(compressed_path, 'wb', compresslevel=9) as f_out:
                        f_out.writelines(f_in)
                
                os.remove(sqlite_path)
                final_path = compressed_path
                
                metadata['compressed'] = True
                metadata['original_size'] = original_size
                metadata['compressed_size'] = os.path.getsize(compressed_path)
                
                logger.info(f"Compressed from {original_size/1024/1024:.2f}MB to {metadata['compressed_size']/1024/1024:.2f}MB")
            
            logger.info(f"SQLite backup created: {final_path}")
            return final_path, metadata
            
        except Exception as e:
            logger.error(f"Error creating SQLite backup: {e}", exc_info=True)
            if 'conn' in locals():
                conn.close()
            raise
    
    async def _optimize_sqlite_settings(self, cursor):
        """Optimize SQLite settings for smaller file size"""
        cursor.execute("PRAGMA page_size = 4096")
        cursor.execute("PRAGMA auto_vacuum = FULL")
        cursor.execute("PRAGMA journal_mode = OFF")
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.execute("PRAGMA temp_store = MEMORY")
        cursor.execute("PRAGMA mmap_size = 268435456")
        cursor.execute("PRAGMA cache_size = 10000")
    
    async def _optimize_database(self, cursor):
        """Final database optimization"""
        cursor.execute("PRAGMA optimize")
        cursor.execute("VACUUM")
        cursor.execute("ANALYZE")
    
    async def _create_sqlite_tables(self, cursor):
        cursor.execute("""
            CREATE TABLE files_backup (
                id INTEGER PRIMARY KEY,
                filename TEXT NOT NULL,
                filesize INTEGER DEFAULT 0,
                fileid TEXT UNIQUE NOT NULL,
                caption TEXT,
                created_at INTEGER  -- Store as timestamp integer
            )
        """)

        cursor.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                is_banned INTEGER DEFAULT 0,
                ban_reason TEXT DEFAULT '',
                created_at INTEGER
            )
        """)

        cursor.execute("""
            CREATE TABLE groups (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                is_disabled INTEGER DEFAULT 0,
                reason TEXT DEFAULT '',
                settings BLOB,  -- Store as compressed binary
                created_at INTEGER
            )
        """)

        cursor.execute("""
            CREATE TABLE fsub_first (
                id INTEGER PRIMARY KEY,
                created_at INTEGER
            )
        """)

        cursor.execute("""
            CREATE TABLE fsub_second (
                id INTEGER PRIMARY KEY,
                created_at INTEGER
            )
        """)

        cursor.execute("""
            CREATE TABLE filters (
                id INTEGER PRIMARY KEY,
                text TEXT UNIQUE NOT NULL,
                reply TEXT,
                btn TEXT,
                file TEXT,
                alert TEXT,
                created_at INTEGER
            )
        """)

        cursor.execute("CREATE INDEX idx_files_created ON files_backup(created_at)")
        cursor.execute("CREATE INDEX idx_users_created ON users(created_at)")
        cursor.execute("CREATE INDEX idx_groups_created ON groups(created_at)")

        logger.info("Optimized SQLite tables created successfully")
    
    async def _export_files_optimized(self, cursor):
        try:
            if not pgDb.pool:
                logger.error("PostgreSQL pool is not initialized")
                return 0
            
            async with pgDb.pool.acquire() as conn:
                try:
                    total_count = await conn.fetchval("SELECT COUNT(*) FROM files_backup")
                    table_name = "files_backup"
                    logger.info(f"Found {total_count} files in files_backup table")
                except Exception as e:
                    logger.error(f"Error accessing files_backup table: {e}")
                    try:
                        total_count = await conn.fetchval("SELECT COUNT(*) FROM files")
                        table_name = "files"
                        logger.info(f"Found {total_count} files in files table")
                    except Exception as e2:
                        logger.error(f"Error accessing files table: {e2}")
                        try:
                            tables = await conn.fetch("""
                                SELECT table_name FROM information_schema.tables 
                                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                            """)
                            table_list = [row['table_name'] for row in tables]
                            logger.error(f"Available tables: {table_list}")
                            
                            for tbl in table_list:
                                if 'file' in tbl.lower():
                                    try:
                                        count = await conn.fetchval(f"SELECT COUNT(*) FROM {tbl}")
                                        logger.info(f"Table {tbl} has {count} records")
                                        if count > 0:
                                            columns = await conn.fetch(f"""
                                                SELECT column_name FROM information_schema.columns 
                                                WHERE table_name = '{tbl}'
                                            """)
                                            col_names = [col['column_name'] for col in columns]
                                            if 'filename' in col_names and 'fileid' in col_names:
                                                total_count = count
                                                table_name = tbl
                                                logger.info(f"Using table {tbl} with {count} files")
                                                break
                                    except Exception as e3:
                                        logger.error(f"Error checking table {tbl}: {e3}")
                            else:
                                logger.error("No suitable files table found")
                                return 0
                        except Exception as e3:
                            logger.error(f"Error listing tables: {e3}")
                            return 0
                
                if total_count == 0:
                    logger.warning(f"No files found in {table_name} table")
                    return 0
                
                logger.info(f"Exporting {total_count} files from {table_name} table in batches...")
                
                exported = 0
                offset = 0
                
                while offset < total_count:
                    try:
                        columns_info = await conn.fetch(f"""
                            SELECT column_name FROM information_schema.columns 
                            WHERE table_name = '{table_name}'
                        """)
                        available_cols = [col['column_name'] for col in columns_info]
                        
                        select_cols = []
                        if 'filename' in available_cols:
                            select_cols.append('filename')
                        elif 'file_name' in available_cols:
                            select_cols.append('file_name as filename')
                        else:
                            select_cols.append("'unknown' as filename")
                            
                        if 'filesize' in available_cols:
                            select_cols.append('filesize')
                        elif 'file_size' in available_cols:
                            select_cols.append('file_size as filesize')
                        else:
                            select_cols.append('0 as filesize')
                            
                        if 'fileid' in available_cols:
                            select_cols.append('fileid')
                        elif 'file_id' in available_cols:
                            select_cols.append('file_id as fileid')
                        else:
                            logger.error("No file ID column found")
                            break
                            
                        if 'caption' in available_cols:
                            select_cols.append('caption')
                        else:
                            select_cols.append('filename as caption')
                            
                        if 'created_at' in available_cols:
                            select_cols.append('created_at')
                        else:
                            select_cols.append('CURRENT_TIMESTAMP as created_at')
                        
                        query = f"""
                            SELECT {', '.join(select_cols)}
                            FROM {table_name}
                            ORDER BY id DESC
                            LIMIT {self.batch_size} OFFSET {offset}
                        """
                        
                        rows = await conn.fetch(query)
                        
                        if not rows:
                            logger.warning(f"No more rows returned at offset {offset}")
                            break
                        
                        files_data = []
                        for row in rows:
                            try:
                                files_data.append((
                                    row['filename'] or 'unknown',
                                    row['filesize'] or 0,
                                    row['fileid'],
                                    row['caption'] or row['filename'] or 'unknown',
                                    int(row['created_at'].timestamp()) if row['created_at'] else None
                                ))
                            except Exception as row_error:
                                logger.error(f"Error processing row: {row_error}, row: {dict(row)}")
                                continue
                        
                        if files_data:
                            cursor.executemany("""
                                INSERT INTO files_backup (filename, filesize, fileid, caption, created_at)
                                VALUES (?, ?, ?, ?, ?)
                            """, files_data)
                            
                            exported += len(files_data)
                        
                        offset += self.batch_size
                        
                        if exported % 50000 == 0:
                            logger.info(f"Exported {exported}/{total_count} files ({exported/total_count*100:.1f}%)")
                            
                    except Exception as batch_error:
                        logger.error(f"Error in batch export at offset {offset}: {batch_error}")
                        offset += self.batch_size
                        continue
                
                logger.info(f"Successfully exported {exported} files from {table_name}")
                return exported
                
        except Exception as e:
            logger.error(f"Critical error in _export_files_optimized: {e}", exc_info=True)
            return 0
    
    async def _export_users_optimized(self, cursor):
        try:
            if not pgDb.pool:
                logger.error("PostgreSQL pool is not initialized")
                return 0
                
            async with pgDb.pool.acquire() as conn:
                try:
                    rows = await conn.fetch("""
                        SELECT id, name, is_banned, ban_reason, created_at 
                        FROM users
                        ORDER BY created_at DESC
                    """)
                except Exception as e:
                    logger.error(f"Error fetching users: {e}")
                    return 0
                
                users_data = []
                for row in rows:
                    try:
                        users_data.append((
                            row['id'],
                            row['name'] or 'Unknown',
                            1 if row['is_banned'] else 0,
                            row['ban_reason'] or '',
                            int(row['created_at'].timestamp()) if row['created_at'] else None
                        ))
                    except Exception as row_error:
                        logger.error(f"Error processing user row: {row_error}")
                        continue
                
                if users_data:
                    cursor.executemany("""
                        INSERT INTO users (id, name, is_banned, ban_reason, created_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, users_data)
                
                logger.info(f"Exported {len(users_data)} users")
                return len(users_data)
                
        except Exception as e:
            logger.error(f"Error in _export_users_optimized: {e}")
            return 0
    
    async def _export_groups_optimized(self, cursor):
        try:
            if not pgDb.pool:
                logger.error("PostgreSQL pool is not initialized")
                return 0
                
            async with pgDb.pool.acquire() as conn:
                try:
                    rows = await conn.fetch("""
                        SELECT g.id, g.title, g.is_disabled, g.reason, g.created_at,
                               gs.button_enabled, gs.botpm_enabled, gs.file_secure, 
                               gs.spell_check, gs.welcome_enabled
                        FROM groups g
                        LEFT JOIN group_settings gs ON g.id = gs.group_id
                        ORDER BY g.created_at DESC
                    """)
                except Exception:
                    try:
                        rows = await conn.fetch("""
                            SELECT id, title, is_disabled, reason, created_at 
                            FROM groups
                            ORDER BY created_at DESC
                        """)
                    except Exception as e:
                        logger.error(f"Error fetching groups: {e}")
                        return 0
                
                groups_data = []
                for row in rows:
                    try:
                        settings_data = {}
                        if 'button_enabled' in row:
                            settings_data = {
                                'button_enabled': row.get('button_enabled', True),
                                'botpm_enabled': row.get('botpm_enabled', True),
                                'file_secure': row.get('file_secure', False),
                                'spell_check': row.get('spell_check', True),
                                'welcome_enabled': row.get('welcome_enabled', True)
                            }
                        
                        compressed_settings = gzip.compress(pickle.dumps(settings_data))
                        
                        groups_data.append((
                            row['id'],
                            row['title'] or 'Unknown Group',
                            1 if row['is_disabled'] else 0,
                            row['reason'] or '',
                            compressed_settings,
                            int(row['created_at'].timestamp()) if row['created_at'] else None
                        ))
                    except Exception as row_error:
                        logger.error(f"Error processing group row: {row_error}")
                        continue
                
                if groups_data:
                    cursor.executemany("""
                        INSERT INTO groups (id, title, is_disabled, reason, settings, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, groups_data)
                
                logger.info(f"Exported {len(groups_data)} groups")
                return len(groups_data)
                
        except Exception as e:
            logger.error(f"Error in _export_groups_optimized: {e}")
            return 0
    
    async def _export_fsub_first_optimized(self, cursor):
        try:
            if not pgDb.pool:
                return 0
                
            async with pgDb.pool.acquire() as conn:
                try:
                    rows = await conn.fetch("""
                        SELECT id, created_at FROM fsub_first
                        ORDER BY created_at DESC
                    """)
                except Exception as e:
                    logger.error(f"Error fetching fsub_first: {e}")
                    return 0
                
                fsub_data = []
                for row in rows:
                    try:
                        fsub_data.append((
                            row['id'],
                            int(row['created_at'].timestamp()) if row['created_at'] else None
                        ))
                    except Exception:
                        continue
                
                if fsub_data:
                    cursor.executemany("""
                        INSERT INTO fsub_first (id, created_at) VALUES (?, ?)
                    """, fsub_data)
                
                logger.info(f"Exported {len(fsub_data)} fsub_first records")
                return len(fsub_data)
                
        except Exception as e:
            logger.error(f"Error in _export_fsub_first_optimized: {e}")
            return 0
    
    async def _export_fsub_second_optimized(self, cursor):
        try:
            if not pgDb.pool:
                return 0
                
            async with pgDb.pool.acquire() as conn:
                try:
                    rows = await conn.fetch("""
                        SELECT id, created_at FROM fsub_second
                        ORDER BY created_at DESC
                    """)
                except Exception as e:
                    logger.error(f"Error fetching fsub_second: {e}")
                    return 0
                
                fsub_data = []
                for row in rows:
                    try:
                        fsub_data.append((
                            row['id'],
                            int(row['created_at'].timestamp()) if row['created_at'] else None
                        ))
                    except Exception:
                        continue
                
                if fsub_data:
                    cursor.executemany("""
                        INSERT INTO fsub_second (id, created_at) VALUES (?, ?)
                    """, fsub_data)
                
                logger.info(f"Exported {len(fsub_data)} fsub_second records")
                return len(fsub_data)
                
        except Exception as e:
            logger.error(f"Error in _export_fsub_second_optimized: {e}")
            return 0
    
    async def _export_filters_optimized(self, cursor):
        try:
            if not pgDb.pool:
                return 0
                
            async with pgDb.pool.acquire() as conn:
                try:
                    rows = await conn.fetch("""
                        SELECT text, reply, btn, file, alert, created_at 
                        FROM filters
                        ORDER BY created_at DESC
                    """)
                except Exception as e:
                    logger.error(f"Error fetching filters: {e}")
                    return 0
                
                filters_data = []
                for row in rows:
                    try:
                        filters_data.append((
                            row['text'],
                            row['reply'],
                            row['btn'],
                            row['file'],
                            row['alert'],
                            int(row['created_at'].timestamp()) if row['created_at'] else None
                        ))
                    except Exception:
                        continue
                
                if filters_data:
                    cursor.executemany("""
                        INSERT INTO filters (text, reply, btn, file, alert, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, filters_data)
                
                logger.info(f"Exported {len(filters_data)} filters")
                return len(filters_data)
                
        except Exception as e:
            logger.error(f"Error in _export_filters_optimized: {e}")
            return 0
    
    async def import_from_sqlite(self, file_path: str):
        try:
            if not pgDb.pool:
                await pgDb.connect()
            
            is_compressed = file_path.endswith('.gz')
            
            if is_compressed:
                decompressed_path = file_path.replace('.gz', '')
                with gzip.open(file_path, 'rb') as f_in:
                    with open(decompressed_path, 'wb') as f_out:
                        f_out.writelines(f_in)
                sqlite_path = decompressed_path
            else:
                sqlite_path = file_path
            
            if not os.path.exists(sqlite_path):
                raise FileNotFoundError(f"Backup file not found: {sqlite_path}")
            
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.cursor()
            
            try:
                cursor.execute("SELECT value FROM _metadata WHERE key = ?", ('metadata',))
                metadata_row = cursor.fetchone()
                if metadata_row:
                    metadata = json.loads(metadata_row[0])
                    logger.info(f"Importing backup from {metadata['export_date']}")
                else:
                    metadata = {}
            except Exception:
                metadata = {}
            
            import_results = {}
            
            files_imported = await self._import_files_optimized(cursor)
            import_results['files'] = files_imported
            
            users_imported = await self._import_users_optimized(cursor)
            import_results['users'] = users_imported
            
            groups_imported = await self._import_groups_optimized(cursor)
            import_results['groups'] = groups_imported
            
            fsub1_imported = await self._import_fsub_first_optimized(cursor)
            import_results['fsub_first'] = fsub1_imported
            
            fsub2_imported = await self._import_fsub_second_optimized(cursor)
            import_results['fsub_second'] = fsub2_imported
            
            filters_imported = await self._import_filters_optimized(cursor)
            import_results['filters'] = filters_imported
            
            conn.close()
            
            if is_compressed and os.path.exists(sqlite_path):
                os.remove(sqlite_path)
            
            return import_results, metadata
            
        except Exception as e:
            logger.error(f"Error importing from SQLite: {e}", exc_info=True)
            if 'conn' in locals():
                conn.close()
            raise
    
    async def _import_files_optimized(self, cursor):
        try:
            cursor.execute("SELECT COUNT(*) FROM files_backup")
            total_count = cursor.fetchone()[0]
            logger.info(f"Importing {total_count} files in batches...")
            
            imported = 0
            skipped = 0
            offset = 0
            
            while offset < total_count:
                cursor.execute(f"""
                    SELECT filename, filesize, fileid, caption, created_at 
                    FROM files_backup 
                    LIMIT {self.batch_size} OFFSET {offset}
                """)
                rows = cursor.fetchall()
                
                if not rows:
                    break
                
                for row in rows:
                    filename, filesize, fileid, caption, created_at_ts = row
                    try:
                        await pgDb.append_file(filename, fileid, filesize, caption)
                        imported += 1
                    except Exception as e:
                        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                            skipped += 1
                        else:
                            logger.error(f"Error importing file {fileid}: {e}")
                            skipped += 1
                
                offset += self.batch_size
                
                if (imported + skipped) % 50000 == 0:
                    logger.info(f"Processed {imported + skipped}/{total_count} files ({(imported + skipped)/total_count*100:.1f}%)")
            
            logger.info(f"Files import: {imported} imported, {skipped} skipped")
            return {'imported': imported, 'skipped': skipped}
            
        except Exception as e:
            logger.error(f"Error in _import_files_optimized: {e}")
            return {'imported': 0, 'skipped': 0}
    
    async def _import_users_optimized(self, cursor):
        try:
            cursor.execute("SELECT id, name, is_banned, ban_reason, created_at FROM users")
            rows = cursor.fetchall()
            
            imported = 0
            skipped = 0
            
            for row in rows:
                user_id, name, is_banned, ban_reason, created_at_ts = row
                try:
                    async with pgDb.pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO users (id, name, is_banned, ban_reason)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (id) DO NOTHING
                        """, user_id, name, bool(is_banned), ban_reason)
                    imported += 1
                except Exception as e:
                    logger.error(f"Error importing user {user_id}: {e}")
                    skipped += 1
            
            logger.info(f"Users import: {imported} imported, {skipped} skipped")
            return {'imported': imported, 'skipped': skipped}
            
        except Exception as e:
            logger.error(f"Error in _import_users_optimized: {e}")
            return {'imported': 0, 'skipped': 0}
    
    async def _import_groups_optimized(self, cursor):
        try:
            cursor.execute("SELECT id, title, is_disabled, reason, settings, created_at FROM groups")
            rows = cursor.fetchall()
            
            imported = 0
            skipped = 0
            
            for row in rows:
                group_id, title, is_disabled, reason, compressed_settings, created_at_ts = row
                try:
                    if compressed_settings:
                        settings = pickle.loads(gzip.decompress(compressed_settings))
                    else:
                        settings = {}
                    
                    async with pgDb.pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO groups (id, title, is_disabled, reason)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (id) DO NOTHING
                        """, group_id, title, bool(is_disabled), reason)
                        
                        if settings:
                            await conn.execute("""
                                INSERT INTO group_settings (group_id, button_enabled, botpm_enabled, file_secure, spell_check, welcome_enabled)
                                VALUES ($1, $2, $3, $4, $5, $6)
                                ON CONFLICT (group_id) DO UPDATE SET
                                    button_enabled = EXCLUDED.button_enabled,
                                    botpm_enabled = EXCLUDED.botpm_enabled,
                                    file_secure = EXCLUDED.file_secure,
                                    spell_check = EXCLUDED.spell_check,
                                    welcome_enabled = EXCLUDED.welcome_enabled,
                                    updated_at = CURRENT_TIMESTAMP
                            """, group_id, 
                            settings.get('button_enabled', True),
                            settings.get('botpm_enabled', True),
                            settings.get('file_secure', False),
                            settings.get('spell_check', True),
                            settings.get('welcome_enabled', True))
                    
                    imported += 1
                except Exception as e:
                    logger.error(f"Error importing group {group_id}: {e}")
                    skipped += 1
            
            logger.info(f"Groups import: {imported} imported, {skipped} skipped")
            return {'imported': imported, 'skipped': skipped}
            
        except Exception as e:
            logger.error(f"Error in _import_groups_optimized: {e}")
            return {'imported': 0, 'skipped': 0}
    
    async def _import_fsub_first_optimized(self, cursor):
        try:
            cursor.execute("SELECT id, created_at FROM fsub_first")
            rows = cursor.fetchall()
            
            imported = 0
            skipped = 0
            
            for row in rows:
                user_id, created_at_ts = row
                try:
                    async with pgDb.pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO fsub_first (id) VALUES ($1)
                            ON CONFLICT (id) DO NOTHING
                        """, user_id)
                    imported += 1
                except Exception as e:
                    logger.error(f"Error importing fsub_first {user_id}: {e}")
                    skipped += 1
            
            logger.info(f"Fsub_first import: {imported} imported, {skipped} skipped")
            return {'imported': imported, 'skipped': skipped}
            
        except Exception as e:
            logger.error(f"Error in _import_fsub_first_optimized: {e}")
            return {'imported': 0, 'skipped': 0}
    
    async def _import_fsub_second_optimized(self, cursor):
        try:
            cursor.execute("SELECT id, created_at FROM fsub_second")
            rows = cursor.fetchall()
            
            imported = 0
            skipped = 0
            
            for row in rows:
                user_id, created_at_ts = row
                try:
                    async with pgDb.pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO fsub_second (id) VALUES ($1)
                            ON CONFLICT (id) DO NOTHING
                        """, user_id)
                    imported += 1
                except Exception as e:
                    logger.error(f"Error importing fsub_second {user_id}: {e}")
                    skipped += 1
            
            logger.info(f"Fsub_second import: {imported} imported, {skipped} skipped")
            return {'imported': imported, 'skipped': skipped}
            
        except Exception as e:
            logger.error(f"Error in _import_fsub_second_optimized: {e}")
            return {'imported': 0, 'skipped': 0}
    
    async def _import_filters_optimized(self, cursor):
        try:
            cursor.execute("SELECT text, reply, btn, file, alert, created_at FROM filters")
            rows = cursor.fetchall()
            
            imported = 0
            skipped = 0
            
            for row in rows:
                text, reply, btn, file, alert, created_at_ts = row
                try:
                    async with pgDb.pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO filters (text, reply, btn, file, alert)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (text) DO NOTHING
                        """, text, reply, btn, file, alert)
                    imported += 1
                except Exception as e:
                    logger.error(f"Error importing filter {text}: {e}")
                    skipped += 1
            
            logger.info(f"Filters import: {imported} imported, {skipped} skipped")
            return {'imported': imported, 'skipped': skipped}
            
        except Exception as e:
            logger.error(f"Error in _import_filters_optimized: {e}")
            return {'imported': 0, 'skipped': 0}

export_import = DataExportImport()

@Client.on_message(filters.command("export") & filters.user(ADMINS))
async def export_data(bot, message):
    try:
        progress_msg = await message.reply("🚀 **EXPORTING DATABASE**\n\nChecking database connection...")
        
        if not pgDb.pool:
            await progress_msg.edit("🔌 **Connecting to PostgreSQL...**")
            await pgDb.connect()
        
        try:
            files_count = await pgDb.total_files()
            await progress_msg.edit(f"📊 **Database Status Check**\n\n• Files found: {files_count:,}\n• Creating optimized backup...")
        except Exception as e:
            await progress_msg.edit(f"⚠️ **Database Check Failed**\n\nError: {str(e)}\n\nProceeding with export...")
        
        backup_path, metadata = await export_import.create_sqlite_backup(compress=True)
        
        if not os.path.exists(backup_path):
            await progress_msg.edit("❌ **Export Failed**\n\nBackup file could not be created.")
            return
        
        file_size = os.path.getsize(backup_path)
        file_size_mb = file_size / (1024 * 1024)
        
        await progress_msg.edit("📤 **Uploading compressed backup file...**")
        
        compression_info = ""
        if metadata.get('compressed') and 'original_size' in metadata:
            original_mb = metadata['original_size'] / (1024 * 1024)
            compression_ratio = (1 - file_size / metadata['original_size']) * 100
            compression_info = f"🗜️ **Compression:** {original_mb:.1f}MB → {file_size_mb:.1f}MB ({compression_ratio:.1f}% saved)\n"
        
        with open(backup_path, 'rb') as f:
            await message.reply_document(
                document=f,
                file_name=os.path.basename(backup_path),
                caption=f"""✅ **DATABASE EXPORT COMPLETED**

📊 **Export Summary:**
• 📁 Files: {metadata['tables'].get('files', 0):,}
• 👥 Users: {metadata['tables'].get('users', 0):,}
• 🏢 Groups: {metadata['tables'].get('groups', 0):,}
• 🔒 Fsub Channel 1: {metadata['tables'].get('fsub_first', 0):,}
• 🔒 Fsub Channel 2: {metadata['tables'].get('fsub_second', 0):,}
• 🔧 Filters: {metadata['tables'].get('filters', 0):,}

📈 **Total Records:** {metadata['total_records']:,}
📅 **Export Date:** {metadata['export_date'][:19]}
💾 **Final File Size:** {file_size_mb:.2f} MB
{compression_info}
⚡ **Optimized Format:** v{metadata.get('version', '2.0')}

**Instructions:**
1. Download this backup file
2. On new VPS, reply to this file with `/import`
3. Bot will restore all data to PostgreSQL

⚠️ **Keep this file secure - it contains all your bot data!**"""
            )
        
        try:
            os.remove(backup_path)
        except:
            pass
            
        await progress_msg.delete()
        
    except Exception as e:
        logger.error(f"Export error: {e}", exc_info=True)
        try:
            await progress_msg.edit(f"❌ **Export Failed**\n\nError: {str(e)}")
        except:
            await message.reply(f"❌ **Export Failed**\n\nError: {str(e)}")

@Client.on_message(filters.command("import") & filters.user(ADMINS))
async def import_data(bot, message):
    try:
        if not message.reply_to_message or not message.reply_to_message.document:
            await message.reply_text("""❌ **Import Failed**

**Usage:** Reply to a backup file with `/import`

**Steps:**
1. Upload or forward the backup file (.db or .db.gz)
2. Reply to that file with `/import`
3. Bot will restore all data to PostgreSQL

**Note:** Supports both compressed and uncompressed backup files.""")
            return
        
        document = message.reply_to_message.document
        
        if not (document.file_name.endswith('.db') or document.file_name.endswith('.db.gz')):
            await message.reply_text("❌ **Invalid File**\n\nPlease reply to a backup file (.db or .db.gz) created by `/export` command.")
            return
        
        progress_msg = await message.reply("📥 **IMPORTING DATABASE**\n\nDownloading backup file...")
        
        if not pgDb.pool:
            await progress_msg.edit("🔌 **Connecting to PostgreSQL...**")
            await pgDb.connect()
        
        backup_path = await message.reply_to_message.download(
            file_name=os.path.join(export_import.temp_dir, document.file_name)
        )
        
        await progress_msg.edit("🔄 **Processing optimized backup file...**")
        
        import_results, metadata = await export_import.import_from_sqlite(backup_path)
        
        total_imported = sum(result.get('imported', 0) for result in import_results.values())
        total_skipped = sum(result.get('skipped', 0) for result in import_results.values())
        
        version_info = f" (v{metadata.get('version', '1.0')})" if metadata else ""
        
        summary = f"""✅ **DATABASE IMPORT COMPLETED**{version_info}

📊 **Import Summary:**
• 📁 Files: {import_results.get('files', {}).get('imported', 0):,} imported, {import_results.get('files', {}).get('skipped', 0):,} skipped
• 👥 Users: {import_results.get('users', {}).get('imported', 0):,} imported, {import_results.get('users', {}).get('skipped', 0):,} skipped
• 🏢 Groups: {import_results.get('groups', {}).get('imported', 0):,} imported, {import_results.get('groups', {}).get('skipped', 0):,} skipped
• 🔒 Fsub Ch1: {import_results.get('fsub_first', {}).get('imported', 0):,} imported, {import_results.get('fsub_first', {}).get('skipped', 0):,} skipped
• 🔒 Fsub Ch2: {import_results.get('fsub_second', {}).get('imported', 0):,} imported, {import_results.get('fsub_second', {}).get('skipped', 0):,} skipped
• 🔧 Filters: {import_results.get('filters', {}).get('imported', 0):,} imported, {import_results.get('filters', {}).get('skipped', 0):,} skipped

📈 **Totals:**
• ✅ Total Imported: {total_imported:,}
• ⏭️ Total Skipped: {total_skipped:,}"""

        if metadata:
            summary += f"\n\n📅 **Backup Date:** {metadata.get('export_date', 'Unknown')[:19]}"
            summary += f"\n📊 **Original Records:** {metadata.get('total_records', 'Unknown'):,}"
            if metadata.get('compressed'):
                summary += f"\n🗜️ **Format:** Compressed & Optimized"
        
        summary += "\n\n🎉 **VPS Transfer Complete!** Your bot is ready to use."
        
        await progress_msg.edit(summary)
        
        try:
            os.remove(backup_path)
        except:
            pass
            
    except Exception as e:
        logger.error(f"Import error: {e}", exc_info=True)
        try:
            await progress_msg.edit(f"❌ **Import Failed**\n\nError: {str(e)}")
        except:
            await message.reply(f"❌ **Import Failed**\n\nError: {str(e)}")

@Client.on_message(filters.command("dbstatus") & filters.user(ADMINS))
async def check_db_status(bot, message):
    try:
        status_msg = await message.reply("🔍 **Checking Database Status...**")
        
        if not pgDb.pool:
            await pgDb.connect()
        
        async with pgDb.pool.acquire() as conn:
            db_info = await conn.fetchrow("SELECT version() as version")
            db_version = db_info['version'].split()[1] if db_info else "Unknown"
            
            tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            
            table_info = []
            total_records = 0
            
            for table in tables:
                table_name = table['table_name']
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                    table_info.append(f"• **{table_name}**: {count:,} records")
                    total_records += count
                    
                    if 'file' in table_name.lower():
                        columns = await conn.fetch(f"""
                            SELECT column_name, data_type FROM information_schema.columns 
                            WHERE table_name = '{table_name}'
                            ORDER BY ordinal_position
                        """)
                        col_info = [f"{col['column_name']}({col['data_type']})" for col in columns[:5]]
                        if len(columns) > 5:
                            col_info.append(f"...+{len(columns)-5} more")
                        table_info.append(f"  └─ Columns: {', '.join(col_info)}")
                        
                except Exception as e:
                    table_info.append(f"• **{table_name}**: ❌ Error - {str(e)[:50]}")
            
            try:
                db_size_query = """
                    SELECT pg_size_pretty(pg_database_size(current_database())) as db_size
                """
                db_size_result = await conn.fetchrow(db_size_query)
                db_size = db_size_result['db_size'] if db_size_result else "Unknown"
            except:
                db_size = "Unknown"
            
            try:
                conn_count = await conn.fetchval("""
                    SELECT count(*) FROM pg_stat_activity 
                    WHERE datname = current_database()
                """)
            except:
                conn_count = "Unknown"
        
        pool_stats = {
            'min_size': getattr(pgDb.pool, '_minsize', 'Unknown'),
            'max_size': getattr(pgDb.pool, '_maxsize', 'Unknown'),
            'current_connections': getattr(pgDb.pool, '_holders', []),
            'free_connections': getattr(pgDb.pool, '_queue', [])
        }
        
        if isinstance(pool_stats['current_connections'], list):
            current_size = len(pool_stats['current_connections'])
        else:
            current_size = "Unknown"
            
        if isinstance(pool_stats['free_connections'], list):
            free_size = len(pool_stats['free_connections'])
        else:
            free_size = "Unknown"

        status_text = f"""📊 **DATABASE STATUS REPORT**

**🔗 Connection:** ✅ Active
**🗄️ PostgreSQL:** v{db_version}
**💾 Database Size:** {db_size}
**🔢 Active Connections:** {conn_count}

**📋 Data Summary:**
**Total Tables:** {len(tables)}
**Total Records:** {total_records:,}

**📊 Table Details:**
{chr(10).join(table_info)}

**🏊 Connection Pool:**
• **Min Size:** {pool_stats['min_size']}
• **Max Size:** {pool_stats['max_size']}
• **Current Size:** {current_size}
• **Free Connections:** {free_size}
• **Pool Status:** {'🟢 Healthy' if current_size != 'Unknown' and current_size > 0 else '🟡 Check Required'}

**⚡ Performance:**
• **DSN:** {pgDb.dsn.split('@')[1] if '@' in pgDb.dsn else 'Hidden'}"""

        await status_msg.edit(status_text)
        
    except Exception as e:
        logger.error(f"DB Status error: {e}", exc_info=True)
        try:
            await status_msg.edit(f"❌ **Database Status Check Failed**\n\n**Error:** {str(e)}\n\n**Troubleshooting:**\n• Check PostgreSQL connection\n• Verify database credentials\n• Ensure database is accessible")
        except:
            await message.reply(f"❌ **Database Status Check Failed**\n\n**Error:** {str(e)}")

@Client.on_message(filters.command("dbtest") & filters.user(ADMINS))
async def test_db_connection(bot, message):
    try:
        test_msg = await message.reply("🧪 **Testing Database Connection...**")
        
        if not pgDb.pool:
            await test_msg.edit("🔌 **Connecting to PostgreSQL...**")
            await pgDb.connect()
        
        test_results = []
        
        try:
            async with pgDb.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                test_results.append("✅ **Basic Connection:** Success")
        except Exception as e:
            test_results.append(f"❌ **Basic Connection:** {str(e)[:50]}")
        
        try:
            files_count = await pgDb.total_files()
            test_results.append(f"✅ **Files Table:** {files_count:,} records")
        except Exception as e:
            test_results.append(f"❌ **Files Table:** {str(e)[:50]}")
        
        try:
            users_count = await pgDb.total_users_count()
            test_results.append(f"✅ **Users Table:** {users_count:,} records")
        except Exception as e:
            test_results.append(f"❌ **Users Table:** {str(e)[:50]}")
        
        try:
            search_results, _, total = await pgDb.get_search_results("test", max_results=1)
            test_results.append(f"✅ **Search Function:** Working (found {total} results)")
        except Exception as e:
            test_results.append(f"❌ **Search Function:** {str(e)[:50]}")
        
        try:
            test_user_exists = await pgDb.is_user_exist(12345)
            test_results.append("✅ **Cache (Redis):** Connected")
        except Exception as e:
            test_results.append(f"❌ **Cache (Redis):** {str(e)[:50]}")
        
        test_summary = f"""🧪 **DATABASE CONNECTION TEST**

**Test Results:**
{chr(10).join(test_results)}

**Overall Status:** {'🟢 All systems operational' if all('✅' in result for result in test_results) else '🟡 Some issues detected'}

**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        
        await test_msg.edit(test_summary)
        
    except Exception as e:
        logger.error(f"DB Test error: {e}", exc_info=True)
        try:
            await test_msg.edit(f"❌ **Database Test Failed**\n\n**Error:** {str(e)}")
        except:
            await message.reply(f"❌ **Database Test Failed**\n\n**Error:** {str(e)}")

async def hourly_backup(bot: Client):
    try:
        if not pgDb.pool:
            await pgDb.connect()
            
        backup_path, metadata = await export_import.create_sqlite_backup(compress=True)
        if not os.path.exists(backup_path):
            logger.error("Scheduled hourly backup failed: Backup file was not created.")
            return
        
        file_size = os.path.getsize(backup_path)
        file_size_mb = file_size / (1024 * 1024)
        
        compression_info = ""
        if metadata.get('compressed') and 'original_size' in metadata:
            original_mb = metadata['original_size'] / (1024 * 1024)
            compression_ratio = (1 - file_size / metadata['original_size']) * 100
            compression_info = f" | 🗜️ {compression_ratio:.1f}% compressed"
        
        caption = f"""🕒 **DAILY BACKUP COMPLETED**

📊 **Summary:**
• 📁 Files: {metadata['tables'].get('files', 0):,}
• 👥 Users: {metadata['tables'].get('users', 0):,}
• 🏢 Groups: {metadata['tables'].get('groups', 0):,}
• 🔒 Fsub 1: {metadata['tables'].get('fsub_first', 0):,}
• 🔒 Fsub 2: {metadata['tables'].get('fsub_second', 0):,}
• 🔧 Filters: {metadata['tables'].get('filters', 0):,}

📅 {metadata['export_date'][:19]} | 🔁 Automated Backup
💾 **Size:** {file_size_mb:.2f} MB{compression_info}
⚡ **Optimized:** v{metadata.get('version', '2.0')}
"""

        send_success = False
        last_send_error = None

        if not DB_CHANNELS:
            logger.warning("No channels configured in DB_CHANNELS. Skipping sending backup.")
        else:
            logger.info(f"Attempting to send compressed backup to channels: {DB_CHANNELS}")
            for chat_id in DB_CHANNELS:
                try:
                    with open(backup_path, 'rb') as f:
                        await bot.send_document(
                            chat_id=chat_id,
                            document=f,
                            file_name=os.path.basename(backup_path),
                            caption=caption
                        )
                    logger.info(f"Hourly backup sent successfully to channel {chat_id}")
                    send_success = True
                except Exception as e:
                    last_send_error = e
                    logger.error(f"❌ Failed to send hourly backup to channel {chat_id}: {e}")

        try:
            os.remove(backup_path)
            logger.info(f"Temporary backup file {backup_path} deleted.")
        except Exception as cleanup_error:
            logger.warning(f"Could not delete temporary backup file {backup_path}: {cleanup_error}")

        if not send_success:
            error_msg = f"Hourly backup completed (file created) but failed to send to any channel. Last error: {last_send_error}"
            logger.error(error_msg)
        else:
            logger.info("Hourly backup cycle completed successfully.")

    except Exception as e:
        error_msg = f"❌ Hourly backup error: {e}"
        logger.error(error_msg, exc_info=True)
