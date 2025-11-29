from motor.motor_asyncio import AsyncIOMotorClient
from info import DATABASE_NAME, DATABASE_URI
from database.postgres import pgDb

class Database:
    def __init__(self, uri, database_name):
        self._client = AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.col = self.db.users
        self.grp = self.db.groups
        self.sub = self.db.sub

    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
            ban_status=dict(
                is_banned=False,
                ban_reason="",
            ),
        )

    def new_group(self, id, title):
        return dict(
            id = id,
            title = title,
            chat_status=dict(
                is_disabled=False,
                reason="",
            ),
        )
    
    async def add_user(self, id, name):
        user = self.new_user(id, name)
        await self.col.insert_one(user)
        try:
            async with pgDb.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO users (id, name, is_banned, ban_reason)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        updated_at = CURRENT_TIMESTAMP
                """, id, name[:100], False, "")
        except Exception as e:
            print("PostgreSQL add_user error:", e)
    
    async def is_user_exist(self, id):
        return await pgDb.is_user_exist(id)
    
    async def total_users_count(self):
        count = await self.col.count_documents({})
        return count
    
    async def remove_ban(self, id):
        ban_status = dict(
            is_banned=False,
            ban_reason=''
        )
        await self.col.update_one({'id': id}, {'$set': {'ban_status': ban_status}})
    
    async def ban_user(self, user_id, ban_reason="No Reason"):
        ban_status = dict(
            is_banned=True,
            ban_reason=ban_reason
        )
        await self.col.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})

    async def get_ban_status(self, id):
        default = dict(
            is_banned=False,
            ban_reason=''
        )
        user = await self.col.find_one({'id':int(id)})
        if not user:
            return default
        return user.get('ban_status', default)

    async def get_all_users(self):
        return self.col.find({})
    
    async def delete_user(self, user_id):
        await self.col.delete_many({'id': int(user_id)})

    async def get_banned(self):
        users = self.col.find({'ban_status.is_banned': True})
        chats = self.grp.find({'chat_status.is_disabled': True})
        b_chats = [chat['id'] async for chat in chats]
        b_users = [user['id'] async for user in users]
        return b_users, b_chats
    
    async def add_chat(self, chat, title):
        chat_data = self.new_group(chat, title)
        await self.grp.insert_one(chat_data)
        try:
            async with pgDb.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO groups (id, title, is_disabled, reason)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        updated_at = CURRENT_TIMESTAMP
                """, chat, title, False, "")
        except Exception as e:
            print("PostgreSQL add_chat error:", e)

    async def get_chat(self, chat):
        chat = await self.grp.find_one({'id':int(chat)})
        return False if not chat else chat.get('chat_status')
        
    async def update_settings(self, id, settings):
        await self.grp.update_one({'id': int(id)}, {'$set': {'settings': settings}})
        
    async def get_settings(self, id):
        default = {
            'button': True,
            'botpm': True,
            'file_secure': False,
            'spell_check': True,
            'welcome': True
        }
        chat = await self.grp.find_one({'id':int(id)})
        if chat:
            return chat.get('settings', default)
        return default
    
    async def get_all_chats(self):
        return self.grp.find({})

    async def set_channel(self, id):
        if not await self.sub.find_one({'key': 'key'}):
            await self.sub.insert_one({'key': 'key'})
        await self.sub.update_one({'key': 'key'}, {'$set': {'channel': id}})
           
    async def get_channel(self):
        ch = await self.sub.find_one({'key': 'key'})
        if ch: return int(ch['channel'])
        return None
    
    async def set_channel2(self, id):
        if not await self.sub.find_one({'key2': 'key2'}):
            await self.sub.insert_one({'key2': 'key2'})
        await self.sub.update_one({'key2': 'key2'}, {'$set': {'channel2': id}})
           
    async def get_channel2(self):
        ch = await self.sub.find_one({'key2': 'key2'})
        if ch: return int(ch['channel2'])
        return None

db = Database(DATABASE_URI, DATABASE_NAME)
