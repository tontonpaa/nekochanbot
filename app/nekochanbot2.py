# Required imports
from dotenv import load_dotenv
load_dotenv() # Load environment variables from .env file
import logging
import discord
from discord.ext import commands, tasks
import os
import re
from google.cloud import firestore
from google.auth.exceptions import DefaultCredentialsError
from datetime import datetime, timedelta, timezone
import asyncio

from flask import Flask
from threading import Thread

# --- Flask App for Keep Alive (Render health checks) ---
app = Flask('')

@app.route('/')
def home():
    return "I'm alive" # Simple response for health check

def run_flask():
    port = int(os.environ.get('PORT', 8080)) # Use Render's PORT or default
    app.run(host='0.0.0.0', port=port) # Listen on all interfaces

def keep_alive():
    """Starts the Flask server in a separate thread."""
    flask_thread = Thread(target=run_flask)
    flask_thread.daemon = True # Allow main program to exit even if this thread is running
    flask_thread.start()

# --- Logging Configuration (More Explicit Setup) ---
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG) 
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG) 
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s:%(module)s:%(lineno)d - %(message)s") # Added logger name
stream_handler.setFormatter(formatter)
if root_logger.hasHandlers():
    root_logger.handlers.clear()
root_logger.addHandler(stream_handler)
logger = logging.getLogger(__name__) # Logger for this specific module
discord_logger = logging.getLogger('discord') # Logger for discord.py library
discord_logger.setLevel(logging.INFO)
discord_http_logger = logging.getLogger('discord.http')
discord_http_logger.setLevel(logging.WARNING)

logger.info("ロギング設定を明示的に行いました。このロガーの実効レベル: %s", logging.getLevelName(logger.getEffectiveLevel()))
logger.debug("これは明示的な設定後のテストDEBUGメッセージです。")
# --- End of Logging Configuration ---


# --- Bot Intents Configuration ---
intents = discord.Intents.default() 
intents.guilds = True
intents.voice_states = True 
intents.message_content = True 

# --- Firestore Client and Constants ---
db = None 
FIRESTORE_COLLECTION_NAME = "discord_tracked_original_vcs_prod_v4" # Incremented version for clarity
STATUS_CATEGORY_NAME = "STATUS" 

# --- VC Tracking Dictionaries and Locks ---
vc_tracking = {} 
vc_locks = {}    

# --- Cooldown and State Settings for VC Name Updates ---
BOT_UPDATE_WINDOW_DURATION = timedelta(minutes=5)  
MAX_UPDATES_IN_WINDOW = 2                       
API_CALL_TIMEOUT = 15.0 # General timeout for Discord API calls
DB_CALL_TIMEOUT = 10.0  # Timeout for Firestore operations
LOCK_ACQUIRE_TIMEOUT = 12.0 # Slightly increased lock acquire timeout, helps in high contention scenarios

vc_rate_limit_windows = {}  
vc_zero_stats = {}          
vc_discord_api_cooldown_until = {} 

# --- Help Text ---
HELP_TEXT_CONTENT = (
    "📘 **コマンド一覧だニャ🐈**\n\n"
    "🔹 `!!nah [数]`\n"
    "→ 指定した数のメッセージをこのチャンネルから削除するニャ。\n"
    "   例: `!!nah 5`\n\n"
    "🔹 `!!nah_vc [VCのチャンネルIDまたは名前]`\n"
    "→ 指定したボイスチャンネルの人数表示用チャンネルを「STATUS」カテゴリに作成/削除するニャ。(トグル式)\n"
    "   ONにすると、STATUSカテゴリに `[元VC名]：〇 users` という名前のVCが作られ、人数が更新されるニャ。\n"
    "   OFFにすると、その人数表示用チャンネルを削除し、追跡を停止するニャ。\n"
    "   例: `!!nah_vc General Voice` または `!!nah_vc 123456789012345678`\n\n"
    "🔹 `!!nah_help` または `/nah_help`\n"
    "→ このヘルプメッセージを表示するニャ🐈\n"
)

# --- Custom Bot Class for Slash Commands ---
class MyBot(commands.Bot):
    async def setup_hook(self):
        @self.tree.command(name="nah_help", description="コマンド一覧を表示するニャ。")
        async def nah_help_slash(interaction: discord.Interaction):
            await interaction.response.send_message(HELP_TEXT_CONTENT, ephemeral=True)

        try:
            await self.tree.sync() 
            logger.info("スラッシュコマンドを同期しました。")
        except Exception as e:
            logger.error(f"スラッシュコマンドの同期中にエラー: {e}", exc_info=True)

# --- Bot Instance ---
bot = MyBot(command_prefix='!!', intents=intents)
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# --- Firestore Helper Functions ---
async def init_firestore():
    global db
    try:
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            db = firestore.AsyncClient()
            # Test connection: Try to get a non-existent document or a limit(1) query
            await asyncio.wait_for(db.collection(FIRESTORE_COLLECTION_NAME).limit(1).get(), timeout=DB_CALL_TIMEOUT)
            logger.info("Firestoreクライアントの初期化に成功しました。")
            return True
        else:
            logger.warning("環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されていません。Firestoreは使用できません。")
            db = None
            return False
    except DefaultCredentialsError:
        logger.error("Firestoreの認証に失敗しました。GOOGLE_APPLICATION_CREDENTIALSを確認してください。")
        db = None
        return False
    except asyncio.TimeoutError:
        logger.error("Firestoreクライアントの初期化テスト中にタイムアウトしました。")
        db = None
        return False
    except Exception as e:
        logger.error(f"Firestoreクライアントの初期化中に予期せぬエラーが発生しました: {e}", exc_info=True)
        db = None
        return False

async def load_tracked_channels_from_db():
    if not db:
        logger.info("Firestoreが無効なため、データベースからの読み込みをスキップします。")
        return

    global vc_tracking
    vc_tracking = {} 
    try:
        logger.info(f"Firestoreから追跡VC情報を読み込んでいます (コレクション: {FIRESTORE_COLLECTION_NAME})...")
        stream = db.collection(FIRESTORE_COLLECTION_NAME).stream()
        # Firestore stream itself doesn't easily support asyncio.wait_for for the whole stream.
        # Individual document processing should be quick. If overall loading is an issue, paginate.
        docs_loaded_count = 0
        async for doc_snapshot in stream:
            doc_data = doc_snapshot.to_dict()
            try:
                original_channel_id = int(doc_snapshot.id)
                guild_id_val = doc_data.get("guild_id")
                status_channel_id_val = doc_data.get("status_channel_id")
                
                guild_id = int(guild_id_val) if guild_id_val is not None else None
                status_channel_id = int(status_channel_id_val) if status_channel_id_val is not None else None
                original_channel_name = doc_data.get("original_channel_name")

                if not all([guild_id, status_channel_id, original_channel_name is not None]): # Check original_channel_name for None specifically
                    logger.warning(f"DB内のドキュメント {doc_snapshot.id} に必要な情報が不足しているか型が不正です。スキップ。 Data: {doc_data}")
                    continue

                vc_tracking[original_channel_id] = {
                    "guild_id": guild_id,
                    "status_channel_id": status_channel_id,
                    "original_channel_name": original_channel_name
                }
                docs_loaded_count += 1
                logger.debug(f"DBからロード: Original VC ID {original_channel_id}, Status VC ID: {status_channel_id}")
            except (ValueError, TypeError) as e_parse:
                logger.warning(f"DB内のドキュメント {doc_snapshot.id} のデータ型解析エラー: {e_parse}。スキップ。 Data: {doc_data}")
        logger.info(f"{docs_loaded_count}件の追跡VC情報をDBからロードしました。")
    except Exception as e: # Catch broader exceptions during streaming
        logger.error(f"Firestoreからのデータ読み込み中にエラー: {e}", exc_info=True)


async def save_tracked_original_to_db(original_channel_id: int, guild_id: int, status_channel_id: int, original_channel_name: str):
    if not db: return
    try:
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await asyncio.wait_for(
            doc_ref.set({
                "guild_id": guild_id, 
                "status_channel_id": status_channel_id,
                "original_channel_name": original_channel_name,
                "updated_at": firestore.SERVER_TIMESTAMP 
            }),
            timeout=DB_CALL_TIMEOUT
        )
        logger.debug(f"DBに保存: Original VC ID {original_channel_id}, Status VC ID {status_channel_id}")
    except asyncio.TimeoutError:
        logger.error(f"Firestoreへのデータ書き込みタイムアウト (Original VC ID: {original_channel_id})")
    except Exception as e:
        logger.error(f"Firestoreへのデータ書き込み中にエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def remove_tracked_original_from_db(original_channel_id: int):
    if not db: return
    try:
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await asyncio.wait_for(doc_ref.delete(), timeout=DB_CALL_TIMEOUT)
        logger.info(f"DBから削除: Original VC ID {original_channel_id}")
    except asyncio.TimeoutError:
        logger.error(f"Firestoreからのデータ削除タイムアウト (Original VC ID: {original_channel_id})")
    except Exception as e:
        logger.error(f"Firestoreからのデータ削除中にエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def get_or_create_status_category(guild: discord.Guild) -> discord.CategoryChannel | None:
    for category in guild.categories:
        if STATUS_CATEGORY_NAME.lower() in category.name.lower():
            logger.info(f"カテゴリ「{category.name}」をSTATUSカテゴリとして使用します。(Guild: {guild.name})")
            return category
    try:
        logger.info(f"「STATUS」を含むカテゴリが見つからなかったため、新規作成します。(Guild: {guild.name})")
        overwrites = { 
            guild.default_role: discord.PermissionOverwrite(read_message_history=True, view_channel=True, connect=False)
        }
        # Discord API calls for creation already have API_CALL_TIMEOUT in _create_status_vc_for_original
        new_category = await guild.create_category(STATUS_CATEGORY_NAME, overwrites=overwrites, reason="VCステータス表示用カテゴリ")
        logger.info(f"カテゴリ「{STATUS_CATEGORY_NAME}」を新規作成しました。(Guild: {guild.name})")
        return new_category
    except discord.Forbidden:
        logger.error(f"カテゴリ「{STATUS_CATEGORY_NAME}」の作成に失敗しました (権限不足) (Guild: {guild.name})")
    except Exception as e: # Includes HTTPException if API call fails for other reasons
        logger.error(f"カテゴリ「{STATUS_CATEGORY_NAME}」の作成中にエラー (Guild: {guild.name}): {e}", exc_info=True)
    return None

async def _create_status_vc_for_original(original_vc: discord.VoiceChannel) -> discord.VoiceChannel | None:
    guild = original_vc.guild
    status_category = await get_or_create_status_category(guild) # This might involve an API call if category needs creation
    if not status_category:
        logger.error(f"STATUSカテゴリの取得/作成に失敗しました ({guild.name} の {original_vc.name} 用)。")
        return None

    user_count = len([m for m in original_vc.members if not m.bot])
    user_count = min(user_count, 999) 

    status_channel_name_base = original_vc.name[:65] # Slightly shorter base for name construction
    status_channel_name = f"{status_channel_name_base}：{user_count} users"
    status_channel_name = re.sub(r'\s{2,}', ' ', status_channel_name).strip()[:100]

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(
            view_channel=True, read_message_history=True, connect=False, speak=False, stream=False,
            send_messages=False 
        )
    }
    try:
        new_status_vc = await asyncio.wait_for(
            guild.create_voice_channel(
                name=status_channel_name, category=status_category,
                overwrites=overwrites, reason=f"{original_vc.name} のステータス表示用VC"
            ),
            timeout=API_CALL_TIMEOUT
        )
        logger.info(f"作成成功: Status VC「{new_status_vc.name}」(ID: {new_status_vc.id}) (Original VC: {original_vc.name})")
        return new_status_vc
    except asyncio.TimeoutError:
        logger.error(f"Status VCの作成タイムアウト ({original_vc.name}, Guild: {guild.name})")
    except discord.Forbidden:
        logger.error(f"Status VCの作成に失敗 (権限不足) ({original_vc.name}, Guild: {guild.name})")
    except Exception as e:
        logger.error(f"Status VCの作成に失敗 ({original_vc.name}): {e}", exc_info=True)
    return None

def get_vc_lock(vc_id: int) -> asyncio.Lock:
    if vc_id not in vc_locks:
        vc_locks[vc_id] = asyncio.Lock()
        logger.debug(f"VC ID {vc_id} のために新しいLockオブジェクトを作成しました。")
    return vc_locks[vc_id]

# --- Core Tracking and Update Logic ---
async def register_new_vc_for_tracking(original_vc: discord.VoiceChannel, send_feedback_to_ctx=None):
    original_vc_id = original_vc.id
    lock = get_vc_lock(original_vc_id)
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UnknownTask"
    
    logger.debug(f"[{task_name}|register_new_vc] Attempting to acquire lock for VC ID: {original_vc_id}")
    try:
        await asyncio.wait_for(lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT)
        logger.debug(f"[{task_name}|register_new_vc] Lock acquired for VC ID: {original_vc_id}")

        # --- Start of critical section ---
        if original_vc_id in vc_tracking: # Double check after acquiring lock
            # (Existing logic for checking if already effectively tracking)
            track_info = vc_tracking[original_vc_id]
            guild_id_for_check = track_info.get("guild_id")
            status_id_for_check = track_info.get("status_channel_id")
            if guild_id_for_check and status_id_for_check:
                guild_for_status_check = bot.get_guild(guild_id_for_check)
                if guild_for_status_check:
                    status_vc_obj = guild_for_status_check.get_channel(status_id_for_check)
                    if isinstance(status_vc_obj, discord.VoiceChannel) and status_vc_obj.category and STATUS_CATEGORY_NAME.lower() in status_vc_obj.category.name.lower():
                        logger.info(f"[{task_name}|register_new_vc] VC {original_vc.name} は既に有効に追跡中です。")
                        if send_feedback_to_ctx:
                            await send_feedback_to_ctx.send(f"VC「{original_vc.name}」は既に追跡中ですニャ。")
                        return False 
            logger.info(f"[{task_name}|register_new_vc] VC {original_vc.name} の追跡情報が無効と判断。クリーンアップして再作成を試みます。")
            await unregister_vc_tracking_internal(original_vc_id, original_vc.guild, is_internal_call=True) 

        logger.info(f"[{task_name}|register_new_vc] VC {original_vc.name} (ID: {original_vc_id}) の新規追跡処理を開始します。")
        new_status_vc = await _create_status_vc_for_original(original_vc)
        if new_status_vc:
            vc_tracking[original_vc_id] = {
                "guild_id": original_vc.guild.id,
                "status_channel_id": new_status_vc.id,
                "original_channel_name": original_vc.name
            }
            await save_tracked_original_to_db(original_vc_id, original_vc.guild.id, new_status_vc.id, original_vc.name)
            
            vc_rate_limit_windows.pop(original_vc_id, None)
            vc_zero_stats.pop(original_vc_id, None)
            vc_discord_api_cooldown_until.pop(original_vc_id, None)

            # Create a new task for the initial update to avoid holding the registration lock for too long
            asyncio.create_task(update_dynamic_status_channel_name(original_vc, new_status_vc), name=f"UpdateTask-PostRegister-{original_vc_id}")
            logger.info(f"[{task_name}|register_new_vc] 追跡開始/再開: Original VC {original_vc.name}, Status VC {new_status_vc.name}. 初期更新タスクをスケジュール。")
            return True
        else:
            logger.error(f"[{task_name}|register_new_vc] {original_vc.name} のステータスVC作成に失敗。追跡は開始されません。")
            if original_vc_id in vc_tracking: del vc_tracking[original_vc_id]
            await remove_tracked_original_from_db(original_vc_id) # Ensure DB is clean if creation failed
            return False
        # --- End of critical section ---

    except asyncio.TimeoutError:
        logger.error(f"[{task_name}|register_new_vc] Timeout acquiring lock for VC ID: {original_vc_id}. Registration skipped.")
        if send_feedback_to_ctx:
            await send_feedback_to_ctx.send(f"VC「{original_vc.name}」の処理が混み合っているようですニャ。少し待ってから試してニャ。")
        return False
    except Exception as e:
        logger.error(f"[{task_name}|register_new_vc] Error during registration for VC {original_vc_id}: {e}", exc_info=True)
        return False
    finally:
        if lock.locked():
            lock.release()
            logger.debug(f"[{task_name}|register_new_vc] Lock for VC ID: {original_vc_id} released in finally.")


async def unregister_vc_tracking(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None):
    lock = get_vc_lock(original_channel_id)
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UnknownTask"
    logger.debug(f"[{task_name}|unregister_vc] Attempting to acquire lock for VC ID: {original_channel_id}")
    try:
        await asyncio.wait_for(lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT)
        logger.debug(f"[{task_name}|unregister_vc] Lock acquired for VC ID: {original_channel_id}")
        # --- Start of critical section ---
        await unregister_vc_tracking_internal(original_channel_id, guild, send_feedback_to_ctx, is_internal_call=False)
        # --- End of critical section ---
    except asyncio.TimeoutError:
        logger.error(f"[{task_name}|unregister_vc] Timeout acquiring lock for VC ID: {original_channel_id}. Unregistration skipped.")
        if send_feedback_to_ctx:
             await send_feedback_to_ctx.send(f"VC ID「{original_channel_id}」の処理が混み合っているようですニャ。少し待ってから試してニャ。")
    except Exception as e:
        logger.error(f"[{task_name}|unregister_vc] Error during unregistration for VC {original_channel_id}: {e}", exc_info=True)
    finally:
        if lock.locked():
            lock.release()
            logger.debug(f"[{task_name}|unregister_vc] Lock for VC ID: {original_channel_id} released in finally.")

async def unregister_vc_tracking_internal(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None, is_internal_call: bool = False):
    # Assumes lock is ALREADY ACQUIRED by the caller.
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UnknownTask"
    logger.info(f"[{task_name}|unregister_internal] VC ID {original_channel_id} の追跡解除処理を開始 (内部呼び出し: {is_internal_call})。")
    track_info = vc_tracking.pop(original_channel_id, None)
    original_vc_name_for_msg = f"ID: {original_channel_id}"

    if track_info:
        original_vc_name_for_msg = track_info.get("original_channel_name", f"ID: {original_channel_id}")
        status_channel_id = track_info.get("status_channel_id")
        
        current_guild_id = track_info.get("guild_id")
        current_guild = guild or (bot.get_guild(current_guild_id) if current_guild_id else None)

        if current_guild and status_channel_id:
            status_vc = current_guild.get_channel(status_channel_id)
            if status_vc and isinstance(status_vc, discord.VoiceChannel):
                logger.debug(f"[{task_name}|unregister_internal] Attempting to delete status VC {status_vc.name} (ID: {status_vc.id})")
                try:
                    await asyncio.wait_for(status_vc.delete(reason="オリジナルVCの追跡停止のため"), timeout=API_CALL_TIMEOUT)
                    logger.info(f"[{task_name}|unregister_internal] 削除成功: Status VC {status_vc.name} (ID: {status_vc.id})")
                except asyncio.TimeoutError:
                    logger.error(f"[{task_name}|unregister_internal] Status VC {status_vc.id} の削除タイムアウト")
                except discord.NotFound:
                    logger.info(f"[{task_name}|unregister_internal] Status VC {status_channel_id} は既に削除されていました。")
                # ... (other exceptions)
            # ... (other checks for status_vc validity)
    
    vc_rate_limit_windows.pop(original_channel_id, None)
    vc_zero_stats.pop(original_channel_id, None)
    vc_discord_api_cooldown_until.pop(original_channel_id, None)
    
    await remove_tracked_original_from_db(original_channel_id) # This now has a timeout
    if not is_internal_call and send_feedback_to_ctx:
        # ... (feedback logic)
        display_name = original_vc_name_for_msg
        if guild: 
             actual_original_vc = guild.get_channel(original_channel_id)
             if actual_original_vc : display_name = actual_original_vc.name
        try:
            await send_feedback_to_ctx.send(f"VC「{display_name}」の人数表示用チャンネルを削除し、追跡を停止したニャ。")
        except Exception as e_feedback: # Catch specific discord errors if needed
            logger.error(f"[{task_name}|unregister_internal] Error sending unregister feedback: {e_feedback}")


async def update_dynamic_status_channel_name(original_vc: discord.VoiceChannel, status_vc: discord.VoiceChannel):
    if not original_vc or not status_vc:
        logger.debug(f"Update_dynamic: スキップ - OriginalVC/StatusVCが無効 {original_vc.id if original_vc else 'N/A'}")
        return

    ovc_id = original_vc.id
    lock = get_vc_lock(ovc_id)
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UnknownTask"
    
    logger.debug(f"[{task_name}|update_dynamic] Attempting to acquire lock for VC ID: {ovc_id} (Lock currently: {'locked' if lock.locked() else 'unlocked'})")
    if lock.locked(): 
        logger.debug(f"[{task_name}|update_dynamic] Lock for VC ID {ovc_id} is ALREADY HELD. Skipping this update cycle.")
        return

    try:
        await asyncio.wait_for(lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT)
        logger.debug(f"[{task_name}|update_dynamic] Lock acquired for VC ID: {ovc_id}")
        # --- Start of critical section ---

        # Re-fetch channel objects from cache to ensure they are current
        current_original_vc = bot.get_channel(ovc_id)
        current_status_vc = bot.get_channel(status_vc.id)

        if not isinstance(current_original_vc, discord.VoiceChannel) or \
           not isinstance(current_status_vc, discord.VoiceChannel):
            logger.warning(f"[{task_name}|update_dynamic] Original VC {ovc_id} or Status VC {status_vc.id} became invalid after lock. Skipping.")
            return 
        original_vc, status_vc = current_original_vc, current_status_vc # Use refreshed objects

        now = datetime.now(timezone.utc)

        if ovc_id in vc_discord_api_cooldown_until and now < vc_discord_api_cooldown_until[ovc_id]:
            cooldown_ends_at = vc_discord_api_cooldown_until[ovc_id]
            logger.debug(f"[{task_name}|update_dynamic] Discord API cooldown for {original_vc.name}. Ends at {cooldown_ends_at.strftime('%H:%M:%S')}. Skip.")
            return 

        current_members = [member for member in original_vc.members if not member.bot]
        count = len(current_members)
        count = min(count, 999)

        track_info = vc_tracking.get(ovc_id)
        if not track_info: # Can happen if unregistered while waiting for lock
            logger.warning(f"[{task_name}|update_dynamic] Original VC {original_vc.name} (ID: {ovc_id}) not in tracking info. Skipping.")
            return 
        base_name = track_info.get("original_channel_name", original_vc.name[:65])
        
        # (0-user logic remains the same)
        desired_name_str = f"{base_name}：{count} users"
        is_special_zero_update_condition = False
        if count == 0:
            if ovc_id not in vc_zero_stats:
                vc_zero_stats[ovc_id] = {"zero_since": now, "notified_zero_explicitly": False}
            zero_stat = vc_zero_stats[ovc_id]
            if now >= zero_stat["zero_since"] + BOT_UPDATE_WINDOW_DURATION and not zero_stat.get("notified_zero_explicitly", False):
                desired_name_str = f"{base_name}：0 users"
                is_special_zero_update_condition = True
        else:
            if ovc_id in vc_zero_stats: del vc_zero_stats[ovc_id]


        try:
            logger.debug(f"[{task_name}|update_dynamic] Fetching current name for status VC {status_vc.id}")
            # Use bot.get_channel first for cached name, fallback to fetch_channel if stale check needed, but fetch has timeout
            current_status_vc_name = status_vc.name # Use cached name first
            # Forcing a fetch to ensure accuracy before edit, but be mindful of API limits
            fresh_status_vc = await asyncio.wait_for(bot.fetch_channel(status_vc.id), timeout=API_CALL_TIMEOUT)
            current_status_vc_name = fresh_status_vc.name # Update with fetched name

            logger.debug(f"[{task_name}|update_dynamic] Current name for {status_vc.id} is '{current_status_vc_name}'")
        except asyncio.TimeoutError:
            logger.error(f"[{task_name}|update_dynamic] Timeout fetching status VC name for {status_vc.id}. Skipping.")
            return 
        except discord.NotFound:
            logger.error(f"[{task_name}|update_dynamic] Status VC {status_vc.id} for {original_vc.name} not found. Will be handled by periodic check.")
            # Consider unregistering here if NotFound is persistent.
            # await unregister_vc_tracking_internal(ovc_id, original_vc.guild, is_internal_call=True)
            return 
        except Exception as e_fetch: # Catch other potential errors from fetch_channel
            logger.error(f"[{task_name}|update_dynamic] Error fetching status VC name {status_vc.id}: {e_fetch}", exc_info=True)
            return 

        final_new_name = re.sub(r'\s{2,}', ' ', desired_name_str).strip()[:100]

        if final_new_name == current_status_vc_name:
            logger.debug(f"[{task_name}|update_dynamic] Name for {status_vc.name} ('{final_new_name}') is already correct.")
            if is_special_zero_update_condition and ovc_id in vc_zero_stats:
                vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True
            return 

        # (Bot rate limit logic remains the same)
        window_info = vc_rate_limit_windows.get(ovc_id)
        can_update_by_bot_rule = False
        if not window_info or now >= window_info["window_start_time"] + BOT_UPDATE_WINDOW_DURATION:
            can_update_by_bot_rule = True
        elif window_info["count"] < MAX_UPDATES_IN_WINDOW:
            can_update_by_bot_rule = True
        
        if not can_update_by_bot_rule:
            logger.debug(f"[{task_name}|update_dynamic] Bot rate limit for {original_vc.name}. Updates in window: {window_info['count'] if window_info else 'N/A'}. Skip.")
            return 

        logger.info(f"[{task_name}|update_dynamic] Attempting name change for {status_vc.name} ('{current_status_vc_name}') to '{final_new_name}'")
        try:
            await asyncio.wait_for(
                status_vc.edit(name=final_new_name, reason="VC参加人数更新 / 0人ポリシー"),
                timeout=API_CALL_TIMEOUT
            )
            logger.info(f"[{task_name}|update_dynamic] SUCCESS name change for {status_vc.name} to '{final_new_name}'")
            # (Update rate limit window logic remains the same)
            current_window_data = vc_rate_limit_windows.get(ovc_id)
            if not current_window_data or now >= current_window_data["window_start_time"] + BOT_UPDATE_WINDOW_DURATION:
                vc_rate_limit_windows[ovc_id] = {"window_start_time": now, "count": 1}
            else:
                current_window_data["count"] += 1
            if is_special_zero_update_condition and ovc_id in vc_zero_stats:
                vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True
            if ovc_id in vc_discord_api_cooldown_until: del vc_discord_api_cooldown_until[ovc_id]

        except asyncio.TimeoutError:
            logger.error(f"[{task_name}|update_dynamic] Timeout editing status VC name for {status_vc.name} (ID: {status_vc.id}).")
        except discord.HTTPException as e_http:
            if e_http.status == 429: # Discord API rate limit
                retry_after = e_http.retry_after if e_http.retry_after is not None else 60.0 # Default to 60s if not specified
                vc_discord_api_cooldown_until[ovc_id] = now + timedelta(seconds=retry_after)
                logger.warning(f"[{task_name}|update_dynamic] Discord API rate limit (429) for {status_vc.name}. Cooldown: {retry_after}s")
            else: # Other HTTP errors
                logger.error(f"[{task_name}|update_dynamic] HTTP error {e_http.status} editing {status_vc.name}: {e_http.text}", exc_info=True)
        except Exception as e_edit: # Other unexpected errors
            logger.error(f"[{task_name}|update_dynamic] Unexpected error editing {status_vc.name}: {e_edit}", exc_info=True)
        # --- End of critical section ---
    
    except asyncio.TimeoutError:
        # This timeout is for acquiring the lock itself
        logger.error(f"[{task_name}|update_dynamic] Timeout acquiring lock for VC ID: {ovc_id}. Update skipped. Lock state before attempt: {'N/A - was not locked check'}")
    except Exception as e_outer_update: # Catch any other exceptions in the outer try block
        logger.error(f"[{task_name}|update_dynamic] Outer error for VC {ovc_id}: {e_outer_update}", exc_info=True)
    finally:
        if lock.locked():
            lock.release()
            logger.debug(f"[{task_name}|update_dynamic] Lock for VC ID: {ovc_id} released in finally.")

# --- Bot Events ---
@bot.event
async def on_ready():
    logger.info(f'ログイン成功: {bot.user.name} (ID: {bot.user.id})')
    logger.info(f"discord.py バージョン: {discord.__version__}")
    
    try:
        activity_name = "VCの人数を見守り中ニャ～"
        activity = discord.CustomActivity(name=activity_name)
        await bot.change_presence(activity=activity)
        logger.info(f"ボットのアクティビティを設定しました: {activity_name}")
    except Exception as e:
        logger.error(f"アクティビティの設定中にエラー: {e}", exc_info=True)
    
    vc_discord_api_cooldown_until.clear() 

    if await init_firestore(): # init_firestore now has timeout for its test query
        await load_tracked_channels_from_db() # load_tracked_channels_from_db needs careful review for timeouts if it becomes slow
    else:
        logger.warning("Firestoreが利用できないため、VC追跡の永続化は無効です。")

    logger.info("起動時の追跡VC状態整合性チェックと更新を開始します...")
    
    tracked_ids_to_process = list(vc_tracking.keys()) 
    
    for original_cid in tracked_ids_to_process:
        logger.info(f"[on_ready] Processing VC ID: {original_cid}")
        lock = get_vc_lock(original_cid) 
        
        logger.debug(f"[on_ready] Attempting to acquire lock for VC ID: {original_cid}")
        try:
            # Longer timeout for on_ready processing as it might involve more steps
            await asyncio.wait_for(lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT * 3) 
            logger.debug(f"[on_ready] Lock acquired for VC ID: {original_cid}")
            # --- Start of critical section for this VC in on_ready ---
                
            if original_cid not in vc_tracking:
                logger.info(f"[on_ready] VC {original_cid} no longer in tracking after lock acquisition. Skipping.")
                continue # Lock released in finally

            track_info = vc_tracking[original_cid]
            guild = bot.get_guild(track_info["guild_id"])

            if not guild:
                logger.warning(f"[on_ready] Guild {track_info['guild_id']} (Original VC {original_cid}) が見つかりません。追跡を解除します。")
                await unregister_vc_tracking_internal(original_cid, None, is_internal_call=True)
                continue # Lock released in finally

            original_vc = guild.get_channel(original_cid)
            if not isinstance(original_vc, discord.VoiceChannel):
                logger.warning(f"[on_ready] Original VC {original_cid} (Guild {guild.name}) が見つからないかVCではありません。追跡を解除します。")
                await unregister_vc_tracking_internal(original_cid, guild, is_internal_call=True)
                continue # Lock released in finally

            status_vc_id = track_info.get("status_channel_id")
            status_vc = guild.get_channel(status_vc_id) if status_vc_id else None
            
            vc_rate_limit_windows.pop(original_cid, None) 
            vc_zero_stats.pop(original_cid, None)

            if isinstance(status_vc, discord.VoiceChannel) and status_vc.category and STATUS_CATEGORY_NAME.lower() in status_vc.category.name.lower():
                logger.info(f"[on_ready] Original VC {original_vc.name} の既存Status VC {status_vc.name} は有効です。名前を更新します。")
                asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc), name=f"UpdateTask-OnReady-{original_cid}")
            else: 
                if status_vc: 
                    logger.warning(f"[on_ready] Status VC {status_vc.id if status_vc else 'N/A'} ({original_vc.name}用) が無効か移動。削除して再作成試行。")
                    try:
                        await asyncio.wait_for(status_vc.delete(reason="無効なステータスVCのため再作成"), timeout=API_CALL_TIMEOUT)
                    except Exception as e_del_ready: 
                        logger.error(f"[on_ready] 無効なステータスVC {status_vc.id if status_vc else 'N/A'} の削除エラー: {e_del_ready}", exc_info=True)
                
                logger.info(f"[on_ready] {original_vc.name} のステータスVCが存在しないか無効。新規作成試行。")
                await unregister_vc_tracking_internal(original_cid, guild, is_internal_call=True) # Clears DB and memory tracking
                
                # Recreate logic directly here, as we hold the lock
                new_status_vc_obj = await _create_status_vc_for_original(original_vc)
                if new_status_vc_obj:
                    vc_tracking[original_cid] = { 
                        "guild_id": guild.id,
                        "status_channel_id": new_status_vc_obj.id,
                        "original_channel_name": original_vc.name 
                    }
                    await save_tracked_original_to_db(original_cid, guild.id, new_status_vc_obj.id, original_vc.name)
                    logger.info(f"[on_ready] {original_vc.name} のステータスVCを正常に再作成しました: {new_status_vc_obj.name}")
                    asyncio.create_task(update_dynamic_status_channel_name(original_vc, new_status_vc_obj), name=f"UpdateTask-OnReady-Recreate-{original_cid}")
                else:
                    logger.error(f"[on_ready] {original_vc.name} のステータスVC再作成に失敗しました。")
            # --- End of critical section for this VC in on_ready ---
        except asyncio.TimeoutError:
            logger.error(f"[on_ready] Timeout acquiring lock for VC ID: {original_cid} during on_ready processing. Skipping this VC for init.")
        except Exception as e_onready_vc:
            logger.error(f"[on_ready] Error processing VC {original_cid}: {e_onready_vc}", exc_info=True)
        finally:
            if lock.locked():
                lock.release()
                logger.debug(f"[on_ready] Lock for VC ID: {original_cid} released in finally.")
            
    logger.info("起動時の追跡VC状態整合性チェックと更新が完了しました。")
    if not periodic_status_update.is_running():
        try:
            periodic_status_update.start()
            logger.info("定期ステータス更新タスクを開始しました。")
        except RuntimeError as e_task_start: 
             logger.warning(f"定期ステータス更新タスクの開始試行中にエラー: {e_task_start}")


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return

    channels_to_check_ids = set()
    if before.channel: channels_to_check_ids.add(before.channel.id)
    if after.channel: channels_to_check_ids.add(after.channel.id)
    
    for original_cid in channels_to_check_ids:
        if original_cid in vc_tracking: # Check if this VC is one we are tracking
            # Fetch fresh info, as vc_tracking might be stale if a task is slow
            track_info = vc_tracking.get(original_cid) 
            if not track_info: 
                logger.debug(f"[on_voice_state_update] VC {original_cid} no longer in vc_tracking after initial check. Skipping.")
                continue 

            guild = bot.get_guild(track_info["guild_id"])
            if not guild: 
                logger.warning(f"[on_voice_state_update] Guild {track_info['guild_id']} for VC {original_cid} not found. Skipping update attempt.")
                continue

            original_vc = guild.get_channel(original_cid)
            status_vc_id = track_info.get("status_channel_id")
            status_vc = guild.get_channel(status_vc_id) if status_vc_id else None

            if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
                logger.debug(f"[on_voice_state_update] Relevant update for tracked VC ID: {original_cid}. Scheduling name update.")
                asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc), name=f"UpdateTask-VoiceState-{original_cid}")
            else: # Log if original or status VC is not as expected
                logger.debug(f"[on_voice_state_update] Original or Status VC invalid for {original_cid}. Original: {type(original_vc)}, Status: {type(status_vc)}. Skipping update.")


@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): return
    if channel.category and STATUS_CATEGORY_NAME.lower() in channel.category.name.lower(): 
        logger.debug(f"[on_guild_channel_create] New VC {channel.name} is a status channel. Ignoring.")
        return
    # Check if this new channel is already known (e.g. as an original or status channel)
    if channel.id in vc_tracking or any(info.get("status_channel_id") == channel.id for info in vc_tracking.values()): 
        logger.debug(f"[on_guild_channel_create] New VC {channel.name} already known or is a status channel. Ignoring.")
        return

    logger.info(f"[on_guild_channel_create] New VC 「{channel.name}」 (ID: {channel.id}) 作成。自動追跡試行。")
    asyncio.create_task(register_new_vc_for_tracking(channel), name=f"RegisterTask-ChannelCreate-{channel.id}")


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): return

    original_channel_id_to_process = None
    is_status_vc_deleted = False
    guild_where_deleted = channel.guild # Get guild from the deleted channel object

    if channel.id in vc_tracking: 
        original_channel_id_to_process = channel.id
        logger.info(f"[on_guild_channel_delete] Tracked original VC {channel.name} (ID: {channel.id}) deleted from guild {guild_where_deleted.name}.")
    else: 
        for ocid, info in list(vc_tracking.items()): 
            if info.get("status_channel_id") == channel.id:
                # Ensure the guild matches if possible, to prevent cross-guild misattribution (unlikely but good check)
                if info.get("guild_id") == guild_where_deleted.id:
                    original_channel_id_to_process = ocid
                    is_status_vc_deleted = True
                    logger.info(f"[on_guild_channel_delete] Status VC {channel.name} (for original ID: {ocid}) deleted from guild {guild_where_deleted.name}.")
                    break
    
    if original_channel_id_to_process:
        logger.info(f"[on_guild_channel_delete] Processing deletion related to original VC ID: {original_channel_id_to_process}")
        
        async def handle_deletion_logic_wrapper(ocid_to_process, deleted_is_status, g_obj):
            lock = get_vc_lock(ocid_to_process)
            task_name_del = asyncio.current_task().get_name() if asyncio.current_task() else "DelWrapTask"
            logger.debug(f"[{task_name_del}|handle_deletion_logic_wrapper] Attempting lock for {ocid_to_process}")
            try:
                await asyncio.wait_for(lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT * 3) # Longer timeout
                logger.debug(f"[{task_name_del}|handle_deletion_logic_wrapper] Lock acquired for {ocid_to_process}")
                # --- Start of critical section ---
                
                if deleted_is_status:
                    original_vc_obj = g_obj.get_channel(ocid_to_process) if g_obj else None
                    if original_vc_obj and isinstance(original_vc_obj, discord.VoiceChannel):
                        logger.info(f"[{task_name_del}|handle_deletion_logic_wrapper] Original VC {original_vc_obj.name} still exists. Attempting to recreate status VC.")
                        await unregister_vc_tracking_internal(ocid_to_process, g_obj, is_internal_call=True)
                        
                        # Recreate logic directly
                        new_status_vc = await _create_status_vc_for_original(original_vc_obj)
                        if new_status_vc:
                            vc_tracking[ocid_to_process] = {
                                "guild_id": original_vc_obj.guild.id,
                                "status_channel_id": new_status_vc.id,
                                "original_channel_name": original_vc_obj.name
                            }
                            await save_tracked_original_to_db(ocid_to_process, original_vc_obj.guild.id, new_status_vc.id, original_vc_obj.name)
                            asyncio.create_task(update_dynamic_status_channel_name(original_vc_obj, new_status_vc), name=f"UpdateTask-PostDeleteRecreate-{ocid_to_process}")
                            logger.info(f"[{task_name_del}|handle_deletion_logic_wrapper] Status VC for {original_vc_obj.name} recreated: {new_status_vc.name}")
                        else:
                            logger.error(f"[{task_name_del}|handle_deletion_logic_wrapper] Failed to recreate status VC for {original_vc_obj.name}. It remains untracked.")
                    else:
                        logger.info(f"[{task_name_del}|handle_deletion_logic_wrapper] Original VC {ocid_to_process} not found after status VC deletion. Unregistering fully.")
                        await unregister_vc_tracking_internal(ocid_to_process, g_obj, is_internal_call=True)
                else: # Original VC was deleted
                    await unregister_vc_tracking_internal(ocid_to_process, g_obj, is_internal_call=True)
                # --- End of critical section ---
            except asyncio.TimeoutError:
                logger.error(f"[{task_name_del}|handle_deletion_logic_wrapper] Timeout acquiring lock for {ocid_to_process}. Deletion processing may be incomplete.")
            except Exception as e_del_handler_wrapper:
                logger.error(f"[{task_name_del}|handle_deletion_logic_wrapper] Error for {ocid_to_process}: {e_del_handler_wrapper}", exc_info=True)
            finally:
                if lock.locked():
                    lock.release()
                    logger.debug(f"[{task_name_del}|handle_deletion_logic_wrapper] Lock for {ocid_to_process} released in finally.")

        asyncio.create_task(handle_deletion_logic_wrapper(original_channel_id_to_process, is_status_vc_deleted, guild_where_deleted), name=f"DeleteTask-{original_channel_id_to_process}")

# --- Periodic Task ---
@tasks.loop(minutes=3) 
async def periodic_status_update():
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "PeriodicTaskLoop"
    logger.debug(f"[{task_name}] 定期ステータス更新タスク実行中... 現在追跡中: {len(vc_tracking)}件")
    if not vc_tracking: return

    for original_cid in list(vc_tracking.keys()): 
        logger.debug(f"[{task_name}|periodic_update] Processing VC ID: {original_cid}")
        
        track_info = vc_tracking.get(original_cid)
        if not track_info:
            logger.warning(f"[{task_name}|periodic_update] VC {original_cid} not in tracking after starting loop iteration. Skipping.")
            continue

        guild = bot.get_guild(track_info["guild_id"])
        if not guild:
            logger.warning(f"[{task_name}|periodic_update] Guild {track_info['guild_id']} (Original VC {original_cid}) not found. Scheduling unregistration.")
            asyncio.create_task(unregister_vc_tracking(original_cid, None), name=f"UnregisterTask-Periodic-NoGuild-{original_cid}")
            continue
        
        original_vc = guild.get_channel(original_cid)
        status_vc_id = track_info.get("status_channel_id")
        status_vc = guild.get_channel(status_vc_id) if status_vc_id else None

        if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
            if status_vc.category is None or STATUS_CATEGORY_NAME.lower() not in status_vc.category.name.lower():
                logger.warning(f"[{task_name}|periodic_update] Status VC {status_vc.name} for {original_vc.name} in wrong category. Scheduling fix.")
                
                async def fix_category_task_wrapper(ovc_obj, g_obj): 
                    fix_task_name = asyncio.current_task().get_name() if asyncio.current_task() else "FixCatTask"
                    logger.debug(f"[{fix_task_name}|fix_category_task_wrapper] Attempting to fix category for {ovc_obj.name}")
                    lock_fix = get_vc_lock(ovc_obj.id)
                    try:
                        await asyncio.wait_for(lock_fix.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT * 2) # Lock for unregister part
                        logger.debug(f"[{fix_task_name}|fix_category_task_wrapper] Lock acquired for unregister part of {ovc_obj.id}")
                        await unregister_vc_tracking_internal(ovc_obj.id, g_obj, is_internal_call=True)
                    except asyncio.TimeoutError:
                        logger.error(f"[{fix_task_name}|fix_category_task_wrapper] Timeout acquiring lock for {ovc_obj.id} during category fix (unregister part).")
                        return 
                    except Exception as e_fix_lock_unregister:
                        logger.error(f"[{fix_task_name}|fix_category_task_wrapper] Error during lock/unregister for {ovc_obj.id}: {e_fix_lock_unregister}", exc_info=True)
                        if lock_fix.locked(): lock_fix.release() 
                        return
                    finally:
                        if lock_fix.locked(): 
                            lock_fix.release()
                            logger.debug(f"[{fix_task_name}|fix_category_task_wrapper] Lock for {ovc_obj.id} (unregister part) released.")
                    
                    # Lock is released. Now call register_new_vc_for_tracking, which will acquire its own lock.
                    logger.info(f"[{fix_task_name}|fix_category_task_wrapper] Re-registering {ovc_obj.name} to fix category.")
                    await register_new_vc_for_tracking(ovc_obj) # This will try to acquire lock again for registration
                    logger.debug(f"[{fix_task_name}|fix_category_task_wrapper] Category fix attempt for {ovc_obj.id} finished.")

                asyncio.create_task(fix_category_task_wrapper(original_vc, guild), name=f"FixCategoryTask-{original_cid}")
                continue 

            asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc), name=f"UpdateTask-Periodic-{original_cid}")
        
        elif not isinstance(original_vc, discord.VoiceChannel) and original_cid in vc_tracking: 
            logger.warning(f"[{task_name}|periodic_update] Original VC {original_cid} ('{track_info.get('original_channel_name', 'N/A')}') invalid. Scheduling unregistration.")
            asyncio.create_task(unregister_vc_tracking(original_cid, guild), name=f"UnregisterTask-Periodic-InvalidOrig-{original_cid}")

        elif isinstance(original_vc, discord.VoiceChannel) and not isinstance(status_vc, discord.VoiceChannel):
            logger.warning(f"[{task_name}|periodic_update] Status VC for {original_vc.name} (ID: {original_cid}) missing/invalid. Scheduling recreation.")
            
            async def recreate_status_vc_task_wrapper(ovc_obj, g_obj): 
                recreate_task_name = asyncio.current_task().get_name() if asyncio.current_task() else "RecreateStatusTask"
                logger.debug(f"[{recreate_task_name}|recreate_status_vc_task_wrapper] Attempting to recreate status for {ovc_obj.name}")
                lock_recreate = get_vc_lock(ovc_obj.id)
                try:
                    await asyncio.wait_for(lock_recreate.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT * 2) # Lock for unregister part
                    logger.debug(f"[{recreate_task_name}|recreate_status_vc_task_wrapper] Lock acquired for unregister part of {ovc_obj.id}")
                    await unregister_vc_tracking_internal(ovc_obj.id, g_obj, is_internal_call=True) 
                except asyncio.TimeoutError:
                    logger.error(f"[{recreate_task_name}|recreate_status_vc_task_wrapper] Timeout acquiring lock for {ovc_obj.id} (unregister part).")
                    return
                except Exception as e_recreate_lock_unregister:
                    logger.error(f"[{recreate_task_name}|recreate_status_vc_task_wrapper] Error during lock/unregister for {ovc_obj.id}: {e_recreate_lock_unregister}", exc_info=True)
                    if lock_recreate.locked(): lock_recreate.release()
                    return
                finally:
                    if lock_recreate.locked(): 
                        lock_recreate.release()
                        logger.debug(f"[{recreate_task_name}|recreate_status_vc_task_wrapper] Lock for {ovc_obj.id} (unregister part) released.")
                
                logger.info(f"[{recreate_task_name}|recreate_status_vc_task_wrapper] Re-registering {ovc_obj.name} to recreate status VC.")
                await register_new_vc_for_tracking(ovc_obj) # This will acquire its own lock
                logger.debug(f"[{recreate_task_name}|recreate_status_vc_task_wrapper] Status VC recreate attempt for {ovc_obj.id} finished.")
            
            asyncio.create_task(recreate_status_vc_task_wrapper(original_vc, guild), name=f"RecreateStatusTask-{original_cid}")
        
        elif original_cid in vc_tracking: 
            logger.warning(f"[{task_name}|periodic_update] Generic invalid state for Original VC {original_cid} ('{track_info.get('original_channel_name', 'N/A')}'). Scheduling unregistration.")
            asyncio.create_task(unregister_vc_tracking(original_cid, guild), name=f"UnregisterTask-Periodic-GenericInvalid-{original_cid}")

# --- Bot Commands ---
@bot.command(name='nah', help="指定した数のメッセージを削除するニャ。 例: !!nah 5")
@commands.has_permissions(manage_messages=True)
@commands.bot_has_permissions(manage_messages=True)
async def nah_command(ctx, num: int):
    if num <= 0:
        await ctx.send("1以上の数を指定してニャ🐈")
        return
    if num > 100: 
        await ctx.send("一度に削除できるのは100件までニャ🐈")
        return
    try:
        deleted_messages = await ctx.channel.purge(limit=num + 1) 
        response_msg = await ctx.send(f"{len(deleted_messages) -1}件のメッセージを削除したニャ🐈")
        await asyncio.sleep(5)
        await response_msg.delete()
    except discord.Forbidden:
        await ctx.send("メッセージを削除する権限がないニャ😿 (ボットの権限を確認してニャ)")
    except discord.HTTPException as e:
        logger.error(f"nahコマンドでHTTPエラー: {e}", exc_info=True)
        await ctx.send(f"メッセージ削除中にエラーが発生したニャ😿: {e.text}")
    except Exception as e:
        logger.error(f"nahコマンドでエラー: {e}", exc_info=True)
        await ctx.send(f"エラーが発生したニャ😿: {e}")

@nah_command.error
async def nah_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("このコマンドを実行する権限がニャいみたいだニャ… (メッセージ管理権限が必要だニャ)")
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send("ボットにメッセージを削除する権限がないニャ😿 (ボットの権限を確認してニャ)")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("数の指定がおかしいニャ。例: `!!nah 5`")
    else:
        logger.error(f"nah_command 未処理のエラー: {error}", exc_info=True)
        await ctx.send("コマンド実行中に予期せぬエラーが発生したニャ。")

@bot.command(name='nah_vc', help="指定VCの人数表示用チャンネルを作成/削除するニャ。")
@commands.has_permissions(manage_channels=True) 
@commands.bot_has_permissions(manage_channels=True) 
async def nah_vc_command(ctx, *, channel_id_or_name: str):
    guild = ctx.guild
    if not guild:
        await ctx.send("このコマンドはサーバー内でのみ使用可能ですニャ🐈")
        return

    target_vc = None
    try: 
        vc_id = int(channel_id_or_name)
        target_vc = guild.get_channel(vc_id)
    except ValueError: 
        for vc_iter in guild.voice_channels: 
            if vc_iter.name.lower() == channel_id_or_name.lower():
                target_vc = vc_iter; break
        if not target_vc: 
            for vc_iter in guild.voice_channels:
                if channel_id_or_name.lower() in vc_iter.name.lower():
                    target_vc = vc_iter; logger.info(f"VC名「{channel_id_or_name}」の部分一致で「{vc_iter.name}」を使用。"); break
    
    if not target_vc or not isinstance(target_vc, discord.VoiceChannel):
        await ctx.send(f"指定された「{channel_id_or_name}」はボイスチャンネルとして見つからなかったニャ😿")
        return

    if target_vc.category and STATUS_CATEGORY_NAME.lower() in target_vc.category.name.lower():
        await ctx.send(f"VC「{target_vc.name}」はSTATUSチャンネルのようだニャ。元のVCを指定してニャ。")
        return

    await ctx.send(f"VC「{target_vc.name}」の追跡設定/解除処理を開始したニャ。完了まで少し待ってニャ。") 

    if target_vc.id in vc_tracking:
        logger.info(f"コマンド: VC「{target_vc.name}」の追跡解除を試みます。")
        asyncio.create_task(unregister_vc_tracking(target_vc.id, guild, send_feedback_to_ctx=ctx), name=f"UnregisterTask-Command-{target_vc.id}")
    else:
        logger.info(f"コマンド: VC「{target_vc.name}」の新規追跡を試みます。")
        asyncio.create_task(register_new_vc_for_tracking(target_vc, send_feedback_to_ctx=ctx), name=f"RegisterTask-Command-{target_vc.id}")
                            
@nah_vc_command.error
async def nah_vc_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("このコマンドを実行する権限がニャいみたいだニャ… (チャンネル管理権限が必要だニャ)")
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send("ボットにチャンネルを管理する権限がないニャ� (ボットの権限を確認してニャ)")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("どのボイスチャンネルか指定してニャ！ 例: `!!nah_vc General`")
    else:
        logger.error(f"nah_vc_command 未処理のエラー: {error}", exc_info=True)
        await ctx.send("コマンド実行中に予期せぬエラーが発生したニャ。")

@bot.command(name='nah_help', help="コマンド一覧を表示するニャ。")
async def nah_help_prefix(ctx: commands.Context):
    await ctx.send(HELP_TEXT_CONTENT)

# --- Main Bot Execution ---
async def start_bot_main():
    if DISCORD_TOKEN is None:
        logger.critical("DISCORD_TOKEN が環境変数に設定されていません。Botを起動できません。")
        return
    
    if os.getenv("RENDER"): 
        keep_alive()

    try:
        logger.info("Botの非同期処理を開始します...")
        await bot.start(DISCORD_TOKEN)
    except discord.LoginFailure:
        logger.critical("Discordへのログインに失敗しました。トークンが正しいか確認してください。")
    except Exception as e:
        logger.critical(f"Botの起動中または実行中に予期せぬエラーが発生しました: {e}", exc_info=True)
    finally:
        if bot.is_connected() and not bot.is_closed(): 
            logger.info("Botをシャットダウンします...")
            try:
                # Ensure all tasks are given a chance to finish or be cancelled
                # This is a more complex shutdown sequence if needed. For now, bot.close() is standard.
                # tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
                # [task.cancel() for task in tasks]
                # logger.info(f"Cancelling {len(tasks)} outstanding tasks.")
                # await asyncio.gather(*tasks, return_exceptions=True)
                await bot.close()
            except Exception as e_close:
                logger.error(f"Botのシャットダウン中にエラー: {e_close}", exc_info=True)
        logger.info("Botがシャットダウンしました。")

# --- Entry Point ---
if __name__ == "__main__":
    try:
        asyncio.run(start_bot_main())
    except KeyboardInterrupt:
        logger.info("ユーザーによりBotが停止されました (KeyboardInterrupt)。")
    except Exception as e: 
        logger.critical(f"メインの実行ループで予期せぬエラーが発生しました: {e}", exc_info=True)

