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
    logger.info(f"Keep-aliveサーバーがポート {os.environ.get('PORT', 8080)} で起動準備完了。")

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(module)s:%(lineno)d %(message)s")
logger = logging.getLogger(__name__)

# --- Bot Intents Configuration ---
intents = discord.Intents.default() # Start with default intents
intents.guilds = True
intents.voice_states = True # Needed for on_voice_state_update
intents.message_content = True # If using prefix commands that read message content
# intents.members = True # Enable if you need member information beyond what's in guild/voice events

# --- Firestore Client and Constants ---
db = None # Firestore async client instance
FIRESTORE_COLLECTION_NAME = "discord_tracked_original_vcs_prod_v3" # Firestore collection name
STATUS_CATEGORY_NAME = "STATUS" # Name for the category holding status VCs

# --- VC Tracking Dictionaries and Locks ---
vc_tracking = {} # Stores info about tracked VCs: {original_vc_id: {"guild_id": ..., "status_channel_id": ..., "original_channel_name": ...}}
vc_locks = {}    # Manages concurrent access to VC data: {vc_id: asyncio.Lock()}

# --- Cooldown and State Settings for VC Name Updates ---
BOT_UPDATE_WINDOW_DURATION = timedelta(minutes=5)  # 5-minute window for bot's own rate limiting
MAX_UPDATES_IN_WINDOW = 2                       # Max updates allowed by bot within its window
API_CALL_TIMEOUT = 15.0                         # Timeout in seconds for Discord API calls like edit/fetch
LOCK_ACQUIRE_TIMEOUT = 10.0                     # Timeout for acquiring a lock

# vc_id as key for these dictionaries:
vc_rate_limit_windows = {}  # Stores {"window_start_time": datetime, "count": int} for bot's rate limiting
vc_zero_stats = {}          # Stores {"zero_since": datetime, "notified_zero_explicitly": bool} for 0-user rule
vc_discord_api_cooldown_until = {} # Stores datetime until which Discord API cooldown (429) is active for a VC

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
        # Register slash command
        @self.tree.command(name="nah_help", description="コマンド一覧を表示するニャ。")
        async def nah_help_slash(interaction: discord.Interaction):
            await interaction.response.send_message(HELP_TEXT_CONTENT, ephemeral=True)

        try:
            await self.tree.sync() # Sync slash commands with Discord
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
            await db.collection(FIRESTORE_COLLECTION_NAME).limit(1).get() # Test connection
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
    except Exception as e:
        logger.error(f"Firestoreクライアントの初期化中に予期せぬエラーが発生しました: {e}", exc_info=True)
        db = None
        return False

async def load_tracked_channels_from_db():
    if not db:
        logger.info("Firestoreが無効なため、データベースからの読み込みをスキップします。")
        return

    global vc_tracking
    vc_tracking = {} # Reset before loading
    try:
        logger.info(f"Firestoreから追跡VC情報を読み込んでいます (コレクション: {FIRESTORE_COLLECTION_NAME})...")
        stream = db.collection(FIRESTORE_COLLECTION_NAME).stream()
        async for doc_snapshot in stream:
            doc_data = doc_snapshot.to_dict()
            try:
                original_channel_id = int(doc_snapshot.id)
                guild_id = int(doc_data.get("guild_id"))
                status_channel_id = int(doc_data.get("status_channel_id"))
                original_channel_name = doc_data.get("original_channel_name")

                if not all([guild_id, status_channel_id, original_channel_name]):
                    logger.warning(f"DB内のドキュメント {doc_snapshot.id} に必要な情報が不足しているか型が不正です。スキップ。 Data: {doc_data}")
                    continue

                vc_tracking[original_channel_id] = {
                    "guild_id": guild_id,
                    "status_channel_id": status_channel_id,
                    "original_channel_name": original_channel_name
                }
                logger.debug(f"DBからロード: Original VC ID {original_channel_id}, Status VC ID: {status_channel_id}")
            except (ValueError, TypeError) as e_parse:
                logger.warning(f"DB内のドキュメント {doc_snapshot.id} のデータ型解析エラー: {e_parse}。スキップ。 Data: {doc_data}")
        logger.info(f"{len(vc_tracking)}件の追跡VC情報をDBからロードしました。")
    except Exception as e:
        logger.error(f"Firestoreからのデータ読み込み中にエラー: {e}", exc_info=True)

async def save_tracked_original_to_db(original_channel_id: int, guild_id: int, status_channel_id: int, original_channel_name: str):
    if not db: return
    try:
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await doc_ref.set({
            "guild_id": guild_id, # Store as int or string consistently
            "status_channel_id": status_channel_id,
            "original_channel_name": original_channel_name,
            "updated_at": firestore.SERVER_TIMESTAMP # Auto-timestamp
        })
        logger.debug(f"DBに保存: Original VC ID {original_channel_id}, Status VC ID {status_channel_id}")
    except Exception as e:
        logger.error(f"Firestoreへのデータ書き込み中にエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def remove_tracked_original_from_db(original_channel_id: int):
    if not db: return
    try:
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await doc_ref.delete()
        logger.info(f"DBから削除: Original VC ID {original_channel_id}")
    except Exception as e:
        logger.error(f"Firestoreからのデータ削除中にエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

# --- Channel Management Helper Functions ---
async def get_or_create_status_category(guild: discord.Guild) -> discord.CategoryChannel | None:
    # Find existing category
    for category in guild.categories:
        if STATUS_CATEGORY_NAME.lower() in category.name.lower(): # Case-insensitive check
            logger.info(f"カテゴリ「{category.name}」をSTATUSカテゴリとして使用します。(Guild: {guild.name})")
            return category
    # Create new category if not found
    try:
        logger.info(f"「STATUS」を含むカテゴリが見つからなかったため、新規作成します。(Guild: {guild.name})")
        overwrites = { # Permissions for the new category
            guild.default_role: discord.PermissionOverwrite(read_message_history=True, view_channel=True, connect=False)
        }
        new_category = await guild.create_category(STATUS_CATEGORY_NAME, overwrites=overwrites, reason="VCステータス表示用カテゴリ")
        logger.info(f"カテゴリ「{STATUS_CATEGORY_NAME}」を新規作成しました。(Guild: {guild.name})")
        return new_category
    except discord.Forbidden:
        logger.error(f"カテゴリ「{STATUS_CATEGORY_NAME}」の作成に失敗しました (権限不足) (Guild: {guild.name})")
    except Exception as e:
        logger.error(f"カテゴリ「{STATUS_CATEGORY_NAME}」の作成中にエラー (Guild: {guild.name}): {e}", exc_info=True)
    return None

async def _create_status_vc_for_original(original_vc: discord.VoiceChannel) -> discord.VoiceChannel | None:
    guild = original_vc.guild
    status_category = await get_or_create_status_category(guild)
    if not status_category:
        logger.error(f"STATUSカテゴリの取得/作成に失敗しました ({guild.name} の {original_vc.name} 用)。")
        return None

    user_count = len([m for m in original_vc.members if not m.bot])
    user_count = min(user_count, 999) # Cap user count at 999

    status_channel_name_base = original_vc.name[:70] # Truncate base name to avoid overly long names
    status_channel_name = f"{status_channel_name_base}：{user_count} users"
    status_channel_name = re.sub(r'\s{2,}', ' ', status_channel_name).strip()[:100] # Clean and cap total length

    # Permissions for the status VC (read-only for everyone)
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(
            view_channel=True, read_message_history=True, connect=False, speak=False, stream=False,
            send_messages=False # Deny all other permissions by default
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
    """Retrieves or creates an asyncio.Lock for the given VC ID."""
    if vc_id not in vc_locks:
        vc_locks[vc_id] = asyncio.Lock()
    return vc_locks[vc_id]

# --- Core Tracking and Update Logic ---
async def register_new_vc_for_tracking(original_vc: discord.VoiceChannel, send_feedback_to_ctx=None):
    original_vc_id = original_vc.id
    lock = get_vc_lock(original_vc_id)
    
    logger.debug(f"[register_new_vc] Attempting to acquire lock for VC ID: {original_vc_id}")
    try:
        async with asyncio.wait_for(lock, timeout=LOCK_ACQUIRE_TIMEOUT): # Acquire lock with timeout
            logger.debug(f"[register_new_vc] Lock acquired for VC ID: {original_vc_id}")

            # Check if already effectively tracking
            if original_vc_id in vc_tracking:
                track_info = vc_tracking[original_vc_id]
                guild_id_for_check = track_info.get("guild_id")
                status_id_for_check = track_info.get("status_channel_id")
                if guild_id_for_check and status_id_for_check:
                    guild_for_status_check = bot.get_guild(guild_id_for_check)
                    if guild_for_status_check:
                        status_vc_obj = guild_for_status_check.get_channel(status_id_for_check)
                        if isinstance(status_vc_obj, discord.VoiceChannel) and status_vc_obj.category and STATUS_CATEGORY_NAME.lower() in status_vc_obj.category.name.lower():
                            logger.info(f"VC {original_vc.name} は既に有効に追跡中です。")
                            if send_feedback_to_ctx:
                                await send_feedback_to_ctx.send(f"VC「{original_vc.name}」は既に追跡中ですニャ。")
                            return False 
                logger.info(f"VC {original_vc.name} の追跡情報が無効と判断。クリーンアップして再作成を試みます。")
                await unregister_vc_tracking_internal(original_vc_id, original_vc.guild, is_internal_call=True, acquired_lock=lock) # Pass lock

            logger.info(f"VC {original_vc.name} (ID: {original_vc_id}) の新規追跡処理を開始します。")
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

                await update_dynamic_status_channel_name(original_vc, new_status_vc)
                logger.info(f"追跡開始/再開: Original VC {original_vc.name}, Status VC {new_status_vc.name}")
                return True
            else:
                logger.error(f"{original_vc.name} のステータスVC作成に失敗。追跡は開始されません。")
                if original_vc_id in vc_tracking: del vc_tracking[original_vc_id]
                await remove_tracked_original_from_db(original_vc_id)
                return False
    except asyncio.TimeoutError:
        logger.error(f"[register_new_vc] Timeout acquiring lock for VC ID: {original_vc_id}. Registration skipped.")
        if send_feedback_to_ctx:
            await send_feedback_to_ctx.send(f"VC「{original_vc.name}」の処理が混み合っているようですニャ。少し待ってから試してニャ。")
        return False
    except Exception as e:
        logger.error(f"[register_new_vc] Error during registration for VC {original_vc_id}: {e}", exc_info=True)
        return False
    finally:
        if lock.locked(): # Should be released by 'async with lock' or if acquire timed out
            logger.warning(f"[register_new_vc] Lock for {original_vc_id} was still held in finally. Forcing release.")
            lock.release() # Should not happen with 'async with' if acquire was successful
        logger.debug(f"[register_new_vc] Lock for VC ID: {original_vc_id} should be released now.")


async def unregister_vc_tracking(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None):
    lock = get_vc_lock(original_channel_id)
    logger.debug(f"[unregister_vc] Attempting to acquire lock for VC ID: {original_channel_id}")
    try:
        async with asyncio.wait_for(lock, timeout=LOCK_ACQUIRE_TIMEOUT):
            logger.debug(f"[unregister_vc] Lock acquired for VC ID: {original_channel_id}")
            await unregister_vc_tracking_internal(original_channel_id, guild, send_feedback_to_ctx, is_internal_call=False, acquired_lock=lock)
    except asyncio.TimeoutError:
        logger.error(f"[unregister_vc] Timeout acquiring lock for VC ID: {original_channel_id}. Unregistration skipped.")
        if send_feedback_to_ctx:
             await send_feedback_to_ctx.send(f"VC ID「{original_channel_id}」の処理が混み合っているようですニャ。少し待ってから試してニャ。")
    except Exception as e:
        logger.error(f"[unregister_vc] Error during unregistration for VC {original_channel_id}: {e}", exc_info=True)
    finally:
        if lock.locked():
            logger.warning(f"[unregister_vc] Lock for {original_channel_id} was still held in finally. Forcing release.")
            lock.release()
        logger.debug(f"[unregister_vc] Lock for VC ID: {original_channel_id} should be released now.")


async def unregister_vc_tracking_internal(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None, is_internal_call: bool = False, acquired_lock: asyncio.Lock | None = None):
    # Assumes lock is already acquired if acquired_lock is passed, or needs to be acquired if None.
    # For simplicity, this internal version now expects the lock to be handled by the caller.
    logger.info(f"VC ID {original_channel_id} の追跡解除処理を開始 (内部呼び出し: {is_internal_call})。")
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
                try:
                    await asyncio.wait_for(status_vc.delete(reason="オリジナルVCの追跡停止のため"), timeout=API_CALL_TIMEOUT)
                    logger.info(f"削除成功: Status VC {status_vc.name} (ID: {status_vc.id})")
                except asyncio.TimeoutError:
                    logger.error(f"Status VC {status_vc.id} の削除タイムアウト")
                except discord.NotFound:
                    logger.info(f"Status VC {status_channel_id} は既に削除されていました。")
                except discord.Forbidden:
                    logger.error(f"Status VC {status_vc.name} (ID: {status_vc.id}) の削除に失敗 (権限不足)")
                except Exception as e:
                    logger.error(f"Status VC {status_vc.name} (ID: {status_vc.id}) の削除中にエラー: {e}", exc_info=True)
            # ... (other checks)
    
    vc_rate_limit_windows.pop(original_channel_id, None)
    vc_zero_stats.pop(original_channel_id, None)
    vc_discord_api_cooldown_until.pop(original_channel_id, None)
    
    await remove_tracked_original_from_db(original_channel_id)
    if not is_internal_call and send_feedback_to_ctx:
        # ... (feedback logic)
        display_name = original_vc_name_for_msg # Simplified for brevity
        if guild: # Try to get current name if original VC still exists
             actual_original_vc = guild.get_channel(original_channel_id)
             if actual_original_vc : display_name = actual_original_vc.name

        await send_feedback_to_ctx.send(f"VC「{display_name}」の人数表示用チャンネルを削除し、追跡を停止したニャ。")

async def update_dynamic_status_channel_name(original_vc: discord.VoiceChannel, status_vc: discord.VoiceChannel):
    if not original_vc or not status_vc:
        logger.debug(f"Update_dynamic: スキップ - OriginalVC/StatusVCが無効 {original_vc.id if original_vc else 'N/A'}")
        return

    ovc_id = original_vc.id
    lock = get_vc_lock(ovc_id)
    
    logger.debug(f"[update_dynamic] Attempting to acquire lock for VC ID: {ovc_id}")
    if lock.locked(): # Non-blocking check first
        logger.debug(f"[update_dynamic] Lock for VC ID {ovc_id} is already held by another task. Skipping this update cycle.")
        return

    try:
        async with asyncio.wait_for(lock, timeout=LOCK_ACQUIRE_TIMEOUT): # Acquire lock with timeout
            logger.debug(f"[update_dynamic] Lock acquired for VC ID: {ovc_id}")

            # Refresh channel objects from bot's cache
            current_original_vc = bot.get_channel(ovc_id)
            current_status_vc = bot.get_channel(status_vc.id)

            if not isinstance(current_original_vc, discord.VoiceChannel) or \
               not isinstance(current_status_vc, discord.VoiceChannel):
                logger.warning(f"Update_dynamic: Original VC {ovc_id} or Status VC {status_vc.id} became invalid. Skipping update.")
                return # Lock will be released by 'async with'
            original_vc, status_vc = current_original_vc, current_status_vc

            now = datetime.now(timezone.utc)

            if ovc_id in vc_discord_api_cooldown_until and now < vc_discord_api_cooldown_until[ovc_id]:
                # ... (Discord API cooldown check)
                logger.debug(f"Update_dynamic: Discord API cooldown for {original_vc.name}. Skip.")
                return

            current_members = [member for member in original_vc.members if not member.bot]
            count = len(current_members)
            count = min(count, 999)

            track_info = vc_tracking.get(ovc_id)
            if not track_info:
                logger.warning(f"Update_dynamic: Original VC {original_vc.name} (ID: {ovc_id}) not in tracking info.")
                return
            base_name = track_info.get("original_channel_name", original_vc.name[:70])
            
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
                fresh_status_vc = await asyncio.wait_for(bot.fetch_channel(status_vc.id), timeout=API_CALL_TIMEOUT)
                current_status_vc_name = fresh_status_vc.name
            except asyncio.TimeoutError:
                logger.error(f"Update_dynamic: Timeout fetching status VC name for {status_vc.id}. Skipping.")
                return
            except discord.NotFound:
                logger.error(f"Update_dynamic: Status VC {status_vc.id} for {original_vc.name} not found. Unregistering.")
                # Caller of unregister_vc_tracking_internal must handle its own lock.
                # Since we hold the lock, we can call the _internal version if we ensure it doesn't re-acquire.
                # For now, let periodic task or other events handle full unregistration.
                # A simpler approach is to just log and return, periodic check will eventually unregister.
                return
            except Exception as e_fetch:
                logger.error(f"Update_dynamic: Error fetching status VC name {status_vc.id}: {e_fetch}", exc_info=True)
                return

            final_new_name = re.sub(r'\s{2,}', ' ', desired_name_str).strip()[:100]

            if final_new_name == current_status_vc_name:
                # ... (name already correct)
                if is_special_zero_update_condition and ovc_id in vc_zero_stats:
                    vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True
                return

            window_info = vc_rate_limit_windows.get(ovc_id)
            can_update_by_bot_rule = False
            if not window_info or now >= window_info["window_start_time"] + BOT_UPDATE_WINDOW_DURATION:
                can_update_by_bot_rule = True
            elif window_info["count"] < MAX_UPDATES_IN_WINDOW:
                can_update_by_bot_rule = True
            
            if not can_update_by_bot_rule:
                logger.debug(f"Update_dynamic: Bot rate limit for {original_vc.name}. Skip.")
                return

            logger.info(f"Update_dynamic: Attempting name change for {status_vc.name} to '{final_new_name}'")
            try:
                await asyncio.wait_for(
                    status_vc.edit(name=final_new_name, reason="VC参加人数更新 / 0人ポリシー"),
                    timeout=API_CALL_TIMEOUT
                )
                logger.info(f"Update_dynamic: SUCCESS name change for {status_vc.name} to '{final_new_name}'")
                # ... (update rate limit window)
                current_window_data = vc_rate_limit_windows.get(ovc_id)
                if not current_window_data or now >= current_window_data["window_start_time"] + BOT_UPDATE_WINDOW_DURATION:
                    vc_rate_limit_windows[ovc_id] = {"window_start_time": now, "count": 1}
                else:
                    current_window_data["count"] += 1
                if is_special_zero_update_condition and ovc_id in vc_zero_stats:
                    vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True
                if ovc_id in vc_discord_api_cooldown_until: del vc_discord_api_cooldown_until[ovc_id]
            except asyncio.TimeoutError:
                logger.error(f"Update_dynamic: Timeout editing status VC name for {status_vc.name} (ID: {status_vc.id}).")
            except discord.HTTPException as e_http:
                if e_http.status == 429:
                    retry_after = e_http.retry_after if e_http.retry_after is not None else 60.0
                    vc_discord_api_cooldown_until[ovc_id] = now + timedelta(seconds=retry_after)
                    logger.warning(f"Update_dynamic: Discord API rate limit (429) for {status_vc.name}. Cooldown: {retry_after}s")
                else:
                    logger.error(f"Update_dynamic: HTTP error {e_http.status} editing {status_vc.name}: {e_http.text}", exc_info=True)
            except Exception as e_edit:
                logger.error(f"Update_dynamic: Unexpected error editing {status_vc.name}: {e_edit}", exc_info=True)
    
    except asyncio.TimeoutError:
        logger.error(f"[update_dynamic] Timeout acquiring lock for VC ID: {ovc_id}. Update skipped.")
    except Exception as e_outer_update:
        logger.error(f"[update_dynamic] Outer error for VC {ovc_id}: {e_outer_update}", exc_info=True)
    finally:
        if lock.locked(): # Should be released by 'async with lock' or if acquire timed out
            logger.warning(f"[update_dynamic] Lock for {ovc_id} was still held in finally. Forcing release.")
            lock.release()
        logger.debug(f"[update_dynamic] Lock for VC ID: {ovc_id} should be released now.")


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
        logger.error(f"アクティビティの設定中にエラー: {e}")
    
    vc_discord_api_cooldown_until.clear()

    if await init_firestore():
        await load_tracked_channels_from_db()
    else:
        logger.warning("Firestoreが利用できないため、VC追跡の永続化は無効です。")

    logger.info("起動時の追跡VC状態整合性チェックと更新を開始します...")
    
    tracked_ids_to_process = list(vc_tracking.keys())
    
    for original_cid in tracked_ids_to_process:
        logger.info(f"[on_ready] Processing VC ID: {original_cid}")
        lock = get_vc_lock(original_cid)
        
        logger.debug(f"[on_ready] Attempting to acquire lock for VC ID: {original_cid}")
        try:
            async with asyncio.wait_for(lock, timeout=LOCK_ACQUIRE_TIMEOUT * 2): # Longer timeout for on_ready processing
                logger.debug(f"[on_ready] Lock acquired for VC ID: {original_cid}")
                if original_cid not in vc_tracking:
                    logger.info(f"[on_ready] VC {original_cid} no longer in tracking after lock acquisition. Skipping.")
                    continue # Lock released by 'async with'

                track_info = vc_tracking[original_cid]
                guild = bot.get_guild(track_info["guild_id"])

                if not guild:
                    logger.warning(f"[on_ready] Guild {track_info['guild_id']} (Original VC {original_cid}) が見つかりません。追跡を解除します。")
                    await unregister_vc_tracking_internal(original_cid, None, is_internal_call=True)
                    continue

                original_vc = guild.get_channel(original_cid)
                if not isinstance(original_vc, discord.VoiceChannel):
                    logger.warning(f"[on_ready] Original VC {original_cid} (Guild {guild.name}) が見つからないかVCではありません。追跡を解除します。")
                    await unregister_vc_tracking_internal(original_cid, guild, is_internal_call=True)
                    continue

                status_vc_id = track_info.get("status_channel_id")
                status_vc = guild.get_channel(status_vc_id) if status_vc_id else None
                
                vc_rate_limit_windows.pop(original_cid, None) # Reset states on ready
                vc_zero_stats.pop(original_cid, None)
                # vc_discord_api_cooldown_until cleared globally

                if isinstance(status_vc, discord.VoiceChannel) and status_vc.category and STATUS_CATEGORY_NAME.lower() in status_vc.category.name.lower():
                    logger.info(f"[on_ready] Original VC {original_vc.name} の既存Status VC {status_vc.name} は有効です。名前を更新します。")
                    await update_dynamic_status_channel_name(original_vc, status_vc)
                else: 
                    if status_vc:
                        logger.warning(f"[on_ready] Status VC {status_vc.id if status_vc else 'N/A'} ({original_vc.name}用) が無効か移動。削除して再作成試行。")
                        try:
                            await asyncio.wait_for(status_vc.delete(reason="無効なステータスVCのため再作成"), timeout=API_CALL_TIMEOUT)
                        except Exception as e_del_ready: logger.error(f"[on_ready] 無効なステータスVC {status_vc.id if status_vc else 'N/A'} の削除エラー: {e_del_ready}")
                    
                    logger.info(f"[on_ready] {original_vc.name} のステータスVCが存在しないか無効。新規作成試行。")
                    # Temporarily remove from memory to allow recreation by register_new_vc_for_tracking logic
                    temp_track_info = vc_tracking.pop(original_cid, None)
                    await remove_tracked_original_from_db(original_cid) # Remove from DB first

                    # Re-register which will create a new one. register_new_vc_for_tracking handles its own lock.
                    # This part is tricky because register_new_vc_for_tracking tries to acquire the same lock.
                    # The lock is released when this 'async with' block exits.
                    # A better way might be to directly call the creation and saving logic here.
                    # For now, we assume the lock will be released before register_new_vc_for_tracking is effectively called by a subsequent event or periodic task if not immediately.
                    # OR: Release the lock before calling it, but that makes the flow complex.
                    # Simplified: Re-create directly.
                    new_status_vc_obj = await _create_status_vc_for_original(original_vc)
                    if new_status_vc_obj:
                        vc_tracking[original_cid] = { # Re-add to tracking
                            "guild_id": guild.id,
                            "status_channel_id": new_status_vc_obj.id,
                            "original_channel_name": original_vc.name
                        }
                        await save_tracked_original_to_db(original_cid, guild.id, new_status_vc_obj.id, original_vc.name)
                        logger.info(f"[on_ready] {original_vc.name} のステータスVCを再作成: {new_status_vc_obj.name}")
                        await update_dynamic_status_channel_name(original_vc, new_status_vc_obj)
                    else:
                        logger.error(f"[on_ready] {original_vc.name} のステータスVC再作成失敗。")
                        if temp_track_info : vc_tracking[original_cid] = temp_track_info # Rollback pop if failed
                        # It remains untracked or in a faulty state in DB if save failed before.

        except asyncio.TimeoutError:
            logger.error(f"[on_ready] Timeout acquiring lock for VC ID: {original_cid} during on_ready processing. Skipping this VC.")
        except Exception as e_onready_vc:
            logger.error(f"[on_ready] Error processing VC {original_cid}: {e_onready_vc}", exc_info=True)
        finally:
            if lock.locked():
                logger.warning(f"[on_ready] Lock for VC {original_cid} was still held in finally. Forcing release.")
                lock.release()
            logger.debug(f"[on_ready] Lock for VC ID: {original_cid} should be released now.")
            
    logger.info("起動時の追跡VC状態整合性チェックと更新が完了しました。")
    if not periodic_status_update.is_running():
        periodic_status_update.start()
        logger.info("定期ステータス更新タスクを開始しました。")

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return

    channels_to_check_ids = set()
    if before.channel: channels_to_check_ids.add(before.channel.id)
    if after.channel: channels_to_check_ids.add(after.channel.id)
    
    for original_cid in channels_to_check_ids:
        if original_cid in vc_tracking:
            logger.debug(f"[on_voice_state_update] Relevant update for tracked VC ID: {original_cid}")
            track_info = vc_tracking.get(original_cid) 
            if not track_info: continue 

            guild = bot.get_guild(track_info["guild_id"])
            if not guild: continue

            original_vc = guild.get_channel(original_cid)
            status_vc_id = track_info.get("status_channel_id")
            status_vc = guild.get_channel(status_vc_id) if status_vc_id else None

            if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
                # update_dynamic_status_channel_name handles its own lock and logging for skipping if locked
                asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc)) # Schedule as task
            else:
                logger.debug(f"[on_voice_state_update] Original or Status VC invalid for {original_cid}. Original: {original_vc}, Status: {status_vc}")


@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): return
    if channel.category and STATUS_CATEGORY_NAME.lower() in channel.category.name.lower(): return # Ignore status VCs
    if channel.id in vc_tracking or any(info.get("status_channel_id") == channel.id for info in vc_tracking.values()): return

    logger.info(f"[on_guild_channel_create] New VC 「{channel.name}」 (ID: {channel.id}) 作成。自動追跡試行。")
    asyncio.create_task(register_new_vc_for_tracking(channel)) # Schedule as task


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): return

    original_channel_id_to_process = None
    is_status_vc_deleted = False

    if channel.id in vc_tracking: # Deleted channel was an original tracked VC
        original_channel_id_to_process = channel.id
        logger.info(f"[on_guild_channel_delete] Tracked original VC {channel.name} (ID: {channel.id}) deleted.")
    else: # Check if it was a status VC
        for ocid, info in list(vc_tracking.items()):
            if info.get("status_channel_id") == channel.id:
                original_channel_id_to_process = ocid
                is_status_vc_deleted = True
                logger.info(f"[on_guild_channel_delete] Status VC {channel.name} (for original ID: {ocid}) deleted.")
                break
    
    if original_channel_id_to_process:
        logger.info(f"[on_guild_channel_delete] Processing deletion related to original VC ID: {original_channel_id_to_process}")
        lock = get_vc_lock(original_channel_id_to_process) # Ensure lock is acquired for this operation
        # This event needs to be careful if it calls register_new_vc_for_tracking or unregister_vc_tracking
        # as they also try to acquire the same lock.
        # We can create a task for the handler logic.
        async def handle_deletion_logic():
            logger.debug(f"[handle_deletion_logic] Attempting lock for {original_channel_id_to_process}")
            try:
                async with asyncio.wait_for(lock, timeout=LOCK_ACQUIRE_TIMEOUT * 2): # Longer timeout for complex logic
                    logger.debug(f"[handle_deletion_logic] Lock acquired for {original_channel_id_to_process}")
                    if is_status_vc_deleted:
                        original_vc = channel.guild.get_channel(original_channel_id_to_process)
                        if original_vc and isinstance(original_vc, discord.VoiceChannel):
                            logger.info(f"Original VC {original_vc.name} still exists. Attempting to recreate status VC.")
                            # Clean up before recreate: remove from memory tracking and DB
                            if original_channel_id_to_process in vc_tracking: del vc_tracking[original_channel_id_to_process]
                            await remove_tracked_original_from_db(original_channel_id_to_process)
                            
                            # Now call the actual creation logic (similar to on_ready)
                            new_status_vc = await _create_status_vc_for_original(original_vc)
                            if new_status_vc:
                                vc_tracking[original_channel_id_to_process] = {
                                    "guild_id": original_vc.guild.id,
                                    "status_channel_id": new_status_vc.id,
                                    "original_channel_name": original_vc.name
                                }
                                await save_tracked_original_to_db(original_channel_id_to_process, original_vc.guild.id, new_status_vc.id, original_vc.name)
                                await update_dynamic_status_channel_name(original_vc, new_status_vc) # Update name
                                logger.info(f"Status VC for {original_vc.name} recreated: {new_status_vc.name}")
                            else:
                                logger.error(f"Failed to recreate status VC for {original_vc.name}.")
                        else:
                            logger.info(f"Original VC {original_channel_id_to_process} not found after status VC deletion. Unregistering fully.")
                            await unregister_vc_tracking_internal(original_channel_id_to_process, channel.guild, is_internal_call=True)
                    else: # Original VC was deleted
                        await unregister_vc_tracking_internal(original_channel_id_to_process, channel.guild, is_internal_call=True)
            except asyncio.TimeoutError:
                logger.error(f"[handle_deletion_logic] Timeout acquiring lock for {original_channel_id_to_process}. Deletion processing may be incomplete.")
            except Exception as e_del_handler:
                logger.error(f"[handle_deletion_logic] Error for {original_channel_id_to_process}: {e_del_handler}", exc_info=True)
            finally:
                if lock.locked():
                    logger.warning(f"[handle_deletion_logic] Lock for {original_channel_id_to_process} was still held in finally. Forcing release.")
                    lock.release()
                logger.debug(f"[handle_deletion_logic] Lock for {original_channel_id_to_process} should be released.")

        if original_channel_id_to_process:
            asyncio.create_task(handle_deletion_logic())


# --- Periodic Task ---
@tasks.loop(minutes=3) # Adjusted to 3 minutes for potentially faster recovery
async def periodic_status_update():
    logger.debug("定期ステータス更新タスク実行中...")
    if not vc_tracking: return

    for original_cid in list(vc_tracking.keys()):
        logger.debug(f"[periodic_update] Processing VC ID: {original_cid}")
        track_info = vc_tracking.get(original_cid)
        if not track_info:
            logger.warning(f"[periodic_update] VC {original_cid} not in tracking after starting loop. Skipping.")
            continue

        guild = bot.get_guild(track_info["guild_id"])
        if not guild:
            logger.warning(f"[periodic_update] Guild {track_info['guild_id']} (Original VC {original_cid}) not found. Unregistering.")
            asyncio.create_task(unregister_vc_tracking(original_cid, None)) # unregister_vc_tracking handles its own lock
            continue
        
        original_vc = guild.get_channel(original_cid)
        status_vc_id = track_info.get("status_channel_id")
        status_vc = guild.get_channel(status_vc_id) if status_vc_id else None

        if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
            if status_vc.category is None or STATUS_CATEGORY_NAME.lower() not in status_vc.category.name.lower():
                logger.warning(f"[periodic_update] Status VC {status_vc.name} for {original_vc.name} in wrong category. Attempting fix.")
                # To fix, we essentially need to re-register. Create a task for this.
                async def fix_category_task():
                    lock = get_vc_lock(original_vc.id)
                    logger.debug(f"[fix_category_task] Attempting lock for {original_vc.id}")
                    try:
                        async with asyncio.wait_for(lock, timeout=LOCK_ACQUIRE_TIMEOUT * 2):
                            logger.debug(f"[fix_category_task] Lock acquired for {original_vc.id}")
                            await unregister_vc_tracking_internal(original_vc.id, guild, is_internal_call=True)
                            # The lock is released when 'async with' exits.
                        # register_new_vc_for_tracking will acquire its own lock.
                        logger.info(f"[fix_category_task] Re-registering {original_vc.name} to fix category.")
                        await register_new_vc_for_tracking(original_vc)
                    except asyncio.TimeoutError:
                        logger.error(f"[fix_category_task] Timeout acquiring lock for {original_vc.id} during category fix.")
                    except Exception as e_fix_cat:
                        logger.error(f"[fix_category_task] Error fixing category for {original_vc.id}: {e_fix_cat}", exc_info=True)
                    finally: # Redundant if async with is used correctly, but for safety
                        if lock.locked(): lock.release()
                asyncio.create_task(fix_category_task())
                continue # Skip normal update for this cycle

            asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc))
        
        elif not isinstance(original_vc, discord.VoiceChannel) and original_cid in vc_tracking: 
            logger.warning(f"[periodic_update] Original VC {original_cid} ('{track_info.get('original_channel_name', 'N/A')}') invalid. Unregistering.")
            asyncio.create_task(unregister_vc_tracking(original_cid, guild))

        elif isinstance(original_vc, discord.VoiceChannel) and not isinstance(status_vc, discord.VoiceChannel):
            logger.warning(f"[periodic_update] Status VC for {original_vc.name} (ID: {original_cid}) missing/invalid. Recreating.")
            # This requires careful lock handling if calling register_new_vc_for_tracking
            async def recreate_status_vc_task():
                lock = get_vc_lock(original_vc.id)
                logger.debug(f"[recreate_status_vc_task] Attempting lock for {original_vc.id}")
                try:
                    async with asyncio.wait_for(lock, timeout=LOCK_ACQUIRE_TIMEOUT * 2):
                        logger.debug(f"[recreate_status_vc_task] Lock acquired for {original_vc.id}")
                        await unregister_vc_tracking_internal(original_vc.id, guild, is_internal_call=True) # Clean up old status attempts
                    # Lock released, now register can acquire it.
                    logger.info(f"[recreate_status_vc_task] Re-registering {original_vc.name} to recreate status VC.")
                    await register_new_vc_for_tracking(original_vc)
                except asyncio.TimeoutError:
                    logger.error(f"[recreate_status_vc_task] Timeout acquiring lock for {original_vc.id} during status VC recreate.")
                except Exception as e_recreate:
                     logger.error(f"[recreate_status_vc_task] Error recreating status VC for {original_vc.id}: {e_recreate}", exc_info=True)
                finally: # Redundant if async with is used correctly
                    if lock.locked(): lock.release()
            asyncio.create_task(recreate_status_vc_task())
        
        elif original_cid in vc_tracking: # Both invalid, or some other inconsistent state
            logger.warning(f"[periodic_update] Generic invalid state for Original VC {original_cid} ('{track_info.get('original_channel_name', 'N/A')}'). Unregistering.")
            asyncio.create_task(unregister_vc_tracking(original_cid, guild))

# --- Bot Commands ---
@bot.command(name='nah', help="指定した数のメッセージを削除するニャ。 例: !!nah 5")
@commands.has_permissions(manage_messages=True)
@commands.bot_has_permissions(manage_messages=True)
async def nah_command(ctx, num: int):
    if num <= 0:
        await ctx.send("1以上の数を指定してニャ🐈")
        return
    if num > 100: # Prevent deleting too many messages at once
        await ctx.send("一度に削除できるのは100件までニャ🐈")
        return
    try:
        deleted_messages = await ctx.channel.purge(limit=num + 1) # +1 for the command message
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
@commands.has_permissions(manage_channels=True) # User needs manage_channels
@commands.bot_has_permissions(manage_channels=True) # Bot needs manage_channels
async def nah_vc_command(ctx, *, channel_id_or_name: str):
    guild = ctx.guild
    if not guild:
        await ctx.send("このコマンドはサーバー内でのみ使用可能ですニャ🐈")
        return

    target_vc = None
    try: # Try to find by ID first
        vc_id = int(channel_id_or_name)
        target_vc = guild.get_channel(vc_id)
    except ValueError: # Not an ID, try by name
        for vc in guild.voice_channels: # Exact match
            if vc.name.lower() == channel_id_or_name.lower():
                target_vc = vc; break
        if not target_vc: # Partial match
            for vc in guild.voice_channels:
                if channel_id_or_name.lower() in vc.name.lower():
                    target_vc = vc; logger.info(f"VC名「{channel_id_or_name}」の部分一致で「{vc.name}」を使用。"); break
    
    if not target_vc or not isinstance(target_vc, discord.VoiceChannel):
        await ctx.send(f"指定された「{channel_id_or_name}」はボイスチャンネルとして見つからなかったニャ😿")
        return

    if target_vc.category and STATUS_CATEGORY_NAME.lower() in target_vc.category.name.lower():
        await ctx.send(f"VC「{target_vc.name}」はSTATUSチャンネルのようだニャ。元のVCを指定してニャ。")
        return

    # register_new_vc_for_tracking and unregister_vc_tracking handle their own locks and feedback.
    if target_vc.id in vc_tracking:
        logger.info(f"コマンド: VC「{target_vc.name}」の追跡解除を試みます。")
        await unregister_vc_tracking(target_vc.id, guild, send_feedback_to_ctx=ctx)
    else:
        logger.info(f"コマンド: VC「{target_vc.name}」の新規追跡を試みます。")
        success = await register_new_vc_for_tracking(target_vc, send_feedback_to_ctx=ctx)
        # Feedback for new registration is tricky if register_new_vc_for_tracking already sent "already tracking"
        # Simple approach: only send generic success if no "already tracking" was sent.
        if success:
            # Check if "already tracking" was sent by the sub-function
            history = [msg async for msg in ctx.channel.history(limit=1, after=ctx.message)] # Check message after command
            already_tracking_msg_found = False
            if history and history[0].author == bot.user and "既に追跡中ですニャ" in history[0].content:
                already_tracking_msg_found = True
            
            if not already_tracking_msg_found:
                 await ctx.send(f"VC「{target_vc.name}」の人数表示用チャンネルを作成し、追跡を開始するニャ！🐈")
        # else: # Failure, register_new_vc_for_tracking might have sent specific feedback or logged error
            # if not lock.locked() check is not reliable here as lock is internal to register_new_vc_for_tracking
            # await ctx.send(f"VC「{target_vc.name}」の追跡設定に失敗したニャ😿（詳細はログを確認してニャ）")
                            
@nah_vc_command.error
async def nah_vc_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("このコマンドを実行する権限がニャいみたいだニャ… (チャンネル管理権限が必要だニャ)")
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send("ボットにチャンネルを管理する権限がないニャ😿 (ボットの権限を確認してニャ)")
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
    
    # Start keep-alive server for hosting platforms like Render
    if os.getenv("RENDER"): # Simple check if running on Render
        keep_alive()

    try:
        logger.info("Botの非同期処理を開始します...")
        await bot.start(DISCORD_TOKEN)
    except discord.LoginFailure:
        logger.critical("Discordへのログインに失敗しました。トークンが正しいか確認してください。")
    except Exception as e:
        logger.critical(f"Botの起動中または実行中に予期せぬエラーが発生しました: {e}", exc_info=True)
    finally:
        if not bot.is_closed():
            logger.info("Botをシャットダウンします...")
            await bot.close()
        logger.info("Botがシャットダウンしました。")

# --- Entry Point ---
if __name__ == "__main__":
    # Ensure .env is loaded if not using a platform that sets env vars
    # load_dotenv() # Already at the top

    # For Render or similar platforms, they might inject PORT.
    # The keep_alive() function handles this.

    try:
        asyncio.run(start_bot_main())
    except KeyboardInterrupt:
        logger.info("ユーザーによりBotが停止されました (KeyboardInterrupt)。")
    except Exception as e: # Catch-all for asyncio.run() or unhandled exceptions in start_bot_main
        logger.critical(f"メインの実行ループで予期せぬエラーが発生しました: {e}", exc_info=True)

