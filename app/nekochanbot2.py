# Ensure sys is imported early for print diagnostics if logging fails
import sys
import os # os is needed for getenv

# --- VERY EARLY LOGGING CONFIGURATION ---
def setup_logging():
    """
    Configures logging to ensure DEBUG messages are captured, especially for Render.
    Reads LOG_LEVEL from environment variable.
    Returns a logger instance for the application to use.
    """
    print("[SETUP_LOGGING_DIAG] Attempting to configure logging...", file=sys.stdout)
    
    try:
        import logging # Import logging module HERE

        # --- Read log level from environment variable ---
        default_log_level_str = "DEBUG" 
        log_level_env_str = os.getenv("LOG_LEVEL", default_log_level_str).upper()
        
        log_level_numeric = getattr(logging, log_level_env_str, None)
        if not isinstance(log_level_numeric, int):
            print(f"[SETUP_LOGGING_WARNING] Invalid LOG_LEVEL '{log_level_env_str}' from environment. Defaulting to {default_log_level_str}.", file=sys.stderr)
            log_level_numeric = logging.DEBUG # Fallback to DEBUG
            log_level_str_for_print = default_log_level_str
        else:
            log_level_str_for_print = log_level_env_str
            print(f"[SETUP_LOGGING_DIAG] LOG_LEVEL from environment: {log_level_str_for_print} (Numeric: {log_level_numeric})", file=sys.stdout)

        log_format = "%(asctime)s %(levelname)-8s %(name)-22s %(module)s:%(lineno)d - %(message)s" # Wider name field
        date_format = '%Y-%m-%d %H:%M:%S'
        
        root_logger = logging.getLogger() 
        
        if root_logger.hasHandlers():
            print(f"[SETUP_LOGGING_DIAG] Root logger has {len(root_logger.handlers)} existing handlers. Clearing them.", file=sys.stdout)
            for handler in list(root_logger.handlers): 
                try:
                    root_logger.removeHandler(handler)
                    # Avoid closing stdout/stderr directly if a handler was using it.
                    # handler.close() # Generally safe, but can be tricky with platform loggers
                    print(f"[SETUP_LOGGING_DIAG] Removed handler: {handler}", file=sys.stdout)
                except Exception as e_handler_remove:
                    print(f"[SETUP_LOGGING_DIAG] Error removing handler {handler}: {e_handler_remove}", file=sys.stderr)
        else:
            print("[SETUP_LOGGING_DIAG] Root logger has no existing handlers.", file=sys.stdout)

        root_logger.setLevel(log_level_numeric) # Set root logger level from env or default
        print(f"[SETUP_LOGGING_DIAG] Root logger level set to: {logging.getLevelName(root_logger.level)} ({root_logger.level})", file=sys.stdout)

        console_handler = logging.StreamHandler(sys.stdout) 
        console_handler.setLevel(log_level_numeric) # Handler must also be at the desired level or lower
        
        formatter = logging.Formatter(log_format, datefmt=date_format)
        console_handler.setFormatter(formatter)
        
        root_logger.addHandler(console_handler)
        print(f"[SETUP_LOGGING_DIAG] Added new StreamHandler to root logger. Handler level: {logging.getLevelName(console_handler.level)}", file=sys.stdout)

        app_logger = logging.getLogger(__name__) 
        # app_logger level will be effectively controlled by the root_logger's level and its own (if set lower).
        # No need to set app_logger.setLevel(log_level_numeric) here if we want it to simply inherit.
        
        app_logger.info( # This should always appear if INFO >= configured log_level_numeric
            "Application logger ('%s') initialized. Configured LOG_LEVEL: %s. Effective level for this logger: %s.",
            app_logger.name,
            log_level_str_for_print, # Display the level name string used
            logging.getLevelName(app_logger.getEffectiveLevel())
        )
        # Test DEBUG message
        app_logger.debug("これはアプリケーションロガーからのテストDEBUGメッセージです。LOG_LEVELがDEBUGの場合に表示されます。")
        
        if not app_logger.isEnabledFor(logging.DEBUG) and log_level_numeric <= logging.DEBUG:
             print(f"[SETUP_LOGGING_WARNING] Application logger '{app_logger.name}' is NOT enabled for DEBUG, but configured level was DEBUG or lower! Effective level: {logging.getLevelName(app_logger.getEffectiveLevel())}", file=sys.stderr)
        elif app_logger.isEnabledFor(logging.DEBUG):
             print(f"[SETUP_LOGGING_DIAG] Application logger '{app_logger.name}' IS enabled for DEBUG.", file=sys.stdout)


        print("[SETUP_LOGGING_DIAG] Logging setup function complete.", file=sys.stdout)
        return app_logger

    except Exception as e_logging_setup:
        print(f"[SETUP_LOGGING_CRITICAL_ERROR] ロギング設定中に重大なエラーが発生しました: {e_logging_setup}", file=sys.stderr)
        try:
            import logging as fallback_logging_module 
            fb_logger = fallback_logging_module.getLogger("fallback_logger")
            fb_handler = fallback_logging_module.StreamHandler(sys.stderr)
            fb_formatter = fallback_logging_module.Formatter('%(asctime)s %(levelname)s (FALLBACK): %(message)s')
            fb_handler.setFormatter(fb_formatter)
            if not fb_logger.handlers: 
                fb_logger.addHandler(fb_handler)
            fb_logger.setLevel(fallback_logging_module.INFO)
            fb_logger.error(f"Fallback logger activated due to setup error: {e_logging_setup}")
            return fb_logger
        except Exception as e_fallback_setup:
            print(f"[SETUP_LOGGING_CRITICAL_ERROR] Fallback logger setup also failed: {e_fallback_setup}", file=sys.stderr)
            return None

logger = setup_logging() 
if logger is None:
    print("CRITICAL: Logging could not be initialized AT ALL. Subsequent log messages will be lost.", file=sys.stderr)
# --- END OF VERY EARLY LOGGING CONFIGURATION ---


from dotenv import load_dotenv
load_dotenv() 

import discord
from discord.ext import commands, tasks
import re 
from google.cloud import firestore
from google.auth.exceptions import DefaultCredentialsError
from datetime import datetime, timedelta, timezone
import asyncio

from flask import Flask
from threading import Thread

if logger: logger.info(f"dotenv loaded. RENDER env var: {os.getenv('RENDER')}")


# --- Flask App for Keep Alive (Render health checks) ---
app = Flask('')
@app.route('/')
def home():
    if logger: logger.debug("Flask / endpoint called (Keep-alive)") 
    return "I'm alive" 

def run_flask():
    port = int(os.environ.get('PORT', 8080)) 
    if logger: logger.info(f"Flaskサーバーを host=0.0.0.0, port={port} で起動します。")
    
    werkzeug_logger = logging.getLogger('werkzeug')
    if werkzeug_logger: werkzeug_logger.setLevel(logging.WARNING) 

    app.run(host='0.0.0.0', port=port, debug=False) 

def keep_alive():
    flask_thread = Thread(target=run_flask, name="FlaskKeepAliveThread")
    flask_thread.daemon = True 
    flask_thread.start()
    if logger: logger.info("Keep-aliveスレッドを開始しました。")

# --- Discord.py logging (optional, control after our setup) ---
if logger: logger.info("Setting discord.py log levels...")
# Set levels for discord.py's own loggers
# These will also be affected by the root logger's level if it's higher (more restrictive)
logging.getLogger('discord').setLevel(logging.INFO) 
logging.getLogger('discord.http').setLevel(logging.WARNING) 
logging.getLogger('discord.gateway').setLevel(logging.INFO) 
if logger: logger.info("Discord.py log levels set.")


# ...(rest of the bot code from V6, including Firestore, VC logic, commands, etc. remains the same)...
# --- Bot Intents Configuration ---
intents = discord.Intents.default() 
intents.guilds = True
intents.voice_states = True 
intents.message_content = True 

# --- Firestore Client and Constants ---
db = None 
FIRESTORE_COLLECTION_NAME = "discord_tracked_original_vcs_prod_v4" 
STATUS_CATEGORY_NAME = "STATUS" 

# --- VC Tracking Dictionaries and Locks ---
vc_tracking = {} 
vc_locks = {}    

# --- Cooldown and State Settings for VC Name Updates ---
BOT_UPDATE_WINDOW_DURATION = timedelta(minutes=5)  
MAX_UPDATES_IN_WINDOW = 2                       
API_CALL_TIMEOUT = 15.0 
DB_CALL_TIMEOUT = 10.0  
LOCK_ACQUIRE_TIMEOUT = 12.0 

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
            if logger: logger.info("スラッシュコマンドを同期しました。")
        except Exception as e:
            if logger: logger.error(f"スラッシュコマンドの同期中にエラー: {e}", exc_info=True)

# --- Bot Instance ---
bot = MyBot(command_prefix='!!', intents=intents)
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# --- Firestore Helper Functions ---
async def init_firestore():
    global db
    try:
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            db = firestore.AsyncClient()
            await asyncio.wait_for(db.collection(FIRESTORE_COLLECTION_NAME).limit(1).get(), timeout=DB_CALL_TIMEOUT)
            if logger: logger.info("Firestoreクライアントの初期化に成功しました。")
            return True
        else:
            if logger: logger.warning("環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されていません。Firestoreは使用できません。")
            db = None
            return False
    except DefaultCredentialsError:
        if logger: logger.error("Firestoreの認証に失敗しました。GOOGLE_APPLICATION_CREDENTIALSを確認してください。")
        db = None
        return False
    except asyncio.TimeoutError:
        if logger: logger.error("Firestoreクライアントの初期化テスト中にタイムアウトしました。")
        db = None
        return False
    except Exception as e:
        if logger: logger.error(f"Firestoreクライアントの初期化中に予期せぬエラーが発生しました: {e}", exc_info=True)
        db = None
        return False

async def load_tracked_channels_from_db():
    if not db:
        if logger: logger.info("Firestoreが無効なため、データベースからの読み込みをスキップします。")
        return

    global vc_tracking
    vc_tracking = {} 
    try:
        if logger: logger.info(f"Firestoreから追跡VC情報を読み込んでいます (コレクション: {FIRESTORE_COLLECTION_NAME})...")
        stream = db.collection(FIRESTORE_COLLECTION_NAME).stream()
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

                if not all([guild_id, status_channel_id, original_channel_name is not None]): 
                    if logger: logger.warning(f"DB内のドキュメント {doc_snapshot.id} に必要な情報が不足しているか型が不正です。スキップ。 Data: {doc_data}")
                    continue

                vc_tracking[original_channel_id] = {
                    "guild_id": guild_id,
                    "status_channel_id": status_channel_id,
                    "original_channel_name": original_channel_name
                }
                docs_loaded_count += 1
                if logger: logger.debug(f"DBからロード: Original VC ID {original_channel_id}, Status VC ID: {status_channel_id}")
            except (ValueError, TypeError) as e_parse:
                if logger: logger.warning(f"DB内のドキュメント {doc_snapshot.id} のデータ型解析エラー: {e_parse}。スキップ。 Data: {doc_data}")
        if logger: logger.info(f"{docs_loaded_count}件の追跡VC情報をDBからロードしました。")
    except Exception as e: 
        if logger: logger.error(f"Firestoreからのデータ読み込み中にエラー: {e}", exc_info=True)


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
        if logger: logger.debug(f"DBに保存: Original VC ID {original_channel_id}, Status VC ID {status_channel_id}")
    except asyncio.TimeoutError:
        if logger: logger.error(f"Firestoreへのデータ書き込みタイムアウト (Original VC ID: {original_channel_id})")
    except Exception as e:
        if logger: logger.error(f"Firestoreへのデータ書き込み中にエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def remove_tracked_original_from_db(original_channel_id: int):
    if not db: return
    try:
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await asyncio.wait_for(doc_ref.delete(), timeout=DB_CALL_TIMEOUT)
        if logger: logger.info(f"DBから削除: Original VC ID {original_channel_id}")
    except asyncio.TimeoutError:
        if logger: logger.error(f"Firestoreからのデータ削除タイムアウト (Original VC ID: {original_channel_id})")
    except Exception as e:
        if logger: logger.error(f"Firestoreからのデータ削除中にエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def get_or_create_status_category(guild: discord.Guild) -> discord.CategoryChannel | None:
    for category in guild.categories:
        if STATUS_CATEGORY_NAME.lower() in category.name.lower():
            if logger: logger.info(f"カテゴリ「{category.name}」をSTATUSカテゴリとして使用します。(Guild: {guild.name})")
            return category
    try:
        if logger: logger.info(f"「STATUS」を含むカテゴリが見つからなかったため、新規作成します。(Guild: {guild.name})")
        overwrites = { 
            guild.default_role: discord.PermissionOverwrite(read_message_history=True, view_channel=True, connect=False)
        }
        new_category = await guild.create_category(STATUS_CATEGORY_NAME, overwrites=overwrites, reason="VCステータス表示用カテゴリ")
        if logger: logger.info(f"カテゴリ「{STATUS_CATEGORY_NAME}」を新規作成しました。(Guild: {guild.name})")
        return new_category
    except discord.Forbidden:
        if logger: logger.error(f"カテゴリ「{STATUS_CATEGORY_NAME}」の作成に失敗しました (権限不足) (Guild: {guild.name})")
    except Exception as e: 
        if logger: logger.error(f"カテゴリ「{STATUS_CATEGORY_NAME}」の作成中にエラー (Guild: {guild.name}): {e}", exc_info=True)
    return None

async def _create_status_vc_for_original(original_vc: discord.VoiceChannel) -> discord.VoiceChannel | None:
    guild = original_vc.guild
    status_category = await get_or_create_status_category(guild) 
    if not status_category:
        if logger: logger.error(f"STATUSカテゴリの取得/作成に失敗しました ({guild.name} の {original_vc.name} 用)。")
        return None

    user_count = len([m for m in original_vc.members if not m.bot])
    user_count = min(user_count, 999) 

    status_channel_name_base = original_vc.name[:65] 
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
        if logger: logger.info(f"作成成功: Status VC「{new_status_vc.name}」(ID: {new_status_vc.id}) (Original VC: {original_vc.name})")
        return new_status_vc
    except asyncio.TimeoutError:
        if logger: logger.error(f"Status VCの作成タイムアウト ({original_vc.name}, Guild: {guild.name})")
    except discord.Forbidden:
        if logger: logger.error(f"Status VCの作成に失敗 (権限不足) ({original_vc.name}, Guild: {guild.name})")
    except Exception as e:
        if logger: logger.error(f"Status VCの作成に失敗 ({original_vc.name}): {e}", exc_info=True)
    return None

def get_vc_lock(vc_id: int) -> asyncio.Lock:
    if vc_id not in vc_locks:
        vc_locks[vc_id] = asyncio.Lock()
        if logger: logger.debug(f"VC ID {vc_id} のために新しいLockオブジェクトを作成しました。")
    return vc_locks[vc_id]

async def register_new_vc_for_tracking(original_vc: discord.VoiceChannel, send_feedback_to_ctx=None):
    original_vc_id = original_vc.id
    lock = get_vc_lock(original_vc_id)
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "RegisterTask"
    
    if logger: logger.debug(f"[{task_name}|register_new_vc] Attempting to acquire lock for VC ID: {original_vc_id}")
    acquired = False
    try:
        await asyncio.wait_for(lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT)
        acquired = True
        if logger: logger.debug(f"[{task_name}|register_new_vc] Lock acquired for VC ID: {original_vc_id}")

        if original_vc_id in vc_tracking:
            track_info = vc_tracking[original_vc_id]
            guild_id_for_check = track_info.get("guild_id")
            status_id_for_check = track_info.get("status_channel_id")
            if guild_id_for_check and status_id_for_check:
                guild_for_status_check = bot.get_guild(guild_id_for_check)
                if guild_for_status_check:
                    status_vc_obj = guild_for_status_check.get_channel(status_id_for_check)
                    if isinstance(status_vc_obj, discord.VoiceChannel) and status_vc_obj.category and STATUS_CATEGORY_NAME.lower() in status_vc_obj.category.name.lower():
                        if logger: logger.info(f"[{task_name}|register_new_vc] VC {original_vc.name} は既に有効に追跡中です。")
                        if send_feedback_to_ctx:
                            await send_feedback_to_ctx.send(f"VC「{original_vc.name}」は既に追跡中ですニャ。")
                        return False 
            if logger: logger.info(f"[{task_name}|register_new_vc] VC {original_vc.name} の追跡情報が無効と判断。クリーンアップして再作成を試みます。")
            await unregister_vc_tracking_internal(original_vc_id, original_vc.guild, is_internal_call=True) 

        if logger: logger.info(f"[{task_name}|register_new_vc] VC {original_vc.name} (ID: {original_vc_id}) の新規追跡処理を開始します。")
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

            asyncio.create_task(update_dynamic_status_channel_name(original_vc, new_status_vc), name=f"UpdateTask-PostRegister-{original_vc_id}")
            if logger: logger.info(f"[{task_name}|register_new_vc] 追跡開始/再開: Original VC {original_vc.name}, Status VC {new_status_vc.name}. 初期更新タスクをスケジュール。")
            return True
        else:
            if logger: logger.error(f"[{task_name}|register_new_vc] {original_vc.name} のステータスVC作成に失敗。追跡は開始されません。")
            if original_vc_id in vc_tracking: del vc_tracking[original_vc_id]
            await remove_tracked_original_from_db(original_vc_id) 
            return False
    except asyncio.TimeoutError:
        if logger: logger.error(f"[{task_name}|register_new_vc] Timeout acquiring lock for VC ID: {original_vc_id}. Registration skipped.")
        if send_feedback_to_ctx:
            await send_feedback_to_ctx.send(f"VC「{original_vc.name}」の処理が混み合っているようですニャ。少し待ってから試してニャ。")
        return False
    except Exception as e:
        if logger: logger.error(f"[{task_name}|register_new_vc] Error during registration for VC {original_vc_id}: {e}", exc_info=True)
        return False
    finally:
        if acquired and lock.locked(): 
            lock.release()
            if logger: logger.debug(f"[{task_name}|register_new_vc] Lock for VC ID: {original_vc_id} released in finally.")
        elif not acquired and logger:
            logger.debug(f"[{task_name}|register_new_vc] Lock for VC ID: {original_vc_id} was not acquired due to timeout. No release needed by this instance.")


async def unregister_vc_tracking(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None):
    lock = get_vc_lock(original_channel_id)
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UnregisterTask"
    if logger: logger.debug(f"[{task_name}|unregister_vc] Attempting to acquire lock for VC ID: {original_channel_id}")
    acquired = False
    try:
        await asyncio.wait_for(lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT)
        acquired = True
        if logger: logger.debug(f"[{task_name}|unregister_vc] Lock acquired for VC ID: {original_channel_id}")
        await unregister_vc_tracking_internal(original_channel_id, guild, send_feedback_to_ctx, is_internal_call=False)
    except asyncio.TimeoutError:
        if logger: logger.error(f"[{task_name}|unregister_vc] Timeout acquiring lock for VC ID: {original_channel_id}. Unregistration skipped.")
        if send_feedback_to_ctx:
             await send_feedback_to_ctx.send(f"VC ID「{original_channel_id}」の処理が混み合っているようですニャ。少し待ってから試してニャ。")
    except Exception as e:
        if logger: logger.error(f"[{task_name}|unregister_vc] Error during unregistration for VC {original_channel_id}: {e}", exc_info=True)
    finally:
        if acquired and lock.locked():
            lock.release()
            if logger: logger.debug(f"[{task_name}|unregister_vc] Lock for VC ID: {original_channel_id} released in finally.")

async def unregister_vc_tracking_internal(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None, is_internal_call: bool = False):
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UnregInternalTask"
    if logger: logger.info(f"[{task_name}|unregister_internal] VC ID {original_channel_id} の追跡解除処理を開始 (内部呼び出し: {is_internal_call})。")
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
                if logger: logger.debug(f"[{task_name}|unregister_internal] Attempting to delete status VC {status_vc.name} (ID: {status_vc.id})")
                try:
                    await asyncio.wait_for(status_vc.delete(reason="オリジナルVCの追跡停止のため"), timeout=API_CALL_TIMEOUT)
                    if logger: logger.info(f"[{task_name}|unregister_internal] 削除成功: Status VC {status_vc.name} (ID: {status_vc.id})")
                except asyncio.TimeoutError:
                    if logger: logger.error(f"[{task_name}|unregister_internal] Status VC {status_vc.id} の削除タイムアウト")
                except discord.NotFound:
                    if logger: logger.info(f"[{task_name}|unregister_internal] Status VC {status_channel_id} は既に削除されていました。")
                except discord.Forbidden as e_forbidden:
                    if logger: logger.error(f"[{task_name}|unregister_internal] Status VC {status_vc.name} (ID: {status_vc.id}) の削除に失敗 (権限不足): {e_forbidden}")
                except Exception as e_delete:
                    if logger: logger.error(f"[{task_name}|unregister_internal] Status VC {status_vc.name} (ID: {status_vc.id}) の削除中にエラー: {e_delete}", exc_info=True)
            elif status_vc:
                 if logger: logger.warning(f"[{task_name}|unregister_internal] Status Channel ID {status_channel_id} for original {original_channel_id} is not a VoiceChannel. Type: {type(status_vc)}")
            else:
                if logger: logger.info(f"[{task_name}|unregister_internal] Status VC ID {status_channel_id} for original {original_channel_id} not found in guild {current_guild.name}.")
        elif not current_guild and status_channel_id:
             if logger: logger.warning(f"[{task_name}|unregister_internal] Guild for original_channel_id {original_channel_id} not found. Cannot delete status VC {status_channel_id}.")
    
    vc_rate_limit_windows.pop(original_channel_id, None)
    vc_zero_stats.pop(original_channel_id, None)
    vc_discord_api_cooldown_until.pop(original_channel_id, None)
    
    await remove_tracked_original_from_db(original_channel_id) 
    if not is_internal_call and send_feedback_to_ctx:
        display_name = original_vc_name_for_msg
        if guild: 
             actual_original_vc = guild.get_channel(original_channel_id)
             if actual_original_vc : display_name = actual_original_vc.name
        try:
            await send_feedback_to_ctx.send(f"VC「{display_name}」の人数表示用チャンネルを削除し、追跡を停止したニャ。")
        except Exception as e_feedback: 
            if logger: logger.error(f"[{task_name}|unregister_internal] Error sending unregister feedback: {e_feedback}")


async def update_dynamic_status_channel_name(original_vc: discord.VoiceChannel, status_vc: discord.VoiceChannel):
    if not original_vc or not status_vc:
        if logger: logger.debug(f"Update_dynamic: スキップ - OriginalVC/StatusVCが無効 {original_vc.id if original_vc else 'N/A'}")
        return

    ovc_id = original_vc.id
    lock = get_vc_lock(ovc_id)
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UpdateTask"
    
    if logger: logger.debug(f"[{task_name}|update_dynamic] Attempting to acquire lock for VC ID: {ovc_id} (Lock currently: {'locked' if lock.locked() else 'unlocked'})")
    if lock.locked(): 
        if logger: logger.debug(f"[{task_name}|update_dynamic] Lock for VC ID {ovc_id} is ALREADY HELD by another task. Skipping this update cycle.")
        return

    acquired = False
    try:
        await asyncio.wait_for(lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT)
        acquired = True
        if logger: logger.debug(f"[{task_name}|update_dynamic] Lock acquired for VC ID: {ovc_id}")
        
        current_original_vc = bot.get_channel(ovc_id)
        current_status_vc = bot.get_channel(status_vc.id)

        if not isinstance(current_original_vc, discord.VoiceChannel) or \
           not isinstance(current_status_vc, discord.VoiceChannel):
            if logger: logger.warning(f"[{task_name}|update_dynamic] Original VC {ovc_id} or Status VC {status_vc.id} became invalid after lock. Skipping.")
            return 
        original_vc, status_vc = current_original_vc, current_status_vc 

        now = datetime.now(timezone.utc)

        if ovc_id in vc_discord_api_cooldown_until and now < vc_discord_api_cooldown_until[ovc_id]:
            cooldown_ends_at = vc_discord_api_cooldown_until[ovc_id]
            if logger: logger.debug(f"[{task_name}|update_dynamic] Discord API cooldown for {original_vc.name}. Ends at {cooldown_ends_at.strftime('%H:%M:%S')}. Skip.")
            return 

        current_members = [member for member in original_vc.members if not member.bot]
        count = len(current_members)
        count = min(count, 999)

        track_info = vc_tracking.get(ovc_id)
        if not track_info: 
            if logger: logger.warning(f"[{task_name}|update_dynamic] Original VC {original_vc.name} (ID: {ovc_id}) not in tracking info. Skipping.")
            return 
        base_name = track_info.get("original_channel_name", original_vc.name[:65])
        
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
            if logger: logger.debug(f"[{task_name}|update_dynamic] Fetching current name for status VC {status_vc.id}")
            fresh_status_vc = await asyncio.wait_for(bot.fetch_channel(status_vc.id), timeout=API_CALL_TIMEOUT)
            current_status_vc_name = fresh_status_vc.name
        except asyncio.TimeoutError: 
            if logger: logger.error(f"[{task_name}|update_dynamic] Timeout fetching status VC name for {status_vc.id}. Skipping.")
            return
        except discord.NotFound: 
            if logger: logger.error(f"[{task_name}|update_dynamic] Status VC {status_vc.id} for {original_vc.name} not found. Periodic check should handle.")
            return
        except Exception as e_fetch: 
            if logger: logger.error(f"[{task_name}|update_dynamic] Error fetching status VC name {status_vc.id}: {e_fetch}", exc_info=True)
            return

        final_new_name = re.sub(r'\s{2,}', ' ', desired_name_str).strip()[:100]

        if final_new_name == current_status_vc_name: 
            if logger: logger.debug(f"[{task_name}|update_dynamic] Name for {status_vc.name} ('{final_new_name}') is already correct.")
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
            if logger: logger.debug(f"[{task_name}|update_dynamic] Bot rate limit for {original_vc.name}. Updates in window: {window_info['count'] if window_info else 'N/A'}. Skip.")
            return 

        if logger: logger.info(f"[{task_name}|update_dynamic] Attempting name change for {status_vc.name} ('{current_status_vc_name}') to '{final_new_name}'")
        try:
            await asyncio.wait_for(
                status_vc.edit(name=final_new_name, reason="VC参加人数更新 / 0人ポリシー"),
                timeout=API_CALL_TIMEOUT
            )
            if logger: logger.info(f"[{task_name}|update_dynamic] SUCCESS name change for {status_vc.name} to '{final_new_name}'")
            current_window_data = vc_rate_limit_windows.get(ovc_id)
            if not current_window_data or now >= current_window_data["window_start_time"] + BOT_UPDATE_WINDOW_DURATION:
                vc_rate_limit_windows[ovc_id] = {"window_start_time": now, "count": 1}
            else:
                current_window_data["count"] += 1
            if is_special_zero_update_condition and ovc_id in vc_zero_stats:
                vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True
            if ovc_id in vc_discord_api_cooldown_until: del vc_discord_api_cooldown_until[ovc_id]

        except asyncio.TimeoutError: 
             if logger: logger.error(f"[{task_name}|update_dynamic] Timeout editing status VC name for {status_vc.name} (ID: {status_vc.id}).")
        except discord.HTTPException as e_http: 
            if e_http.status == 429:
                retry_after = e_http.retry_after if e_http.retry_after is not None else 60.0
                vc_discord_api_cooldown_until[ovc_id] = now + timedelta(seconds=retry_after)
                if logger: logger.warning(f"[{task_name}|update_dynamic] Discord API rate limit (429) for {status_vc.name}. Cooldown: {retry_after}s")
            else:
                if logger: logger.error(f"[{task_name}|update_dynamic] HTTP error {e_http.status} editing {status_vc.name}: {e_http.text}", exc_info=True)
        except Exception as e_edit: 
            if logger: logger.error(f"[{task_name}|update_dynamic] Unexpected error editing {status_vc.name}: {e_edit}", exc_info=True)
    
    except asyncio.TimeoutError:
        if logger: logger.error(f"[{task_name}|update_dynamic] Timeout acquiring lock for VC ID: {ovc_id}. Update skipped.")
    except Exception as e_outer_update: 
        if logger: logger.error(f"[{task_name}|update_dynamic] Outer error for VC {ovc_id}: {e_outer_update}", exc_info=True)
    finally:
        if acquired and lock.locked():
            lock.release()
            if logger: logger.debug(f"[{task_name}|update_dynamic] Lock for VC ID: {ovc_id} released in finally.")

@bot.event
async def on_ready():
    if logger: logger.info(f'ログイン成功: {bot.user.name} (ID: {bot.user.id})')
    if logger: logger.info(f"discord.py バージョン: {discord.__version__}")
    
    try:
        activity_name = "VCの人数を見守り中ニャ～"
        activity = discord.CustomActivity(name=activity_name)
        await bot.change_presence(activity=activity)
        if logger: logger.info(f"ボットのアクティビティを設定しました: {activity_name}")
    except Exception as e:
        if logger: logger.error(f"アクティビティの設定中にエラー: {e}", exc_info=True)
    
    vc_discord_api_cooldown_until.clear() 

    if await init_firestore(): 
        await load_tracked_channels_from_db() 
    else:
        if logger: logger.warning("Firestoreが利用できないため、VC追跡の永続化は無効です。")

    if logger: logger.info("起動時の追跡VC状態整合性チェックと更新を開始します...")
    
    tracked_ids_to_process = list(vc_tracking.keys()) 
    
    for original_cid in tracked_ids_to_process:
        if logger: logger.info(f"[on_ready] Processing VC ID: {original_cid}")
        lock = get_vc_lock(original_cid) 
        acquired_on_ready_lock = False
        try:
            await asyncio.wait_for(lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT * 3) 
            acquired_on_ready_lock = True
            if logger: logger.debug(f"[on_ready] Lock acquired for VC ID: {original_cid}")
                
            if original_cid not in vc_tracking: 
                if logger: logger.info(f"[on_ready] VC {original_cid} no longer in tracking after lock acquisition. Skipping.")
                continue 

            track_info = vc_tracking[original_cid]
            guild = bot.get_guild(track_info["guild_id"])

            if not guild: 
                if logger: logger.warning(f"[on_ready] Guild {track_info['guild_id']} (Original VC {original_cid}) が見つかりません。追跡を解除します。")
                await unregister_vc_tracking_internal(original_cid, None, is_internal_call=True)
                continue 

            original_vc = guild.get_channel(original_cid)
            if not isinstance(original_vc, discord.VoiceChannel): 
                if logger: logger.warning(f"[on_ready] Original VC {original_cid} (Guild {guild.name}) が見つからないかVCではありません。追跡を解除します。")
                await unregister_vc_tracking_internal(original_cid, guild, is_internal_call=True)
                continue 

            status_vc_id = track_info.get("status_channel_id")
            status_vc = guild.get_channel(status_vc_id) if status_vc_id else None
            
            vc_rate_limit_windows.pop(original_cid, None) 
            vc_zero_stats.pop(original_cid, None)

            if isinstance(status_vc, discord.VoiceChannel) and status_vc.category and STATUS_CATEGORY_NAME.lower() in status_vc.category.name.lower():
                if logger: logger.info(f"[on_ready] Original VC {original_vc.name} の既存Status VC {status_vc.name} は有効です。名前を更新します。")
                asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc), name=f"UpdateTask-OnReady-{original_cid}")
            else: 
                if status_vc: 
                    if logger: logger.warning(f"[on_ready] Status VC {status_vc.id if status_vc else 'N/A'} ({original_vc.name}用) が無効か移動。削除して再作成試行。")
                    try:
                        await asyncio.wait_for(status_vc.delete(reason="無効なステータスVCのため再作成"), timeout=API_CALL_TIMEOUT)
                    except Exception as e_del_ready: 
                        if logger: logger.error(f"[on_ready] 無効なステータスVC {status_vc.id if status_vc else 'N/A'} の削除エラー: {e_del_ready}", exc_info=True)
                
                if logger: logger.info(f"[on_ready] {original_vc.name} のステータスVCが存在しないか無効。新規作成試行。")
                await unregister_vc_tracking_internal(original_cid, guild, is_internal_call=True) 
                
                new_status_vc_obj = await _create_status_vc_for_original(original_vc)
                if new_status_vc_obj: 
                    vc_tracking[original_cid] = { 
                        "guild_id": guild.id,
                        "status_channel_id": new_status_vc_obj.id,
                        "original_channel_name": original_vc.name 
                    }
                    await save_tracked_original_to_db(original_cid, guild.id, new_status_vc_obj.id, original_vc.name)
                    if logger: logger.info(f"[on_ready] {original_vc.name} のステータスVCを正常に再作成しました: {new_status_vc_obj.name}")
                    asyncio.create_task(update_dynamic_status_channel_name(original_vc, new_status_vc_obj), name=f"UpdateTask-OnReady-Recreate-{original_cid}")
                else:
                    if logger: logger.error(f"[on_ready] {original_vc.name} のステータスVC再作成に失敗しました。")
        except asyncio.TimeoutError:
            if logger: logger.error(f"[on_ready] Timeout acquiring lock for VC ID: {original_cid} during on_ready processing. Skipping this VC for init.")
        except Exception as e_onready_vc:
            if logger: logger.error(f"[on_ready] Error processing VC {original_cid}: {e_onready_vc}", exc_info=True)
        finally:
            if acquired_on_ready_lock and lock.locked():
                lock.release()
                if logger: logger.debug(f"[on_ready] Lock for VC ID: {original_cid} released in finally.")
            
    if logger: logger.info("起動時の追跡VC状態整合性チェックと更新が完了しました。")
    if not periodic_status_update.is_running():
        try:
            periodic_status_update.start()
            if logger: logger.info("定期ステータス更新タスクを開始しました。")
        except RuntimeError as e_task_start: 
             if logger: logger.warning(f"定期ステータス更新タスクの開始試行中にエラー: {e_task_start}")


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return
    channels_to_check_ids = set()
    if before.channel: channels_to_check_ids.add(before.channel.id)
    if after.channel: channels_to_check_ids.add(after.channel.id)
    
    for original_cid in channels_to_check_ids:
        if original_cid in vc_tracking:
            track_info = vc_tracking.get(original_cid) 
            if not track_info: 
                if logger: logger.debug(f"[on_voice_state_update] VC {original_cid} no longer in vc_tracking. Skipping.")
                continue 
            guild = bot.get_guild(track_info["guild_id"])
            if not guild: 
                if logger: logger.warning(f"[on_voice_state_update] Guild {track_info['guild_id']} for VC {original_cid} not found. Skipping.")
                continue
            original_vc = guild.get_channel(original_cid)
            status_vc_id = track_info.get("status_channel_id")
            status_vc = guild.get_channel(status_vc_id) if status_vc_id else None
            if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
                if logger: logger.debug(f"[on_voice_state_update] Relevant update for tracked VC ID: {original_cid}. Scheduling name update.")
                asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc), name=f"UpdateTask-VoiceState-{original_cid}")
            else:
                if logger: logger.debug(f"[on_voice_state_update] Original or Status VC invalid for {original_cid}. Original: {type(original_vc)}, Status: {type(status_vc)}")

@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): return
    if channel.category and STATUS_CATEGORY_NAME.lower() in channel.category.name.lower(): 
        if logger: logger.debug(f"[on_guild_channel_create] New VC {channel.name} is a status channel. Ignoring.")
        return
    if channel.id in vc_tracking or any(info.get("status_channel_id") == channel.id for info in vc_tracking.values()): 
        if logger: logger.debug(f"[on_guild_channel_create] New VC {channel.name} already known or is a status channel. Ignoring.")
        return
    if logger: logger.info(f"[on_guild_channel_create] New VC 「{channel.name}」 (ID: {channel.id}) 作成。自動追跡試行。")
    asyncio.create_task(register_new_vc_for_tracking(channel), name=f"RegisterTask-ChannelCreate-{channel.id}")

@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): return
    original_channel_id_to_process = None
    is_status_vc_deleted = False
    guild_where_deleted = channel.guild 

    if channel.id in vc_tracking: 
        original_channel_id_to_process = channel.id
        if logger: logger.info(f"[on_guild_channel_delete] Tracked original VC {channel.name} (ID: {channel.id}) deleted from guild {guild_where_deleted.name}.")
    else: 
        for ocid, info in list(vc_tracking.items()): 
            if info.get("status_channel_id") == channel.id:
                if info.get("guild_id") == guild_where_deleted.id:
                    original_channel_id_to_process = ocid
                    is_status_vc_deleted = True
                    if logger: logger.info(f"[on_guild_channel_delete] Status VC {channel.name} (for original ID: {ocid}) deleted from guild {guild_where_deleted.name}.")
                    break
    
    if original_channel_id_to_process:
        if logger: logger.info(f"[on_guild_channel_delete] Processing deletion related to original VC ID: {original_channel_id_to_process}")
        async def handle_deletion_logic_wrapper(ocid_to_process, deleted_is_status, g_obj):
            lock = get_vc_lock(ocid_to_process)
            task_name_del = asyncio.current_task().get_name() if asyncio.current_task() else "DelWrapTask"
            acquired_del_lock = False
            if logger: logger.debug(f"[{task_name_del}|handle_deletion_logic_wrapper] Attempting lock for {ocid_to_process}")
            try:
                await asyncio.wait_for(lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT * 3)
                acquired_del_lock = True
                if logger: logger.debug(f"[{task_name_del}|handle_deletion_logic_wrapper] Lock acquired for {ocid_to_process}")
                
                if deleted_is_status:
                    original_vc_obj = g_obj.get_channel(ocid_to_process) if g_obj else None
                    if original_vc_obj and isinstance(original_vc_obj, discord.VoiceChannel):
                        if logger: logger.info(f"[{task_name_del}|handle_deletion_logic_wrapper] Original VC {original_vc_obj.name} still exists. Attempting to recreate status VC.")
                        await unregister_vc_tracking_internal(ocid_to_process, g_obj, is_internal_call=True)
                        
                        new_status_vc = await _create_status_vc_for_original(original_vc_obj)
                        if new_status_vc:
                            vc_tracking[ocid_to_process] = {
                                "guild_id": original_vc_obj.guild.id,
                                "status_channel_id": new_status_vc.id,
                                "original_channel_name": original_vc_obj.name
                            }
                            await save_tracked_original_to_db(ocid_to_process, original_vc_obj.guild.id, new_status_vc.id, original_vc_obj.name)
                            asyncio.create_task(update_dynamic_status_channel_name(original_vc_obj, new_status_vc), name=f"UpdateTask-PostDeleteRecreate-{ocid_to_process}")
                            if logger: logger.info(f"[{task_name_del}|handle_deletion_logic_wrapper] Status VC for {original_vc_obj.name} recreated: {new_status_vc.name}")
                        else:
                            if logger: logger.error(f"[{task_name_del}|handle_deletion_logic_wrapper] Failed to recreate status VC for {original_vc_obj.name}. It remains untracked.")
                    else:
                        if logger: logger.info(f"[{task_name_del}|handle_deletion_logic_wrapper] Original VC {ocid_to_process} not found after status VC deletion. Unregistering fully.")
                        await unregister_vc_tracking_internal(ocid_to_process, g_obj, is_internal_call=True)
                else: 
                    await unregister_vc_tracking_internal(ocid_to_process, g_obj, is_internal_call=True)
            except asyncio.TimeoutError:
                if logger: logger.error(f"[{task_name_del}|handle_deletion_logic_wrapper] Timeout acquiring lock for {ocid_to_process}. Deletion processing may be incomplete.")
            except Exception as e_del_handler_wrapper:
                if logger: logger.error(f"[{task_name_del}|handle_deletion_logic_wrapper] Error for {ocid_to_process}: {e_del_handler_wrapper}", exc_info=True)
            finally:
                if acquired_del_lock and lock.locked():
                    lock.release()
                    if logger: logger.debug(f"[{task_name_del}|handle_deletion_logic_wrapper] Lock for {ocid_to_process} released in finally.")
        asyncio.create_task(handle_deletion_logic_wrapper(original_channel_id_to_process, is_status_vc_deleted, guild_where_deleted), name=f"DeleteTask-{original_channel_id_to_process}")

@tasks.loop(minutes=3) 
async def periodic_status_update():
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "PeriodicTaskLoop"
    if logger: logger.debug(f"[{task_name}] 定期ステータス更新タスク実行中... 現在追跡中: {len(vc_tracking)}件")
    if not vc_tracking: return

    for original_cid in list(vc_tracking.keys()): 
        if logger: logger.debug(f"[{task_name}|periodic_update] Processing VC ID: {original_cid}")
        
        track_info = vc_tracking.get(original_cid)
        if not track_info: 
            if logger: logger.warning(f"[{task_name}|periodic_update] VC {original_cid} not in tracking after starting loop iteration. Skipping.")
            continue

        guild = bot.get_guild(track_info["guild_id"])
        if not guild: 
            if logger: logger.warning(f"[{task_name}|periodic_update] Guild {track_info['guild_id']} (Original VC {original_cid}) not found. Scheduling unregistration.")
            asyncio.create_task(unregister_vc_tracking(original_cid, None), name=f"UnregisterTask-Periodic-NoGuild-{original_cid}")
            continue
        
        original_vc = guild.get_channel(original_cid)
        status_vc_id = track_info.get("status_channel_id")
        status_vc = guild.get_channel(status_vc_id) if status_vc_id else None

        if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
            if status_vc.category is None or STATUS_CATEGORY_NAME.lower() not in status_vc.category.name.lower():
                if logger: logger.warning(f"[{task_name}|periodic_update] Status VC {status_vc.name} for {original_vc.name} in wrong category. Scheduling fix.")
                
                async def fix_category_task_wrapper(ovc_obj, g_obj): 
                    fix_task_name_inner = asyncio.current_task().get_name() if asyncio.current_task() else "FixCatTaskInner"
                    if logger: logger.debug(f"[{fix_task_name_inner}|fix_category_task_wrapper] Attempting to fix category for {ovc_obj.name}")
                    lock_fix = get_vc_lock(ovc_obj.id)
                    acquired_fix_lock = False
                    try:
                        await asyncio.wait_for(lock_fix.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT * 2)
                        acquired_fix_lock = True
                        if logger: logger.debug(f"[{fix_task_name_inner}|fix_category_task_wrapper] Lock acquired for unregister part of {ovc_obj.id}")
                        await unregister_vc_tracking_internal(ovc_obj.id, g_obj, is_internal_call=True)
                    except asyncio.TimeoutError: 
                        if logger: logger.error(f"[{fix_task_name_inner}|fix_category_task_wrapper] Timeout acquiring lock for {ovc_obj.id} (unregister part).")
                        return 
                    except Exception as e_fix_lock_unregister: 
                        if logger: logger.error(f"[{fix_task_name_inner}|fix_category_task_wrapper] Error during lock/unregister for {ovc_obj.id}: {e_fix_lock_unregister}", exc_info=True)
                        if acquired_fix_lock and lock_fix.locked(): lock_fix.release() 
                        return
                    finally: 
                        if acquired_fix_lock and lock_fix.locked(): 
                            lock_fix.release()
                            if logger: logger.debug(f"[{fix_task_name_inner}|fix_category_task_wrapper] Lock for {ovc_obj.id} (unregister part) released.")
                    
                    if logger: logger.info(f"[{fix_task_name_inner}|fix_category_task_wrapper] Re-registering {ovc_obj.name} to fix category.")
                    await register_new_vc_for_tracking(ovc_obj) 
                    if logger: logger.debug(f"[{fix_task_name_inner}|fix_category_task_wrapper] Category fix attempt for {ovc_obj.id} finished.")
                asyncio.create_task(fix_category_task_wrapper(original_vc, guild), name=f"FixCategoryTask-{original_cid}")
                continue 
            asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc), name=f"UpdateTask-Periodic-{original_cid}")
        
        elif not isinstance(original_vc, discord.VoiceChannel) and original_cid in vc_tracking: 
            if logger: logger.warning(f"[{task_name}|periodic_update] Original VC {original_cid} ('{track_info.get('original_channel_name', 'N/A')}') invalid. Scheduling unregistration.")
            asyncio.create_task(unregister_vc_tracking(original_cid, guild), name=f"UnregisterTask-Periodic-InvalidOrig-{original_cid}")
        elif isinstance(original_vc, discord.VoiceChannel) and not isinstance(status_vc, discord.VoiceChannel):
            if logger: logger.warning(f"[{task_name}|periodic_update] Status VC for {original_vc.name} (ID: {original_cid}) missing/invalid. Scheduling recreation.")
            
            async def recreate_status_vc_task_wrapper(ovc_obj, g_obj): 
                recreate_task_name_inner = asyncio.current_task().get_name() if asyncio.current_task() else "RecreateStatusTaskInner"
                if logger: logger.debug(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Attempting to recreate status for {ovc_obj.name}")
                lock_recreate = get_vc_lock(ovc_obj.id)
                acquired_recreate_lock = False
                try:
                    await asyncio.wait_for(lock_recreate.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT * 2)
                    acquired_recreate_lock = True
                    if logger: logger.debug(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Lock acquired for unregister part of {ovc_obj.id}")
                    await unregister_vc_tracking_internal(ovc_obj.id, g_obj, is_internal_call=True) 
                except asyncio.TimeoutError: 
                    if logger: logger.error(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Timeout acquiring lock for {ovc_obj.id} (unregister part).")
                    return
                except Exception as e_recreate_lock_unregister: 
                    if logger: logger.error(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Error during lock/unregister for {ovc_obj.id}: {e_recreate_lock_unregister}", exc_info=True)
                    if acquired_recreate_lock and lock_recreate.locked(): lock_recreate.release()
                    return
                finally: 
                    if acquired_recreate_lock and lock_recreate.locked(): 
                        lock_recreate.release()
                        if logger: logger.debug(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Lock for {ovc_obj.id} (unregister part) released.")
                
                if logger: logger.info(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Re-registering {ovc_obj.name} to recreate status VC.")
                await register_new_vc_for_tracking(ovc_obj) 
                if logger: logger.debug(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Status VC recreate attempt for {ovc_obj.id} finished.")
            
            asyncio.create_task(recreate_status_vc_task_wrapper(original_vc, guild), name=f"RecreateStatusTask-{original_cid}")
        
        elif original_cid in vc_tracking: 
            if logger: logger.warning(f"[{task_name}|periodic_update] Generic invalid state for Original VC {original_cid} ('{track_info.get('original_channel_name', 'N/A')}'). Scheduling unregistration.")
            asyncio.create_task(unregister_vc_tracking(original_cid, guild), name=f"UnregisterTask-Periodic-GenericInvalid-{original_cid}")

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
        if logger: logger.error(f"nahコマンドでHTTPエラー: {e}", exc_info=True)
        await ctx.send(f"メッセージ削除中にエラーが発生したニャ😿: {e.text}")
    except Exception as e: 
        if logger: logger.error(f"nahコマンドでエラー: {e}", exc_info=True)
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
        if logger: logger.error(f"nah_command 未処理のエラー: {error}", exc_info=True)
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
                    target_vc = vc_iter; 
                    if logger: logger.info(f"VC名「{channel_id_or_name}」の部分一致で「{vc_iter.name}」を使用。"); break
    
    if not target_vc or not isinstance(target_vc, discord.VoiceChannel): 
        await ctx.send(f"指定された「{channel_id_or_name}」はボイスチャンネルとして見つからなかったニャ😿")
        return

    if target_vc.category and STATUS_CATEGORY_NAME.lower() in target_vc.category.name.lower(): 
        await ctx.send(f"VC「{target_vc.name}」はSTATUSチャンネルのようだニャ。元のVCを指定してニャ。")
        return

    await ctx.send(f"VC「{target_vc.name}」の追跡設定/解除処理を開始したニャ。完了まで少し待ってニャ。") 

    if target_vc.id in vc_tracking:
        if logger: logger.info(f"コマンド: VC「{target_vc.name}」の追跡解除を試みます。")
        asyncio.create_task(unregister_vc_tracking(target_vc.id, guild, send_feedback_to_ctx=ctx), name=f"UnregisterTask-Command-{target_vc.id}")
    else:
        if logger: logger.info(f"コマンド: VC「{target_vc.name}」の新規追跡を試みます。")
        asyncio.create_task(register_new_vc_for_tracking(target_vc, send_feedback_to_ctx=ctx), name=f"RegisterTask-Command-{target_vc.id}")
                            
@nah_vc_command.error
async def nah_vc_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("このコマンドを実行する権限がニャいみたいだニャ… (チャンネル管理権限が必要だニャ)")
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send("ボットにチャンネルを管理する権限がないニャ😿 (ボットの権限を確認してニャ)")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("どのボイスチャンネルか指定してニャ！ 例: `!!nah_vc General`")
    else:
        if logger: logger.error(f"nah_vc_command 未処理のエラー: {error}", exc_info=True)
        await ctx.send("コマンド実行中に予期せぬエラーが発生したニャ。")

@bot.command(name='nah_help', help="コマンド一覧を表示するニャ。")
async def nah_help_prefix(ctx: commands.Context):
    await ctx.send(HELP_TEXT_CONTENT)

# --- Main Bot Execution ---
async def start_bot_main():
    global logger # Ensure we are using the global logger potentially re-assigned in setup_logging
    if not logger:
        print("CRITICAL: Logger is None in start_bot_main. Attempting re-setup.", file=sys.stderr)
        logger = setup_logging() # Try to set it up again if it was None
        if not logger:
            print("CRITICAL: Fallback logger also failed in start_bot_main. Exiting.", file=sys.stderr)
            return 

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
                await bot.close()
            except Exception as e_close:
                logger.error(f"Botのシャットダウン中にエラー: {e_close}", exc_info=True)
        logger.info("Botがシャットダウンしました。")

# --- Entry Point ---
if __name__ == "__main__":
    if not logger: 
        print("FATAL: Logger was not set up correctly before __main__. Exiting.", file=sys.stderr)
        sys.exit("Logger setup failed")

    try:
        asyncio.run(start_bot_main())
    except KeyboardInterrupt:
        if logger: logger.info("ユーザーによりBotが停止されました (KeyboardInterrupt)。")
        else: print("ユーザーによりBotが停止されました (KeyboardInterrupt)。", file=sys.stderr)
    except Exception as e: 
        if logger: logger.critical(f"メインの実行ループで予期せぬエラーが発生しました: {e}", exc_info=True)
        else: print(f"メインの実行ループで予期せぬエラーが発生しました: {e}", file=sys.stderr)

