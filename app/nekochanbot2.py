# Ensure sys is imported early for print diagnostics if logging fails
import sys
import os 
import traceback # For printing tracebacks

# --- Custom Print Logging Configuration ---
# This should be one of the VERY FIRST things your application does.
# Controlled by LOG_LEVEL_PRINT and DEBUG_PRINT_ENABLED environment variables.

DEBUG_PRINT_ENABLED = os.getenv("DEBUG_PRINT_ENABLED", "false").lower() == "true"
LOG_LEVEL_PRINT_ENV = os.getenv("LOG_LEVEL_PRINT", "DEBUG").upper()

_log_level_map_print = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}
_CURRENT_LOG_LEVEL_PRINT_NUM = _log_level_map_print.get(LOG_LEVEL_PRINT_ENV, 10) # Default to DEBUG numeric

_datetime_module = None 
_timezone_module = None

def _ensure_datetime_imported():
    global _datetime_module, _timezone_module
    if _datetime_module is None:
        from datetime import datetime as dt_actual, timezone as tz_actual # Import here
        _datetime_module = dt_actual
        _timezone_module = tz_actual

def _get_timestamp_for_print():
    _ensure_datetime_imported()
    if _datetime_module is None or _timezone_module is None: 
        return "TIMESTAMP_ERROR"
    return _datetime_module.now(_timezone_module.utc).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3] + "Z"

def print_log_custom(level_str, message, *args, exc_info_data=None):
    level_num = _log_level_map_print.get(level_str.upper(), 0)

    if level_num >= _CURRENT_LOG_LEVEL_PRINT_NUM:
        task_name_part = ""
        try:
            _asyncio_module_for_log = sys.modules.get('asyncio')
            if _asyncio_module_for_log:
                current_task = _asyncio_module_for_log.current_task()
                if current_task:
                    task_name_part = f"Task-{current_task.get_name()}|"
        except RuntimeError: 
            pass
        except AttributeError: 
            pass

        formatted_message = f"{_get_timestamp_for_print()} {level_str:<8s} {task_name_part}- {message}"
        output_stream = sys.stderr if level_str in ["ERROR", "CRITICAL"] else sys.stdout
        try:
            full_message = formatted_message
            if args:
                full_message = formatted_message % args
            print(full_message, file=output_stream, flush=True)
            if exc_info_data: 
                if isinstance(exc_info_data, Exception):
                    print(f"{_get_timestamp_for_print()} ERROR    - Exception: {type(exc_info_data).__name__}: {exc_info_data}", file=sys.stderr, flush=True)
                    exc_type, exc_value, tb = sys.exc_info()
                    if exc_type is not None: 
                         traceback_str = "".join(traceback.format_exception(exc_type, exc_value, tb))
                         print(f"{_get_timestamp_for_print()} ERROR    - Traceback:\n{traceback_str}", file=sys.stderr, flush=True)
                elif isinstance(exc_info_data, str): 
                    print(f"{_get_timestamp_for_print()} ERROR    - Traceback:\n{exc_info_data}", file=sys.stderr, flush=True)
        except Exception as e_print:
            print(f"{_get_timestamp_for_print()} PRINT_ERROR - Failed to format/print log: {e_print} | Original Level: {level_str} | Original Message: {message}", file=sys.stderr, flush=True)

def print_debug(message, *args):
    if DEBUG_PRINT_ENABLED: 
        print_log_custom("DEBUG", message, *args)

def print_info(message, *args):
    print_log_custom("INFO", message, *args)

def print_warning(message, *args):
    print_log_custom("WARNING", message, *args)

def print_error(message, *args, exc_info=False):
    if exc_info:
        exc_type, exc_value, tb = sys.exc_info()
        if exc_type is not None:
            tb_str = "".join(traceback.format_exception(exc_type, exc_value, tb))
            print_log_custom("ERROR", message + "\n" + tb_str, *args) 
            return 
    print_log_custom("ERROR", message, *args)

from datetime import datetime, timedelta, timezone 
_ensure_datetime_imported() 

print_info("カスタムprintロギングシステム初期化。LOG_LEVEL_PRINT: %s, DEBUG_PRINT_ENABLED: %s", LOG_LEVEL_PRINT_ENV, DEBUG_PRINT_ENABLED)
print_debug("これはカスタムprintデバッグメッセージです。表示されればDEBUG出力は有効です。")
# --- End of Custom Print Logging Configuration ---

from dotenv import load_dotenv
load_dotenv() 

import discord
from discord.ext import commands, tasks
import re 
import asyncio 

from flask import Flask
from threading import Thread

print_info(f"dotenvロード完了。RENDER env var: {os.getenv('RENDER')}")

# --- Flask App for Keep Alive (Render health checks) ---
app = Flask('')
@app.route('/')
def home():
    print_debug("Flask / endpoint called (Keep-alive)") 
    return "I'm alive" 

def run_flask():
    port = int(os.environ.get('PORT', 8080)) 
    print_info(f"Flaskサーバーを host=0.0.0.0, port={port} で起動します。")
    app.run(host='0.0.0.0', port=port, debug=False) 

def keep_alive():
    flask_thread = Thread(target=run_flask, name="FlaskKeepAliveThread")
    flask_thread.daemon = True 
    flask_thread.start()
    print_info("Keep-aliveスレッドを開始しました。")

# --- Bot Intents Configuration ---
intents = discord.Intents.default() 
intents.guilds = True
intents.voice_states = True 
intents.message_content = True 

# --- Firestore Client and Constants ---
db = None 
FIRESTORE_COLLECTION_NAME = "discord_tracked_original_vcs_prod_v4" 
STATUS_CATEGORY_NAME = "STATUS" 

# --- VC Tracking Dictionaries ---
vc_tracking = {} 
# vc_locks = {} # REMOVED

# --- Cooldown and State Settings for VC Name Updates ---
API_CALL_TIMEOUT = 20.0 
DB_CALL_TIMEOUT = 15.0  
# LOCK_ACQUIRE_TIMEOUT = 15.0 # REMOVED
ZERO_USER_TIMEOUT_DURATION = timedelta(minutes=5) 

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
            print_info("スラッシュコマンドを同期しました。")
        except Exception as e:
            print_error(f"スラッシュコマンドの同期中にエラー: {e}", exc_info=True)

# --- Bot Instance ---
bot = MyBot(command_prefix='!!', intents=intents)
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# --- Firestore Helper Functions ---
async def init_firestore():
    global db, firestore # Ensure firestore is global if used for SERVER_TIMESTAMP
    try:
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            from google.cloud import firestore as google_firestore # Import here
            firestore = google_firestore # Make it globally available for SERVER_TIMESTAMP

            db = firestore.AsyncClient()
            await asyncio.wait_for(db.collection(FIRESTORE_COLLECTION_NAME).limit(1).get(), timeout=DB_CALL_TIMEOUT)
            print_info("Firestoreクライアントの初期化に成功しました。")
            return True
        else:
            print_warning("環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されていません。Firestoreは使用できません。")
            db = None
            return False
    except DefaultCredentialsError:
        print_error("Firestoreの認証に失敗しました。GOOGLE_APPLICATION_CREDENTIALSを確認してください。", exc_info=True)
        db = None
        return False
    except asyncio.TimeoutError:
        print_error("Firestoreクライアントの初期化テスト中にタイムアウトしました。")
        db = None
        return False
    except ImportError:
        print_error("Google Cloud Firestoreライブラリが見つかりません。 `pip install google-cloud-firestore` を実行してください。")
        db = None
        return False
    except Exception as e:
        print_error(f"Firestoreクライアントの初期化中に予期せぬエラーが発生しました: {e}", exc_info=True)
        db = None
        return False

async def load_tracked_channels_from_db():
    if not db:
        print_info("Firestoreが無効なため、データベースからの読み込みをスキップします。")
        return
    global vc_tracking
    vc_tracking = {} 
    try:
        print_info(f"Firestoreから追跡VC情報を読み込んでいます (コレクション: {FIRESTORE_COLLECTION_NAME})...")
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
                    print_warning(f"DB内のドキュメント {doc_snapshot.id} に必要な情報が不足しているか型が不正です。スキップ。 Data: {doc_data}")
                    continue
                vc_tracking[original_channel_id] = {
                    "guild_id": guild_id,
                    "status_channel_id": status_channel_id,
                    "original_channel_name": original_channel_name
                }
                docs_loaded_count += 1
                print_debug(f"DBからロード: Original VC ID {original_channel_id}, Status VC ID: {status_channel_id}")
            except (ValueError, TypeError) as e_parse:
                print_warning(f"DB内のドキュメント {doc_snapshot.id} のデータ型解析エラー: {e_parse}。スキップ。 Data: {doc_data}")
        print_info(f"{docs_loaded_count}件の追跡VC情報をDBからロードしました。")
    except Exception as e: 
        print_error(f"Firestoreからのデータ読み込み中にエラー: {e}", exc_info=True)

async def save_tracked_original_to_db(original_channel_id: int, guild_id: int, status_channel_id: int, original_channel_name: str):
    if not db: return
    try:
        if 'firestore' not in globals() or globals()['firestore'] is None:
            print_error("Firestoreモジュールが利用可能でないため、DBへの保存をスキップします (save)。")
            return
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
        print_debug(f"DBに保存: Original VC ID {original_channel_id}, Status VC ID {status_channel_id}")
    except asyncio.TimeoutError:
        print_error(f"Firestoreへのデータ書き込みタイムアウト (Original VC ID: {original_channel_id})")
    except Exception as e:
        print_error(f"Firestoreへのデータ書き込み中にエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def remove_tracked_original_from_db(original_channel_id: int):
    if not db: return
    try:
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await asyncio.wait_for(doc_ref.delete(), timeout=DB_CALL_TIMEOUT)
        print_info(f"DBから削除: Original VC ID {original_channel_id}")
    except asyncio.TimeoutError:
        print_error(f"Firestoreからのデータ削除タイムアウト (Original VC ID: {original_channel_id})")
    except Exception as e:
        print_error(f"Firestoreからのデータ削除中にエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def get_or_create_status_category(guild: discord.Guild) -> discord.CategoryChannel | None:
    for category in guild.categories:
        if STATUS_CATEGORY_NAME.lower() in category.name.lower():
            print_info(f"カテゴリ「{category.name}」をSTATUSカテゴリとして使用します。(Guild: {guild.name})")
            return category
    try:
        print_info(f"「STATUS」を含むカテゴリが見つからなかったため、新規作成します。(Guild: {guild.name})")
        overwrites = { 
            guild.default_role: discord.PermissionOverwrite(read_message_history=True, view_channel=True, connect=False)
        }
        new_category = await guild.create_category(STATUS_CATEGORY_NAME, overwrites=overwrites, reason="VCステータス表示用カテゴリ")
        print_info(f"カテゴリ「{STATUS_CATEGORY_NAME}」を新規作成しました。(Guild: {guild.name})")
        return new_category
    except discord.Forbidden:
        print_error(f"カテゴリ「{STATUS_CATEGORY_NAME}」の作成に失敗しました (権限不足) (Guild: {guild.name})")
    except Exception as e: 
        print_error(f"カテゴリ「{STATUS_CATEGORY_NAME}」の作成中にエラー (Guild: {guild.name}): {e}", exc_info=True)
    return None

async def _create_status_vc_for_original(original_vc: discord.VoiceChannel) -> discord.VoiceChannel | None:
    guild = original_vc.guild
    status_category = await get_or_create_status_category(guild) 
    if not status_category:
        print_error(f"STATUSカテゴリの取得/作成に失敗しました ({guild.name} の {original_vc.name} 用)。")
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
        print_info(f"作成成功: Status VC「{new_status_vc.name}」(ID: {new_status_vc.id}) (Original VC: {original_vc.name})")
        return new_status_vc
    except asyncio.TimeoutError:
        print_error(f"Status VCの作成タイムアウト ({original_vc.name}, Guild: {guild.name})")
    except discord.Forbidden:
        print_error(f"Status VCの作成に失敗 (権限不足) ({original_vc.name}, Guild: {guild.name})")
    except Exception as e:
        print_error(f"Status VCの作成に失敗 ({original_vc.name}): {e}", exc_info=True)
    return None

# Removed get_vc_lock function as locks are removed

async def register_new_vc_for_tracking(original_vc: discord.VoiceChannel, send_feedback_to_ctx=None):
    original_vc_id = original_vc.id
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "RegisterTask"
    print_debug(f"[{task_name}|register_new_vc] Processing for VC ID: {original_vc_id} (No lock)")
    
    try: # No lock acquisition block
        if original_vc_id in vc_tracking:
            track_info = vc_tracking[original_vc_id]
            guild_id_for_check = track_info.get("guild_id")
            status_id_for_check = track_info.get("status_channel_id")
            if guild_id_for_check and status_id_for_check:
                guild_for_status_check = bot.get_guild(guild_id_for_check)
                if guild_for_status_check:
                    status_vc_obj = guild_for_status_check.get_channel(status_id_for_check)
                    if isinstance(status_vc_obj, discord.VoiceChannel) and status_vc_obj.category and STATUS_CATEGORY_NAME.lower() in status_vc_obj.category.name.lower():
                        print_info(f"[{task_name}|register_new_vc] VC {original_vc.name} は既に有効に追跡中です。")
                        if send_feedback_to_ctx:
                            await send_feedback_to_ctx.send(f"VC「{original_vc.name}」は既に追跡中ですニャ。")
                        return False 
            print_info(f"[{task_name}|register_new_vc] VC {original_vc.name} の追跡情報が無効と判断。クリーンアップして再作成を試みます。")
            await unregister_vc_tracking_internal(original_vc_id, original_vc.guild, is_internal_call=True) 

        print_info(f"[{task_name}|register_new_vc] VC {original_vc.name} (ID: {original_vc_id}) の新規追跡処理を開始します。")
        new_status_vc = await _create_status_vc_for_original(original_vc)
        if new_status_vc:
            vc_tracking[original_vc_id] = {
                "guild_id": original_vc.guild.id,
                "status_channel_id": new_status_vc.id,
                "original_channel_name": original_vc.name
            }
            await save_tracked_original_to_db(original_vc_id, original_vc.guild.id, new_status_vc.id, original_vc.name)
            
            vc_zero_stats.pop(original_vc_id, None)
            vc_discord_api_cooldown_until.pop(original_vc_id, None)

            asyncio.create_task(update_dynamic_status_channel_name(original_vc, new_status_vc), name=f"UpdateTask-PostRegister-{original_vc_id}")
            print_info(f"[{task_name}|register_new_vc] 追跡開始/再開: Original VC {original_vc.name}, Status VC {new_status_vc.name}. 初期更新タスクをスケジュール。")
            return True
        else:
            print_error(f"[{task_name}|register_new_vc] {original_vc.name} のステータスVC作成に失敗。追跡は開始されません。")
            if original_vc_id in vc_tracking: del vc_tracking[original_vc_id]
            await remove_tracked_original_from_db(original_vc_id) 
            return False
    except Exception as e: # Catch any other error during the process
        print_error(f"[{task_name}|register_new_vc] Error during registration for VC {original_vc_id}: {e}", exc_info=True)
        return False

async def unregister_vc_tracking(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None):
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UnregisterTask"
    print_debug(f"[{task_name}|unregister_vc] Processing for VC ID: {original_channel_id} (No lock)")
    try:
        await unregister_vc_tracking_internal(original_channel_id, guild, send_feedback_to_ctx, is_internal_call=False)
    except Exception as e:
        print_error(f"[{task_name}|unregister_vc] Error during unregistration for VC {original_channel_id}: {e}", exc_info=True)

async def unregister_vc_tracking_internal(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None, is_internal_call: bool = False):
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UnregInternalTask"
    print_info(f"[{task_name}|unregister_internal] VC ID {original_channel_id} の追跡解除処理を開始 (内部呼び出し: {is_internal_call})。")
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
                print_debug(f"[{task_name}|unregister_internal] Attempting to delete status VC {status_vc.name} (ID: {status_vc.id})")
                try:
                    await asyncio.wait_for(status_vc.delete(reason="オリジナルVCの追跡停止のため"), timeout=API_CALL_TIMEOUT)
                    print_info(f"[{task_name}|unregister_internal] 削除成功: Status VC {status_vc.name} (ID: {status_vc.id})")
                except asyncio.TimeoutError: # ... (handle exceptions)
                    print_error(f"[{task_name}|unregister_internal] Status VC {status_vc.id} の削除タイムアウト")
                except discord.NotFound:
                    print_info(f"[{task_name}|unregister_internal] Status VC {status_channel_id} は既に削除されていました。")
                except discord.Forbidden as e_forbidden:
                    print_error(f"[{task_name}|unregister_internal] Status VC {status_vc.name} (ID: {status_vc.id}) の削除に失敗 (権限不足): {e_forbidden}")
                except Exception as e_delete:
                    print_error(f"[{task_name}|unregister_internal] Status VC {status_vc.name} (ID: {status_vc.id}) の削除中にエラー: {e_delete}", exc_info=True)

    vc_zero_stats.pop(original_channel_id, None)
    vc_discord_api_cooldown_until.pop(original_channel_id, None)
    await remove_tracked_original_from_db(original_channel_id) 
    if not is_internal_call and send_feedback_to_ctx:
        # ... (feedback logic as before)
        display_name = original_vc_name_for_msg
        if guild: 
             actual_original_vc = guild.get_channel(original_channel_id)
             if actual_original_vc : display_name = actual_original_vc.name
        try:
            await send_feedback_to_ctx.send(f"VC「{display_name}」の人数表示用チャンネルを削除し、追跡を停止したニャ。")
        except Exception as e_feedback: 
            print_error(f"[{task_name}|unregister_internal] Error sending unregister feedback: {e_feedback}")


async def update_dynamic_status_channel_name(original_vc: discord.VoiceChannel, status_vc: discord.VoiceChannel):
    if not original_vc or not status_vc:
        print_debug(f"Update_dynamic: スキップ - OriginalVC/StatusVCが無効 {original_vc.id if original_vc else 'N/A'}")
        return

    ovc_id = original_vc.id
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UpdateTask"
    print_debug(f"[{task_name}|update_dynamic] Processing for VC ID: {ovc_id} (No lock)")
    
    try: # No lock acquisition block
        current_original_vc = bot.get_channel(ovc_id)
        current_status_vc = bot.get_channel(status_vc.id)

        if not isinstance(current_original_vc, discord.VoiceChannel) or \
           not isinstance(current_status_vc, discord.VoiceChannel):
            print_warning(f"[{task_name}|update_dynamic] Original VC {ovc_id} or Status VC {status_vc.id} became invalid. Skipping.")
            return 
        original_vc, status_vc = current_original_vc, current_status_vc 

        now = datetime.now(timezone.utc)

        if ovc_id in vc_discord_api_cooldown_until and now < vc_discord_api_cooldown_until[ovc_id]:
            cooldown_ends_at = vc_discord_api_cooldown_until[ovc_id]
            print_debug(f"[{task_name}|update_dynamic] Discord API cooldown for {original_vc.name}. Ends at {cooldown_ends_at.strftime('%H:%M:%S')}. Skip.")
            return 

        current_members = [member for member in original_vc.members if not member.bot]
        count = len(current_members)
        count = min(count, 999)

        track_info = vc_tracking.get(ovc_id)
        if not track_info: 
            print_warning(f"[{task_name}|update_dynamic] Original VC {original_vc.name} (ID: {ovc_id}) not in tracking info. Skipping.")
            return 
        base_name = track_info.get("original_channel_name", original_vc.name[:65])
        
        desired_name_str = f"{base_name}：{count} users"
        is_special_zero_update_condition = False
        if count == 0:
            if ovc_id not in vc_zero_stats:
                vc_zero_stats[ovc_id] = {"zero_since": now, "notified_zero_explicitly": False}
            zero_stat = vc_zero_stats[ovc_id]
            if now >= zero_stat["zero_since"] + ZERO_USER_TIMEOUT_DURATION and not zero_stat.get("notified_zero_explicitly", False): # Corrected constant
                desired_name_str = f"{base_name}：0 users"
                is_special_zero_update_condition = True
        else:
            if ovc_id in vc_zero_stats: del vc_zero_stats[ovc_id]

        try:
            print_debug(f"[{task_name}|update_dynamic] Fetching current name for status VC {status_vc.id}")
            fresh_status_vc = await asyncio.wait_for(bot.fetch_channel(status_vc.id), timeout=API_CALL_TIMEOUT)
            current_status_vc_name = fresh_status_vc.name
        except asyncio.TimeoutError: 
            print_error(f"[{task_name}|update_dynamic] Timeout fetching status VC name for {status_vc.id}. Skipping.")
            return
        except discord.NotFound: 
            print_error(f"[{task_name}|update_dynamic] Status VC {status_vc.id} for {original_vc.name} not found. Periodic check should handle.")
            return
        except Exception as e_fetch: 
            print_error(f"[{task_name}|update_dynamic] Error fetching status VC name {status_vc.id}: {e_fetch}", exc_info=True)
            return

        final_new_name = re.sub(r'\s{2,}', ' ', desired_name_str).strip()[:100]

        if final_new_name == current_status_vc_name: 
            print_debug(f"[{task_name}|update_dynamic] Name for {status_vc.name} ('{final_new_name}') is already correct.")
            if is_special_zero_update_condition and ovc_id in vc_zero_stats:
                 vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True
            return 
        
        print_info(f"[{task_name}|update_dynamic] Attempting name change for {status_vc.name} ('{current_status_vc_name}') to '{final_new_name}'")
        try:
            await asyncio.wait_for(
                status_vc.edit(name=final_new_name, reason="VC参加人数更新 / 0人ポリシー"),
                timeout=API_CALL_TIMEOUT
            )
            print_info(f"[{task_name}|update_dynamic] SUCCESS name change for {status_vc.name} to '{final_new_name}'")
            
            if is_special_zero_update_condition and ovc_id in vc_zero_stats:
                vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True
            if ovc_id in vc_discord_api_cooldown_until: del vc_discord_api_cooldown_until[ovc_id]

        except asyncio.TimeoutError: 
             print_error(f"[{task_name}|update_dynamic] Timeout editing status VC name for {status_vc.name} (ID: {status_vc.id}).")
        except discord.HTTPException as e_http: 
            if e_http.status == 429:
                retry_after = e_http.retry_after if e_http.retry_after is not None else 60.0
                vc_discord_api_cooldown_until[ovc_id] = now + timedelta(seconds=retry_after)
                print_warning(f"[{task_name}|update_dynamic] Discord API rate limit (429) for {status_vc.name}. Cooldown: {retry_after}s. Update skipped this cycle.")
            else:
                print_error(f"[{task_name}|update_dynamic] HTTP error {e_http.status} editing {status_vc.name}: {e_http.text}", exc_info=True)
        except Exception as e_edit: 
            print_error(f"[{task_name}|update_dynamic] Unexpected error editing {status_vc.name}: {e_edit}", exc_info=True)
    
    except Exception as e_outer_update: # Catch any other exceptions in the outer try block
        print_error(f"[{task_name}|update_dynamic] Outer error for VC {ovc_id}: {e_outer_update}", exc_info=True)


@bot.event
async def on_ready():
    print_info(f'ログイン成功: {bot.user.name} (ID: {bot.user.id})')
    print_info(f"discord.py バージョン: {discord.__version__}")
    
    try:
        activity_name = "VCの人数を見守り中ニャ～"
        activity = discord.CustomActivity(name=activity_name)
        await bot.change_presence(activity=activity)
        print_info(f"ボットのアクティビティを設定しました: {activity_name}")
    except Exception as e:
        print_error(f"アクティビティの設定中にエラー: {e}", exc_info=True)
    
    vc_discord_api_cooldown_until.clear() 

    if await init_firestore(): 
        await load_tracked_channels_from_db() 
    else:
        print_warning("Firestoreが利用できないため、VC追跡の永続化は無効です。")

    print_info("起動時の追跡VC状態整合性チェックと更新を開始します...")
    
    tracked_ids_to_process = list(vc_tracking.keys()) 
    
    for original_cid in tracked_ids_to_process:
        print_info(f"[on_ready] Processing VC ID: {original_cid} (No Lock in this loop iteration)")
        # Directly call logic or schedule a task that itself does not re-acquire this loop's conceptual lock
        async def process_vc_on_ready_task(cid): # Renamed for clarity
            task_name_on_ready = asyncio.current_task().get_name() if asyncio.current_task() else f"OnReadyTask-{cid}"
            # No lock acquisition here, direct processing
            print_debug(f"[{task_name_on_ready}] Executing on_ready logic for VC ID {cid}")
            try:
                if cid not in vc_tracking:
                    print_info(f"[{task_name_on_ready}] VC {cid} no longer in tracking. Skipping.")
                    return

                track_info_on_ready = vc_tracking[cid] 
                guild_on_ready = bot.get_guild(track_info_on_ready["guild_id"])

                if not guild_on_ready:
                    print_warning(f"[{task_name_on_ready}] Guild {track_info_on_ready['guild_id']} (Original VC {cid}) not found. Unregistering.")
                    await unregister_vc_tracking_internal(cid, None, is_internal_call=True) # unregister_internal assumes no lock by itself
                    return

                original_vc_on_ready = guild_on_ready.get_channel(cid)
                if not isinstance(original_vc_on_ready, discord.VoiceChannel):
                    print_warning(f"[{task_name_on_ready}] Original VC {cid} (Guild {guild_on_ready.name}) invalid. Unregistering.")
                    await unregister_vc_tracking_internal(cid, guild_on_ready, is_internal_call=True)
                    return

                status_vc_id_on_ready = track_info_on_ready.get("status_channel_id")
                status_vc_on_ready = guild_on_ready.get_channel(status_vc_id_on_ready) if status_vc_id_on_ready else None
                
                vc_zero_stats.pop(cid, None) # Reset zero stats on ready

                if isinstance(status_vc_on_ready, discord.VoiceChannel) and status_vc_on_ready.category and STATUS_CATEGORY_NAME.lower() in status_vc_on_ready.category.name.lower():
                    print_info(f"[{task_name_on_ready}] Original VC {original_vc_on_ready.name} existing Status VC {status_vc_on_ready.name} is valid. Scheduling name update.")
                    asyncio.create_task(update_dynamic_status_channel_name(original_vc_on_ready, status_vc_on_ready), name=f"UpdateTask-OnReady-{cid}")
                else:
                    if status_vc_on_ready:
                        print_warning(f"[{task_name_on_ready}] Status VC {status_vc_on_ready.id if status_vc_on_ready else 'N/A'} for {original_vc_on_ready.name} invalid/moved. Deleting and recreating.")
                        try:
                            await asyncio.wait_for(status_vc_on_ready.delete(reason="Invalid status VC during on_ready"), timeout=API_CALL_TIMEOUT)
                        except Exception as e_del_on_ready:
                            print_error(f"[{task_name_on_ready}] Error deleting invalid status VC {status_vc_on_ready.id if status_vc_on_ready else 'N/A'}: {e_del_on_ready}", exc_info=True)
                    
                    print_info(f"[{task_name_on_ready}] Status VC for {original_vc_on_ready.name} missing or invalid. Recreating.")
                    await unregister_vc_tracking_internal(cid, guild_on_ready, is_internal_call=True) 
                    
                    new_status_vc_obj_on_ready = await _create_status_vc_for_original(original_vc_on_ready)
                    if new_status_vc_obj_on_ready:
                        vc_tracking[cid] = {
                            "guild_id": guild_on_ready.id,
                            "status_channel_id": new_status_vc_obj_on_ready.id,
                            "original_channel_name": original_vc_on_ready.name
                        }
                        await save_tracked_original_to_db(cid, guild_on_ready.id, new_status_vc_obj_on_ready.id, original_vc_on_ready.name)
                        print_info(f"[{task_name_on_ready}] Status VC for {original_vc_on_ready.name} recreated: {new_status_vc_obj_on_ready.name}")
                        asyncio.create_task(update_dynamic_status_channel_name(original_vc_on_ready, new_status_vc_obj_on_ready), name=f"UpdateTask-OnReady-Recreate-{cid}")
                    else:
                        print_error(f"[{task_name_on_ready}] Failed to recreate status VC for {original_vc_on_ready.name}.")
            
            except Exception as e_on_ready_task_no_lock: # Catch errors within the task
                print_error(f"[{task_name_on_ready}] Error processing VC {cid} in on_ready task (no lock version): {e_on_ready_task}", exc_info=True)

        asyncio.create_task(process_vc_on_ready_task(original_cid), name=f"OnReadyProcTask-VC-{original_cid}")
            
    print_info("起動時の追跡VC状態整合性チェックのタスク投入が完了しました。")
    if not periodic_status_update.is_running():
        try:
            periodic_status_update.start()
            print_info("定期ステータス更新タスクを開始しました。")
        except RuntimeError as e_task_start: 
             print_warning(f"定期ステータス更新タスクの開始試行中にエラー: {e_task_start}")

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
                print_debug(f"[on_voice_state_update] VC {original_cid} no longer in vc_tracking. Skipping.")
                continue 
            guild = bot.get_guild(track_info["guild_id"])
            if not guild: 
                print_warning(f"[on_voice_state_update] Guild {track_info['guild_id']} for VC {original_cid} not found. Skipping.")
                continue
            original_vc = guild.get_channel(original_cid)
            status_vc_id = track_info.get("status_channel_id")
            status_vc = guild.get_channel(status_vc_id) if status_vc_id else None
            if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
                print_debug(f"[on_voice_state_update] Relevant update for tracked VC ID: {original_cid}. Scheduling name update.")
                asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc), name=f"UpdateTask-VoiceState-{original_cid}")
            else:
                print_debug(f"[on_voice_state_update] Original or Status VC invalid for {original_cid}. Original: {type(original_vc)}, Status: {type(status_vc)}")

@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): return
    if channel.category and STATUS_CATEGORY_NAME.lower() in channel.category.name.lower(): 
        print_debug(f"[on_guild_channel_create] New VC {channel.name} is a status channel. Ignoring.")
        return
    if channel.id in vc_tracking or any(info.get("status_channel_id") == channel.id for info in vc_tracking.values()): 
        print_debug(f"[on_guild_channel_create] New VC {channel.name} already known or is a status channel. Ignoring.")
        return
    print_info(f"[on_guild_channel_create] New VC 「{channel.name}」 (ID: {channel.id}) 作成。自動追跡試行。")
    asyncio.create_task(register_new_vc_for_tracking(channel), name=f"RegisterTask-ChannelCreate-{channel.id}")

@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): return
    original_channel_id_to_process = None
    is_status_vc_deleted = False
    guild_where_deleted = channel.guild 

    if channel.id in vc_tracking: 
        original_channel_id_to_process = channel.id
        print_info(f"[on_guild_channel_delete] Tracked original VC {channel.name} (ID: {channel.id}) deleted from guild {guild_where_deleted.name}.")
    else: 
        for ocid, info in list(vc_tracking.items()): 
            if info.get("status_channel_id") == channel.id:
                if info.get("guild_id") == guild_where_deleted.id:
                    original_channel_id_to_process = ocid
                    is_status_vc_deleted = True
                    print_info(f"[on_guild_channel_delete] Status VC {channel.name} (for original ID: {ocid}) deleted from guild {guild_where_deleted.name}.")
                    break
    
    if original_channel_id_to_process:
        print_info(f"[on_guild_channel_delete] Processing deletion related to original VC ID: {original_channel_id_to_process}")
        async def handle_deletion_logic_wrapper(ocid_to_process, deleted_is_status, g_obj):
            # No lock acquisition in this wrapper as sub-functions (unregister_internal, _create, save, update) will run directly or as new tasks.
            # This reduces complexity of nested locks but increases risk of race conditions if not careful.
            # For deletion, usually less critical than rapid updates.
            task_name_del = asyncio.current_task().get_name() if asyncio.current_task() else "DelWrapTask"
            print_debug(f"[{task_name_del}|handle_deletion_logic_wrapper] Processing deletion for {ocid_to_process} (No lock at this level)")
            try:
                if deleted_is_status:
                    original_vc_obj = g_obj.get_channel(ocid_to_process) if g_obj else None
                    if original_vc_obj and isinstance(original_vc_obj, discord.VoiceChannel):
                        print_info(f"[{task_name_del}|handle_deletion_logic_wrapper] Original VC {original_vc_obj.name} still exists. Attempting to recreate status VC.")
                        await unregister_vc_tracking_internal(ocid_to_process, g_obj, is_internal_call=True) # This is now lock-free
                        
                        new_status_vc = await _create_status_vc_for_original(original_vc_obj)
                        if new_status_vc:
                            vc_tracking[ocid_to_process] = {
                                "guild_id": original_vc_obj.guild.id,
                                "status_channel_id": new_status_vc.id,
                                "original_channel_name": original_vc_obj.name
                            }
                            await save_tracked_original_to_db(ocid_to_process, original_vc_obj.guild.id, new_status_vc.id, original_vc_obj.name)
                            asyncio.create_task(update_dynamic_status_channel_name(original_vc_obj, new_status_vc), name=f"UpdateTask-PostDeleteRecreate-{ocid_to_process}")
                            print_info(f"[{task_name_del}|handle_deletion_logic_wrapper] Status VC for {original_vc_obj.name} recreated: {new_status_vc.name}")
                        else:
                            print_error(f"[{task_name_del}|handle_deletion_logic_wrapper] Failed to recreate status VC for {original_vc_obj.name}. It remains untracked.")
                    else:
                        print_info(f"[{task_name_del}|handle_deletion_logic_wrapper] Original VC {ocid_to_process} not found after status VC deletion. Unregistering fully.")
                        await unregister_vc_tracking_internal(ocid_to_process, g_obj, is_internal_call=True)
                else: 
                    await unregister_vc_tracking_internal(ocid_to_process, g_obj, is_internal_call=True)
            except Exception as e_del_handler_wrapper:
                print_error(f"[{task_name_del}|handle_deletion_logic_wrapper] Error for {ocid_to_process}: {e_del_handler_wrapper}", exc_info=True)
        asyncio.create_task(handle_deletion_logic_wrapper(original_channel_id_to_process, is_status_vc_deleted, guild_where_deleted), name=f"DeleteTask-{original_channel_id_to_process}")

@tasks.loop(minutes=3) 
async def periodic_status_update():
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "PeriodicTaskLoop"
    print_debug(f"[{task_name}] 定期ステータス更新タスク実行中... 現在追跡中: {len(vc_tracking)}件")
    if not vc_tracking: return

    for original_cid in list(vc_tracking.keys()): 
        print_debug(f"[{task_name}|periodic_update] Processing VC ID: {original_cid}")
        
        track_info = vc_tracking.get(original_cid)
        if not track_info: 
            print_warning(f"[{task_name}|periodic_update] VC {original_cid} not in tracking after starting loop iteration. Skipping.")
            continue

        guild = bot.get_guild(track_info["guild_id"])
        if not guild: 
            print_warning(f"[{task_name}|periodic_update] Guild {track_info['guild_id']} (Original VC {original_cid}) not found. Scheduling unregistration.")
            asyncio.create_task(unregister_vc_tracking(original_cid, None), name=f"UnregisterTask-Periodic-NoGuild-{original_cid}") # unregister_vc_tracking is now lock-free at its top level
            continue
        
        original_vc = guild.get_channel(original_cid)
        status_vc_id = track_info.get("status_channel_id")
        status_vc = guild.get_channel(status_vc_id) if status_vc_id else None

        if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
            if status_vc.category is None or STATUS_CATEGORY_NAME.lower() not in status_vc.category.name.lower():
                print_warning(f"[{task_name}|periodic_update] Status VC {status_vc.name} for {original_vc.name} in wrong category. Scheduling fix.")
                
                async def fix_category_task_wrapper(ovc_obj, g_obj): 
                    fix_task_name_inner = asyncio.current_task().get_name() if asyncio.current_task() else "FixCatTaskInner"
                    print_debug(f"[{fix_task_name_inner}|fix_category_task_wrapper] Attempting to fix category for {ovc_obj.name} (No explicit lock in wrapper)")
                    try:
                        # Direct calls as locking is removed from these functions' top level
                        await unregister_vc_tracking_internal(ovc_obj.id, g_obj, is_internal_call=True)
                        print_info(f"[{fix_task_name_inner}|fix_category_task_wrapper] Re-registering {ovc_obj.name} to fix category.")
                        await register_new_vc_for_tracking(ovc_obj) 
                        print_debug(f"[{fix_task_name_inner}|fix_category_task_wrapper] Category fix attempt for {ovc_obj.id} finished.")
                    except Exception as e_fix_cat_wrap: 
                        print_error(f"[{fix_task_name_inner}|fix_category_task_wrapper] Error in category fix for {ovc_obj.id}: {e_fix_cat_wrap}", exc_info=True)
                asyncio.create_task(fix_category_task_wrapper(original_vc, guild), name=f"FixCategoryTask-{original_cid}")
                continue 
            asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc), name=f"UpdateTask-Periodic-{original_cid}")
        
        elif not isinstance(original_vc, discord.VoiceChannel) and original_cid in vc_tracking: 
            print_warning(f"[{task_name}|periodic_update] Original VC {original_cid} ('{track_info.get('original_channel_name', 'N/A')}') invalid. Scheduling unregistration.")
            asyncio.create_task(unregister_vc_tracking(original_cid, guild), name=f"UnregisterTask-Periodic-InvalidOrig-{original_cid}")
        elif isinstance(original_vc, discord.VoiceChannel) and not isinstance(status_vc, discord.VoiceChannel):
            print_warning(f"[{task_name}|periodic_update] Status VC for {original_vc.name} (ID: {original_cid}) missing/invalid. Scheduling recreation.")
            
            async def recreate_status_vc_task_wrapper(ovc_obj, g_obj): 
                recreate_task_name_inner = asyncio.current_task().get_name() if asyncio.current_task() else "RecreateStatusTaskInner"
                print_debug(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Attempting to recreate status for {ovc_obj.name} (No explicit lock in wrapper)")
                try:
                    await unregister_vc_tracking_internal(ovc_obj.id, g_obj, is_internal_call=True) 
                    print_info(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Re-registering {ovc_obj.name} to recreate status VC.")
                    await register_new_vc_for_tracking(ovc_obj) 
                    print_debug(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Status VC recreate attempt for {ovc_obj.id} finished.")
                except Exception as e_recreate_wrap:
                     print_error(f"[{recreate_task_name_inner}|recreate_status_vc_task_wrapper] Error in status VC recreate for {ovc_obj.id}: {e_recreate_wrap}", exc_info=True)
            
            asyncio.create_task(recreate_status_vc_task_wrapper(original_vc, guild), name=f"RecreateStatusTask-{original_cid}")
        
        elif original_cid in vc_tracking: 
            print_warning(f"[{task_name}|periodic_update] Generic invalid state for Original VC {original_cid} ('{track_info.get('original_channel_name', 'N/A')}'). Scheduling unregistration.")
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
        print_error(f"nahコマンドでHTTPエラー: {e}", exc_info=True)
        await ctx.send(f"メッセージ削除中にエラーが発生したニャ😿: {e.text}")
    except Exception as e: 
        print_error(f"nahコマンドでエラー: {e}", exc_info=True)
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
        print_error(f"nah_command 未処理のエラー: {error}", exc_info=True)
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
                    print_info(f"VC名「{channel_id_or_name}」の部分一致で「{vc_iter.name}」を使用。"); break
    
    if not target_vc or not isinstance(target_vc, discord.VoiceChannel): 
        await ctx.send(f"指定された「{channel_id_or_name}」はボイスチャンネルとして見つからなかったニャ😿")
        return

    if target_vc.category and STATUS_CATEGORY_NAME.lower() in target_vc.category.name.lower(): 
        await ctx.send(f"VC「{target_vc.name}」はSTATUSチャンネルのようだニャ。元のVCを指定してニャ。")
        return

    await ctx.send(f"VC「{target_vc.name}」の追跡設定/解除処理を開始したニャ。完了まで少し待ってニャ。") 

    if target_vc.id in vc_tracking:
        print_info(f"コマンド: VC「{target_vc.name}」の追跡解除を試みます。")
        asyncio.create_task(unregister_vc_tracking(target_vc.id, guild, send_feedback_to_ctx=ctx), name=f"UnregisterTask-Command-{target_vc.id}")
    else:
        print_info(f"コマンド: VC「{target_vc.name}」の新規追跡を試みます。")
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
        print_error(f"nah_vc_command 未処理のエラー: {error}", exc_info=True)
        await ctx.send("コマンド実行中に予期せぬエラーが発生したニャ。")

@bot.command(name='nah_help', help="コマンド一覧を表示するニャ。")
async def nah_help_prefix(ctx: commands.Context):
    await ctx.send(HELP_TEXT_CONTENT)

# --- Main Bot Execution ---
async def start_bot_main():
    if DISCORD_TOKEN is None:
        print_error("DISCORD_TOKEN が環境変数に設定されていません。Botを起動できません。")
        return
    
    if os.getenv("RENDER"): 
        keep_alive()

    try:
        print_info("Botの非同期処理を開始します...")
        await bot.start(DISCORD_TOKEN)
    except discord.LoginFailure:
        print_error("Discordへのログインに失敗しました。トークンが正しいか確認してください。", exc_info=True)
    except Exception as e:
        print_error(f"Botの起動中または実行中に予期せぬエラーが発生しました: {e}", exc_info=True)
    finally:
        if bot.is_connected() and not bot.is_closed(): 
            print_info("Botをシャットダウンします...")
            try:
                await bot.close()
            except Exception as e_close:
                print_error(f"Botのシャットダウン中にエラー: {e_close}", exc_info=True)
        print_info("Botがシャットダウンしました。")

# --- Entry Point ---
if __name__ == "__main__":
    try:
        asyncio.run(start_bot_main())
    except KeyboardInterrupt:
        print_info("ユーザーによりBotが停止されました (KeyboardInterrupt)。")
    except Exception as e: 
        print_error(f"メインの実行ループで予期せぬエラーが発生しました: {e}", exc_info=True)

