# Ensure sys is imported early for print diagnostics if logging fails
import sys
import os
import traceback # For printing tracebacks

# --- Custom Print Logging Configuration ---
DEBUG_PRINT_ENABLED = os.getenv("DEBUG_PRINT_ENABLED", "false").lower() == "true"
LOG_LEVEL_PRINT_ENV = os.getenv("LOG_LEVEL_PRINT", "DEBUG").upper()
_log_level_map_print = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
_CURRENT_LOG_LEVEL_PRINT_NUM = _log_level_map_print.get(LOG_LEVEL_PRINT_ENV, 10)
_datetime_module = None
_timezone_module = None

def _ensure_datetime_imported():
    global _datetime_module, _timezone_module
    if _datetime_module is None:
        from datetime import datetime as dt_actual, timezone as tz_actual
        _datetime_module = dt_actual
        _timezone_module = tz_actual

def _get_timestamp_for_print():
    _ensure_datetime_imported()
    if _datetime_module is None or _timezone_module is None: return "TIMESTAMP_ERROR"
    return _datetime_module.now(_timezone_module.utc).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3] + "Z"

def print_log_custom(level_str, message, *args, exc_info_data=None):
    level_num = _log_level_map_print.get(level_str.upper(), 0)
    if level_num >= _CURRENT_LOG_LEVEL_PRINT_NUM:
        task_name_part = ""
        try:
            _asyncio_module_for_log = sys.modules.get('asyncio')
            if _asyncio_module_for_log:
                current_task = _asyncio_module_for_log.current_task()
                if current_task: task_name_part = f"Task-{current_task.get_name()}|"
        except RuntimeError: pass
        except AttributeError: pass
        formatted_message = f"{_get_timestamp_for_print()} {level_str:<8s} {task_name_part}- {message}"
        output_stream = sys.stderr if level_str in ["ERROR", "CRITICAL"] else sys.stdout
        try:
            full_message = formatted_message % args if args else formatted_message
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
    if DEBUG_PRINT_ENABLED: print_log_custom("DEBUG", message, *args)
def print_info(message, *args): print_log_custom("INFO", message, *args)
def print_warning(message, *args): print_log_custom("WARNING", message, *args)
def print_error(message, *args, exc_info=False):
    if exc_info:
        exc_type, exc_value, tb = sys.exc_info()
        if exc_type is not None:
            tb_str = "".join(traceback.format_exception(exc_type, exc_value, tb))
            print_log_custom("ERROR", message + "\n" + tb_str, *args); return
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

# --- Flask App for Keep Alive ---
app = Flask('')
@app.route('/')
def home(): print_debug("Flask / endpoint called"); return "I'm alive"
def run_flask():
    port = int(os.environ.get('PORT', 8080)); print_info(f"Flaskサーバー起動: host=0.0.0.0, port={port}")
    app.run(host='0.0.0.0', port=port, debug=False)
def keep_alive():
    Thread(target=run_flask, name="FlaskKeepAliveThread", daemon=True).start()
    print_info("Keep-aliveスレッド開始。")

# --- Bot Intents Configuration ---
intents = discord.Intents.default(); intents.guilds = True; intents.voice_states = True; intents.message_content = True

# --- Firestore Client and Constants ---
db = None
FIRESTORE_COLLECTION_NAME = "discord_tracked_original_vcs_prod_v4"
STATUS_CATEGORY_NAME = "STATUS"

# --- VC Tracking Dictionaries and State ---
vc_tracking = {}
vc_processing_flags = {} # NEW: {vc_id: True} if currently being processed

# --- Cooldown and State Settings ---
API_CALL_TIMEOUT = 20.0
DB_CALL_TIMEOUT = 15.0
ZERO_USER_TIMEOUT_DURATION = timedelta(minutes=5)
vc_zero_stats = {}
vc_discord_api_cooldown_until = {}

# --- Help Text ---
HELP_TEXT_CONTENT = ("📘 **コマンド一覧だニャ🐈**\n\n...") # Shortened for brevity

# --- Custom Bot Class for Slash Commands ---
class MyBot(commands.Bot):
    async def setup_hook(self):
        @self.tree.command(name="nah_help", description="コマンド一覧を表示するニャ。")
        async def nah_help_slash(interaction: discord.Interaction): await interaction.response.send_message(HELP_TEXT_CONTENT, ephemeral=True)
        try: await self.tree.sync(); print_info("スラッシュコマンド同期完了。")
        except Exception as e: print_error(f"スラッシュコマンド同期エラー: {e}", exc_info=True)

bot = MyBot(command_prefix='!!', intents=intents)
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

async def init_firestore():
    global db, firestore
    try:
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            from google.cloud import firestore as google_firestore
            firestore = google_firestore
            db = firestore.AsyncClient()
            await asyncio.wait_for(db.collection(FIRESTORE_COLLECTION_NAME).limit(1).get(), timeout=DB_CALL_TIMEOUT)
            print_info("Firestoreクライアント初期化成功。")
            return True
        else: print_warning("GOOGLE_APPLICATION_CREDENTIALS未設定。Firestore無効。"); db = None; return False
    except Exception as e: print_error(f"Firestore初期化エラー: {e}", exc_info=True); db = None; return False

async def load_tracked_channels_from_db():
    if not db: print_info("Firestore無効、DBからのロードスキップ。"); return
    global vc_tracking; vc_tracking = {}
    try:
        print_info(f"Firestoreから追跡VC情報ロード中 (コレクション: {FIRESTORE_COLLECTION_NAME})...")
        stream = db.collection(FIRESTORE_COLLECTION_NAME).stream()
        count = 0
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
                    print_warning(f"DBドキュメント {doc_snapshot.id} 情報不足/型不正。スキップ。 Data: {doc_data}")
                    continue
                vc_tracking[original_channel_id] = {"guild_id": guild_id, "status_channel_id": status_channel_id, "original_channel_name": original_channel_name}
                count += 1
                print_debug(f"DBロード: Original VC ID {original_channel_id}, Status VC ID: {status_channel_id}")
            except (ValueError, TypeError) as e_parse: print_warning(f"DBドキュメント {doc_snapshot.id} データ解析エラー: {e_parse}。スキップ。 Data: {doc_data}")
        print_info(f"{count}件の追跡VC情報をDBからロード完了。")
    except Exception as e: print_error(f"Firestoreデータロード中エラー: {e}", exc_info=True)

async def save_tracked_original_to_db(original_channel_id: int, guild_id: int, status_channel_id: int, original_channel_name: str):
    if not db: return
    try:
        if 'firestore' not in globals() or globals()['firestore'] is None: print_error("Firestoreモジュール利用不可、DB保存スキップ。"); return
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await asyncio.wait_for(doc_ref.set({"guild_id": guild_id, "status_channel_id": status_channel_id, "original_channel_name": original_channel_name, "updated_at": firestore.SERVER_TIMESTAMP }), timeout=DB_CALL_TIMEOUT)
        print_debug(f"DB保存: Original VC ID {original_channel_id}, Status VC ID {status_channel_id}")
    except asyncio.TimeoutError: print_error(f"Firestore書き込みタイムアウト (Original VC ID: {original_channel_id})")
    except Exception as e: print_error(f"Firestore書き込みエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def remove_tracked_original_from_db(original_channel_id: int):
    if not db: return
    try:
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await asyncio.wait_for(doc_ref.delete(), timeout=DB_CALL_TIMEOUT)
        print_info(f"DB削除: Original VC ID {original_channel_id}")
    except asyncio.TimeoutError: print_error(f"Firestore削除タイムアウト (Original VC ID: {original_channel_id})")
    except Exception as e: print_error(f"Firestore削除エラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def get_or_create_status_category(guild: discord.Guild) -> discord.CategoryChannel | None:
    for category in guild.categories:
        if STATUS_CATEGORY_NAME.lower() in category.name.lower():
            print_info(f"カテゴリ「{category.name}」をSTATUSカテゴリとして使用。(Guild: {guild.name})")
            return category
    try:
        print_info(f"「STATUS」カテゴリが見つからず新規作成。(Guild: {guild.name})")
        overwrites = {guild.default_role: discord.PermissionOverwrite(read_message_history=True, view_channel=True, connect=False)}
        new_category = await guild.create_category(STATUS_CATEGORY_NAME, overwrites=overwrites, reason="VCステータス表示用カテゴリ")
        print_info(f"カテゴリ「{STATUS_CATEGORY_NAME}」新規作成成功。(Guild: {guild.name})")
        return new_category
    except discord.Forbidden: print_error(f"カテゴリ「{STATUS_CATEGORY_NAME}」作成失敗 (権限不足) (Guild: {guild.name})")
    except Exception as e: print_error(f"カテゴリ「{STATUS_CATEGORY_NAME}」作成中エラー (Guild: {guild.name}): {e}", exc_info=True)
    return None


async def _create_status_vc_for_original(original_vc: discord.VoiceChannel) -> discord.VoiceChannel | None:
    guild = original_vc.guild
    status_category = await get_or_create_status_category(guild)
    if not status_category: print_error(f"STATUSカテゴリ取得/作成失敗 ({guild.name}の{original_vc.name}用)。"); return None
    user_count = min(len([m for m in original_vc.members if not m.bot]), 999)
    status_channel_name_base = original_vc.name[:65]
    status_channel_name = re.sub(r'\s{2,}', ' ', f"{status_channel_name_base}：{user_count} users").strip()[:100]
    overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=True, read_message_history=True, connect=False, speak=False, stream=False, send_messages=False)}
    try:
        new_status_vc = await asyncio.wait_for(guild.create_voice_channel(name=status_channel_name, category=status_category, overwrites=overwrites, reason=f"{original_vc.name} のステータス表示用VC"), timeout=API_CALL_TIMEOUT)
        print_info(f"作成成功: Status VC「{new_status_vc.name}」(ID: {new_status_vc.id}) (Original VC: {original_vc.name})")
        return new_status_vc
    except asyncio.TimeoutError: print_error(f"Status VC作成タイムアウト ({original_vc.name}, Guild: {guild.name})")
    except discord.Forbidden: print_error(f"Status VC作成失敗 (権限不足) ({original_vc.name}, Guild: {guild.name})")
    except Exception as e: print_error(f"Status VC作成失敗 ({original_vc.name}): {e}", exc_info=True)
    return None

async def register_new_vc_for_tracking(original_vc: discord.VoiceChannel, send_feedback_to_ctx=None):
    original_vc_id = original_vc.id
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "RegisterTask"

    if vc_processing_flags.get(original_vc_id):
        print_debug(f"[{task_name}|register_new_vc] VC ID {original_vc_id} は現在処理中のためスキップ。")
        return False

    vc_processing_flags[original_vc_id] = True
    print_debug(f"[{task_name}|register_new_vc] Processing flag SET for VC ID: {original_vc_id}")

    try:
        if original_vc_id in vc_tracking:
            track_info = vc_tracking[original_vc_id]
            guild_id_for_check = track_info.get("guild_id")
            status_id_for_check = track_info.get("status_channel_id")
            if guild_id_for_check and status_id_for_check:
                guild_for_status_check = bot.get_guild(guild_id_for_check)
                if guild_for_status_check:
                    status_vc_obj = guild_for_status_check.get_channel(status_id_for_check)
                    if isinstance(status_vc_obj, discord.VoiceChannel) and status_vc_obj.category and STATUS_CATEGORY_NAME.lower() in status_vc_obj.category.name.lower():
                        print_info(f"[{task_name}|register_new_vc] VC {original_vc.name} は既に有効に追跡中。")
                        if send_feedback_to_ctx: await send_feedback_to_ctx.send(f"VC「{original_vc.name}」は既に追跡中ですニャ。")
                        return False
            print_info(f"[{task_name}|register_new_vc] VC {original_vc.name} 追跡情報が無効。クリーンアップして再作成試行。")
            await unregister_vc_tracking_internal(original_vc_id, original_vc.guild, is_internal_call=True)

        print_info(f"[{task_name}|register_new_vc] VC {original_vc.name} (ID: {original_vc_id}) 新規追跡処理開始。")
        new_status_vc = await _create_status_vc_for_original(original_vc)
        if new_status_vc:
            vc_tracking[original_vc_id] = {"guild_id": original_vc.guild.id, "status_channel_id": new_status_vc.id, "original_channel_name": original_vc.name}
            await save_tracked_original_to_db(original_vc_id, original_vc.guild.id, new_status_vc.id, original_vc.name)
            vc_zero_stats.pop(original_vc_id, None)
            vc_discord_api_cooldown_until.pop(original_vc_id, None)
            asyncio.create_task(update_dynamic_status_channel_name(original_vc, new_status_vc), name=f"UpdateTask-PostRegister-{original_vc_id}")
            print_info(f"[{task_name}|register_new_vc] 追跡開始/再開: Original VC {original_vc.name}, Status VC {new_status_vc.name}. 初期更新タスクスケジュール。")
            return True
        else:
            print_error(f"[{task_name}|register_new_vc] {original_vc.name} ステータスVC作成失敗。追跡開始されず。")
            if original_vc_id in vc_tracking: del vc_tracking[original_vc_id]
            await remove_tracked_original_from_db(original_vc_id)
            return False
    except Exception as e:
        print_error(f"[{task_name}|register_new_vc] VC {original_vc_id} 登録中エラー: {e}", exc_info=True)
        return False
    finally:
        vc_processing_flags.pop(original_vc_id, None)
        print_debug(f"[{task_name}|register_new_vc] Processing flag CLEARED for VC ID: {original_vc_id}")

async def unregister_vc_tracking(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None):
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UnregisterTask"
    if vc_processing_flags.get(original_channel_id):
        print_debug(f"[{task_name}|unregister_vc] VC ID {original_channel_id} は現在処理中のため登録解除スキップ。")
        return
    vc_processing_flags[original_channel_id] = True
    print_debug(f"[{task_name}|unregister_vc] Processing flag SET for VC ID: {original_channel_id}")
    try:
        await unregister_vc_tracking_internal(original_channel_id, guild, send_feedback_to_ctx, is_internal_call=False)
    except Exception as e:
        print_error(f"[{task_name}|unregister_vc] VC {original_channel_id} 登録解除中エラー: {e}", exc_info=True)
    finally:
        vc_processing_flags.pop(original_channel_id, None)
        print_debug(f"[{task_name}|unregister_vc] Processing flag CLEARED for VC ID: {original_channel_id}")


async def unregister_vc_tracking_internal(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None, is_internal_call: bool = False):
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UnregInternalTask"
    print_info(f"[{task_name}|unregister_internal] VC ID {original_channel_id} 追跡解除処理開始 (内部呼び出し: {is_internal_call})。")
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
                print_debug(f"[{task_name}|unregister_internal] Status VC {status_vc.name} (ID: {status_vc.id}) 削除試行。")
                try:
                    await asyncio.wait_for(status_vc.delete(reason="オリジナルVCの追跡停止のため"), timeout=API_CALL_TIMEOUT)
                    print_info(f"[{task_name}|unregister_internal] 削除成功: Status VC {status_vc.name} (ID: {status_vc.id})")
                except asyncio.TimeoutError: print_error(f"[{task_name}|unregister_internal] Status VC {status_vc.id} 削除タイムアウト")
                except discord.NotFound: print_info(f"[{task_name}|unregister_internal] Status VC {status_channel_id} は既に削除済み。")
                except discord.Forbidden as e: print_error(f"[{task_name}|unregister_internal] Status VC {status_vc.name} 削除失敗 (権限不足): {e}")
                except Exception as e: print_error(f"[{task_name}|unregister_internal] Status VC {status_vc.name} 削除中エラー: {e}", exc_info=True)
    vc_zero_stats.pop(original_channel_id, None)
    vc_discord_api_cooldown_until.pop(original_channel_id, None)
    await remove_tracked_original_from_db(original_channel_id)
    if not is_internal_call and send_feedback_to_ctx:
        display_name = original_vc_name_for_msg
        if guild: actual_original_vc = guild.get_channel(original_channel_id); display_name = actual_original_vc.name if actual_original_vc else display_name
        try: await send_feedback_to_ctx.send(f"VC「{display_name}」の人数表示用チャンネルを削除し、追跡を停止したニャ。")
        except Exception as e: print_error(f"[{task_name}|unregister_internal] 登録解除フィードバック送信エラー: {e}")


async def update_dynamic_status_channel_name(original_vc: discord.VoiceChannel, status_vc: discord.VoiceChannel):
    if not original_vc or not status_vc: print_debug(f"Update_dynamic: スキップ - OriginalVC/StatusVCが無効"); return

    ovc_id = original_vc.id
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "UpdateTask"

    if vc_processing_flags.get(ovc_id):
        print_debug(f"[{task_name}|update_dynamic] VC ID {ovc_id} は現在処理中のため名前更新スキップ。")
        return

    vc_processing_flags[ovc_id] = True
    print_debug(f"[{task_name}|update_dynamic] Processing flag SET for VC ID: {ovc_id}")

    try:
        current_original_vc = bot.get_channel(ovc_id)
        current_status_vc = bot.get_channel(status_vc.id)

        if not isinstance(current_original_vc, discord.VoiceChannel) or \
           not isinstance(current_status_vc, discord.VoiceChannel):
            print_warning(f"[{task_name}|update_dynamic] Original VC {ovc_id} or Status VC {status_vc.id} invalid after flag set. Skipping.")
            return
        original_vc, status_vc = current_original_vc, current_status_vc

        now = datetime.now(timezone.utc)
        if ovc_id in vc_discord_api_cooldown_until and now < vc_discord_api_cooldown_until[ovc_id]:
            print_debug(f"[{task_name}|update_dynamic] Discord API cooldown for {original_vc.name}. Skip.")
            return
        current_members = [member for member in original_vc.members if not member.bot]
        count = min(len(current_members), 999)
        track_info = vc_tracking.get(ovc_id)
        if not track_info: print_warning(f"[{task_name}|update_dynamic] VC {original_vc.name} not in tracking. Skip."); return
        base_name = track_info.get("original_channel_name", original_vc.name[:65])
        desired_name_str = f"{base_name}：{count} users"
        is_special_zero_update_condition = False
        if count == 0:
            if ovc_id not in vc_zero_stats: vc_zero_stats[ovc_id] = {"zero_since": now, "notified_zero_explicitly": False}
            zero_stat = vc_zero_stats[ovc_id]
            if now >= zero_stat["zero_since"] + ZERO_USER_TIMEOUT_DURATION and not zero_stat.get("notified_zero_explicitly", False):
                desired_name_str = f"{base_name}：0 users"; is_special_zero_update_condition = True
        else:
            if ovc_id in vc_zero_stats: del vc_zero_stats[ovc_id]

        try:
            print_debug(f"[{task_name}|update_dynamic] Fetching current name for status VC {status_vc.id}")
            fresh_status_vc = await asyncio.wait_for(bot.fetch_channel(status_vc.id), timeout=API_CALL_TIMEOUT)
            current_status_vc_name = fresh_status_vc.name
        except asyncio.TimeoutError: print_error(f"[{task_name}|update_dynamic] Timeout fetching status VC name for {status_vc.id}. Skipping."); return
        except discord.NotFound: print_error(f"[{task_name}|update_dynamic] Status VC {status_vc.id} for {original_vc.name} not found."); return
        except Exception as e: print_error(f"[{task_name}|update_dynamic] Error fetching status VC name {status_vc.id}: {e}", exc_info=True); return

        final_new_name = re.sub(r'\s{2,}', ' ', desired_name_str).strip()[:100]
        if final_new_name == current_status_vc_name:
            print_debug(f"[{task_name}|update_dynamic] Name for {status_vc.name} ('{final_new_name}') is already correct.")
            if is_special_zero_update_condition and ovc_id in vc_zero_stats: vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True
            return

        print_info(f"[{task_name}|update_dynamic] Attempting name change for {status_vc.name} ('{current_status_vc_name}') to '{final_new_name}'")
        try:
            await asyncio.wait_for(status_vc.edit(name=final_new_name, reason="VC参加人数更新"), timeout=API_CALL_TIMEOUT)
            print_info(f"[{task_name}|update_dynamic] SUCCESS name change for {status_vc.name} to '{final_new_name}'")
            if is_special_zero_update_condition and ovc_id in vc_zero_stats: vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True
            if ovc_id in vc_discord_api_cooldown_until: del vc_discord_api_cooldown_until[ovc_id]
        except asyncio.TimeoutError: print_error(f"[{task_name}|update_dynamic] Timeout editing status VC name for {status_vc.name}.")
        except discord.HTTPException as e:
            if e.status == 429:
                retry_after = e.retry_after if e.retry_after is not None else 60.0
                vc_discord_api_cooldown_until[ovc_id] = now + timedelta(seconds=retry_after)
                print_warning(f"[{task_name}|update_dynamic] Discord API rate limit (429) for {status_vc.name}. Cooldown: {retry_after}s.")
            else: print_error(f"[{task_name}|update_dynamic] HTTP error {e.status} editing {status_vc.name}: {e.text}", exc_info=True)
        except Exception as e: print_error(f"[{task_name}|update_dynamic] Unexpected error editing {status_vc.name}: {e}", exc_info=True)

    except Exception as e_outer_update:
        print_error(f"[{task_name}|update_dynamic] Outer error for VC {ovc_id}: {e_outer_update}", exc_info=True)
    finally:
        vc_processing_flags.pop(ovc_id, None)
        print_debug(f"[{task_name}|update_dynamic] Processing flag CLEARED for VC ID: {ovc_id}")


@bot.event
async def on_ready():
    print_info(f'ログイン成功: {bot.user.name} (ID: {bot.user.id})')
    try:
        activity_name = "VCの人数を見守り中ニャ～"
        activity = discord.CustomActivity(name=activity_name) # discord.Gameから変更
        await bot.change_presence(activity=activity)
        print_info(f"ボットのアクティビティを設定しました: {activity_name}")
    except Exception as e: print_error(f"アクティビティの設定中にエラー: {e}", exc_info=True)

    vc_discord_api_cooldown_until.clear()
    if await init_firestore(): await load_tracked_channels_from_db()
    else: print_warning("Firestore利用不可、永続化無効。")
    print_info("起動時の追跡VC状態整合性チェックと更新を開始します...")
    tracked_ids_to_process = list(vc_tracking.keys())
    for original_cid in tracked_ids_to_process:
        print_info(f"[on_ready] Processing VC ID: {original_cid}")
        async def process_vc_on_ready_task(cid):
            task_name_on_ready = asyncio.current_task().get_name() if asyncio.current_task() else f"OnReadyTask-{cid}"
            if vc_processing_flags.get(cid):
                print_debug(f"[{task_name_on_ready}] VC ID {cid} は起動時処理ですでに処理中(他タスク)。スキップ。")
                return
            vc_processing_flags[cid] = True
            print_debug(f"[{task_name_on_ready}] Processing flag SET for VC ID {cid}")
            try:
                if cid not in vc_tracking: print_info(f"[{task_name_on_ready}] VC {cid} no longer in tracking. Skipping."); return
                track_info_on_ready = vc_tracking[cid]
                guild_on_ready = bot.get_guild(track_info_on_ready["guild_id"])
                if not guild_on_ready: print_warning(f"[{task_name_on_ready}] Guild {track_info_on_ready['guild_id']} (VC {cid}) not found. Unregistering."); await unregister_vc_tracking_internal(cid, None, is_internal_call=True); return
                original_vc_on_ready = guild_on_ready.get_channel(cid)
                if not isinstance(original_vc_on_ready, discord.VoiceChannel): print_warning(f"[{task_name_on_ready}] Original VC {cid} (Guild {guild_on_ready.name}) invalid. Unregistering."); await unregister_vc_tracking_internal(cid, guild_on_ready, is_internal_call=True); return
                status_vc_id_on_ready = track_info_on_ready.get("status_channel_id")
                status_vc_on_ready = guild_on_ready.get_channel(status_vc_id_on_ready) if status_vc_id_on_ready else None
                vc_zero_stats.pop(cid, None)
                if isinstance(status_vc_on_ready, discord.VoiceChannel) and status_vc_on_ready.category and STATUS_CATEGORY_NAME.lower() in status_vc_on_ready.category.name.lower():
                    print_info(f"[{task_name_on_ready}] Original VC {original_vc_on_ready.name} existing Status VC {status_vc_on_ready.name} valid. Updating name.")
                    await update_dynamic_status_channel_name(original_vc_on_ready, status_vc_on_ready)
                else:
                    if status_vc_on_ready:
                        print_warning(f"[{task_name_on_ready}] Status VC {status_vc_on_ready.id if status_vc_on_ready else 'N/A'} for {original_vc_on_ready.name} invalid/moved. Deleting.")
                        try: await asyncio.wait_for(status_vc_on_ready.delete(reason="Invalid status VC during on_ready"), timeout=API_CALL_TIMEOUT)
                        except Exception as e_del: print_error(f"[{task_name_on_ready}] Error deleting invalid status VC: {e_del}", exc_info=True)
                    print_info(f"[{task_name_on_ready}] Status VC for {original_vc_on_ready.name} missing/invalid. Recreating.")
                    await unregister_vc_tracking_internal(cid, guild_on_ready, is_internal_call=True)
                    new_status_vc_obj_on_ready = await _create_status_vc_for_original(original_vc_on_ready)
                    if new_status_vc_obj_on_ready:
                        vc_tracking[cid] = {"guild_id": guild_on_ready.id, "status_channel_id": new_status_vc_obj_on_ready.id, "original_channel_name": original_vc_on_ready.name}
                        await save_tracked_original_to_db(cid, guild_on_ready.id, new_status_vc_obj_on_ready.id, original_vc_on_ready.name)
                        print_info(f"[{task_name_on_ready}] Status VC for {original_vc_on_ready.name} recreated: {new_status_vc_obj_on_ready.name}")
                        await update_dynamic_status_channel_name(original_vc_on_ready, new_status_vc_obj_on_ready)
                    else: print_error(f"[{task_name_on_ready}] Failed to recreate status VC for {original_vc_on_ready.name}.")
            except Exception as e_on_ready_task: print_error(f"[{task_name_on_ready}] Error processing VC {cid} in on_ready task: {e_on_ready_task}", exc_info=True)
            finally: vc_processing_flags.pop(cid, None); print_debug(f"[{task_name_on_ready}] Processing flag CLEARED for VC ID {cid}")
        asyncio.create_task(process_vc_on_ready_task(original_cid), name=f"OnReadyProcTask-VC-{original_cid}")
    print_info("起動時の追跡VC状態整合性チェックのタスク投入完了。")

    if not periodic_status_update.is_running():
        try: periodic_status_update.start(); print_info("定期ステータス更新タスク開始。")
        except RuntimeError as e: print_warning(f"定期タスク開始エラー: {e}")

    # Add this block to start the new periodic keep-alive log task
    if not periodic_keep_alive_ping.is_running():
        try:
            periodic_keep_alive_ping.start()
            print_info("定期キープアライブPINGタスク開始。")
        except RuntimeError as e:
            print_warning(f"定期キープアライブPINGタスク開始エラー: {e}")


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return
    channels_to_check_ids = set()
    if before.channel: channels_to_check_ids.add(before.channel.id)
    if after.channel: channels_to_check_ids.add(after.channel.id)
    for original_cid in channels_to_check_ids:
        if original_cid in vc_tracking:
            track_info = vc_tracking.get(original_cid)
            if not track_info: print_debug(f"[on_voice_state_update] VC {original_cid} no longer in tracking. Skipping."); continue
            guild = bot.get_guild(track_info["guild_id"])
            if not guild: print_warning(f"[on_voice_state_update] Guild {track_info['guild_id']} for VC {original_cid} not found. Skipping."); continue
            original_vc = guild.get_channel(original_cid)
            status_vc_id = track_info.get("status_channel_id")
            status_vc = guild.get_channel(status_vc_id) if status_vc_id else None
            if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
                print_debug(f"[on_voice_state_update] Relevant update for tracked VC ID: {original_cid}. Scheduling name update.")
                asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc), name=f"UpdateTask-VoiceState-{original_cid}")


@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): return
    if channel.category and STATUS_CATEGORY_NAME.lower() in channel.category.name.lower(): print_debug(f"[on_guild_channel_create] New VC {channel.name} is a status channel. Ignoring."); return
    if channel.id in vc_tracking or any(info.get("status_channel_id") == channel.id for info in vc_tracking.values()): print_debug(f"[on_guild_channel_create] New VC {channel.name} already known. Ignoring."); return
    print_info(f"[on_guild_channel_create] New VC 「{channel.name}」 (ID: {channel.id}) 作成。自動追跡試行。")
    asyncio.create_task(register_new_vc_for_tracking(channel), name=f"RegisterTask-ChannelCreate-{channel.id}")


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): return
    original_channel_id_to_process = None; is_status_vc_deleted = False
    guild_where_deleted = channel.guild
    if channel.id in vc_tracking: original_channel_id_to_process = channel.id; print_info(f"[on_guild_channel_delete] Tracked original VC {channel.name} deleted.")
    else:
        for ocid, info in list(vc_tracking.items()):
            if info.get("status_channel_id") == channel.id and info.get("guild_id") == guild_where_deleted.id:
                original_channel_id_to_process = ocid; is_status_vc_deleted = True; print_info(f"[on_guild_channel_delete] Status VC {channel.name} for original ID {ocid} deleted."); break
    if original_channel_id_to_process:
        print_info(f"[on_guild_channel_delete] Processing deletion for original VC ID: {original_channel_id_to_process}")
        async def handle_deletion_logic_wrapper(ocid, deleted_is_status, g_obj):
            task_name_del = asyncio.current_task().get_name() if asyncio.current_task() else "DelWrapTask"
            if vc_processing_flags.get(ocid): print_debug(f"[{task_name_del}] VC ID {ocid} は他で処理中のため削除処理スキップ。"); return
            vc_processing_flags[ocid] = True; print_debug(f"[{task_name_del}] Processing flag SET for VC ID {ocid}")
            try:
                if deleted_is_status:
                    original_vc_obj = g_obj.get_channel(ocid) if g_obj else None
                    if original_vc_obj and isinstance(original_vc_obj, discord.VoiceChannel):
                        print_info(f"[{task_name_del}] Original VC {original_vc_obj.name} still exists. Recreating status VC.")
                        await unregister_vc_tracking_internal(ocid, g_obj, is_internal_call=True)
                        new_status_vc = await _create_status_vc_for_original(original_vc_obj)
                        if new_status_vc:
                            vc_tracking[ocid] = {"guild_id": original_vc_obj.guild.id, "status_channel_id": new_status_vc.id, "original_channel_name": original_vc_obj.name}
                            await save_tracked_original_to_db(ocid, original_vc_obj.guild.id, new_status_vc.id, original_vc_obj.name)
                            asyncio.create_task(update_dynamic_status_channel_name(original_vc_obj, new_status_vc), name=f"UpdateTask-PostDeleteRecreate-{ocid}")
                            print_info(f"[{task_name_del}] Status VC for {original_vc_obj.name} recreated.")
                        else: print_error(f"[{task_name_del}] Failed to recreate status VC for {original_vc_obj.name}.")
                    else: print_info(f"[{task_name_del}] Original VC {ocid} not found after status VC deletion. Unregistering."); await unregister_vc_tracking_internal(ocid, g_obj, is_internal_call=True)
                else: await unregister_vc_tracking_internal(ocid, g_obj, is_internal_call=True)
            except Exception as e: print_error(f"[{task_name_del}] Error in deletion logic for {ocid}: {e}", exc_info=True)
            finally: vc_processing_flags.pop(ocid, None); print_debug(f"[{task_name_del}] Processing flag CLEARED for VC ID {ocid}")
        asyncio.create_task(handle_deletion_logic_wrapper(original_channel_id_to_process, is_status_vc_deleted, guild_where_deleted), name=f"DeleteTask-{original_channel_id_to_process}")


@tasks.loop(minutes=3)
async def periodic_status_update():
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "PeriodicTaskLoop"
    print_debug(f"[{task_name}] 定期ステータス更新タスク実行中... 現在追跡中: {len(vc_tracking)}件")
    if not vc_tracking: return
    for original_cid in list(vc_tracking.keys()):
        print_debug(f"[{task_name}|periodic_update] Processing VC ID: {original_cid}")
        track_info = vc_tracking.get(original_cid);
        if not track_info: print_warning(f"[{task_name}|periodic_update] VC {original_cid} no longer in tracking. Skipping."); continue
        guild = bot.get_guild(track_info["guild_id"])
        if not guild: print_warning(f"[{task_name}|periodic_update] Guild {track_info['guild_id']} for VC {original_cid} not found. Scheduling unreg."); asyncio.create_task(unregister_vc_tracking(original_cid, None), name=f"UnregTask-Periodic-NoGuild-{original_cid}"); continue
        original_vc = guild.get_channel(original_cid)
        status_vc = guild.get_channel(track_info.get("status_channel_id")) if track_info.get("status_channel_id") else None

        if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
            if status_vc.category is None or STATUS_CATEGORY_NAME.lower() not in status_vc.category.name.lower():
                print_warning(f"[{task_name}|periodic_update] Status VC {status_vc.name} for {original_vc.name} in wrong category. Scheduling fix.")
                async def fix_cat_task(ovc, g):
                    fix_task_name_inner = asyncio.current_task().get_name() if asyncio.current_task() else "FixCatInnerTask"
                    if vc_processing_flags.get(ovc.id): print_debug(f"[{fix_task_name_inner}] VC {ovc.id} already processing, category fix skipped."); return
                    vc_processing_flags[ovc.id] = True; print_debug(f"[{fix_task_name_inner}] Flag SET for category fix {ovc.id}")
                    try: await unregister_vc_tracking_internal(ovc.id, g, is_internal_call=True); await register_new_vc_for_tracking(ovc)
                    except Exception as e: print_error(f"[{fix_task_name_inner}] Error fixing category for {ovc.id}: {e}", exc_info=True)
                    finally: vc_processing_flags.pop(ovc.id, None); print_debug(f"[{fix_task_name_inner}] Flag CLEARED for category fix {ovc.id}")
                asyncio.create_task(fix_cat_task(original_vc, guild), name=f"FixCategoryTask-{original_cid}"); continue
            asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc), name=f"UpdateTask-Periodic-{original_cid}")
        elif not isinstance(original_vc, discord.VoiceChannel) and original_cid in vc_tracking:
            print_warning(f"[{task_name}|periodic_update] Original VC {original_cid} invalid. Scheduling unreg.")
            asyncio.create_task(unregister_vc_tracking(original_cid, guild), name=f"UnregTask-Periodic-InvalidOrig-{original_cid}")
        elif isinstance(original_vc, discord.VoiceChannel) and not isinstance(status_vc, discord.VoiceChannel):
            print_warning(f"[{task_name}|periodic_update] Status VC for {original_vc.name} missing. Scheduling recreate.")
            async def recreate_status_task(ovc, g):
                recreate_task_name_inner = asyncio.current_task().get_name() if asyncio.current_task() else "RecreateStatusInnerTask"
                if vc_processing_flags.get(ovc.id): print_debug(f"[{recreate_task_name_inner}] VC {ovc.id} already processing, status recreate skipped."); return
                vc_processing_flags[ovc.id] = True; print_debug(f"[{recreate_task_name_inner}] Flag SET for status recreate {ovc.id}")
                try: await unregister_vc_tracking_internal(ovc.id, g, is_internal_call=True); await register_new_vc_for_tracking(ovc)
                except Exception as e: print_error(f"[{recreate_task_name_inner}] Error recreating status VC for {ovc.id}: {e}", exc_info=True)
                finally: vc_processing_flags.pop(ovc.id, None); print_debug(f"[{recreate_task_name_inner}] Flag CLEARED for status recreate {ovc.id}")
            asyncio.create_task(recreate_status_task(original_vc, guild), name=f"RecreateStatusTask-{original_cid}")
        elif original_cid in vc_tracking:
            print_warning(f"[{task_name}|periodic_update] Generic invalid state for VC {original_cid}. Scheduling unreg.")
            asyncio.create_task(unregister_vc_tracking(original_cid, guild), name=f"UnregTask-Periodic-GenericInvalid-{original_cid}")

# --- New Periodic Keep-Alive Ping Task ---
@tasks.loop(minutes=1)
async def periodic_keep_alive_ping():
    """1分ごとにログを出力してRenderのスリープを防ぐニャ。"""
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "KeepAlivePingTask"
    print_info(f"[{task_name}] Periodic keep-alive log: POST HOST")

# --- Bot Commands (nah, nah_vc, nah_help and their error handlers remain largely the same as V12) ---
@bot.command(name='nah', help="指定した数のメッセージを削除するニャ。 例: !!nah 5")
@commands.has_permissions(manage_messages=True)
@commands.bot_has_permissions(manage_messages=True)
async def nah_command(ctx, num: int):
    if num <= 0: await ctx.send("1以上の数を指定してニャ🐈"); return
    if num > 100: await ctx.send("一度に削除できるのは100件までニャ🐈"); return
    try:
        deleted_messages = await ctx.channel.purge(limit=num + 1)
        response_msg = await ctx.send(f"{len(deleted_messages) -1}件のメッセージを削除したニャ🐈")
        await asyncio.sleep(5); await response_msg.delete()
    except discord.Forbidden: await ctx.send("メッセージを削除する権限がないニャ😿")
    except discord.HTTPException as e: print_error(f"nahコマンドHTTPエラー: {e}", exc_info=True); await ctx.send(f"メッセージ削除中エラーニャ😿: {e.text}")
    except Exception as e: print_error(f"nahコマンドエラー: {e}", exc_info=True); await ctx.send(f"エラー発生ニャ😿: {e}")

@nah_command.error
async def nah_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions): await ctx.send("このコマンドの権限がニャい… (メッセージ管理権限が必要だニャ)")
    elif isinstance(error, commands.BotMissingPermissions): await ctx.send("ボットにメッセージ削除権限がないニャ😿")
    elif isinstance(error, commands.BadArgument): await ctx.send("数の指定がおかしいニャ。例: `!!nah 5`")
    else: print_error(f"nah_command 未処理エラー: {error}", exc_info=True); await ctx.send("コマンド実行中予期せぬエラー発生ニャ。")

@bot.command(name='nah_vc', help="指定VCの人数表示用チャンネルを作成/削除するニャ。")
@commands.has_permissions(manage_channels=True)
@commands.bot_has_permissions(manage_channels=True)
async def nah_vc_command(ctx, *, channel_id_or_name: str):
    guild = ctx.guild
    if not guild: await ctx.send("このコマンドはサーバー内でのみ使用可能ですニャ🐈"); return
    target_vc = None
    try: vc_id = int(channel_id_or_name); target_vc = guild.get_channel(vc_id)
    except ValueError:
        for vc_iter in guild.voice_channels:
            if vc_iter.name.lower() == channel_id_or_name.lower(): target_vc = vc_iter; break
        if not target_vc:
            for vc_iter in guild.voice_channels:
                if channel_id_or_name.lower() in vc_iter.name.lower(): target_vc = vc_iter; print_info(f"VC名「{channel_id_or_name}」の部分一致で「{vc_iter.name}」を使用。"); break
    if not target_vc or not isinstance(target_vc, discord.VoiceChannel): await ctx.send(f"「{channel_id_or_name}」はボイスチャンネルとして見つからなかったニャ😿"); return
    if target_vc.category and STATUS_CATEGORY_NAME.lower() in target_vc.category.name.lower(): await ctx.send(f"VC「{target_vc.name}」はSTATUSチャンネルのようだニャ。元のVCを指定してニャ。"); return
    await ctx.send(f"VC「{target_vc.name}」の追跡設定/解除処理を開始したニャ。完了まで少し待ってニャ。")
    if target_vc.id in vc_tracking:
        print_info(f"コマンド: VC「{target_vc.name}」の追跡解除を試みます。")
        asyncio.create_task(unregister_vc_tracking(target_vc.id, guild, send_feedback_to_ctx=ctx), name=f"UnregisterTask-Command-{target_vc.id}")
    else:
        print_info(f"コマンド: VC「{target_vc.name}」の新規追跡を試みます。")
        asyncio.create_task(register_new_vc_for_tracking(target_vc, send_feedback_to_ctx=ctx), name=f"RegisterTask-Command-{target_vc.id}")

@nah_vc_command.error
async def nah_vc_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions): await ctx.send("このコマンドの権限がニャい… (チャンネル管理権限が必要だニャ)")
    elif isinstance(error, commands.BotMissingPermissions): await ctx.send("ボットにチャンネル管理権限がないニャ😿")
    elif isinstance(error, commands.MissingRequiredArgument): await ctx.send("どのボイスチャンネルか指定してニャ！ 例: `!!nah_vc General`")
    else: print_error(f"nah_vc_command 未処理エラー: {error}", exc_info=True); await ctx.send("コマンド実行中予期せぬエラー発生ニャ。")

@bot.command(name='nah_help', help="コマンド一覧を表示するニャ。")
async def nah_help_prefix(ctx: commands.Context): await ctx.send(HELP_TEXT_CONTENT)

# --- Main Bot Execution ---
async def start_bot_main():
    if DISCORD_TOKEN is None: print_error("DISCORD_TOKEN未設定。Bot起動不可。"); return
    if os.getenv("RENDER"): keep_alive()
    try:
        print_info("Bot非同期処理開始...")
        await bot.start(DISCORD_TOKEN)
    except discord.LoginFailure: print_error("Discordログイン失敗。トークン確認を。", exc_info=True)
    except Exception as e: print_error(f"Bot起動/実行中エラー: {e}", exc_info=True)
    finally:
        if bot.is_connected() and not bot.is_closed():
            print_info("Botシャットダウン中...")
            try: await bot.close()
            except Exception as e: print_error(f"Botシャットダウンエラー: {e}", exc_info=True)
        print_info("Botシャットダウン完了。")

# --- Entry Point ---
if __name__ == "__main__":
    try: asyncio.run(start_bot_main())
    except KeyboardInterrupt: print_info("ユーザーによりBot停止 (KeyboardInterrupt)。")
    except Exception as e: print_error(f"メイン実行ループエラー: {e}", exc_info=True)

