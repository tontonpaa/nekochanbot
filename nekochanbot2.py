# Ensure sys is imported early for print diagnostics if logging fails
import sys
import os
import traceback # トレースバックを出力するためにインポート

# --- Custom Print Logging Configuration ---
# --- カスタムprintロギング設定 ---
DEBUG_PRINT_ENABLED = os.getenv("DEBUG_PRINT_ENABLED", "false").lower() == "true"
LOG_LEVEL_PRINT_ENV = os.getenv("LOG_LEVEL_PRINT", "INFO").upper()
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
# --- カスタムprintロギング設定ここまで ---

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
# --- Flaskアプリによる常時起動設定 ---
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
# --- BotのIntents設定 ---
intents = discord.Intents.default(); intents.guilds = True; intents.voice_states = True; intents.message_content = True

# --- Firestore Client and Constants ---
# --- Firestoreクライアントと定数 ---
db = None
FIRESTORE_COLLECTION_NAME = "discord_tracked_original_vcs_prod_v4"
SUMMARY_FIRESTORE_COLLECTION_NAME = "discord_summary_vcs_prod_v1"
STATUS_CATEGORY_NAME = "STATUS"

# --- VC Tracking Dictionaries and State ---
# --- VC追跡用の辞書と状態 ---
vc_tracking = {}
summary_vc_tracking = {}
vc_processing_flags = {}
summary_vc_processing_flags = {}
command_cooldowns = {} # NEW: コマンドの二重実行防止用

# --- Cooldown and State Settings ---
# --- クールダウンと状態に関する設定 ---
API_CALL_TIMEOUT = 20.0
DB_CALL_TIMEOUT = 15.0
ZERO_USER_TIMEOUT_DURATION = timedelta(minutes=5)
vc_zero_stats = {}
vc_discord_api_cooldown_until = {}
summary_vc_api_cooldown_until = {}

# --- Help Text ---
# --- ヘルプテキスト ---
HELP_TEXT_CONTENT = (
    "📘 **コマンド一覧だニャ🐈**\n\n"
    "🔹 `!!nah [数]`\n"
    "→ 指定した数のメッセージをこのチャンネルから削除するニャ。\n"
    "   例: `!!nah 5`\n\n"
    "🔹 `!!nah_vc [VCのチャンネルIDまたは名前]`\n"
    "→ 指定したボイスチャンネルの人数表示用チャンネルを「STATUS」カテゴリに作成/削除するニャ。(トグル式)\n"
    "   ONにすると、STATUSカテゴリに `[元VC名]：〇 users` という名前のVCが作られ、5分毎に人数が更新されるニャ。\n"
    "   OFFにすると、その人数表示用チャンネルを削除し、追跡を停止するニャ。\n"
    "   例: `!!nah_vc General Voice` または `!!nah_vc 123456789012345678`\n\n"
    "🔹 `!!nah_sum`\n"
    "→ このサーバーにあるすべてのVC接続人数を集計する鍵付きVCを作成/削除するニャ🐈\n\n"
    "🔹 `!!nah_help` または `/nah_help`\n"
    "→ このヘルプメッセージを表示するニャ🐈\n"
)

# --- Custom Bot Class for Slash Commands ---
# --- スラッシュコマンド用のカスタムBotクラス ---
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

# --- Persistence Functions ---
async def load_tracked_channels_from_db():
    if not db: return
    global vc_tracking; vc_tracking = {}
    try:
        stream = db.collection(FIRESTORE_COLLECTION_NAME).stream()
        async for doc_snapshot in stream:
            doc_data = doc_snapshot.to_dict()
            try:
                vc_tracking[int(doc_snapshot.id)] = {
                    "guild_id": int(doc_data["guild_id"]),
                    "status_channel_id": int(doc_data["status_channel_id"]),
                    "original_channel_name": doc_data["original_channel_name"]
                }
            except (ValueError, TypeError, KeyError):
                print_warning(f"DBドキュメント {doc_snapshot.id} データ解析エラー。スキップ。")
        print_info(f"{len(vc_tracking)}件の追跡VC情報をDBからロード完了。")
    except Exception as e: print_error(f"Firestoreデータロード中エラー: {e}", exc_info=True)

async def save_tracked_original_to_db(original_channel_id, guild_id, status_channel_id, original_channel_name):
    if not db: return
    try:
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await doc_ref.set({
            "guild_id": guild_id,
            "status_channel_id": status_channel_id,
            "original_channel_name": original_channel_name
        })
    except Exception as e: print_error(f"Firestore書き込みエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def remove_tracked_original_from_db(original_channel_id):
    if not db: return
    try:
        await db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id)).delete()
    except Exception as e: print_error(f"Firestore削除エラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def load_summary_vcs_from_db():
    if not db: return
    global summary_vc_tracking; summary_vc_tracking = {}
    try:
        stream = db.collection(SUMMARY_FIRESTORE_COLLECTION_NAME).stream()
        async for doc_snapshot in stream:
            doc_data = doc_snapshot.to_dict()
            try:
                summary_vc_tracking[int(doc_snapshot.id)] = int(doc_data["summary_vc_id"])
            except (ValueError, TypeError, KeyError):
                print_warning(f"サマリーVC DBドキュメント {doc_snapshot.id} データ解析エラー。スキップ。")
        print_info(f"{len(summary_vc_tracking)}件のサマリーVC情報をDBからロード完了。")
    except Exception as e: print_error(f"サマリーVCのFirestoreデータロード中エラー: {e}", exc_info=True)

async def save_summary_vc_to_db(guild_id, summary_vc_id):
    if not db: return
    try:
        await db.collection(SUMMARY_FIRESTORE_COLLECTION_NAME).document(str(guild_id)).set({"summary_vc_id": summary_vc_id})
    except Exception as e: print_error(f"サマリーVCのFirestore書き込みエラー (Guild ID: {guild_id}): {e}", exc_info=True)

async def remove_summary_vc_from_db(guild_id):
    if not db: return
    try:
        await db.collection(SUMMARY_FIRESTORE_COLLECTION_NAME).document(str(guild_id)).delete()
    except Exception as e: print_error(f"サマリーVCのFirestore削除エラー (Guild ID: {guild_id}): {e}", exc_info=True)

# --- Core Logic Functions ---
async def get_or_create_status_category(guild: discord.Guild):
    for category in guild.categories:
        if STATUS_CATEGORY_NAME.lower() in category.name.lower():
            return category
    try:
        overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=True, connect=False)}
        return await guild.create_category(STATUS_CATEGORY_NAME, overwrites=overwrites)
    except Exception as e:
        print_error(f"カテゴリ「{STATUS_CATEGORY_NAME}」作成中エラー: {e}", exc_info=True)
        return None

async def update_dynamic_status_channel_name(original_vc, status_vc):
    if not original_vc or not status_vc: return
    ovc_id = original_vc.id
    if vc_processing_flags.get(ovc_id): return
    vc_processing_flags[ovc_id] = True
    try:
        base_name = status_vc.name.split("：")[0].strip() if "：" in status_vc.name else vc_tracking.get(ovc_id, {}).get("original_channel_name", original_vc.name)
        count = len([m for m in original_vc.members if not m.bot])
        new_name = f"{base_name}：{count} users"
        if new_name != status_vc.name:
            await status_vc.edit(name=new_name, reason="個別VC人数更新")
    except Exception as e:
        print_error(f"個別VC名更新エラー (VC ID: {ovc_id}): {e}", exc_info=True)
    finally:
        vc_processing_flags.pop(ovc_id, None)

async def update_summary_vc_name(guild):
    guild_id = guild.id
    if summary_vc_processing_flags.get(guild_id): return
    summary_vc_processing_flags[guild_id] = True
    try:
        summary_vc_id = summary_vc_tracking.get(guild_id)
        if not summary_vc_id: return
        summary_vc = guild.get_channel(summary_vc_id)
        if not isinstance(summary_vc, discord.VoiceChannel):
            summary_vc_tracking.pop(guild_id, None)
            await remove_summary_vc_from_db(guild_id)
            return

        base_name = summary_vc.name.split("：")[0].strip() if "：" in summary_vc.name else "Study/Work"
        total_user_count = sum(len([m for m in vc.members if not m.bot]) for vc in guild.voice_channels if not (vc.category and STATUS_CATEGORY_NAME.lower() in vc.category.name.lower()))
        new_name = f"{base_name}：{total_user_count} users"
        if new_name != summary_vc.name:
            await summary_vc.edit(name=new_name, reason="サーバー全体のVC参加人数更新")
    except Exception as e:
        print_error(f"サマリーVC名更新エラー (Guild ID: {guild_id}): {e}", exc_info=True)
    finally:
        summary_vc_processing_flags.pop(guild_id, None)

async def register_new_vc_for_tracking(original_vc, send_feedback_to_ctx=None):
    if vc_processing_flags.get(original_vc.id): return
    vc_processing_flags[original_vc.id] = True
    try:
        guild = original_vc.guild
        status_category = await get_or_create_status_category(guild)
        if not status_category:
            if send_feedback_to_ctx: await send_feedback_to_ctx.send("STATUSカテゴリの作成に失敗しましたニャ😿")
            return
        
        count = len([m for m in original_vc.members if not m.bot])
        status_channel_name = f"{original_vc.name[:65]}：{count} users"
        overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=True, connect=False)}
        
        new_status_vc = await guild.create_voice_channel(name=status_channel_name, category=status_category, overwrites=overwrites)
        vc_tracking[original_vc.id] = {"guild_id": guild.id, "status_channel_id": new_status_vc.id, "original_channel_name": original_vc.name}
        await save_tracked_original_to_db(original_vc.id, guild.id, new_status_vc.id, original_vc.name)
        if send_feedback_to_ctx: await send_feedback_to_ctx.send(f"VC「{original_vc.name}」の追跡を開始したニャ。")

    except Exception as e:
        print_error(f"新規VC追跡エラー (VC ID: {original_vc.id}): {e}", exc_info=True)
        if send_feedback_to_ctx: await send_feedback_to_ctx.send("追跡開始中にエラーが発生しましたニャ😿")
    finally:
        vc_processing_flags.pop(original_vc.id, None)

async def unregister_vc_tracking(original_channel_id, guild, send_feedback_to_ctx=None):
    if vc_processing_flags.get(original_channel_id): return
    vc_processing_flags[original_channel_id] = True
    try:
        track_info = vc_tracking.pop(original_channel_id, None)
        if track_info:
            status_vc_id = track_info["status_channel_id"]
            status_vc = guild.get_channel(status_vc_id)
            if status_vc:
                await status_vc.delete(reason="追跡停止")
            await remove_tracked_original_from_db(original_channel_id)
        if send_feedback_to_ctx:
            vc_name = track_info.get("original_channel_name", f"ID: {original_channel_id}") if track_info else f"ID: {original_channel_id}"
            await send_feedback_to_ctx.send(f"VC「{vc_name}」の追跡を停止したニャ。")
    except Exception as e:
        print_error(f"VC追跡解除エラー (VC ID: {original_channel_id}): {e}", exc_info=True)
        if send_feedback_to_ctx: await send_feedback_to_ctx.send("追跡停止中にエラーが発生しましたニャ�")
    finally:
        vc_processing_flags.pop(original_channel_id, None)

# --- Bot Events ---
@bot.event
async def on_ready():
    print_info(f'ログイン成功: {bot.user.name}')
    await bot.change_presence(activity=discord.CustomActivity(name="VCの人数を見守り中ニャ～"))
    if await init_firestore():
        await load_tracked_channels_from_db()
        await load_summary_vcs_from_db()
    periodic_status_update.start()
    periodic_keep_alive_ping.start()

@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot: return
    guild = member.guild
    channels_to_update = set()
    if before.channel: channels_to_update.add(before.channel.id)
    if after.channel: channels_to_update.add(after.channel.id)

    for cid in channels_to_update:
        if cid in vc_tracking:
            track_info = vc_tracking[cid]
            original_vc = guild.get_channel(cid)
            status_vc = guild.get_channel(track_info.get("status_channel_id"))
            if original_vc and status_vc:
                asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc))
    
    if guild.id in summary_vc_tracking:
        asyncio.create_task(update_summary_vc_name(guild))

# --- Bot Tasks ---
@tasks.loop(minutes=3)
async def periodic_status_update():
    for original_cid, track_info in list(vc_tracking.items()):
        guild = bot.get_guild(track_info["guild_id"])
        if guild:
            original_vc = guild.get_channel(original_cid)
            status_vc = guild.get_channel(track_info.get("status_channel_id"))
            if original_vc and status_vc:
                asyncio.create_task(update_dynamic_status_channel_name(original_vc, status_vc))
    
    for guild_id in list(summary_vc_tracking.keys()):
        guild = bot.get_guild(guild_id)
        if guild:
            asyncio.create_task(update_summary_vc_name(guild))

@tasks.loop(minutes=1)
async def periodic_keep_alive_ping():
    print_info("Periodic keep-alive log")

# --- Bot Commands ---
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
    except Exception as e:
        print_error(f"nahコマンドエラー: {e}", exc_info=True)

@nah_command.error
async def nah_command_error(ctx, error):
    if isinstance(error, (commands.MissingPermissions, commands.BotMissingPermissions)):
        print_error(f"nah_commandで権限エラー: {error}")
        return
    elif isinstance(error, commands.BadArgument):
        await ctx.send("数の指定がおかしいニャ。例: `!!nah 5`")
    else:
        print_error(f"nah_command 未処理エラー: {error}", exc_info=True)
        await ctx.send("コマンド実行中に予期せぬエラー発生ニャ。")

@bot.command(name='nah_vc', help="指定VCの人数表示用チャンネルを作成/削除するニャ。")
@commands.has_permissions(manage_channels=True)
@commands.bot_has_permissions(manage_channels=True)
async def nah_vc_command(ctx, *, channel_id_or_name: str):
    guild = ctx.guild
    if not guild: return
    target_vc = None
    try: vc_id = int(channel_id_or_name); target_vc = guild.get_channel(vc_id)
    except ValueError:
        for vc_iter in guild.voice_channels:
            if vc_iter.name.lower() == channel_id_or_name.lower(): target_vc = vc_iter; break
        if not target_vc:
            for vc_iter in guild.voice_channels:
                if channel_id_or_name.lower() in vc_iter.name.lower(): target_vc = vc_iter; break
    if not target_vc or not isinstance(target_vc, discord.VoiceChannel) or (target_vc.category and STATUS_CATEGORY_NAME.lower() in target_vc.category.name.lower()):
        await ctx.send(f"「{channel_id_or_name}」は有効なボイスチャンネルとして見つからなかったニャ😿"); return
    
    if target_vc.id in vc_tracking:
        await unregister_vc_tracking(target_vc.id, guild, send_feedback_to_ctx=ctx)
    else:
        await register_new_vc_for_tracking(target_vc, send_feedback_to_ctx=ctx)

@nah_vc_command.error
async def nah_vc_command_error(ctx, error):
    if isinstance(error, (commands.MissingPermissions, commands.BotMissingPermissions)):
        print_error(f"nah_vc_commandで権限エラー: {error}")
        return
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("どのボイスチャンネルか指定してニャ！ 例: `!!nah_vc General`")
    else:
        print_error(f"nah_vc_command 未処理エラー: {error}", exc_info=True)
        await ctx.send("コマンド実行中に予期せぬエラー発生ニャ。")

@bot.command(name='nah_sum', help="サーバー全体のVC接続人数を集計する鍵付きVCを作成/削除するニャ。")
@commands.has_permissions(manage_channels=True)
@commands.bot_has_permissions(manage_channels=True)
async def nah_sum_command(ctx):
    guild = ctx.guild
    if not guild: return
    guild_id = guild.id
    now = datetime.now(timezone.utc)

    # 5秒のクールダウン
    last_run = command_cooldowns.get(guild_id)
    if last_run and (now - last_run) < timedelta(seconds=5):
        print_info(f"nah_sum command for guild {guild_id} is on cooldown.")
        return
    command_cooldowns[guild_id] = now
    
    if summary_vc_processing_flags.get(guild_id):
        return
    summary_vc_processing_flags[guild_id] = True

    try:
        existing_summary_vc_id = summary_vc_tracking.get(guild_id)
        if existing_summary_vc_id:
            await ctx.send("集計用チャンネルを削除しますニャ...", delete_after=5)
            summary_vc = guild.get_channel(existing_summary_vc_id)
            if summary_vc:
                await summary_vc.delete(reason="nah_sumコマンドによる削除")
            summary_vc_tracking.pop(guild_id, None)
            await remove_summary_vc_from_db(guild_id)
            await ctx.send("サーバー全体の人数集計用チャンネルを削除したニャ。", delete_after=5)
        else:
            await ctx.send("集計用チャンネルを作成しますニャ...", delete_after=5)
            status_category = await get_or_create_status_category(guild)
            if not status_category:
                await ctx.send("STATUSカテゴリの作成/取得に失敗しましたニャ😿", delete_after=10)
                return
            initial_name = "Study/Work：集計中... users"
            overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=True, connect=False)}
            new_summary_vc = await guild.create_voice_channel(name=initial_name, category=status_category, overwrites=overwrites)
            summary_vc_tracking[guild_id] = new_summary_vc.id
            await save_summary_vc_to_db(guild_id, new_summary_vc.id)
            asyncio.create_task(update_summary_vc_name(guild))
            await ctx.send("サーバー全体の人数集計用チャンネルを作成したニャ！", delete_after=5)
    except Exception as e:
        print_error(f"nah_sumコマンドエラー: {e}", exc_info=True)
        await ctx.send("コマンドの実行中にエラーが発生しましたニャ😿", delete_after=10)
    finally:
        summary_vc_processing_flags.pop(guild_id, None)

@nah_sum_command.error
async def nah_sum_command_error(ctx, error):
    if isinstance(error, (commands.MissingPermissions, commands.BotMissingPermissions)):
        print_error(f"nah_sum_commandで権限エラー: {error}")
        return
    else:
        print_error(f"nah_sum_command 未処理エラー: {error}", exc_info=True)
        await ctx.send("コマンド実行中に予期せぬエラーが発生しましたニャ。")

@bot.command(name='nah_help', help="コマンド一覧を表示するニャ。")
async def nah_help_prefix(ctx): await ctx.send(HELP_TEXT_CONTENT)

# --- Main Bot Execution ---
async def start_bot_main():
    if not DISCORD_TOKEN:
        print_error("DISCORD_TOKEN未設定。Bot起動不可。")
        return
    if os.getenv("RENDER"): keep_alive()
    async with bot:
        await bot.start(DISCORD_TOKEN)

if __name__ == "__main__":
    try:
        asyncio.run(start_bot_main())
    except KeyboardInterrupt:
        print_info("ユーザーによりBot停止。")
    except Exception as e:
        print_error(f"メイン実行ループエラー: {e}", exc_info=True)
