# ... (既存のimport文や設定はそのまま) ...
from dotenv import load_dotenv
load_dotenv() # .envファイルから環境変数を読み込む
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

# --- Flask App for Keep Alive ---
app = Flask('')

@app.route('/')
def home():
    return "I'm alive"

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    """Flaskサーバーを別スレッドで起動する"""
    flask_thread = Thread(target=run_flask)
    flask_thread.start()
    logger.info(f"Keep-aliveサーバーがポート {os.environ.get('PORT', 8080)} で起動準備完了。")

# --- ロギング設定 ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- BotのIntents設定 ---
intents = discord.Intents.all()
intents.message_content = True # Ensure message content intent is enabled if needed for prefix commands

# --- Firestoreクライアント ---
db = None
FIRESTORE_COLLECTION_NAME = "discord_tracked_original_vcs_prod_v2" # Firestore collection name
STATUS_CATEGORY_NAME = "STATUS" # Name of the category for status VCs

# --- VC追跡用辞書など ---
vc_tracking = {} # Stores info about tracked VCs
vc_locks = {}    # For managing concurrent access to VC data

# --- New Cooldown and State Settings ---
BOT_UPDATE_WINDOW_DURATION = timedelta(minutes=5)  # 5-minute window for bot's own rate limiting
MAX_UPDATES_IN_WINDOW = 2                       # Max updates allowed by bot within its window

# vc_id をキーとする辞書
# Stores {"window_start_time": datetime, "count": int} for bot's rate limiting
vc_rate_limit_windows = {}
# Stores {"zero_since": datetime, "notified_zero_explicitly": bool} for 0-user rule
vc_zero_stats = {}
# Stores datetime until which Discord API cooldown is active for a VC
vc_discord_api_cooldown_until = {}


# --- Help Text (Global for easy access) ---
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
    vc_tracking = {}
    try:
        logger.info(f"Firestoreから追跡VC情報を読み込んでいます (コレクション: {FIRESTORE_COLLECTION_NAME})...")
        stream = db.collection(FIRESTORE_COLLECTION_NAME).stream()
        async for doc_snapshot in stream:
            doc_data = doc_snapshot.to_dict()
            original_channel_id = int(doc_snapshot.id)
            guild_id = doc_data.get("guild_id")
            status_channel_id = doc_data.get("status_channel_id")
            original_channel_name = doc_data.get("original_channel_name")

            if not all([guild_id, status_channel_id, original_channel_name]):
                logger.warning(f"DB内のドキュメント {doc_snapshot.id} に必要な情報が不足しています。スキップします。")
                continue

            vc_tracking[original_channel_id] = {
                "guild_id": guild_id,
                "status_channel_id": status_channel_id,
                "original_channel_name": original_channel_name
            }
            logger.info(f"DBからロード: Original VC ID {original_channel_id} (Guild ID: {guild_id}), Status VC ID: {status_channel_id}, Original Name: '{original_channel_name}'")
        logger.info(f"{len(vc_tracking)}件の追跡VC情報をDBからロードしました。")
    except Exception as e:
        logger.error(f"Firestoreからのデータ読み込み中にエラー: {e}", exc_info=True)

async def save_tracked_original_to_db(original_channel_id: int, guild_id: int, status_channel_id: int, original_channel_name: str):
    if not db:
        return
    try:
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await doc_ref.set({
            "guild_id": guild_id,
            "status_channel_id": status_channel_id,
            "original_channel_name": original_channel_name,
            "updated_at": firestore.SERVER_TIMESTAMP
        })
        logger.debug(f"DBに保存: Original VC ID {original_channel_id}, Status VC ID {status_channel_id}")
    except Exception as e:
        logger.error(f"Firestoreへのデータ書き込み中にエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

async def remove_tracked_original_from_db(original_channel_id: int):
    if not db:
        return
    try:
        doc_ref = db.collection(FIRESTORE_COLLECTION_NAME).document(str(original_channel_id))
        await doc_ref.delete()
        logger.info(f"DBから削除: Original VC ID {original_channel_id}")
    except Exception as e:
        logger.error(f"Firestoreからのデータ削除中にエラー (Original VC ID: {original_channel_id}): {e}", exc_info=True)

# --- Channel Management Helper Functions ---
async def get_or_create_status_category(guild: discord.Guild) -> discord.CategoryChannel | None:
    for category in guild.categories:
        if STATUS_CATEGORY_NAME in category.name:
            logger.info(f"カテゴリ「{category.name}」をSTATUSカテゴリとして使用します。(Guild: {guild.name})")
            return category
    try:
        logger.info(f"「STATUS」を含むカテゴリが見つからなかったため、新規作成します。(Guild: {guild.name})")
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_message_history=True, view_channel=True)
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
    user_count = min(user_count, 999)

    status_channel_name_base = original_vc.name
    status_channel_name = f"{status_channel_name_base}：{user_count} users"
    status_channel_name = re.sub(r'\s{2,}', ' ', status_channel_name).strip()[:100]

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(
            view_channel=True, read_message_history=True, connect=False, speak=False, stream=False,
            send_messages=False, manage_channels=False, manage_roles=False, manage_webhooks=False,
            create_instant_invite=False, send_tts_messages=False, embed_links=False, attach_files=False,
            mention_everyone=False, use_external_emojis=False, add_reactions=False, priority_speaker=False,
            mute_members=False, deafen_members=False, move_members=False, use_voice_activation=False,
            use_embedded_activities=False
        )
    }
    try:
        new_status_vc = await guild.create_voice_channel(
            name=status_channel_name, category=status_category,
            overwrites=overwrites, reason=f"{original_vc.name} のステータス表示用VC"
        )
        logger.info(f"作成成功: Status VC「{new_status_vc.name}」(ID: {new_status_vc.id}) (Original VC: {original_vc.name})")
        return new_status_vc
    except discord.Forbidden:
        logger.error(f"Status VCの作成に失敗 (権限不足) ({original_vc.name}, Guild: {guild.name})")
    except Exception as e:
        logger.error(f"Status VCの作成に失敗 ({original_vc.name}): {e}", exc_info=True)
    return None

def get_vc_lock(vc_id: int) -> asyncio.Lock:
    """指定されたVC IDに対応するasyncio.Lockオブジェクトを取得または作成する。"""
    if vc_id not in vc_locks:
        vc_locks[vc_id] = asyncio.Lock()
    return vc_locks[vc_id]

# --- Core Tracking and Update Logic ---
async def register_new_vc_for_tracking(original_vc: discord.VoiceChannel, send_feedback_to_ctx=None):
    original_vc_id = original_vc.id
    lock = get_vc_lock(original_vc_id)

    async with lock:
        if original_vc_id in vc_tracking:
            track_info = vc_tracking[original_vc_id]
            guild_id_for_check = track_info.get("guild_id")
            if guild_id_for_check:
                guild_for_status_check = bot.get_guild(guild_id_for_check)
                if guild_for_status_check:
                    status_vc_obj = guild_for_status_check.get_channel(track_info.get("status_channel_id"))
                    if isinstance(status_vc_obj, discord.VoiceChannel) and status_vc_obj.category and STATUS_CATEGORY_NAME in status_vc_obj.category.name:
                        logger.info(f"VC {original_vc.name} (ID: {original_vc_id}) はロック取得後に確認した結果、既に有効に追跡中です。")
                        if send_feedback_to_ctx:
                            await send_feedback_to_ctx.send(f"VC「{original_vc.name}」は既に追跡中ですニャ。")
                        return False # Already effectively tracking
            logger.info(f"VC {original_vc.name} (ID: {original_vc_id}) の追跡情報が無効と判断。クリーンアップして再作成を試みます。")
            await unregister_vc_tracking_internal(original_vc_id, original_vc.guild, is_internal_call=True)
            # vc_tracking entry for original_vc_id is now removed by unregister_vc_tracking_internal

        logger.info(f"VC {original_vc.name} (ID: {original_vc_id}) の新規追跡処理を開始します。")
        new_status_vc = await _create_status_vc_for_original(original_vc)
        if new_status_vc:
            vc_tracking[original_vc_id] = {
                "guild_id": original_vc.guild.id,
                "status_channel_id": new_status_vc.id,
                "original_channel_name": original_vc.name
            }
            await save_tracked_original_to_db(original_vc_id, original_vc.guild.id, new_status_vc.id, original_vc.name)
            
            # Reset rate limit and zero stats for the newly tracked VC
            vc_rate_limit_windows.pop(original_vc_id, None)
            vc_zero_stats.pop(original_vc_id, None)
            vc_discord_api_cooldown_until.pop(original_vc_id, None)

            await update_dynamic_status_channel_name(original_vc, new_status_vc) # Attempt initial update
            logger.info(f"追跡開始/再開: Original VC {original_vc.name} (ID: {original_vc_id}), Status VC {new_status_vc.name} (ID: {new_status_vc.id})")
            return True
        else:
            logger.error(f"{original_vc.name} のステータスVC作成に失敗しました。追跡は開始されません。")
            # Ensure no partial tracking data remains if creation failed
            if original_vc_id in vc_tracking: del vc_tracking[original_vc_id]
            await remove_tracked_original_from_db(original_vc_id)
            return False

async def unregister_vc_tracking(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None):
    """Wrapper for unregister_vc_tracking_internal with lock management."""
    lock = get_vc_lock(original_channel_id)
    async with lock:
        await unregister_vc_tracking_internal(original_channel_id, guild, send_feedback_to_ctx, is_internal_call=False)

async def unregister_vc_tracking_internal(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None, is_internal_call: bool = False):
    """Core logic for unregistering a VC. Lock should be acquired by caller or via wrapper."""
    logger.info(f"VC ID {original_channel_id} の追跡解除処理を開始 (内部呼び出し: {is_internal_call})。")
    track_info = vc_tracking.pop(original_channel_id, None)
    original_vc_name_for_msg = f"ID: {original_channel_id}" # Default if not found in track_info

    if track_info:
        original_vc_name_for_msg = track_info.get("original_channel_name", f"ID: {original_channel_id}")
        status_channel_id = track_info.get("status_channel_id")
        
        current_guild = guild or (bot.get_guild(track_info.get("guild_id")) if track_info.get("guild_id") else None)

        if current_guild and status_channel_id:
            status_vc = current_guild.get_channel(status_channel_id)
            if status_vc and isinstance(status_vc, discord.VoiceChannel):
                try:
                    await status_vc.delete(reason="オリジナルVCの追跡停止のため")
                    logger.info(f"削除成功: Status VC {status_vc.name} (ID: {status_vc.id})")
                except discord.NotFound:
                    logger.info(f"Status VC {status_channel_id} は既に削除されていました。")
                except discord.Forbidden:
                    logger.error(f"Status VC {status_vc.name} (ID: {status_vc.id}) の削除に失敗 (権限不足)")
                except Exception as e:
                    logger.error(f"Status VC {status_vc.name} (ID: {status_vc.id}) の削除中にエラー: {e}", exc_info=True)
            elif status_vc :
                logger.warning(f"Status Channel ID {status_channel_id} はVCではありませんでした。削除はスキップ。")
            else:
                logger.info(f"DBに記録のあったStatus VC {status_channel_id} がGuild {current_guild.name} に見つかりませんでした。")
        elif status_channel_id:
            logger.warning(f"GuildオブジェクトなしでStatus VC {status_channel_id} の削除はスキップされました (Original VC ID: {original_channel_id})。")

    # Clean up state dictionaries
    vc_rate_limit_windows.pop(original_channel_id, None)
    vc_zero_stats.pop(original_channel_id, None)
    vc_discord_api_cooldown_until.pop(original_channel_id, None)
    
    await remove_tracked_original_from_db(original_channel_id)
    if not is_internal_call:
        logger.info(f"追跡停止完了: Original VC ID {original_channel_id} ({original_vc_name_for_msg})")
        if send_feedback_to_ctx and guild: # Guild context is needed to get channel name for feedback
            actual_original_vc = guild.get_channel(original_channel_id)
            display_name = actual_original_vc.name if actual_original_vc else original_vc_name_for_msg
            try:
                await send_feedback_to_ctx.send(f"VC「{display_name}」の人数表示用チャンネルを削除し、追跡を停止したニャ。")
            except Exception as e:
                logger.error(f"unregister_vc_tracking でのフィードバック送信中にエラー: {e}")


async def update_dynamic_status_channel_name(original_vc: discord.VoiceChannel, status_vc: discord.VoiceChannel):
    if not original_vc or not status_vc:
        logger.debug("ステータス更新スキップ: オリジナルVCまたはステータスVCが無効です。")
        return

    ovc_id = original_vc.id
    lock = get_vc_lock(ovc_id)
    async with lock:
        # Refresh channel objects from bot's cache, as passed ones might be stale
        current_original_vc = bot.get_channel(ovc_id)
        current_status_vc = bot.get_channel(status_vc.id)

        if not isinstance(current_original_vc, discord.VoiceChannel) or \
           not isinstance(current_status_vc, discord.VoiceChannel):
            logger.warning(f"Update_dynamic: Original VC {ovc_id} or Status VC {status_vc.id} became invalid after acquiring lock. Skipping update.")
            # Consider unregistering here if this happens frequently
            return
        original_vc = current_original_vc
        status_vc = current_status_vc

        now = datetime.now(timezone.utc)

        # 1. Check Discord API rate limit (from 429 errors)
        if ovc_id in vc_discord_api_cooldown_until and now < vc_discord_api_cooldown_until[ovc_id]:
            api_cooldown_expiry = vc_discord_api_cooldown_until[ovc_id]
            remaining_cooldown = (api_cooldown_expiry - now).total_seconds()
            logger.debug(f"Discord API cooldown for {original_vc.name}. Try after {api_cooldown_expiry} ({remaining_cooldown:.1f}s left). Skip.")
            return

        # --- Determine desired channel name & identify special conditions (e.g., 0 users for 5 mins) ---
        current_members = [member for member in original_vc.members if not member.bot]
        count = len(current_members)
        count = min(count, 999)

        track_info = vc_tracking.get(ovc_id)
        if not track_info:
            logger.warning(f"ステータス更新エラー: Original VC {original_vc.name} (ID: {ovc_id}) が追跡情報にありません。")
            return
        base_name = track_info.get("original_channel_name", original_vc.name)
        
        desired_name_str = f"{base_name}：{count} users"
        is_special_zero_update_condition = False # Is this a forced "0 users" update due to 5-min rule?

        # Update/check 0-user statistics
        if count == 0:
            if ovc_id not in vc_zero_stats:
                vc_zero_stats[ovc_id] = {"zero_since": now, "notified_zero_explicitly": False}
            
            zero_stat = vc_zero_stats[ovc_id]
            if now >= zero_stat["zero_since"] + BOT_UPDATE_WINDOW_DURATION and \
               not zero_stat.get("notified_zero_explicitly", False):
                desired_name_str = f"{base_name}：0 users" # Force name to "0 users"
                is_special_zero_update_condition = True
        else: # count > 0
            if ovc_id in vc_zero_stats:
                del vc_zero_stats[ovc_id] # Reset zero stats as VC is no longer empty

        # --- Compare with current channel name to see if an update is needed ---
        try:
            fresh_status_vc = await bot.fetch_channel(status_vc.id) # Get the very latest name via API
            current_status_vc_name = fresh_status_vc.name
        except discord.NotFound:
            logger.error(f"Status VC {status_vc.id} for {original_vc.name} not found during name check. Unregistering.")
            await unregister_vc_tracking_internal(ovc_id, original_vc.guild, is_internal_call=True)
            return
        except discord.Forbidden:
            logger.error(f"Status VC {status_vc.id} for {original_vc.name} forbidden to fetch. Permissions issue?")
            return
        except Exception as e_fetch_name:
            logger.error(f"Error fetching status VC name {status_vc.id}: {e_fetch_name}", exc_info=True)
            return

        final_new_name = re.sub(r'\s{2,}', ' ', desired_name_str).strip()[:100]

        if final_new_name == current_status_vc_name:
            logger.debug(f"Status VC name for {original_vc.name} ('{current_status_vc_name}') is already correct (Count: {count}). No API call.")
            if is_special_zero_update_condition and ovc_id in vc_zero_stats:
                 vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True # Mark as notified even if name was already "0 users"
            return # No change needed

        # --- Apply bot's own rate limit (MAX_UPDATES_IN_WINDOW per BOT_UPDATE_WINDOW_DURATION) ---
        window_info = vc_rate_limit_windows.get(ovc_id)
        can_update_by_bot_rule = False

        if not window_info or now >= window_info["window_start_time"] + BOT_UPDATE_WINDOW_DURATION:
            can_update_by_bot_rule = True # New window or expired window, this update is allowed (counts as 1st)
        elif window_info["count"] < MAX_UPDATES_IN_WINDOW:
            can_update_by_bot_rule = True # Existing window, still has update capacity
        
        if not can_update_by_bot_rule:
            # Log shows the desired change even if skipped by bot's rate limit
            logger.debug(f"Bot rate limit for {original_vc.name}. "
                         f"Already {window_info['count'] if window_info else 'N/A'} updates in current window. "
                         f"Desired change: '{current_status_vc_name}' -> '{final_new_name}'. Skip.")
            return

        # --- Execute channel name change ---
        logger.info(f"ステータスVC名変更試行: Original {original_vc.name} (Status VC ID: {status_vc.id}) '{current_status_vc_name}' -> '{final_new_name}'")
        try:
            await status_vc.edit(name=final_new_name, reason="VC参加人数更新 / 0人ポリシー")
            logger.info(f"Status VC名更新 SUCCESS: '{current_status_vc_name}' -> '{final_new_name}' (Original: {original_vc.name} ID: {ovc_id})")

            # Update bot's rate limit window info
            current_window_data = vc_rate_limit_windows.get(ovc_id)
            if not current_window_data or now >= current_window_data["window_start_time"] + BOT_UPDATE_WINDOW_DURATION:
                vc_rate_limit_windows[ovc_id] = {"window_start_time": now, "count": 1}
            else:
                current_window_data["count"] += 1
            
            if is_special_zero_update_condition and ovc_id in vc_zero_stats:
                vc_zero_stats[ovc_id]["notified_zero_explicitly"] = True
            
            if ovc_id in vc_discord_api_cooldown_until: # Clear Discord API cooldown if successful
                del vc_discord_api_cooldown_until[ovc_id]

        except discord.HTTPException as e:
            if e.status == 429: # Rate limited by Discord API
                retry_after_seconds = e.retry_after if e.retry_after is not None else BOT_UPDATE_WINDOW_DURATION.total_seconds()
                vc_discord_api_cooldown_until[ovc_id] = now + timedelta(seconds=retry_after_seconds)
                logger.warning(f"Status VC名更新レートリミット (Discord): {status_vc.name} (ID: {status_vc.id}). Discord retry_after: {retry_after_seconds}秒。クールダウン適用。")
                # Bot's own window count is NOT incremented here as the update failed
            else:
                logger.error(f"Status VC名更新失敗 (HTTPエラー {e.status}): {status_vc.name} (ID: {status_vc.id}): {e.text}", exc_info=True)
        except discord.Forbidden:
            logger.error(f"Status VC名更新失敗 (権限不足): {status_vc.name} (ID: {status_vc.id}). Original: {original_vc.name}")
        except Exception as e:
            logger.error(f"Status VC名更新中に予期せぬエラー: {status_vc.name} (ID: {status_vc.id}): {e}", exc_info=True)


# --- Bot Events ---
@bot.event
async def on_ready():
    logger.info(f'ログイン成功: {bot.user.name} (ID: {bot.user.id})')
    logger.info(f"discord.py バージョン: {discord.__version__}")
    
    try:
        activity_name = "VCの人数を見守り中ニャ～"
        activity = discord.CustomActivity(name=activity_name)
        # Fallback if CustomActivity is not suitable or causes issues, use Playing
        # activity = discord.Game(name="VCの人数を見守り中ニャ～") 
        await bot.change_presence(activity=activity)
        logger.info(f"ボットのアクティビティを設定しました: {activity_name}")
    except Exception as e:
        logger.error(f"アクティビティの設定中にエラー: {e}")
    
    vc_discord_api_cooldown_until.clear() # Clear any stale API cooldowns on startup

    if await init_firestore():
        await load_tracked_channels_from_db()
    else:
        logger.warning("Firestoreが利用できないため、VC追跡の永続化は無効です。")

    logger.info("起動時の追跡VC状態整合性チェックと更新を開始します...")
    
    tracked_ids_to_process = list(vc_tracking.keys()) # Iterate over a copy
    
    for original_cid in tracked_ids_to_process:
        lock = get_vc_lock(original_cid)
        async with lock:
            if original_cid not in vc_tracking: # Check if removed during iteration by another process
                continue

            track_info = vc_tracking[original_cid]
            guild = bot.get_guild(track_info["guild_id"])

            if not guild:
                logger.warning(f"Guild {track_info['guild_id']} (Original VC {original_cid}) が見つかりません。追跡を解除します。")
                await unregister_vc_tracking_internal(original_cid, None, is_internal_call=True)
                continue

            original_vc = guild.get_channel(original_cid)
            if not isinstance(original_vc, discord.VoiceChannel):
                logger.warning(f"Original VC {original_cid} (Guild {guild.name}) が見つからないかVCではありません。追跡を解除します。")
                await unregister_vc_tracking_internal(original_cid, guild, is_internal_call=True)
                continue

            status_vc = guild.get_channel(track_info.get("status_channel_id"))
            
            # Reset rate limit and zero stats for this VC on ready, for a fresh start
            vc_rate_limit_windows.pop(original_cid, None)
            vc_zero_stats.pop(original_cid, None)
            # vc_discord_api_cooldown_until is globally cleared above

            if isinstance(status_vc, discord.VoiceChannel) and status_vc.category and STATUS_CATEGORY_NAME in status_vc.category.name:
                logger.info(f"起動時: Original VC {original_vc.name} の既存Status VC {status_vc.name} は有効です。名前を更新します。")
                await update_dynamic_status_channel_name(original_vc, status_vc)
            else: # Status VC is invalid or missing, try to recreate
                if status_vc: # Invalid (e.g., wrong category, wrong type)
                    logger.warning(f"起動時: Status VC {status_vc.id if status_vc else 'ID不明'} ({original_vc.name}用) が無効か移動されました。削除して再作成を試みます。")
                    try:
                        await status_vc.delete(reason="無効なステータスVCのため再作成")
                    except Exception as e_del:
                        logger.error(f"無効なステータスVC {status_vc.id if status_vc else 'ID不明'} の削除エラー: {e_del}")
                
                logger.info(f"起動時: {original_vc.name} のステータスVCが存在しないか無効です。新規作成を試みます。")
                # Ensure previous tracking info is cleared before attempting to re-register parts of it
                if original_cid in vc_tracking: del vc_tracking[original_cid] # Temporarily remove to allow _create_status_vc_for_original
                await remove_tracked_original_from_db(original_cid)


                new_status_vc_obj = await _create_status_vc_for_original(original_vc)
                if new_status_vc_obj:
                    # Re-add to tracking with new status VC ID
                    vc_tracking[original_cid] = {
                        "guild_id": guild.id,
                        "status_channel_id": new_status_vc_obj.id,
                        "original_channel_name": original_vc.name # Use current original_vc.name
                    }
                    await save_tracked_original_to_db(original_cid, guild.id, new_status_vc_obj.id, original_vc.name)
                    logger.info(f"起動時: {original_vc.name} のステータスVCを正常に再作成しました: {new_status_vc_obj.name}")
                    await update_dynamic_status_channel_name(original_vc, new_status_vc_obj)
                else:
                    logger.error(f"起動時: {original_vc.name} のステータスVC再作成に失敗しました。追跡は完全に解除されたままです。")
                    # No unregister call needed here as it was effectively unregistered above
            
    logger.info("起動時の追跡VC状態整合性チェックと更新が完了しました。")
    if not periodic_status_update.is_running():
        periodic_status_update.start()


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot:
        return

    channels_to_check_ids = set()
    if before.channel:
        channels_to_check_ids.add(before.channel.id)
    if after.channel:
        channels_to_check_ids.add(after.channel.id)
    
    for original_cid in channels_to_check_ids:
        if original_cid in vc_tracking:
            lock = get_vc_lock(original_cid)
            if lock.locked(): # Non-blocking check if lock is held
                logger.debug(f"VC {original_cid} は現在ロック中のため、on_voice_state_updateからの更新をスキップします。")
                continue

            track_info = vc_tracking.get(original_cid) 
            if not track_info: continue 

            guild = bot.get_guild(track_info["guild_id"])
            if not guild: continue

            original_vc = guild.get_channel(original_cid)
            status_vc = guild.get_channel(track_info.get("status_channel_id")) # Use .get for status_channel_id as it might be missing

            if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
                logger.debug(f"追跡中のオリジナルVC {original_vc.name} に関連するボイス状態更新。ステータスVC {status_vc.name} を更新します。")
                await update_dynamic_status_channel_name(original_vc, status_vc)
            # No explicit unregister here; periodic_status_update or on_ready will handle inconsistencies

@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel):
        return

    if channel.category and STATUS_CATEGORY_NAME in channel.category.name:
        logger.info(f"「STATUS」を含むカテゴリ内に新しいVC {channel.name} が作成されました。自動追跡は無視します。")
        return

    # Check if this new channel is already known (e.g. as an original or status channel)
    if channel.id in vc_tracking or any(info.get("status_channel_id") == channel.id for info in vc_tracking.values()):
        logger.info(f"新しく作成されたチャンネル {channel.name} は既に追跡中かステータスVCです。on_guild_channel_createでは何もしません。")
        return
    
    lock = get_vc_lock(channel.id) # Lock for the new channel's ID
    if lock.locked():
        logger.info(f"新しく作成されたチャンネル {channel.name} はロック中のため、自動追跡をスキップします。")
        return

    # Optional: Small delay to allow Discord to fully process channel creation if issues arise
    # logger.info(f"新しいボイスチャンネル「{channel.name}」(ID: {channel.id}) が作成されました。短時間後に自動追跡を開始します。")
    # await asyncio.sleep(2) # Example delay

    # Re-check lock and tracking status after any delay
    if lock.locked():
        logger.info(f"VC {channel.name} (ID: {channel.id}) は遅延後確認でロック中でした。on_guild_channel_createからの登録をスキップします。")
        return
    if channel.id in vc_tracking:
        logger.info(f"VC {channel.name} (ID: {channel.id}) は遅延後確認で既に追跡中でした。on_guild_channel_createからの登録をスキップします。")
        return

    logger.info(f"新しいボイスチャンネル「{channel.name}」(ID: {channel.id}) の自動追跡を開始します。")
    # register_new_vc_for_tracking will handle feedback if ctx is passed, but here it's None
    await register_new_vc_for_tracking(channel)


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel):
        return

    original_channel_id_to_process = None
    is_status_vc_deleted = False

    if channel.id in vc_tracking: # The deleted channel was an original tracked VC
        original_channel_id_to_process = channel.id
        logger.info(f"追跡対象のオリジナルVC {channel.name} (ID: {channel.id}) が削除されました。")
    else: # Check if the deleted channel was a status VC
        for ocid, info in list(vc_tracking.items()): # Iterate over a copy
            if info.get("status_channel_id") == channel.id:
                original_channel_id_to_process = ocid
                is_status_vc_deleted = True
                logger.info(f"Status VC {channel.name} (ID: {channel.id}) (Original VC ID: {ocid}用) が削除されました。")
                break
    
    if original_channel_id_to_process:
        lock = get_vc_lock(original_channel_id_to_process)
        async with lock:
            if is_status_vc_deleted:
                # Status VC was deleted, try to recreate it if original still exists
                original_vc = channel.guild.get_channel(original_channel_id_to_process)
                if original_vc and isinstance(original_vc, discord.VoiceChannel):
                    logger.info(f"Original VC {original_vc.name} はまだ存在します。ステータスVCの再作成を試みます。")
                    
                    # Clean up old tracking info related to the deleted status VC
                    if original_channel_id_to_process in vc_tracking:
                        del vc_tracking[original_channel_id_to_process]
                    await remove_tracked_original_from_db(original_channel_id_to_process)
                    # Also clear any state for this original_channel_id_to_process before re-registering
                    vc_rate_limit_windows.pop(original_channel_id_to_process, None)
                    vc_zero_stats.pop(original_channel_id_to_process, None)
                    vc_discord_api_cooldown_until.pop(original_channel_id_to_process, None)
                    
                    logger.info(f"Original VC {original_vc.name} のための新しいステータスVCを作成します。")
                    # Re-register, this will create a new status VC and save to DB
                    success = await register_new_vc_for_tracking(original_vc) # This handles all setup
                    if success:
                         logger.info(f"Status VC for {original_vc.name} を再作成しました。")
                    else:
                         logger.error(f"Status VC for {original_vc.name} の再作成に失敗。追跡は行われません。")
                         # register_new_vc_for_tracking should have cleaned up if it failed
                else: 
                    logger.info(f"Status VC削除後、Original VC {original_channel_id_to_process} も見つかりません。追跡を完全に解除します。")
                    await unregister_vc_tracking_internal(original_channel_id_to_process, channel.guild, is_internal_call=True)
            else: # Original VC was deleted
                await unregister_vc_tracking_internal(original_channel_id_to_process, channel.guild, is_internal_call=True)

# --- Periodic Task ---
@tasks.loop(minutes=5) # Consider adjusting interval based on typical Discord rate limits and desired responsiveness
async def periodic_status_update():
    logger.debug("定期ステータス更新タスク実行中...")
    if not vc_tracking:
        return

    for original_cid in list(vc_tracking.keys()): # Iterate over a copy
        lock = get_vc_lock(original_cid)
        if lock.locked(): # Non-blocking check
            logger.debug(f"VC {original_cid} はロック中のため、定期更新をスキップします。")
            continue
        
        track_info = vc_tracking.get(original_cid) # Re-fetch in case modified during iteration
        if not track_info: continue

        guild = bot.get_guild(track_info["guild_id"])
        if not guild:
            logger.warning(f"定期更新: Guild {track_info['guild_id']} (Original VC {original_cid}) が見つかりません。追跡解除します。")
            await unregister_vc_tracking(original_cid, None) # This handles lock internally
            continue
        
        original_vc = guild.get_channel(original_cid)
        status_vc_id = track_info.get("status_channel_id")
        status_vc = guild.get_channel(status_vc_id) if status_vc_id else None


        if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
            # Both VCs exist, check if status VC is in the correct category
            if status_vc.category is None or STATUS_CATEGORY_NAME not in status_vc.category.name:
                logger.warning(f"定期更新: Status VC {status_vc.name} ({original_vc.name}用) が「STATUS」を含むカテゴリにありません。修正を試みます。")
                async with lock: # Acquire lock for modification
                    # Unregister (deletes status VC and DB entry) then re-register (creates new status VC in correct place)
                    await unregister_vc_tracking_internal(original_vc.id, guild, is_internal_call=True)
                # register_new_vc_for_tracking handles its own lock
                await register_new_vc_for_tracking(original_vc)
                continue # Move to next original_cid as this one was reprocessed

            await update_dynamic_status_channel_name(original_vc, status_vc)

        elif not isinstance(original_vc, discord.VoiceChannel) and original_cid in vc_tracking: 
            logger.warning(f"定期更新: Original VC {original_cid} ({track_info.get('original_channel_name', 'N/A')}) が無効になりました。追跡解除します。")
            await unregister_vc_tracking(original_cid, guild) # Handles lock

        elif isinstance(original_vc, discord.VoiceChannel) and not isinstance(status_vc, discord.VoiceChannel):
            # Original VC exists, but status VC is missing or invalid
            logger.warning(f"定期更新: {original_vc.name} (ID: {original_cid}) のステータスVCが存在しないか無効です。再作成を試みます。")
            async with lock: # Acquire lock for modification
                 await unregister_vc_tracking_internal(original_vc.id, guild, is_internal_call=True) # Clean up old status attempts
            await register_new_vc_for_tracking(original_vc) # Recreate

        elif original_cid in vc_tracking: # Neither seems valid but still in tracking, clean up
            logger.warning(f"定期更新: Original VC {original_cid} ({track_info.get('original_channel_name', 'N/A')}) の状態が無効です。追跡解除します。")
            await unregister_vc_tracking(original_cid, guild) # Handles lock


# --- Bot Commands ---
@bot.command(name='nah', help="指定した数のメッセージを削除するニャ。 例: !!nah 5")
@commands.has_permissions(manage_messages=True)
@commands.bot_has_permissions(manage_messages=True)
async def nah_command(ctx, num: int):
    if num <= 0:
        await ctx.send("1以上の数を指定してニャ🐈")
        return
    try:
        # num + 1 to include the command message itself
        deleted_messages = await ctx.channel.purge(limit=num + 1)
        # len(deleted_messages) - 1 because we don't count the command message in the feedback
        response_msg = await ctx.send(f"{len(deleted_messages) -1}件のメッセージを削除したニャ🐈")
        await asyncio.sleep(5) # Wait 5 seconds
        await response_msg.delete() # Delete the bot's confirmation message
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
        logger.error(f"nah_command 未処理のエラー: {error}")
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
    except ValueError: # Not an ID, try by name
        # Exact match first
        for vc in guild.voice_channels:
            if vc.name.lower() == channel_id_or_name.lower():
                target_vc = vc
                break
        # Partial match if no exact match
        if not target_vc:
            for vc in guild.voice_channels:
                if channel_id_or_name.lower() in vc.name.lower():
                    target_vc = vc
                    logger.info(f"VC名「{channel_id_or_name}」の部分一致で「{vc.name}」が見つかりました。")
                    break
    
    if not target_vc or not isinstance(target_vc, discord.VoiceChannel):
        await ctx.send(f"指定された「{channel_id_or_name}」はボイスチャンネルとして見つからなかったニャ😿")
        return

    if target_vc.category and STATUS_CATEGORY_NAME in target_vc.category.name:
        await ctx.send(f"VC「{target_vc.name}」は「STATUS」を含むカテゴリ内のチャンネルのようだニャ。人数表示の対象となる元のVCを指定してニャ。")
        return

    lock = get_vc_lock(target_vc.id)
    if lock.locked():
        await ctx.send(f"VC「{target_vc.name}」は現在処理中ですニャ。少し待ってから試してニャ。")
        return

    if target_vc.id in vc_tracking:
        logger.info(f"コマンド: VC「{target_vc.name}」の追跡解除を試みます。")
        # unregister_vc_tracking sends its own feedback if send_feedback_to_ctx is provided
        await unregister_vc_tracking(target_vc.id, guild, send_feedback_to_ctx=ctx)
    else:
        logger.info(f"コマンド: VC「{target_vc.name}」の新規追跡を試みます。")
        success = await register_new_vc_for_tracking(target_vc, send_feedback_to_ctx=ctx)
        if success:
            # register_new_vc_for_tracking might send "already tracking" if it cleaned up and re-registered.
            # Only send a generic success message if register_new_vc_for_tracking didn't already send "already tracking".
            # This is a bit tricky to determine perfectly without more complex state passing.
            # A simple check could be to see if a message was recently sent by the bot in this channel.
            # For now, we'll rely on register_new_vc_for_tracking's own feedback for "already tracking".
            # If it truly was a new registration, send this:
            history = [msg async for msg in ctx.channel.history(limit=2)]
            bot_already_responded_recently = False
            if len(history) > 1 and history[0].author == bot.user and ("既に追跡中ですニャ" in history[0].content or "追跡を開始するニャ" in history[0].content):
                bot_already_responded_recently = True
            
            if not bot_already_responded_recently :
                 await ctx.send(f"VC「{target_vc.name}」の人数表示用チャンネルを作成し、追跡を開始するニャ！🐈")

        elif not lock.locked(): # If not locked and success is False
            # Check if "already tracking" was sent by register_new_vc_for_tracking's internal logic
            is_already_tracking_message_sent = False
            async for msg in ctx.channel.history(limit=1, after=ctx.message): # Check messages after command
                if msg.author == bot.user and "既に追跡中ですニャ" in msg.content:
                    is_already_tracking_message_sent = True
                    break
            if not is_already_tracking_message_sent:
                 await ctx.send(f"VC「{target_vc.name}」の追跡設定に失敗したニャ😿（ステータスチャンネルの作成に失敗した可能性がありますニャ）")
                            
@nah_vc_command.error
async def nah_vc_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("このコマンドを実行する権限がニャいみたいだニャ… (チャンネル管理権限が必要だニャ)")
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send("ボットにチャンネルを管理する権限がないニャ😿 (ボットの権限を確認してニャ)")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("どのボイスチャンネルか指定してニャ！ 例: `!!nah_vc General`")
    else:
        logger.error(f"nah_vc_command 未処理のエラー: {error}")
        await ctx.send("コマンド実行中に予期せぬエラーが発生したニャ。")


@bot.command(name='nah_help', help="コマンド一覧を表示するニャ。")
async def nah_help_prefix(ctx: commands.Context):
    await ctx.send(HELP_TEXT_CONTENT)


# --- Main Bot Execution ---
async def start_bot_main():
    if DISCORD_TOKEN is None:
        logger.critical("DISCORD_TOKEN が環境変数に設定されていません。Botを起動できません。")
        return

    keep_alive() # Start the Flask keep-alive server

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
    try:
        asyncio.run(start_bot_main())
    except KeyboardInterrupt:
        logger.info("ユーザーによりBotが停止されました (KeyboardInterrupt)。")
    except Exception as e:
        logger.critical(f"メインの実行ループで予期せぬエラーが発生しました: {e}", exc_info=True)
