from dotenv import load_dotenv
load_dotenv()
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

app = Flask('')

@app.route('/')
def home():
    return "I'm alive"

def run():
  app.run(host='0.0.0.0',port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()

# ロギング設定
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# BotのIntents設定
intents = discord.Intents.all() 

# Firestoreクライアント
db = None
FIRESTORE_COLLECTION_NAME = "discord_tracked_original_vcs_prod_v2" 
STATUS_CATEGORY_NAME = "STATUS" 

# VC追跡用辞書: {original_channel_id: {"guild_id": guild_id, "status_channel_id": status_channel_id, "original_channel_name": name}}
vc_tracking = {}
# 各オリジナルVC IDごとのロックオブジェクトを管理する辞書
vc_locks = {} # {original_channel_id: asyncio.Lock()}

# --- Cooldown Settings ---
CHANNEL_NAME_UPDATE_COOLDOWN = timedelta(minutes=5) 
channel_last_successful_update_at = {}


# --- Help Text (Global for easy access) ---
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

# --- Lock Helper ---
def get_vc_lock(vc_id: int) -> asyncio.Lock:
    """指定されたVC IDに対応するasyncio.Lockオブジェクトを取得または作成する。"""
    if vc_id not in vc_locks:
        vc_locks[vc_id] = asyncio.Lock()
    return vc_locks[vc_id]

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

async def register_new_vc_for_tracking(original_vc: discord.VoiceChannel, send_feedback_to_ctx=None):
    original_vc_id = original_vc.id
    lock = get_vc_lock(original_vc_id)

    async with lock: # 特定のVCに対する処理をロック
        # 既に有効に追跡中か最終確認
        if original_vc_id in vc_tracking:
            track_info = vc_tracking[original_vc_id]
            guild_id_for_check = track_info.get("guild_id")
            if guild_id_for_check:
                guild_for_status_check = bot.get_guild(guild_id_for_check)
                if guild_for_status_check:
                    status_vc = guild_for_status_check.get_channel(track_info.get("status_channel_id"))
                    if isinstance(status_vc, discord.VoiceChannel) and status_vc.category and STATUS_CATEGORY_NAME in status_vc.category.name:
                        logger.info(f"VC {original_vc.name} (ID: {original_vc_id}) はロック取得後に確認した結果、既に有効に追跡中です。")
                        if send_feedback_to_ctx:
                            await send_feedback_to_ctx.send(f"VC「{original_vc.name}」は既に追跡中ですニャ。")
                        return False 
            logger.info(f"VC {original_vc.name} (ID: {original_vc_id}) の追跡情報が無効です。クリーンアップして再作成します。")
            # 無効な追跡情報をクリーンアップ (unregister_vc_tracking はロック内で呼ばない方が良い場合もあるが、ここではシンプルに)
            await unregister_vc_tracking_internal(original_vc_id, original_vc.guild, is_internal_call=True)


        logger.info(f"VC {original_vc.name} (ID: {original_vc_id}) の新規追跡処理を開始します。")
        new_status_vc = await _create_status_vc_for_original(original_vc)
        if new_status_vc:
            vc_tracking[original_vc_id] = {
                "guild_id": original_vc.guild.id,
                "status_channel_id": new_status_vc.id,
                "original_channel_name": original_vc.name 
            }
            await save_tracked_original_to_db(original_vc_id, original_vc.guild.id, new_status_vc.id, original_vc.name) 
            channel_last_successful_update_at[original_vc_id] = datetime.now(timezone.utc) - CHANNEL_NAME_UPDATE_COOLDOWN 
            await update_dynamic_status_channel_name(original_vc, new_status_vc) 
            logger.info(f"追跡開始/再開: Original VC {original_vc.name} (ID: {original_vc_id}), Status VC {new_status_vc.name} (ID: {new_status_vc.id})")
            return True
        else:
            logger.error(f"{original_vc.name} のステータスVC作成に失敗しました。追跡は開始されません。")
            return False

async def unregister_vc_tracking(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None):
    """unregister_vc_tracking_internalをロック付きで呼び出すラッパー関数"""
    lock = get_vc_lock(original_channel_id)
    async with lock:
        await unregister_vc_tracking_internal(original_channel_id, guild, send_feedback_to_ctx, is_internal_call=False)

async def unregister_vc_tracking_internal(original_channel_id: int, guild: discord.Guild | None, send_feedback_to_ctx=None, is_internal_call: bool = False):
    """追跡解除のコアロジック（ロック管理は呼び出し元で行うか、この関数がラッパー経由で呼ばれることを想定）"""
    logger.info(f"VC ID {original_channel_id} の追跡解除処理を開始 (内部呼び出し: {is_internal_call})。")
    track_info = vc_tracking.pop(original_channel_id, None) 
    original_vc_name_for_msg = f"ID: {original_channel_id}" 

    if track_info:
        original_vc_name_for_msg = track_info.get("original_channel_name", f"ID: {original_channel_id}")
        status_channel_id = track_info.get("status_channel_id")
        
        current_guild = guild or (bot.get_guild(track_info.get("guild_id")) if track_info.get("guild_id") else None)

        if current_guild and status_channel_id:
            status_vc = current_guild.get_channel(status_channel_id)
            if status_vc and isinstance(status_vc, discord.VoiceChannel): # ステータスVCがVCであることを確認
                try:
                    await status_vc.delete(reason="オリジナルVCの追跡停止のため")
                    logger.info(f"削除成功: Status VC {status_vc.name} (ID: {status_vc.id})")
                except discord.NotFound:
                    logger.info(f"Status VC {status_channel_id} は既に削除されていました。")
                except discord.Forbidden:
                    logger.error(f"Status VC {status_vc.name} (ID: {status_vc.id}) の削除に失敗 (権限不足)")
                except Exception as e:
                    logger.error(f"Status VC {status_vc.name} (ID: {status_vc.id}) の削除中にエラー: {e}", exc_info=True)
            elif status_vc : # チャンネルは存在するがVCではない場合
                logger.warning(f"Status Channel ID {status_channel_id} はVCではありませんでした。削除はスキップ。")
            else:
                logger.info(f"DBに記録のあったStatus VC {status_channel_id} がGuild {current_guild.name} に見つかりませんでした。")
        elif status_channel_id:
            logger.warning(f"GuildオブジェクトなしでStatus VC {status_channel_id} の削除はスキップされました (Original VC ID: {original_channel_id})。")

    if original_channel_id in channel_last_successful_update_at: 
        del channel_last_successful_update_at[original_channel_id]
    
    await remove_tracked_original_from_db(original_channel_id) 
    if not is_internal_call: # 内部呼び出しでない場合のみログとフィードバック
        logger.info(f"追跡停止完了: Original VC ID {original_channel_id} ({original_vc_name_for_msg})")
        if send_feedback_to_ctx and guild: 
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

    original_channel_id = original_vc.id
    now = datetime.now(timezone.utc)

    # クールダウンチェック (ロックの外で行う)
    if original_channel_id in channel_last_successful_update_at:
        if now < channel_last_successful_update_at[original_channel_id] + CHANNEL_NAME_UPDATE_COOLDOWN:
            time_remaining = (channel_last_successful_update_at[original_channel_id] + CHANNEL_NAME_UPDATE_COOLDOWN) - now
            # logger.info(f"クールダウン中: {original_vc.name}。残り {time_remaining.total_seconds():.1f}秒。") # 頻繁なのでデバッグレベルに
            logger.debug(f"Cooldown for {original_vc.name}. {time_remaining.total_seconds():.1f}s left.")
            return
    
    lock = get_vc_lock(original_channel_id)
    async with lock: # 名前変更処理自体もロック
        # ロック取得後に再度チャンネルの有効性を確認（チャンネルが削除された場合など）
        if not bot.get_channel(original_vc.id) or not bot.get_channel(status_vc.id):
            logger.warning(f"Update_dynamic_status_channel_name: Original VC {original_vc.id} or Status VC {status_vc.id} became invalid after acquiring lock. Skipping update.")
            return

        current_members = [member for member in original_vc.members if not member.bot] 
        count = len(current_members)
        count = min(count, 999)

        track_info = vc_tracking.get(original_vc.id)
        if not track_info:
            logger.warning(f"ステータス更新エラー: Original VC {original_vc.name} (ID: {original_vc.id}) が追跡情報にありません。ベース名が不明です。")
            return
        
        base_name = track_info.get("original_channel_name", original_vc.name) 
        
        new_name = f"{base_name}：{count} users" 
        final_new_name = re.sub(r'\s{2,}', ' ', new_name).strip()[:100] 

        # status_vc.name をAPIから再取得して比較 (キャッシュ対策)
        try:
            current_status_vc_object = await bot.fetch_channel(status_vc.id)
            current_status_vc_name = current_status_vc_object.name
        except (discord.NotFound, discord.Forbidden):
            logger.error(f"Status VC {status_vc.id} の最新情報の取得に失敗。更新をスキップ。")
            return


        if final_new_name != current_status_vc_name: 
            logger.info(f"ステータスVC名変更試行: Original {original_vc.name} (Status VC ID: {status_vc.id}) '{current_status_vc_name}' -> '{final_new_name}'")
            try:
                await status_vc.edit(name=final_new_name, reason="VC参加人数更新")
                logger.info(f"Status VC名更新 SUCCESS: '{current_status_vc_name}' -> '{final_new_name}' (Original: {original_vc.name} ID: {original_vc.id})")
                channel_last_successful_update_at[original_channel_id] = now 
            except discord.Forbidden:
                logger.error(f"Status VC名更新失敗 (権限不足): {status_vc.name} (ID: {status_vc.id}). Original: {original_vc.name}")
            except discord.HTTPException as e:
                if e.status == 429: 
                    retry_after = e.retry_after if e.retry_after else CHANNEL_NAME_UPDATE_COOLDOWN.total_seconds()
                    logger.warning(f"Status VC名更新レートリミット: {status_vc.name} (ID: {status_vc.id}). Discord retry_after: {retry_after}秒。クールダウン適用。")
                    channel_last_successful_update_at[original_channel_id] = now + timedelta(seconds=retry_after) 
                else:
                    logger.error(f"Status VC名更新失敗 (HTTPエラー {e.status}): {status_vc.name} (ID: {status_vc.id}): {e.text}")
            except Exception as e:
                logger.error(f"Status VC名更新中に予期せぬエラー: {status_vc.name} (ID: {status_vc.id}): {e}", exc_info=True)
        else:
            # logger.info(f"ステータスVC名変更不要: Original {original_vc.name} ('{current_status_vc_name}') は既に正しいです (人数: {count})。")
            logger.debug(f"Status VC name for {original_vc.name} ('{current_status_vc_name}') is already correct (Count: {count}).")
            channel_last_successful_update_at[original_channel_id] = now 


@bot.event
async def on_ready():
    logger.info(f'ログイン成功: {bot.user.name} (ID: {bot.user.id})')
    logger.info(f"discord.py バージョン: {discord.__version__}")
    
    # --- ここにアクティビティ設定コードを追加 ---
    try:
        activity = discord.CustomActivity(name="にゃんだふるなVCサポートをお届けするニャ！")
        await bot.change_presence(activity=activity)
        logger.info("ボットのアクティビティを設定しました。")
    except Exception as e:
        logger.error(f"アクティビティの設定中にエラー: {e}")
    # ------------------------------------
    
    if await init_firestore():
        await load_tracked_channels_from_db()
    else:
        logger.warning("Firestoreが利用できないため、VC追跡の永続化は無効です。")

    logger.info("起動時の追跡VC状態整合性チェックと更新を開始します...")
    
    tracked_ids_to_process = list(vc_tracking.keys()) 
    
    for original_cid in tracked_ids_to_process:
        lock = get_vc_lock(original_cid)
        async with lock: # on_readyでの処理もロック
            if original_cid not in vc_tracking: # ロック取得後に削除された可能性
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
            
            # 既存ステータスVCの検証と再利用
            if isinstance(status_vc, discord.VoiceChannel) and status_vc.category and STATUS_CATEGORY_NAME in status_vc.category.name:
                logger.info(f"起動時: Original VC {original_vc.name} の既存Status VC {status_vc.name} は有効です。名前を更新します。")
                channel_last_successful_update_at[original_vc.id] = datetime.now(timezone.utc) - CHANNEL_NAME_UPDATE_COOLDOWN # 即時更新可能に
                await update_dynamic_status_channel_name(original_vc, status_vc)
            else: # ステータスVCが無効または存在しない場合、再作成
                if status_vc: 
                    logger.warning(f"起動時: Status VC {status_vc.id} ({original_vc.name}用) が無効か移動されました。削除して再作成を試みます。")
                    try:
                        await status_vc.delete(reason="無効なステータスVCのため再作成")
                    except Exception as e:
                        logger.error(f"無効なステータスVC {status_vc.id} の削除エラー: {e}")
                
                logger.info(f"起動時: {original_vc.name} のステータスVCが存在しないか無効です。新規作成を試みます。")
                new_status_vc_obj = await _create_status_vc_for_original(original_vc) 
                if new_status_vc_obj:
                    vc_tracking[original_cid]["status_channel_id"] = new_status_vc_obj.id 
                    await save_tracked_original_to_db(original_cid, guild.id, new_status_vc_obj.id, vc_tracking[original_cid]["original_channel_name"]) 
                    logger.info(f"起動時: {original_vc.name} のステータスVCを正常に再作成しました: {new_status_vc_obj.name}")
                    channel_last_successful_update_at[original_vc.id] = datetime.now(timezone.utc) - CHANNEL_NAME_UPDATE_COOLDOWN
                    await update_dynamic_status_channel_name(original_vc, new_status_vc_obj)
                else:
                    logger.error(f"起動時: {original_vc.name} のステータスVC再作成に失敗しました。追跡を解除します。")
                    await unregister_vc_tracking_internal(original_cid, guild, is_internal_call=True)
            
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
            if lock.locked():
                logger.info(f"VC {original_cid} は現在ロック中のため、on_voice_state_updateからの更新をスキップします。")
                continue

            # update_dynamic_status_channel_name 内でロックするので、ここではロック取得しない
            track_info = vc_tracking.get(original_cid) # ロックの外で track_info を取得
            if not track_info: continue # まれにロックチェック後に vc_tracking から消える可能性

            guild = bot.get_guild(track_info["guild_id"])
            if not guild: continue

            original_vc = guild.get_channel(original_cid)
            status_vc = guild.get_channel(track_info["status_channel_id"])

            if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
                logger.debug(f"追跡中のオリジナルVC {original_vc.name} に関連するボイス状態更新。ステータスVC {status_vc.name} を更新します。")
                await update_dynamic_status_channel_name(original_vc, status_vc)
            # else: 無効な場合は periodic_update や on_ready で処理される想定


@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): 
        return

    if channel.category and STATUS_CATEGORY_NAME in channel.category.name:
        logger.info(f"「STATUS」を含むカテゴリ内に新しいVC {channel.name} が作成されました。自動追跡は無視します。")
        return

    # 既に何らかの形で追跡中、またはステータスVCとして登録済みの場合は無視
    if channel.id in vc_tracking or any(info.get("status_channel_id") == channel.id for info in vc_tracking.values()):
        logger.info(f"新しく作成されたチャンネル {channel.name} は既に追跡中かステータスVCです。on_guild_channel_createでは何もしません。")
        return
    
    lock = get_vc_lock(channel.id)
    if lock.locked():
        logger.info(f"新しく作成されたチャンネル {channel.name} はロック中のため、自動追跡をスキップします。")
        return

    logger.info(f"新しいボイスチャンネル「{channel.name}」(ID: {channel.id}) が作成されました。2秒後に自動追跡を開始します。")
    await asyncio.sleep(2) 

    # 遅延後、再度ロックと追跡状態を確認
    if lock.locked():
        logger.info(f"VC {channel.name} (ID: {channel.id}) は遅延後確認でロック中でした。on_guild_channel_createからの登録をスキップします。")
        return
    if channel.id in vc_tracking:
        logger.info(f"VC {channel.name} (ID: {channel.id}) は遅延後確認で既に追跡中でした。on_guild_channel_createからの登録をスキップします。")
        return

    await register_new_vc_for_tracking(channel)


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.VoiceChannel): 
        return

    original_channel_id_to_process = None
    is_status_vc_deleted = False

    # 削除されたのがオリジナルVCか確認
    if channel.id in vc_tracking: 
        original_channel_id_to_process = channel.id
        logger.info(f"追跡対象のオリジナルVC {channel.name} (ID: {channel.id}) が削除されました。")
    else: # 削除されたのがステータスVCか確認
        for ocid, info in list(vc_tracking.items()): 
            if info.get("status_channel_id") == channel.id:
                original_channel_id_to_process = ocid
                is_status_vc_deleted = True
                logger.info(f"Status VC {channel.name} (ID: {channel.id}) (Original VC ID: {ocid}用) が削除されました。")
                break 
    
    if original_channel_id_to_process:
        lock = get_vc_lock(original_channel_id_to_process)
        async with lock:
            if is_status_vc_deleted:
                # ステータスVCが削除されたが、オリジナルVCはまだ存在するかもしれない
                original_vc = channel.guild.get_channel(original_channel_id_to_process)
                if original_vc and isinstance(original_vc, discord.VoiceChannel):
                    logger.info(f"Original VC {original_vc.name} はまだ存在します。ステータスVCの再作成を試みます。")
                    # 古い追跡情報をDBとメモリから削除
                    await remove_tracked_original_from_db(original_channel_id_to_process)
                    if original_channel_id_to_process in vc_tracking:
                        del vc_tracking[original_channel_id_to_process]
                    
                    # このロック内で新しいステータスVCを作成して登録する
                    logger.info(f"Original VC {original_vc.name} のための新しいステータスVCを作成します。")
                    new_status_vc = await _create_status_vc_for_original(original_vc)
                    if new_status_vc:
                        vc_tracking[original_vc.id] = {
                            "guild_id": original_vc.guild.id,
                            "status_channel_id": new_status_vc.id,
                            "original_channel_name": original_vc.name
                        }
                        await save_tracked_original_to_db(original_vc.id, original_vc.guild.id, new_status_vc.id, original_vc.name)
                        logger.info(f"Status VC for {original_vc.name} を再作成しました: {new_status_vc.name}")
                    else:
                        logger.error(f"Status VC for {original_vc.name} の再作成に失敗。追跡を完全に解除します。")
                        await unregister_vc_tracking_internal(original_channel_id_to_process, channel.guild, is_internal_call=True)
                else: # オリジナルVCも見つからない
                    logger.info(f"Status VC削除後、Original VC {original_channel_id_to_process} も見つかりません。追跡を解除します。")
                    await unregister_vc_tracking_internal(original_channel_id_to_process, channel.guild, is_internal_call=True)
            else: # オリジナルVC自体が削除された場合
                await unregister_vc_tracking_internal(original_channel_id_to_process, channel.guild, is_internal_call=True)


@tasks.loop(minutes=5)
async def periodic_status_update():
    logger.debug("定期ステータス更新タスク実行中...") # INFOからDEBUGに変更
    if not vc_tracking: 
        # logger.info("定期更新: 現在追跡中のVCはありません。スキップします。") # 頻繁なのでDEBUGレベルに
        return

    for original_cid in list(vc_tracking.keys()): 
        lock = get_vc_lock(original_cid)
        if lock.locked():
            logger.debug(f"VC {original_cid} はロック中のため、定期更新をスキップします。")
            continue
        
        # ロックを取得せずに track_info を取得（update_dynamic_status_channel_name内でロックするため）
        track_info = vc_tracking.get(original_cid)
        if not track_info: continue


        guild = bot.get_guild(track_info["guild_id"])
        if not guild:
            logger.warning(f"定期更新: Guild {track_info['guild_id']} (Original VC {original_cid}) が見つかりません。追跡解除します。")
            await unregister_vc_tracking(original_cid, None) # ロック付きの unregister を呼ぶ
            continue
        
        original_vc = guild.get_channel(original_cid)
        status_vc = guild.get_channel(track_info.get("status_channel_id"))

        if isinstance(original_vc, discord.VoiceChannel) and isinstance(status_vc, discord.VoiceChannel):
            if status_vc.category is None or STATUS_CATEGORY_NAME not in status_vc.category.name:
                logger.warning(f"定期更新: Status VC {status_vc.name} ({original_vc.name}用) が「STATUS」を含むカテゴリにありません。修正を試みます。")
                # ここでの修正は register_new_vc_for_tracking を呼ぶことになるのでロックが必要
                async with lock: # register_new_vc_for_tracking を呼ぶ前にロック
                    # unregister_vc_tracking_internal を呼んでから register_new_vc_for_tracking を呼ぶ
                    await unregister_vc_tracking_internal(original_vc.id, guild, is_internal_call=True)
                # ロックを解放した後に register_new_vc_for_tracking を呼ぶ
                await register_new_vc_for_tracking(original_vc)
                continue 

            await update_dynamic_status_channel_name(original_vc, status_vc) 
        elif not isinstance(original_vc, discord.VoiceChannel) and original_cid in vc_tracking: # original_cid in vc_tracking を追加
            logger.warning(f"定期更新: Original VC {original_cid} が無効になりました。追跡解除します。")
            await unregister_vc_tracking(original_cid, guild)
        elif not isinstance(status_vc, discord.VoiceChannel) and isinstance(original_vc, discord.VoiceChannel):
            logger.warning(f"定期更新: {original_vc.name} (ID: {original_cid}) のステータスVCが存在しないか無効です。再作成を試みます。")
            # ここもロックが必要
            async with lock:
                await unregister_vc_tracking_internal(original_vc.id, guild, is_internal_call=True)
            await register_new_vc_for_tracking(original_vc)
        elif original_cid in vc_tracking: # その他のケースでまだ追跡情報が残っている場合
            logger.warning(f"定期更新: Original VC {original_cid} の状態が無効です。追跡解除します。")
            await unregister_vc_tracking(original_cid, guild)


# --- Bot Commands ---
@bot.command(name='nah', help="指定した数のメッセージを削除するニャ。 例: !!nah 5")
@commands.has_permissions(manage_messages=True) 
@commands.bot_has_permissions(manage_messages=True) 
async def nah_command(ctx, num: int):
    if num <= 0:
        await ctx.send("1以上の数を指定してニャ🐈")
        return
    try:
        deleted_messages = await ctx.channel.purge(limit=num + 1) 
        response_msg = await ctx.send(f"{len(deleted_messages) -1}件のメッセージを削除したニャ🐈")
        await response_msg.delete(delay=5) 
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
    except ValueError: 
        for vc in guild.voice_channels:
            if vc.name.lower() == channel_id_or_name.lower():
                target_vc = vc
                break
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

    # 追跡状態の確認は register/unregister 関数内で行うので、ここでは単純に呼び出す
    if target_vc.id in vc_tracking: 
        # 既に追跡中であれば解除処理を呼び出す
        logger.info(f"コマンド: VC「{target_vc.name}」の追跡解除を試みます。")
        await unregister_vc_tracking(target_vc.id, guild, send_feedback_to_ctx=ctx)
    else: 
        # 未追跡であれば登録処理を呼び出す
        logger.info(f"コマンド: VC「{target_vc.name}」の新規追跡を試みます。")
        success = await register_new_vc_for_tracking(target_vc, send_feedback_to_ctx=ctx)
        if success:
            # register_new_vc_for_tracking が False を返した場合（既に有効に追跡中だった場合など）は、
            # send_feedback_to_ctx でメッセージが送られているはずなので、ここでは成功時のみ
            await ctx.send(f"VC「{target_vc.name}」の人数表示用チャンネルを作成し、追跡を開始するニャ！🐈")
        elif not lock.locked(): # ロックされておらず、かつ失敗した場合（作成失敗など）
            await ctx.send(f"VC「{target_vc.name}」の追跡設定に失敗したニャ😿")
            
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


if __name__ == "__main__":
    if DISCORD_TOKEN is None:
        logger.critical("DISCORD_TOKEN が環境変数に設定されていません。")
    else:
        try:
            logger.info("Botを起動します...")
            keep_alive() # Webサーバーを起動してBotをオンラインに保つ
            bot.run(DISCORD_TOKEN)
        except discord.LoginFailure:
            logger.critical("Discordへのログインに失敗しました。トークンが正しいか確認してください。")
        except Exception as e:
            logger.critical(f"Botの起動中に予期せぬエラーが発生しました: {e}", exc_info=True)