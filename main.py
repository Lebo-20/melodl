import os
import asyncio
import logging
import shutil
import tempfile
import random
from telethon import TelegramClient, events, Button
from dotenv import load_dotenv

load_dotenv()

# Local imports
from api import (
    get_drama_detail, get_all_episodes, get_latest_dramas,
    get_latest_idramas, get_idrama_detail, get_idrama_all_episodes,
    search_dramas
)
from downloader import download_all_episodes
from merge import merge_episodes
from uploader import upload_drama, sanitize_filename
from database import init_db, is_drama_uploaded, add_uploaded_drama, record_failure, get_failure_count

# Configuration (Use environment variables or replace these directly)
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
AUTO_CHANNEL = int(os.environ.get("AUTO_CHANNEL", ADMIN_ID)) # Default post to admin
AUTO_TOPIC = os.environ.get("AUTO_TOPIC")
if AUTO_TOPIC and AUTO_TOPIC.isdigit():
    AUTO_TOPIC = int(AUTO_TOPIC)
else:
    AUTO_TOPIC = None
PROCESSED_FILE = "processed.json"

# Initialize state
def load_processed():
    if os.path.exists(PROCESSED_FILE):
        import json
        with open(PROCESSED_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_processed(data):
    import json
    with open(PROCESSED_FILE, "w") as f:
        json.dump(list(data), f)

processed_ids = load_processed()

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Bot State
class BotState:
    is_auto_running = True
    manual_tasks = 0 # Count of active manual commands
    processing_ids = set() # Set for realtime duplicate detection
    limit = asyncio.Semaphore(3) # Max 3 concurrent downloads total

# Initialize client
client = TelegramClient('dramabox_bot', API_ID, API_HASH)

def get_panel_buttons():
    status_text = "🟢 RUNNING" if BotState.is_auto_running else "🔴 STOPPED"
    return [
        [Button.inline("▶️ Start Auto", b"start_auto"), Button.inline("⏹ Stop Auto", b"stop_auto")],
        [Button.inline(f"📊 Status: {status_text}", b"status")]
    ]

@client.on(events.NewMessage(pattern='/update'))
async def update_bot(event):
    if event.sender_id != ADMIN_ID:
        return
    import subprocess
    import sys
    
    status_msg = await event.reply("🔄 Menarik pembaruan dari GitHub...")
    try:
        # Run git pull
        result = subprocess.run(["git", "pull", "origin", "main"], capture_output=True, text=True)
        await status_msg.edit(f"✅ Repositori berhasil di-pull:\n```\n{result.stdout}\n```\n\nSedang memulai ulang sistem (Restarting)...")
        
        # Restart the script forcefully replacing the current process image
        os.execl(sys.executable, sys.executable, *sys.argv)
    except Exception as e:
        await status_msg.edit(f"❌ Gagal melakukan update: {e}")

@client.on(events.NewMessage(pattern='/panel'))
async def panel(event):
    if event.chat_id != ADMIN_ID:
        return
    await event.reply("🎛 **Dramabox Control Panel**", buttons=get_panel_buttons())

@client.on(events.CallbackQuery())
async def panel_callback(event):
    if event.sender_id != ADMIN_ID:
        return
        
    data = event.data
    
    try:
        if data == b"start_auto":
            BotState.is_auto_running = True
            await event.answer("Auto-mode started!")
            await event.edit("🎛 **Dramabox Control Panel**", buttons=get_panel_buttons())
        elif data == b"stop_auto":
            BotState.is_auto_running = False
            await event.answer("Auto-mode stopped!")
            await event.edit("🎛 **Dramabox Control Panel**", buttons=get_panel_buttons())
        elif data == b"status":
            await event.answer(f"Status: {'Running' if BotState.is_auto_running else 'Stopped'}")
            await event.edit("🎛 **Dramabox Control Panel**", buttons=get_panel_buttons())
    except Exception as e:
        if "message is not modified" in str(e).lower() or "Message string and reply markup" in str(e):
            pass # Ignore if button is already in that state
        else:
            logger.error(f"Callback error: {e}")

@client.on(events.NewMessage(pattern='/start'))
async def start(event):
    await event.reply("Welcome to Dramabox Downloader Bot! 🎉\n\nGunakan perintah\n`/download {bookId}`\n`/download {title}`\n`/search {judul}`\nuntuk mulai.")

@client.on(events.NewMessage(pattern=r'/search (.+)'))
async def on_search(event):
    query = event.pattern_match.group(1).strip()
    status_msg = await event.reply(f"🔍 Mencari `{query}`...")
    
    # Check if query is an ID
    if query.isdigit() and len(query) > 10:
        detail = await get_drama_detail(query)
        if detail:
            title = detail.get("title") or detail.get("book_name") or f"Drama {query}"
            buttons = [[Button.inline(f"🎬 {title} (ID: {query})", f"dl_{query}".encode())]]
            await status_msg.edit(f"✅ ID ditemukan:", buttons=buttons)
            return

    results = await search_dramas(query)

    if not results:
        await status_msg.edit(f"❌ Tidak ditemukan hasil untuk `{query}`.")
        return
        
    buttons = []
    # Show top 8 results
    for res in results[:8]:
        title = res.get("book_name") or res.get("title")
        book_id = str(res.get("book_id") or res.get("id"))
        if title and book_id:
            buttons.append([Button.inline(f"🎬 {title}", f"dl_{book_id}".encode())])
            
    await status_msg.edit(f"✅ Ditemukan {len(results)} drama untuk `{query}`:", buttons=buttons)

@client.on(events.CallbackQuery(pattern=r'^dl_(.+)'))
async def dl_callback(event):
    book_id = event.pattern_match.group(1).decode()
    chat_id = event.chat_id

    
    if BotState.limit.locked():
        await event.answer("⚠️ Semua slot penuh (maks 3). Mohon tunggu sebentar!", alert=True)
        return
        
    await event.answer("Mulai memproses...")
    status_msg = await client.send_message(chat_id, f"⏳ Memulai download drama ID: `{book_id}`...")
    
    BotState.manual_tasks += 1
    async with BotState.limit:
        try:
            # If it's the admin, they might want it in AUTO_CHANNEL, 
            # but for simplicity let's use the chat where they requested it
            # or keep AUTO_CHANNEL only for auto_mode.
            target_chat = chat_id 
            target_topic = AUTO_TOPIC if target_chat == AUTO_CHANNEL else None
            
            success = await process_drama_full(book_id, target_chat, status_msg, topic_id=target_topic)
            if success:
                processed_ids.add(book_id)
                save_processed(processed_ids)
        finally:
            BotState.manual_tasks -= 1

@client.on(events.NewMessage(pattern=r'/download (.+)'))
async def on_download(event):
    chat_id = event.chat_id
    
    if BotState.limit.locked():
        await event.reply("⚠️ Semua slot penuh (maks 3). Antrian sedang memproses drama lain.")
        return
        
    query = event.pattern_match.group(1).strip()

    book_id = None
    
    # Check if it looks like an ID (long numeric string)
    if query.isdigit() and len(query) > 10:
        book_id = query
        logger.info(f"Direct ID download: {book_id}")
    else:
        # It's a title, search for it
        await event.reply(f"🔍 Mencari `{query}` untuk didownload...")
        results = await search_dramas(query)
        if not results:
            await event.reply(f"❌ Drama `{query}` tidak ditemukan.")
            return
        book_id = results[0].get("book_id") or results[0].get("id")
        title = results[0].get("book_name") or results[0].get("title")
        await event.reply(f"✅ Ditemukan: **{title}** (ID: `{book_id}`)")
    
    # 1. Fetch data
    detail = await get_drama_detail(book_id)
    if not detail:
        await event.reply(f"❌ Gagal mendapatkan detail drama `{book_id}`.")
        return
        
    episodes = await get_all_episodes(book_id)
    if not episodes:
        await event.reply(f"❌ Drama `{book_id}` tidak memiliki episode.")
        return
    
    title = (
        detail.get("title") or 
        detail.get("book_name") or 
        detail.get("name") or 
        detail.get("bookName") or 
        (detail.get("data", {}) if isinstance(detail.get("data"), dict) else {}).get("name") or
        (detail.get("data", {}) if isinstance(detail.get("data"), dict) else {}).get("title") or
        f"Drama_{book_id}"
    )
    status_msg = await event.reply(f"🎬 Drama: **{title}**\n📽 Total Episodes: {len(episodes)}\n\n⏳ Sedang memproses...")
    
    BotState.manual_tasks += 1
    async with BotState.limit:
        try:
            target_topic = AUTO_TOPIC if chat_id == AUTO_CHANNEL else None
            success = await process_drama_full(book_id, chat_id, status_msg, topic_id=target_topic)
            if success:
                processed_ids.add(book_id)
                save_processed(processed_ids)
        finally:
            BotState.manual_tasks -= 1

async def process_drama_full(book_id, chat_id, status_msg=None, topic_id=None):
    """Refactored logic to be reusable for auto-mode and support Melolo API."""
    # 1. Fetch data with retries
    max_api_retries = 3
    detail = None
    episodes = None
    
    for i in range(max_api_retries):
        detail = await get_drama_detail(book_id)
        episodes = await get_all_episodes(book_id)
        if detail and episodes:
            break
        await asyncio.sleep(2)
    
    if not detail or not episodes:
        err_msg = f"❌ Detail atau Episode `{book_id}` tidak ditemukan."
        if status_msg: await status_msg.edit(err_msg)
        logger.error(err_msg)
        # If we failed to get detail, we still try to record failure if possible
        placeholder_title = f"Unknown_ID_{book_id}"
        record_failure(placeholder_title)
        return False

    title = (
        detail.get("title") or 
        detail.get("book_name") or 
        detail.get("name") or 
        detail.get("bookName") or 
        (detail.get("data", {}) if isinstance(detail.get("data"), dict) else {}).get("name") or
        (detail.get("data", {}) if isinstance(detail.get("data"), dict) else {}).get("title") or
        f"Drama_{book_id}"
    )
    
    # Check realtime processing set
    if book_id in BotState.processing_ids:
        msg = f"⏳ **{title}** sedang diproses oleh worker lain..."
        if status_msg: await status_msg.edit(msg)
        return False
        
    # DB Check for deduplication and failure limits
    if is_drama_uploaded(title, book_id=book_id):
        msg = f"⏭ **{title}** sudah pernah di-upload. Melewati..."
        if status_msg: await status_msg.edit(msg)
        logger.info(msg)
        return True
        
    fail_count = get_failure_count(title)
    if fail_count >= 2:
        msg = f"⏭ **{title}** gagal diproses sebanyak {fail_count} kali. Melewati..."
        if status_msg: await status_msg.edit(msg)
        logger.warning(msg)
        return False

    description = detail.get("intro") or "No description available."
    poster = detail.get("cover") or ""
    
    # 2. Setup temp directory
    temp_dir = tempfile.mkdtemp(prefix=f"melolo_{book_id}_")
    video_dir = os.path.join(temp_dir, "episodes")
    os.makedirs(video_dir, exist_ok=True)
    
    BotState.processing_ids.add(book_id)
    try:
        if status_msg: await status_msg.edit(f"🎬 Processing **{title}**...")
        
        # 3. Download
        is_fully_successful, success_count, total_count = await download_all_episodes(episodes, video_dir, book_id=book_id)
        
        if success_count == 0:
            err_msg = f"❌ Download Gagal Total: **{title}** (0/{total_count} episode)"
            if status_msg: await status_msg.edit(err_msg)
            logger.error(err_msg)
            record_failure(title)
            return False
            
        if not is_fully_successful:
            warn_msg = f"⚠️ Download Parsial: **{title}** ({success_count}/{total_count} episode berhasil)"
            logger.warning(warn_msg)
            # We continue to merge even if partial, as requested by user "Jangan batalkan semua"


        # 4. Merge
        if status_msg: await status_msg.edit(f"📽 Merging {success_count}/{total_count} episodes...")
        safe_title = sanitize_filename(title)
        output_video_path = os.path.join(temp_dir, f"{safe_title}.mp4")
        merge_success = await merge_episodes(video_dir, output_video_path)
        if not merge_success:
            err_msg = f"❌ Merge Gagal (FFmpeg Error): **{title}**"
            if status_msg: await status_msg.edit(err_msg)
            logger.error(err_msg)
            record_failure(title)
            return False

        # 5. Upload
        if status_msg: await status_msg.edit(f"📤 Uploading **{title}** to channel ({success_count}/{total_count})...")
        upload_success = await upload_drama(
            client, chat_id, 
            title, description, 
            poster, output_video_path,
            ep_info=f"{success_count}/{total_count}",
            topic_id=topic_id
        )
        
        if upload_success:
            # Mark as uploaded in DB
            add_uploaded_drama(title, book_id)
            
            if status_msg: 
                try: await status_msg.delete()
                except: pass
            return True
        else:
            err_msg = f"❌ Upload Gagal (Telegram Error): **{title}**"
            if status_msg: await status_msg.edit(err_msg)
            logger.error(err_msg)
            record_failure(title)
            return False
            
    except Exception as e:
        logger.error(f"Error processing {book_id}: {e}")
        if status_msg: await status_msg.edit(f"❌ Error: {e}")
        record_failure(title)
        return False
    finally:
        BotState.processing_ids.discard(book_id)
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

async def auto_mode_loop():
    """Loop to find and process new dramas automatically using Melolo home feed."""
    global processed_ids
    
    logger.info("🚀 Melolo Auto-Mode Started.")
    
    is_initial_run = True
    
    while True:
        if not BotState.is_auto_running:
            await asyncio.sleep(5)
            continue
            
        try:
            interval = 5 if is_initial_run else 15 
            logger.info(f"🔍 Scanning for new dramas (Next scan in {interval}m)...")
            
            # Fetch trending from home
            new_dramas = await get_latest_dramas(pages=2 if is_initial_run else 1) or []
            queue = [d for d in new_dramas if str(d.get("book_id") or d.get("id")) not in processed_ids]
            
            if not queue and not is_initial_run:
                # Try a different offset if nothing new found in first page
                logger.info("ℹ️ No new dramas in first page. Rotating offset...")
                # Calling with offset=None triggers internal rotation in api.py
                new_dramas = await get_latest_dramas(pages=1, offset=None) or []
                queue = [d for d in new_dramas if str(d.get("book_id") or d.get("id")) not in processed_ids]
            
            new_found = 0
            
            for drama in queue:
                if not BotState.is_auto_running:
                    break
                    
                book_id = str(drama.get("book_id") or drama.get("id", ""))
                if not book_id:
                    continue
                    
                if book_id not in processed_ids:
                    new_found += 1
                    title = (
                        drama.get("book_name") or 
                        drama.get("title") or 
                        drama.get("name") or 
                        drama.get("bookName") or 
                        "Unknown"
                    )
                    
                    # PRE-CHECK Failure limits to avoid any processing
                    if book_id in BotState.processing_ids:
                        continue
                        
                    if is_drama_uploaded(title, book_id=book_id):
                        logger.info(f"⏭ Skip {title} (Already uploaded)")
                        processed_ids.add(book_id)
                        continue
                        
                    fail_count = get_failure_count(title)
                    if fail_count >= 2:
                        logger.warning(f"🚫 Skip {title} (Failed {fail_count} times)")
                        processed_ids.add(book_id) # Add to cache to avoid re-checking feed
                        continue

                    logger.info(f"✨ [MELOLO] New drama: {title} ({book_id}). Starting process...")
                    
                    while BotState.manual_tasks > 0:
                        await asyncio.sleep(5)
                        
                    if BotState.limit.locked():
                        await asyncio.sleep(60) 
                        continue
                        
                    async with BotState.limit:
                        # Notify admin
                        try:
                            await client.send_message(ADMIN_ID, f"🆕 **Auto-System Mendeteksi Drama Baru!**\n🎬 `[MELOLO] {title}`\n🆔 `{book_id}`\n⏳ Memproses download & merge...")
                        except: pass
                        
                        # Process to target channel
                        success = await process_drama_full(book_id, AUTO_CHANNEL, topic_id=AUTO_TOPIC)
                        
                        if success:
                            logger.info(f"✅ Finished {title}")
                            processed_ids.add(book_id)
                            save_processed(processed_ids)
                            try:
                                await client.send_message(ADMIN_ID, f"✅ Sukses Auto-Post: **{title}** ke channel.\n⏳ Auto-mode istirahat selama 10 menit...")
                            except: pass

                            
                            # Istirahat 10 menit setelah berhasil upload di auto mode
                            for _ in range(10 * 60):
                                if not BotState.is_auto_running:
                                    break
                                await asyncio.sleep(1)

                        else:
                            logger.error(f"❌ Failed to process {title}")
                            # Don't stop auto_running, just notify and move on
                            try:
                                await client.send_message(ADMIN_ID, f"🚨 **ERROR**: Auto-mode gagal memproses `{title}`.\nMelanjutkan ke drama berikutnya...")
                            except: pass
                            # Prevent hitting API/Telegram rate limits too hard
                            await asyncio.sleep(10)
            
            if new_found == 0:
                logger.info("😴 No new dramas found in this scan.")
            
            is_initial_run = False
            
            # Wait for next interval but break early if auto_running is changed
            for _ in range(interval * 60):
                if not BotState.is_auto_running:
                    break
                await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"⚠️ Error in auto_mode_loop: {e}")
            await asyncio.sleep(60) # retry after 1 min

if __name__ == '__main__':
    logger.info("Initializing Dramabox Auto-Bot...")
    init_db()
    
    with client:
        # Start auto loop and keep the client running
        client.loop.create_task(auto_mode_loop())
        
        logger.info("Bot is active and monitoring.")
        client.run_until_disconnected()
