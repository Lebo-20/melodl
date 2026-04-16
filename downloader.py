import os
import asyncio
import httpx
import logging

logger = logging.getLogger(__name__)

import shutil

async def download_aria2c(url: str, path: str):
    """Downloads a file using aria2c for maximum speed."""
    if not shutil.which("aria2c"):
        logger.error("aria2c not found in PATH. Falling back to httpx.")
        return False
        
    dir_path = os.path.dirname(path)
    filename = os.path.basename(path)
    
    cmd = [
        "aria2c",
        "--console-log-level=error",
        "-x", "16",       # 16 connections per server
        "-s", "16",       # 16 connections
        "-k", "1M",       # 1MB chunks
        "--summary-interval=0",
        "--auto-file-renaming=false",
        "--allow-overwrite=true",
        "-d", dir_path,
        "-o", filename,
        url
    ]
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.communicate()
        return process.returncode == 0
    except Exception as e:
        logger.error(f"Aria2c error: {e}")
        return False

async def download_file(client: httpx.AsyncClient, url: str, path: str, progress_callback=None):
    """Downloads a single file with potential progress tracking."""
    try:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            
            total_size = int(response.headers.get("Content-Length", 0))
            download_size = 0
            
            with open(path, "wb") as f:
                async for chunk in response.aiter_bytes():
                    f.write(chunk)
                    download_size += len(chunk)
                    if progress_callback:
                        await progress_callback(download_size, total_size)
        return True
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        return False



from api import get_video_url

async def download_all_episodes(episodes, download_dir: str, semaphore_count: int = 5):
    """
    Downloads all episodes concurrently.
    episodes: list of dicts with 'episode' and 'vid' for Melolo API
    """
    os.makedirs(download_dir, exist_ok=True)
    semaphore = asyncio.Semaphore(semaphore_count)
    failed_episodes = []

    async def limited_download(ep):
        async with semaphore:
            ep_num = str(ep.get('episode', 'unk')).zfill(3)
            filename = f"episode_{ep_num}.mp4"
            filepath = os.path.join(download_dir, filename)
            
            vid = ep.get('vid')
            if not vid:
                logger.error(f"No Video ID found for episode {ep_num}")
                failed_episodes.append(ep_num)
                return False
                
            max_retries = 2 # get_video_url already has 3 retries
            for attempt in range(max_retries):
                try:
                    # Fetch URL from vid (fresh URL for each attempt)
                    url = await get_video_url(vid, ep_num)
                    if not url:
                        # Already logged in api.py
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        failed_episodes.append(ep_num)
                        return False
                        
                    success = False
                    if shutil.which("aria2c"):
                        success = await download_aria2c(url, filepath)
                    
                    if not success:
                        # Fallback to httpx
                        async with httpx.AsyncClient(timeout=120) as client:
                            success = await download_file(client, url, filepath)

                    if success:
                        # Verify file size
                        if os.path.exists(filepath) and os.path.getsize(filepath) > 100000: # >100KB
                            logger.info(f"✅ Successfully downloaded episode {ep_num}")
                            return True
                        else:
                            logger.warning(f"⚠️ File episode {ep_num} too small, likely corrupted - Attempt {attempt+1}")
                except Exception as e:
                    logger.error(f"❌ Error downloading episode {ep_num} - Attempt {attempt+1}: {e}")

                
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
            
            failed_episodes.append(ep_num)
            return False

    results = await asyncio.gather(*(limited_download(ep) for ep in episodes))
    
    success_count = sum(1 for r in results if r is True)
    total_count = len(episodes)
    is_fully_successful = success_count == total_count
    
    if is_fully_successful:
        logger.info(f"✅ All {total_count} episodes downloaded successfully.")
    else:
        logger.error(f"⚠️ Partial download completed: {success_count}/{total_count} episodes.")
        if failed_episodes:
            logger.error(f"❌ Failed episodes: {', '.join(sorted(failed_episodes))}")
        
    # Return success status (for now returning True if at least one succeeded to prevent total block, 
    # but user wants to know if they succeeded)
    # Actually, the original code returned 'success' (all). 
    # Let's return success_count > 0 so the process can continue to merge/upload if some episodes are done?
    # Or keep it as 'is_fully_successful' but ensure the main script doesn't just crash.
    return is_fully_successful, success_count, total_count

