import httpx
import logging
import asyncio

logger = logging.getLogger(__name__)

BASE_URL = "https://melolo.dramabos.my.id/api"
AUTH_CODE = "A8D6AB170F7B89F2182561D3B32F390D"

async def get_latest_dramas(pages=1, offset=0):
    """Fetches trending dramas from Melolo API home section."""
    all_dramas = []
    
    async with httpx.AsyncClient(timeout=30) as client:
        current_offset = offset
        for p in range(pages):
            url = f"{BASE_URL}/home"
            params = {
                "lang": "id",
                "offset": current_offset
            }
            try:
                response = await client.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    # Melolo structure: data.cell.cell_data -> list of sections -> each has 'books'
                    cell_data = data.get("data", {}).get("cell", {}).get("cell_data", [])
                    if not cell_data:
                        break
                    
                    found_in_page = []
                    for section in cell_data:
                        books = section.get("books", [])
                        found_in_page.extend(books)
                    
                    if not found_in_page:
                        break
                        
                    all_dramas.extend(found_in_page)
                    # Use next_offset from response if available
                    current_offset = data.get("data", {}).get("next_offset", current_offset + 18)
                else:
                    break
            except Exception as e:
                logger.error(f"Error fetching home offset {current_offset}: {e}")
                break
    
    return all_dramas

# Compatibility alias for old sources
async def get_latest_idramas(pages=1):
    return await get_latest_dramas(pages=pages)

async def get_drama_detail(book_id: str):
    """Fetches drama detail from Melolo API."""
    url = f"{BASE_URL}/detail/{book_id}"
    params = {"lang": "id"}
    
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if data and data.get("code") == 0:
                return data
            return None
        except Exception as e:
            logger.error(f"Error fetching drama detail for {book_id}: {e}")
            return None

# Compatibility alias
async def get_idrama_detail(book_id: str):
    return await get_drama_detail(book_id)

async def get_all_episodes(book_id: str):
    """Fetches episodes list from drama detail."""
    detail = await get_drama_detail(book_id)
    if detail and "videos" in detail:
        return detail["videos"]
    return []

# Compatibility alias
async def get_idrama_all_episodes(book_id: str):
    return await get_all_episodes(book_id)

async def search_dramas(query: str):
    """Searches dramas by title."""
    url = f"{BASE_URL}/search"
    params = {
        "lang": "id",
        "q": query
    }
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if data and data.get("code") == 0:
                # Search structure: data has 'data' which is list of books?
                # Looking at my test: {"code":0,"count":33,"data":[...]}
                return data.get("data", [])
            return []
        except Exception as e:
            logger.error(f"Error searching for {query}: {e}")
            return []

async def get_video_url(vid: str, episode_num: str = "Unknown"):
    """
    Fetches the actual play URL for a video ID with retries and advanced parsing.
    """
    url = f"{BASE_URL}/video/{vid}"
    params = {
        "lang": "id",
        "code": AUTH_CODE
    }
    
    max_retries = 3
    delays = [1, 2, 3]
    
    async with httpx.AsyncClient(timeout=30) as client:
        for attempt in range(max_retries):
            try:
                response = await client.get(url, params=params)
                
                if response.status_code != 200:
                    logger.error(f"API Error {response.status_code} for vid {vid} (Ep {episode_num}) - Attempt {attempt+1}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delays[attempt])
                        continue
                    return None

                data = response.json()
                
                # Debug logging if needed or if data is suspicious
                if not data:
                    logger.error(f"Empty response for vid {vid} (Ep {episode_num})")
                    continue

                # Advanced Parsing with Fallbacks
                video_url = None
                
                # Check root or 'data' key
                target = data.get("data") if isinstance(data.get("data"), dict) else data
                
                # Fallback sequence
                video_url = (
                    target.get("url") or 
                    target.get("play_url") or 
                    target.get("video_url") or 
                    target.get("playUrl")
                )
                
                # Check streams array or list array if still not found
                if not video_url:
                    # Check 'streams'
                    if "streams" in target and isinstance(target["streams"], list) and len(target["streams"]) > 0:
                        stream = target["streams"][0]
                        if isinstance(stream, dict):
                            video_url = stream.get("url") or stream.get("play_url")
                    
                    # Check 'list' (seen in some Melolo responses)
                    if not video_url and "list" in target and isinstance(target["list"], list) and len(target["list"]) > 0:
                        # Prefer 720p or highest if available, else first
                        for item in target["list"]:
                            if item.get("definition") == "720p":
                                video_url = item.get("url")
                                break
                        if not video_url:
                            video_url = target["list"][0].get("url")


                if video_url:
                    return video_url
                
                # If still no URL, log full response for debugging
                logger.error(f"❌ No URL found in API response for vid {vid} (Episode {episode_num})")
                logger.error(f"DEBUG - Full Response: {data}")
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(delays[attempt])
                
            except Exception as e:
                logger.error(f"Error fetching video URL for {vid} (Attempt {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(delays[attempt])
                    
    return None

