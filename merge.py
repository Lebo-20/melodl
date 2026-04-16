import os
import subprocess
import logging

logger = logging.getLogger(__name__)

async def merge_episodes(video_dir: str, output_path: str):
    """
    Merges all .mp4 files in video_dir into a single output_path file.
    video_dir: Directory containing episode_.mp4 files.
    output_path: Path for final merged video.
    """
    try:
        # Get all video files in numeric order
        files = [f for f in os.listdir(video_dir) if f.endswith(".mp4")]
        if not files:
            logger.error("No video files found to merge.")
            return False
            
        files.sort() # Sorted alphabetically/numerically like episode_001.mp4
        
        list_file_path = os.path.join(video_dir, "list.txt")
        with open(list_file_path, "w") as f:
            for file in files:
                f.write(f"file '{file}'\n")

        # ffmpeg commands
        fast_cmd = [
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", list_file_path,
            "-c", "copy",
            output_path
        ]
        
        fallback_cmd = [
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", list_file_path,
            "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            output_path
        ]

        logger.info(f"Running ffmpeg fast-merge non-blocking...")
        process = await asyncio.create_subprocess_exec(
            *fast_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await process.communicate()
        
        if process.returncode != 0:
            logger.warning(f"Fast merge failed, error: {stderr.decode()[:200]}")
            logger.info("Running ffmpeg fallback-merge (re-encoding)...")
            process = await asyncio.create_subprocess_exec(
                *fallback_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Fallback merge failure: {stderr.decode()}")
                return False
            
        logger.info(f"Successfully merged episodes into {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error during merge: {e}")
        return False
