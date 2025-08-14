# Code optimized by fyaz05
# Code from Eyaadh
# Modified by Gemini for HLS Streaming Support

import time
import math
import logging
import mimetypes
import traceback
import asyncio
import os
import aiofiles
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from WebStreamer.bot import multi_clients, work_loads, StreamBot
from WebStreamer.vars import Var
from WebStreamer.server.exceptions import FIleNotFound, InvalidHash
from WebStreamer import utils, StartTime, __version__
from WebStreamer.utils.render_template import render_page
from WebStreamer.utils.file_properties import get_media_from_message
from WebStreamer.utils.human_readable import humanbytes

# A temporary directory to store the generated HLS files
TEMP_HLS_DIR = "./hls_temp"
if not os.path.exists(TEMP_HLS_DIR):
    os.makedirs(TEMP_HLS_DIR)

routes = web.RouteTableDef()

# --- Existing Routes ---

@routes.get("/stats", allow_head=True)
async def root_route_handler(_):
    """Handler for status endpoint."""
    return web.json_response({
        "server_status": "running",
        "uptime": utils.get_readable_time(time.time() - StartTime),
        "telegram_bot": "@" + StreamBot.username,
        "connected_bots": len(multi_clients),
        "loads": {
            f"bot{c+1}": l for c, (_, l) in enumerate(
                sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
            )
        },
        "version": __version__,
    })

@routes.get("/watch/{path}", allow_head=True)
async def watch_handler(request: web.Request):
    """Handler for watch endpoint."""
    try:
        path = request.match_info["path"]
        return web.Response(text=await render_page(path), content_type='text/html')
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        return web.Response(status=500, text="Internal Server Error")
    except Exception as e:
        logging.critical(e)
        logging.debug(traceback.format_exc())
        raise web.HTTPInternalServerError(text=str(e))

@routes.get("/dl/{path}", allow_head=True)
async def download_handler(request: web.Request):
    """Handler for download endpoint."""
    try:
        path = request.match_info["path"]
        return await media_streamer(request, path)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        return web.Response(status=500, text="Internal Server Error")
    except Exception as e:
        logging.critical(e)
        logging.debug(traceback.format_exc())
        raise web.HTTPInternalServerError(text=str(e))

@routes.get("/stream/{path}", allow_head=True)
async def stream_handler(request: web.Request):
    """Handler for stream endpoint."""
    try:
        path = request.match_info["path"]
        return await media_streamer(request, path)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        return web.Response(status=500, text="Internal Server Error")
    except Exception as e:
        logging.critical(e)
        logging.debug(traceback.format_exc())
        raise web.HTTPInternalServerError(text=str(e))

class_cache = {}

async def media_streamer(request: web.Request, db_id: str):
    """Stream media file."""
    try:
        range_header = request.headers.get("Range")
        client_index = min(work_loads, key=work_loads.get)
        fastest_client = multi_clients[client_index]

        if Var.MULTI_CLIENT:
            logging.info(f"Client {client_index} is now serving {request.headers.get('X-FORWARDED-FOR', request.remote)}")

        if fastest_client in class_cache:
            tg_connect = class_cache[fastest_client]
        else:
            tg_connect = utils.ByteStreamer(fastest_client)
            class_cache[fastest_client] = tg_connect

        file_id = await tg_connect.get_file_properties(db_id, multi_clients)
        file_size = file_id.file_size
        from_bytes, until_bytes = parse_range_header(range_header, file_size)

        if (until_bytes >= file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
            return web.Response(
                status=416,
                body="416: Range not satisfiable",
                headers={"Content-Range": f"bytes */{file_size}"},
            )

        chunk_size = 1024 * 1024
        until_bytes = min(until_bytes, file_size - 1)
        offset = from_bytes - (from_bytes % chunk_size)
        first_part_cut = from_bytes - offset
        last_part_cut = until_bytes % chunk_size + 1
        req_length = until_bytes - from_bytes + 1
        part_count = math.ceil((until_bytes + 1) / chunk_size) - math.floor(offset / chunk_size)
        
        body = tg_connect.yield_file(file_id, client_index, offset, first_part_cut, last_part_cut, part_count, chunk_size)

        mime_type = file_id.mime_type or mimetypes.guess_type(utils.get_name(file_id))[0] or "application/octet-stream"
        disposition = "attachment" if "application/" in mime_type or "text/" in mime_type else "inline"

        return web.Response(
            status=206 if range_header else 200,
            body=body,
            headers={
                "Content-Type": mime_type,
                "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
                "Content-Length": str(req_length),
                "Content-Disposition": f'{disposition}; filename="{utils.get_name(file_id)}"',
                "Accept-Ranges": "bytes",
            },
        )
    except Exception as e:
        traceback.print_exc()
        logging.critical(f"An unhandled exception occurred in media_streamer: {e}")
        return web.Response(status=500, text="Internal Server Error")


def parse_range_header(header, file_size):
    """Parse Range header and return tuple of (from_bytes, until_bytes)."""
    from_bytes, until_bytes = 0, file_size - 1
    if header:
        range_parts = header.replace("bytes=", "").split("-")
        if range_parts[0]:
            from_bytes = int(range_parts[0])
        if range_parts[1]:
            until_bytes = int(range_parts[1]) if range_parts[1] else file_size - 1
    return from_bytes, until_bytes

# --- New HLS Streaming Routes ---
# Create a new route for the HLS manifest (.m3u8)

@routes.get("/hls/{path}/manifest.m3u8", allow_head=True)
async def hls_manifest_handler(request: web.Request):
    """
    Handler for the HLS manifest file.
    This function will trigger the video segmentation process with FFmpeg.
    """
    path = request.match_info["path"]
    hls_output_dir = os.path.join(TEMP_HLS_DIR, path)

    # Check if HLS files are already generated
    if os.path.exists(os.path.join(hls_output_dir, 'manifest.m3u8')):
        logging.info(f"HLS files for {path} already exist, serving manifest.")
        return web.FileResponse(
            os.path.join(hls_output_dir, 'manifest.m3u8'),
            headers={"Access-Control-Allow-Origin": "*"}
        )

    # If not, download the file from Telegram and process it
    try:
        client_index = min(work_loads, key=work_loads.get)
        fastest_client = multi_clients[client_index]
        tg_connect = class_cache.get(fastest_client, utils.ByteStreamer(fastest_client))
        file_id = await tg_connect.get_file_properties(path, multi_clients)
        file_name = utils.get_name(file_id)
        
        # Create a temporary directory for this HLS stream
        os.makedirs(hls_output_dir, exist_ok=True)
        temp_input_path = os.path.join(hls_output_dir, file_name)

        logging.info(f"Downloading file {file_name} from Telegram to {temp_input_path}...")
        # A simplified download-to-file logic. The real implementation is in ByteStreamer.
        # This part assumes you can save the file to disk.
        async with aiofiles.open(temp_input_path, "wb") as f:
            async for chunk in tg_connect.yield_file(file_id, client_index, 0, 0, file_id.file_size, 1, 1024*1024):
                await f.write(chunk)
        logging.info(f"Download complete. Starting FFmpeg process.")

        # FFmpeg command to transcode and segment the video
        # -i: input file
        # -c:v: video codec, `copy` avoids re-encoding, which is very fast
        # -hls_time 10: segment duration in seconds
        # -hls_playlist_type event: makes a growing playlist
        # -hls_flags delete_segments: deletes old segments (not used here to keep them)
        # manifest.m3u8: output manifest file
        # The command assumes FFmpeg is in the system's PATH
        
        ffmpeg_command = [
            'ffmpeg', '-i', temp_input_path,
            '-codec:v', 'copy',
            '-codec:a', 'copy',
            '-hls_time', '10',
            '-hls_playlist_type', 'event',
            '-hls_segment_filename', os.path.join(hls_output_dir, 'segment%03d.ts'),
            os.path.join(hls_output_dir, 'manifest.m3u8')
        ]
        
        process = await asyncio.create_subprocess_exec(
            *ffmpeg_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logging.error(f"FFmpeg failed with error:\n{stderr.decode()}")
            raise Exception("FFmpeg transcoding failed.")
        
        logging.info(f"FFmpeg finished for {path}. Serving manifest.")
        
        return web.FileResponse(
            os.path.join(hls_output_dir, 'manifest.m3u8'),
            headers={"Access-Control-Allow-Origin": "*"}
        )
    
    except Exception as e:
        logging.error(f"Error generating HLS stream for {path}: {e}")
        # Clean up temp files in case of error
        try:
            for item in os.listdir(hls_output_dir):
                os.remove(os.path.join(hls_output_dir, item))
            os.rmdir(hls_output_dir)
        except Exception as cleanup_err:
            logging.error(f"Failed to cleanup temp HLS dir: {cleanup_err}")
        raise web.HTTPInternalServerError(text=str(e))

@routes.get("/hls/{path}/{segment}", allow_head=True)
async def hls_segment_handler(request: web.Request):
    """
    Handler for individual HLS video segments (.ts files).
    """
    path = request.match_info["path"]
    segment_name = request.match_info["segment"]
    
    segment_path = os.path.join(TEMP_HLS_DIR, path, segment_name)
    
    if not os.path.exists(segment_path):
        raise web.HTTPNotFound(text=f"Segment file not found: {segment_name}")
        
    return web.FileResponse(
        segment_path,
        headers={"Content-Type": "video/MP2T", "Access-Control-Allow-Origin": "*"}
    )

