import asyncio
import httpx
import re
import hashlib
import random
import string
import time
import os
from fastapi import FastAPI, HTTPException, Header
from sse_starlette.sse import EventSourceResponse
import uvicorn

# --- NAVIDROME CONFIGURATION ---
NAVIDROME_URL = os.getenv("NAVIDROME_URL", "")
USERNAME = os.getenv("USERNAME", "")
PASSWORD = os.getenv("PASSWORD", "")

# --- SECURITY CONFIGURATION ---
# The custom header token your Android app will need to send
SHARED_SECRET_TOKEN = os.getenv("SHARED_SECRET_TOKEN", "")

# --- GLOBAL TRACK HISTORY STORAGE ---
# Structure: {"Station Name": ["Song 1", "Song 2", ...]}
# Capped at exactly 10 items max.
TRACK_HISTORY = {}

app = FastAPI(title="Internet Radio Tracker API")


@app.get("/")
async def root():
    """Simple root welcome path displaying endpoints."""
    return {
        "message": "RadioScraper API is active!",
        "endpoints": {
            "static_history": "/api/history",
            "live_stream": "/api/stream-history"
        }
    }


@app.get("/api/history")
async def get_history(x_app_token: str = Header(None)):
    """Returns the last 10 tracks played, protected by a simple header token."""
    if x_app_token != SHARED_SECRET_TOKEN:
        raise HTTPException(status_code=403, detail="Unauthorized app connection.")
    return TRACK_HISTORY


@app.get("/api/stream-history")
async def stream_history(x_app_token: str = Header(None)):
    """Streams live track updates, protecting the continuous connection with a token."""
    if x_app_token != SHARED_SECRET_TOKEN:
        raise HTTPException(status_code=403, detail="Unauthorized app connection.")

    async def event_generator():
        last_sent_snapshot = {}
        
        while True:
            # Create a quick snapshot of the absolute latest track for every active station
            current_snapshot = {k: v[-1] for k, v in TRACK_HISTORY.items() if v}
            
            if current_snapshot != last_sent_snapshot:
                # Something changed! Send the new live track update chunk to the app
                yield {
                    "event": "track_update",
                    "data": current_snapshot
                }
                last_sent_snapshot = current_snapshot
                
            # Sleep for 1 second before checking memory again
            await asyncio.sleep(1)

    # Adding the X-Accel-Buffering: no header tells proxies (like Cloudflare) 
    # to deliver the bytes to the phone immediately instead of caching them.
    headers = {"X-Accel-Buffering": "no"}
    return EventSourceResponse(event_generator(), headers=headers)


def get_subsonic_auth_params(username, password):
    salt = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    token = hashlib.md5((password + salt).encode('utf-8')).hexdigest()
    return {"u": username, "t": token, "s": salt, "v": "1.16.1", "c": "radioscraper", "f": "json"}


async def fetch_navidrome_radios(client):
    endpoint = f"{NAVIDROME_URL}/rest/getInternetRadioStations"
    try:
        response = await client.get(endpoint, params=get_subsonic_auth_params(USERNAME, PASSWORD), timeout=10)
        subsonic_response = response.json().get("subsonic-response", {})
        stations = subsonic_response.get("internetRadioStations", {}).get("internetRadioStation", [])
        return [stations] if isinstance(stations, dict) else stations
    except Exception as e:
        print(f"Failed to fetch radios from Navidrome: {e}")
        return []


def save_track_to_history(station_name, track_title):
    """Safely updates history dictionary, filtering duplicates and tracking placeholders."""
    if not track_title or "⚠️" in track_title or "❌" in track_title:
        return

    # Filter out generic Radio BOB placeholder strings
    if "RADIO BOB" in track_title.upper() and ("-" not in track_title or len(track_title.split("-")) < 2 or "LIVESTREAM" in track_title.upper()):
        return

    # Clean up any trailing/leading whitespaces
    track_title = track_title.strip()

    # Initialize list if station hasn't been scraped yet
    if station_name not in TRACK_HISTORY:
        TRACK_HISTORY[station_name] = []

    station_history = TRACK_HISTORY[station_name]

    # Don't add the track if it's identical to the most recently added song
    if station_history and station_history[-1] == track_title:
        return

    # Add the fresh track to the end of the history queue
    station_history.append(track_title)

    # Enforce history length limit of 10 songs maximum
    if len(station_history) > 10:
        station_history.pop(0)
        
    print(f"Added to History -> [{station_name}] : {track_title}")


async def get_icy_metadata_async(client, name, url):
    """Connects to a single stream URL concurrently and monitors it for up to 60s for metadata updates."""
    headers = {'Icy-MetaData': '1'}
    short_name = name[:32] + "..." if len(name) > 35 else name
    
    start_time = time.time()
    initial_track = None
    captured_track = None
    
    try:
        async with client.stream("GET", url, headers=headers, timeout=5.0) as response:
            metaint = response.headers.get('icy-metaint')
            if not metaint:
                return

            metaint = int(metaint)
            byte_iterator = response.aiter_bytes()
            stream_buffer = bytearray()

            async def read_exact_bytes(amount):
                nonlocal stream_buffer
                while len(stream_buffer) < amount:
                    try:
                        chunk = await anext(byte_iterator)
                        stream_buffer.extend(chunk)
                    except StopAsyncIteration:
                        break
                if len(stream_buffer) < amount:
                    return None
                result = stream_buffer[:amount]
                stream_buffer = stream_buffer[amount:]
                return result

            while time.time() - start_time < 60.0:
                if not await read_exact_bytes(metaint):
                    break
                length_byte = await read_exact_bytes(1)
                if not length_byte:
                    break

                metadata_length = ord(length_byte) * 16
                if metadata_length > 0:
                    metadata_bytes = await read_exact_bytes(metadata_length)
                    if not metadata_bytes:
                        break
                        
                    metadata_str = metadata_bytes.decode('utf-8', errors='ignore')
                    title_match = re.search(r"StreamTitle='(.*?)';", metadata_str)
                    current_track = title_match.group(1) if title_match else None
                    
                    if current_track:
                        if initial_track is None:
                            initial_track = current_track
                            if "RADIO BOB" not in current_track.upper():
                                captured_track = current_track
                                break
                        elif current_track != initial_track:
                            captured_track = current_track
                            break
                
                await asyncio.sleep(0.01)

            if not captured_track and initial_track:
                captured_track = initial_track

            if captured_track:
                save_track_to_history(name, captured_track)
                
    except Exception:
        pass


async def scraper_loop():
    """Background loop worker that runs the concurrent scraper cycle indefinitely."""
    # Runs a check every 45 seconds to keep up with live song changes
    INTERVAL_SECONDS = 45 
    
    limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
    
    async with httpx.AsyncClient(follow_redirects=True, limits=limits) as client:
        while True:
            print(f"\n[{time.strftime('%H:%M:%S')}] Starting dynamic metadata scraping cycle...")
            stations = await fetch_navidrome_radios(client)
            
            if stations:
                tasks = []
                for station in stations:
                    name = station.get("name")
                    url = station.get("streamUrl")
                    if url and name:
                        tasks.append(get_icy_metadata_async(client, name, url))
                
                await asyncio.gather(*tasks)
                print(f"Cycle finished. Next execution in {INTERVAL_SECONDS} seconds.")
            
            await asyncio.sleep(INTERVAL_SECONDS)


async def main():
    """Main event coordinator setting up FastAPI and the worker to run side-by-side."""
    asyncio.create_task(scraper_loop())
    
    config = uvicorn.Config(app=app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
