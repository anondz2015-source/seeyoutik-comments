import os
import time
import asyncio
import threading
import requests
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, GiftEvent, LikeEvent, MemberEvent

app = Flask(__name__)
CORS(app)

EULER_API_KEY = os.environ.get("EULER_API_KEY", "euler_TON_API_KEY_ICI")

clients = {}
chat_store = {}
stream_store = {}

def get_stream_from_euler(room_id):
    if not EULER_API_KEY or "TON_API_KEY" in EULER_API_KEY:
        print("EULER_API_KEY not configured")
        return None
    try:
        r = requests.get(
            f"https://api.eulerstream.com/webcast/room/info/?room_id={room_id}",
            headers={"x-api-key": EULER_API_KEY},
            timeout=15
        )
        print("Euler HTTP:", r.status_code)
        if r.status_code != 200:
            print("Euler body:", r.text[:300])
            return None
        data = r.json()
        su = (data.get("data") or {}).get("stream_url") or data.get("stream_url")
        if not su:
            print("No stream_url in Euler response")
            return None
        flv = su.get("flv_pull_data") or su.get("FLV_pull_data")
        hls = su.get("hls_pull_data") or su.get("HLS_pull_data")
        for raw in (flv, hls):
            if raw:
                for p in raw.split(":"):
                    if p.startswith("http"):
                        return p
        return None
    except Exception as e:
        print("Euler error:", e)
        return None

@app.route("/start")
def start():
    username = request.args.get("username")
    if not username:
        return jsonify({"ok": False, "error": "username required"}), 400

    chat_store.setdefault(username, [])

    if username in clients and username in stream_store:
        return jsonify({
            "ok": True,
            "status": "already",
            "roomId": clients[username].room_id,
            "stream": stream_store[username]
        })

    try:
        client = TikTokLiveClient(unique_id=f"@{username}")

        @client.on(CommentEvent)
        async def on_comment(event):
            user = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "User")
            text = getattr(event, "comment", "")
            if not text:
                return
            chat_store.setdefault(username, [])
            chat_store[username].append({
                "type": "comment", "user": user, "comment": text,
                "timestamp": int(time.time() * 1000)
            })
            if len(chat_store[username]) > 200:
                chat_store[username] = chat_store[username][-200:]
            print("CHAT:", user, ":", text)

        @client.on(GiftEvent)
        async def on_gift(event):
            user = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "User")
            gname = "Gift"
            if hasattr(event, "gift") and event.gift:
                gname = getattr(event.gift, "name", "Gift")
            chat_store.setdefault(username, [])
            chat_store[username].append({
                "type": "gift", "user": user, "gift": gname,
                "count": getattr(event, "repeat_count", 1) or 1,
                "timestamp": int(time.time() * 1000)
            })
            print("GIFT:", user, gname)

        @client.on(LikeEvent)
        async def on_like(event):
            user = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "User")
            chat_store.setdefault(username, [])
            chat_store[username].append({
                "type": "like", "user": user,
                "count": getattr(event, "count", 1) or 1,
                "timestamp": int(time.time() * 1000)
            })

        @client.on(MemberEvent)
        async def on_member(event):
            user = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "User")
            chat_store.setdefault(username, [])
            chat_store[username].append({
                "type": "join", "user": user,
                "timestamp": int(time.time() * 1000)
            })

        def run_client():
            try:
                asyncio.run(client.connect())
            except Exception as e:
                print("Client error:", e)

        threading.Thread(target=run_client, daemon=True).start()

        for _ in range(60):
            if client.room_id:
                break
            time.sleep(0.25)

        room_id = client.room_id
        print("Room ID:", room_id)

        stream_url = None
        if room_id:
            stream_url = get_stream_from_euler(room_id)
            if stream_url:
                stream_store[username] = stream_url
                print("STREAM:", stream_url[:100])

        clients[username] = client

        return jsonify({
            "ok": True, "status": "listening",
            "roomId": room_id, "stream": stream_url
        })

    except Exception as e:
        print("start error:", e)
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/comments")
def comments():
    username = request.args.get("username")
    since = int(request.args.get("since", 0))
    if not username:
        return jsonify({"ok": False}), 400
    events = [e for e in chat_store.get(username, []) if e["timestamp"] > since]
    return jsonify({"ok": True, "events": events})

@app.route("/proxy")
def proxy():
    url = request.args.get("url")
    if not url:
        return "url required", 400
    try:
        r = requests.get(url, stream=True, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.tiktok.com/",
            "Origin": "https://www.tiktok.com"
        })
        def generate():
            for chunk in r.iter_content(chunk_size=16384):
                if chunk:
                    yield chunk
        return Response(
            generate(),
            content_type=r.headers.get("content-type", "video/x-flv"),
            headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "no-cache"}
        )
    except Exception as e:
        print("Proxy error:", e)
        return f"proxy error: {e}", 500

@app.route("/")
def index():
    return jsonify({"ok": True, "service": "SEEYOUTIK", "active": len(clients)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
