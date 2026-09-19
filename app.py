import asyncio
import time
import re
from collections import deque
from typing import Dict, Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    ConnectEvent,
    DisconnectEvent,
    CommentEvent,
    GiftEvent,
    LikeEvent,
    FollowEvent,
    ShareEvent,
    JoinEvent,
)

app = FastAPI(title="SEEYOUTIK LIVE Comments")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

MAX_COMMENTS = 100
MAX_USERS = 3

USERNAME_RE = re.compile(r"^[A-Za-z0-9._]{1,40}$")

rooms: Dict[str, Dict[str, Any]] = {}
rooms_lock = asyncio.Lock()


def clean_username(username: str) -> str:
    username = username.strip()

    if username.startswith("@"):
        username = username[1:]

    return username.lower()


def valid_username(username: str) -> bool:
    return bool(USERNAME_RE.fullmatch(username))


def make_event(event_type: str, **data):
    return {
        "type": event_type,
        "timestamp": time.time(),
        **data,
    }


async def add_event(username: str, event: dict):
    room = rooms.get(username)

    if room:
        room["events"].append(event)


def create_client(username: str):

    client = TikTokLiveClient(
        unique_id=username
    )

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent):

        print(f"[CONNECTED] @{username}")

        room = rooms.get(username)

        if room:
            room["connected"] = True
            room["error"] = None

            await add_event(
                username,
                make_event(
                    "system",
                    message=f"Connected to @{username}"
                )
            )

    @client.on(DisconnectEvent)
    async def on_disconnect(event: DisconnectEvent):

        print(f"[DISCONNECTED] @{username}")

        room = rooms.get(username)

        if room:
            room["connected"] = False

            await add_event(
                username,
                make_event(
                    "system",
                    message="TikTok LIVE connection closed"
                )
            )

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):

        try:
            nickname = event.user.nickname or "Unknown"
        except Exception:
            nickname = "Unknown"

        try:
            comment = event.comment or ""
        except Exception:
            comment = ""

        if not comment:
            return

        print(
            f"[COMMENT] @{username} | "
            f"{nickname}: {comment}"
        )

        await add_event(
            username,
            make_event(
                "comment",
                user=nickname,
                comment=comment
            )
        )

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent):

        try:
            if event.gift.streakable and event.streaking:
                return
        except Exception:
            pass

        try:
            user = event.user.nickname or "Unknown"
        except Exception:
            user = "Unknown"

        try:
            gift_name = event.gift.name or "Gift"
        except Exception:
            gift_name = "Gift"

        try:
            count = event.repeat_count
        except Exception:
            count = 1

        print(
            f"[GIFT] @{username} | "
            f"{user} sent {gift_name} x{count}"
        )

        await add_event(
            username,
            make_event(
                "gift",
                user=user,
                gift=gift_name,
                count=count
            )
        )

    @client.on(LikeEvent)
    async def on_like(event: LikeEvent):

        try:
            user = event.user.nickname or "Unknown"
        except Exception:
            user = "Unknown"

        try:
            count = event.count
        except Exception:
            count = 1

        await add_event(
            username,
            make_event(
                "like",
                user=user,
                count=count
            )
        )

    @client.on(FollowEvent)
    async def on_follow(event: FollowEvent):

        try:
            user = event.user.nickname or "Unknown"
        except Exception:
            user = "Unknown"

        print(
            f"[FOLLOW] @{username} | {user}"
        )

        await add_event(
            username,
            make_event(
                "follow",
                user=user
            )
        )

    @client.on(ShareEvent)
    async def on_share(event: ShareEvent):

        try:
            user = event.user.nickname or "Unknown"
        except Exception:
            user = "Unknown"

        await add_event(
            username,
            make_event(
                "share",
                user=user
            )
        )

    @client.on(JoinEvent)
    async def on_join(event: JoinEvent):

        try:
            user = event.user.nickname or "Unknown"
        except Exception:
            user = "Unknown"

        await add_event(
            username,
            make_event(
                "join",
                user=user
            )
        )

    return client


async def run_client(username: str):

    client = create_client(username)

    room = rooms.get(username)

    if room:
        room["client"] = client

    try:

        print(
            f"[STARTING] TikTok LIVE @{username}"
        )

        await client.connect(
            fetch_room_info=True
        )

    except Exception as error:

        print(
            f"[ERROR] @{username}: {error}"
        )

        room = rooms.get(username)

        if room:

            room["connected"] = False
            room["error"] = str(error)

            await add_event(
                username,
                make_event(
                    "error",
                    message=str(error)
                )
            )

    finally:

        room = rooms.get(username)

        if room:
            room["connected"] = False

            if room.get("client") is client:
                room["client"] = None

        print(
            f"[STOPPED] @{username}"
        )


@app.get("/")
async def root():

    return {
        "service": "SEEYOUTIK LIVE Comments",
        "status": "online"
    }


@app.get("/health")
async def health():

    return {
        "status": "ok",
        "rooms": len(rooms)
    }


@app.get("/start")
async def start(
    username: str = Query(...)
):

    username = clean_username(username)

    if not valid_username(username):

        return {
            "ok": False,
            "error": "Invalid TikTok username"
        }

    async with rooms_lock:

        existing = rooms.get(username)

        if existing:

            return {
                "ok": True,
                "status": (
                    "connected"
                    if existing["connected"]
                    else "connecting"
                ),
                "username": username
            }

        if len(rooms) >= MAX_USERS:

            return {
                "ok": False,
                "error": "Maximum active users reached"
            }

        rooms[username] = {
            "username": username,
            "connected": False,
            "client": None,
            "task": None,
            "error": None,
            "events": deque(maxlen=MAX_COMMENTS),
            "created": time.time()
        }

        task = asyncio.create_task(
            run_client(username)
        )

        rooms[username]["task"] = task

    return {
        "ok": True,
        "status": "connecting",
        "username": username
    }


@app.get("/comments")
async def comments(
    username: str = Query(...),
    since: float = Query(0)
):

    username = clean_username(username)

    if not valid_username(username):

        return {
            "ok": False,
            "error": "Invalid TikTok username",
            "events": []
        }

    room = rooms.get(username)

    if not room:

        return {
            "ok": False,
            "status": "not_started",
            "events": []
        }

    events = []

    for event in room["events"]:

        if event["timestamp"] > since:
            events.append(event)

    return {
        "ok": True,
        "username": username,
        "connected": room["connected"],
        "error": room["error"],
        "events": events,
        "server_time": time.time()
    }


@app.get("/status")
async def status(
    username: str = Query(...)
):

    username = clean_username(username)

    room = rooms.get(username)

    if not room:

        return {
            "ok": True,
            "username": username,
            "status": "not_started",
            "connected": False
        }

    return {
        "ok": True,
        "username": username,
        "status": (
            "connected"
            if room["connected"]
            else "connecting"
        ),
        "connected": room["connected"],
        "error": room["error"]
    }


@app.get("/stop")
async def stop(
    username: str = Query(...)
):

    username = clean_username(username)

    async with rooms_lock:

        room = rooms.get(username)

        if not room:

            return {
                "ok": True,
                "status": "not_started"
            }

        client = room.get("client")

        if client:

            try:
                await client.disconnect()
            except Exception:
                pass

        task = room.get("task")

        if task:

            try:
                await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=5
                )
            except Exception:
                pass

        rooms.pop(username, None)

    return {
        "ok": True,
        "status": "stopped",
        "username": username
    }
