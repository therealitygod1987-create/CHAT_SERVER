import json
import os
from collections import defaultdict
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", ">:3")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8765"))

app = FastAPI(title="Class Chat Server")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

clients = {}
rooms = defaultdict(set)


def stamp():
    return datetime.now().strftime("%I:%M %p").lstrip("0")


async def send_json(ws, packet):
    await ws.send_text(json.dumps(packet))


async def send_system(ws, body):
    await send_json(ws, {
        "type": "system",
        "body": body,
        "timestamp": stamp()
    })


async def broadcast(room, packet):
    dead = []
    for client in list(rooms[room]):
        try:
            await send_json(client, packet)
        except Exception:
            dead.append(client)

    for client in dead:
        rooms[room].discard(client)
        clients.pop(client, None)


async def leave_all_rooms(ws):
    for room_name in list(rooms.keys()):
        rooms[room_name].discard(ws)
        if not rooms[room_name]:
            del rooms[room_name]


async def join_room(ws, room_name):
    await leave_all_rooms(ws)
    rooms[room_name].add(ws)
    if ws in clients:
        clients[ws]["room"] = room_name


async def handle_command(ws, raw):
    text = raw.strip()
    if not text.startswith("/join "):
        return False

    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await send_system(ws, "Usage: /join roomName")
        return True

    room_name = parts[1].strip()
    await join_room(ws, room_name)
    await send_system(ws, f"Joined room {room_name}.")
    return True


async def handle_packet(ws, packet):
    meta = clients.get(ws)
    if not meta:
        return

    packet_type = packet.get("type")
    room = str(packet.get("room") or meta["room"]).strip() or "Global"
    meta["room"] = room
    rooms[room].add(ws)

    if packet_type == "message":
        body = str(packet.get("body", "")).strip()
        if not body:
            return
        await broadcast(room, {
            "type": "message",
            "username": meta["username"],
            "room": room,
            "body": body,
            "timestamp": stamp()
        })
        return

    if packet_type == "file":
        await broadcast(room, {
            "type": "file",
            "username": meta["username"],
            "room": room,
            "name": packet.get("name", "download"),
            "data": packet.get("data", "#"),
            "caption": "Shared a file",
            "timestamp": stamp()
        })
        return

    await send_system(ws, f"Unknown packet type: {packet_type}")


@app.get("/health")
async def health():
    return {
        "ok": True,
        "clients": len(clients),
        "rooms": len(rooms)
    }


@app.get("/")
async def root():
    return {
        "name": "Class Chat Server",
        "websocket": "/ws",
        "health": "/health"
    }


@app.websocket("/ws")
async def websocket_handler(ws: WebSocket):
    await ws.accept()
    print("Client connected")

    try:
        await ws.send_text("LOGIN_USERNAME")
        username = (await ws.receive_text()).strip()[:24] or "Guest"

        await ws.send_text("LOGIN_PASSWORD")
        password = await ws.receive_text()
        if password != ACCESS_PASSWORD:
            await send_system(ws, "Wrong password.")
            await ws.close(code=1008)
            return

        await ws.send_text("LOGIN_ADMINKEY")
        admin_key = await ws.receive_text()

        clients[ws] = {
            "username": username,
            "admin_key": admin_key,
            "room": "Global"
        }
        rooms["Global"].add(ws)

        await send_system(ws, f"Connected as {username}.")
        print(f"{username} authenticated and joined Global")
        await broadcast("Global", {
            "type": "system",
            "body": f"{username} joined Global.",
            "timestamp": stamp()
        })

        while True:
            raw = await ws.receive_text()

            if raw.startswith("/"):
                handled = await handle_command(ws, raw)
                if handled:
                    continue

            try:
                packet = json.loads(raw)
            except json.JSONDecodeError:
                packet = {
                    "type": "message",
                    "body": raw,
                    "room": clients.get(ws, {}).get("room", "Global")
                }

            await handle_packet(ws, packet)
    except WebSocketDisconnect:
        pass
    finally:
        meta = clients.pop(ws, None)
        await leave_all_rooms(ws)
        if meta:
            print(f"{meta['username']} disconnected")
            await broadcast("Global", {
                "type": "system",
                "body": f"{meta['username']} disconnected.",
                "timestamp": stamp()
            })


if __name__ == "__main__":
    try:
        print(f"Starting WebSocket server on ws://{HOST}:{PORT}/ws")
        uvicorn.run(app, host=HOST, port=PORT)
    except Exception as error:
        print("Server crashed:", repr(error))
        input("Press Enter to close...")
