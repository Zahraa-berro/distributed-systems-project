
import asyncio
import hashlib
import os
import shutil
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response

app = FastAPI(title="Storage Node")

NODE_ID = os.getenv("NODE_ID", "node-unknown")
DATA_DIR = Path("/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:8000").rstrip("/")
HEARTBEAT_INTERVAL = 5
heartbeat_task = None


def get_storage_stats():
    """Return the actual free disk space and stored chunk count."""
    disk = shutil.disk_usage(DATA_DIR)

    chunk_count = sum(
        1
        for path in DATA_DIR.iterdir()
        if path.is_file()
        and len(path.name) == 64
        and all(c in "0123456789abcdef" for c in path.name)
    )

    return {
        "node_id": NODE_ID,
        "free_space_bytes": disk.free,
        "chunk_count": chunk_count,
    }


async def heartbeat_loop():
    """Send storage statistics to the gateway every five seconds."""
    while True:
        try:
            payload = get_storage_stats()

            async with httpx.AsyncClient(timeout=3.0) as client:
                response = await client.post(
                    f"{GATEWAY_URL}/internal/heartbeats",
                    json=payload,
                )
                response.raise_for_status()

            print(f"[HEARTBEAT] Sent successfully: {payload}")

        except Exception as exc:
            print(f"[HEARTBEAT] Failed to send: {exc}")

        await asyncio.sleep(HEARTBEAT_INTERVAL)


@app.on_event("startup")
async def start_heartbeat():
    global heartbeat_task
    heartbeat_task = asyncio.create_task(heartbeat_loop())


@app.on_event("shutdown")
async def stop_heartbeat():
    global heartbeat_task

    if heartbeat_task is not None:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass


@app.get("/health")
def health():
    return {"status": "ok", "node_id": NODE_ID}


@app.put("/chunks/{digest}")
async def put_chunk(digest: str, request: Request):
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise HTTPException(400, "Invalid SHA-256 digest")

    data = await request.body()
    actual = hashlib.sha256(data).hexdigest()

    if actual != digest:
        raise HTTPException(400, "SHA-256 mismatch")

    path = DATA_DIR / digest

    if not path.exists():
        temp = DATA_DIR / (digest + ".tmp")
        temp.write_bytes(data)
        temp.replace(path)

    return {
        "node_id": NODE_ID,
        "sha256": digest,
        "size_bytes": len(data),
        "stored": True,
    }


@app.get("/chunks/{digest}")
def get_chunk(digest: str):
    path = DATA_DIR / digest

    if not path.is_file():
        raise HTTPException(404, "Chunk not found")

    data = path.read_bytes()

    if hashlib.sha256(data).hexdigest() != digest:
        raise HTTPException(500, "Stored chunk failed integrity check")

    return Response(content=data, media_type="application/octet-stream")


@app.head("/chunks/{digest}")
def head_chunk(digest: str):
    path = DATA_DIR / digest

    if not path.is_file():
        raise HTTPException(404, "Chunk not found")

    return Response(
        status_code=200,
        headers={"Content-Length": str(path.stat().st_size)},
    )


@app.delete("/chunks/{digest}")
def delete_chunk(digest: str):
    path = DATA_DIR / digest

    if path.exists():
        path.unlink()

    return {"node_id": NODE_ID, "deleted": digest}
