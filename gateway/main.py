
import hashlib
import json
import os
import bisect
from pathlib import Path
from typing import Dict, List
from datetime import datetime, timezone

import httpx
import psycopg
from psycopg.types.json import Jsonb
from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from fastapi.responses import Response

app = FastAPI(title="Distributed Storage Gateway", version="0.4.0")

NODE_URLS = [
    x.strip().rstrip("/")
    for x in os.getenv(
        "NODE_URLS",
        "http://node1:8001,http://node2:8001,"
        "http://node3:8001,http://node4:8001",
    ).split(",")
    if x.strip()
]

if not NODE_URLS:
    raise RuntimeError("At least one storage node must be configured")

REPLICATION_FACTOR = min(
    int(os.getenv("REPLICATION_FACTOR", "3")),
    len(NODE_URLS),
)
VIRTUAL_NODES = int(os.getenv("VIRTUAL_NODES", "64"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", str(4 * 1024 * 1024)))
QUORUM = min(2, REPLICATION_FACTOR)

if REPLICATION_FACTOR < 1 or VIRTUAL_NODES < 1 or CHUNK_SIZE < 1:
    raise RuntimeError("Invalid replication, virtual-node, or chunk-size setting")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://storage_user:local_storage_password@postgres:5432/storage_metadata",
)

META_FILE = Path("/app/metadata/objects.json")
META_FILE.parent.mkdir(parents=True, exist_ok=True)

HEARTBEATS = {}


def connect_db():
    return psycopg.connect(DATABASE_URL)


def normalize_item(key: str, original_item: Dict, version: int, created_at):
    item = dict(original_item)
    chunks = item_chunks(item)

    item.update({
        "bucket": "default",
        "key": key,
        "version": version,
        "created_at": created_at.isoformat(),
        "chunk_hashes": [
            chunk["sha256"]
            for chunk in chunks
            if "sha256" in chunk
        ],
    })

    return item


def write_object(cur, key: str, original_item: Dict, version: int, created_at):
    item = normalize_item(key, original_item, version, created_at)
    chunk_hashes = item["chunk_hashes"]

    cur.execute(
        """
        INSERT INTO objects (
            bucket, object_key, size, version,
            chunk_hashes, created_at, metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (bucket, object_key) DO UPDATE SET
            size = EXCLUDED.size,
            version = EXCLUDED.version,
            chunk_hashes = EXCLUDED.chunk_hashes,
            created_at = EXCLUDED.created_at,
            metadata = EXCLUDED.metadata
        """,
        (
            "default",
            key,
            item.get("size_bytes", 0),
            version,
            Jsonb(chunk_hashes),
            created_at,
            Jsonb(item),
        ),
    )


def initialize_database():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS objects (
                    bucket TEXT NOT NULL DEFAULT 'default',
                    object_key TEXT NOT NULL,
                    size BIGINT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    chunk_hashes JSONB NOT NULL DEFAULT '[]'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    metadata JSONB NOT NULL,
                    PRIMARY KEY (bucket, object_key)
                )
            """)

            cur.execute("SELECT COUNT(*) FROM objects")
            row_count = cur.fetchone()[0]

    # Migrate the old JSON metadata once, only when PostgreSQL is empty.
    if row_count == 0:
        try:
            old_meta = json.loads(META_FILE.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            old_meta = {}

        if old_meta:
            with connect_db() as conn:
                with conn.cursor() as cur:
                    for key, item in old_meta.items():
                        created_at = datetime.now(timezone.utc)
                        write_object(cur, key, item, 1, created_at)


@app.on_event("startup")
def startup_database():
    initialize_database()


def load_meta() -> Dict:
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT object_key, metadata
                FROM objects
                WHERE bucket = 'default'
            """)
            return {
                key: item
                for key, item in cur.fetchall()
            }


def save_meta(meta: Dict):
    # Synchronize the default bucket with the current metadata dictionary.
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT object_key, version, created_at
                FROM objects
                WHERE bucket = 'default'
            """)
            existing = {
                key: (version, created_at)
                for key, version, created_at in cur.fetchall()
            }

            cur.execute("DELETE FROM objects WHERE bucket = 'default'")

            for key, original_item in meta.items():
                previous = existing.get(key)

                if previous:
                    old_version, created_at = previous
                    version = old_version + 1
                else:
                    version = 1
                    created_at = datetime.now(timezone.utc)

                write_object(cur, key, original_item, version, created_at)


# Consistent-hashing ring with virtual nodes.
ring = []

for node_url in NODE_URLS:
    for vnode in range(VIRTUAL_NODES):
        key = hashlib.sha256(
            f"{node_url}#{vnode}".encode()
        ).hexdigest()
        ring.append((key, node_url))

ring.sort()
ring_keys = [entry[0] for entry in ring]


def owners(chunk_digest: str) -> List[str]:
    """Choose distinct storage nodes using the chunk digest."""
    start = bisect.bisect_left(ring_keys, chunk_digest)
    result = []

    for offset in range(len(ring)):
        node = ring[(start + offset) % len(ring)][1]

        if node not in result:
            result.append(node)

        if len(result) >= REPLICATION_FACTOR:
            break

    return result


async def node_call(method: str, url: str, **kwargs):
    async with httpx.AsyncClient(timeout=120.0) as client:
        return await client.request(method, url, **kwargs)


def item_chunks(item: Dict) -> List[Dict]:
    """Support new chunked metadata and older whole-object metadata."""
    if "chunks" in item:
        return item["chunks"]

    return [{
        "sha256": item["sha256"],
        "size_bytes": item.get("size_bytes", 0),
        "replicas": item.get("replicas", []),
    }]


def digest_is_referenced(meta: Dict, digest: str) -> bool:
    for item in meta.values():
        if any(
            chunk.get("sha256") == digest
            for chunk in item_chunks(item)
        ):
            return True

    return False


def referenced_replicas(meta: Dict, digest: str) -> List[str]:
    result = []

    for item in meta.values():
        for chunk in item_chunks(item):
            if chunk.get("sha256") != digest:
                continue

            for node in chunk.get("replicas", item.get("replicas", [])):
                if node not in result:
                    result.append(node)

    return result


async def verified_replicas(
    digest: str,
    replicas: List[str],
) -> List[str]:
    valid = []

    for node in replicas:
        try:
            response = await node_call("GET", f"{node}/chunks/{digest}")

            if (
                response.status_code == 200
                and hashlib.sha256(response.content).hexdigest() == digest
            ):
                valid.append(node)
        except Exception:
            continue

    return valid


async def store_chunk(
    data: bytes,
    digest: str,
    meta: Dict,
    chunk_cache: Dict[str, List[str]],
):
    """Reuse valid content or write the chunk to its selected nodes."""
    if digest in chunk_cache:
        return chunk_cache[digest], True

    previous_replicas = referenced_replicas(meta, digest)
    valid_existing = await verified_replicas(digest, previous_replicas)
    reused = bool(valid_existing)

    successful = list(valid_existing)
    failures = {}

    if len(successful) < QUORUM:
        for node in owners(digest):
            if node in successful:
                continue

            try:
                response = await node_call(
                    "PUT",
                    f"{node}/chunks/{digest}",
                    content=data,
                    headers={"X-Content-SHA256": digest},
                )

                if response.status_code == 200:
                    successful.append(node)
                else:
                    failures[node] = (
                        f"HTTP {response.status_code}: {response.text}"
                    )
            except Exception as exc:
                failures[node] = str(exc)

            if len(successful) >= REPLICATION_FACTOR:
                break

    if len(successful) < QUORUM:
        raise HTTPException(
            503,
            {
                "message": "Not enough chunk replicas stored",
                "chunk_sha256": digest,
                "replicas": successful,
                "failures": failures,
            },
        )

    chunk_cache[digest] = successful
    return successful, reused


@app.post("/internal/heartbeats")
def receive_heartbeat(payload: dict = Body(...)):
    node_id = payload.get("node_id")
    free_space = payload.get("free_space_bytes")
    chunk_count = payload.get("chunk_count")

    if not isinstance(node_id, str) or not node_id:
        raise HTTPException(400, "Invalid node_id")

    if (
        type(free_space) is not int
        or free_space < 0
        or type(chunk_count) is not int
        or chunk_count < 0
    ):
        raise HTTPException(400, "Invalid storage statistics")

    HEARTBEATS[node_id] = {
        "node_id": node_id,
        "free_space_bytes": free_space,
        "chunk_count": chunk_count,
        "last_heartbeat": datetime.now(timezone.utc).isoformat(),
    }

    return {"received": True, "node_id": node_id}


@app.get("/heartbeats")
def get_heartbeats():
    return {"count": len(HEARTBEATS), "heartbeats": HEARTBEATS}


@app.get("/")
def root():
    return {"service": "Distributed Storage Gateway", "docs": "/docs"}


@app.get("/health")
async def health():
    statuses = {}

    for node in NODE_URLS:
        try:
            response = await node_call("GET", f"{node}/health")
            statuses[node] = (
                response.json()
                if response.status_code == 200
                else {"status": "error", "code": response.status_code}
            )
        except Exception as exc:
            statuses[node] = {"status": "unreachable", "detail": str(exc)}

    return {
        "status": "ok",
        "replication_factor": REPLICATION_FACTOR,
        "nodes": statuses,
    }


@app.get("/cluster")
def cluster():
    return {
        "nodes": NODE_URLS,
        "node_count": len(NODE_URLS),
        "replication_factor": REPLICATION_FACTOR,
        "virtual_nodes_per_node": VIRTUAL_NODES,
        "chunk_size_bytes": CHUNK_SIZE,
    }


@app.post("/objects/{name}")
async def put_object(name: str, file: UploadFile = File(...)):
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise HTTPException(400, "Use a simple object name without slashes.")

    meta = load_meta()
    file_hash = hashlib.sha256()
    file_size = 0
    chunk_records = []
    chunk_cache = {}
    deduplicated = False

    while True:
        block = await file.read(CHUNK_SIZE)
        at_eof = not block

        if at_eof and chunk_records:
            break

        digest = hashlib.sha256(block).hexdigest()
        file_hash.update(block)
        file_size += len(block)

        replicas, reused = await store_chunk(
            block, digest, meta, chunk_cache
        )

        deduplicated = deduplicated or reused
        chunk_records.append({
            "sha256": digest,
            "size_bytes": len(block),
            "replicas": replicas,
        })

        if at_eof:
            break

    file_digest = file_hash.hexdigest()

    all_replicas = []
    for chunk in chunk_records:
        for node in chunk["replicas"]:
            if node not in all_replicas:
                all_replicas.append(node)

    meta[name] = {
        "sha256": file_digest,
        "size_bytes": file_size,
        "content_type": file.content_type or "application/octet-stream",
        "chunks": chunk_records,
        "chunk_size_bytes": CHUNK_SIZE,
        "chunk_count": len(chunk_records),
        "replicas": all_replicas,
        "requested_replicas": owners(file_digest),
    }

    save_meta(meta)

    saved_item = load_meta()[name]

    return {
        "name": name,
        "sha256": file_digest,
        "size_bytes": file_size,
        "chunk_count": len(chunk_records),
        "chunk_size_bytes": CHUNK_SIZE,
        "replicas": all_replicas,
        "deduplicated_by_content_hash": deduplicated,
        "bucket": saved_item["bucket"],
        "version": saved_item["version"],
        "created_at": saved_item["created_at"],
        "chunk_hashes": saved_item["chunk_hashes"],
    }


@app.get("/objects")
def list_objects():
    meta = load_meta()

    return {
        "count": len(meta),
        "objects": [
            {"name": name, **item}
            for name, item in sorted(meta.items())
        ],
    }


@app.get("/objects/{name}")
async def get_object(name: str):
    item = load_meta().get(name)

    if not item:
        raise HTTPException(404, "Object not found")

    assembled = bytearray()
    file_hash = hashlib.sha256()

    for chunk in item_chunks(item):
        digest = chunk["sha256"]
        chunk_data = None

        for node in chunk.get("replicas", item.get("replicas", [])):
            try:
                response = await node_call("GET", f"{node}/chunks/{digest}")

                if response.status_code != 200:
                    continue

                if hashlib.sha256(response.content).hexdigest() != digest:
                    continue

                chunk_data = response.content
                break
            except Exception:
                continue

        if chunk_data is None:
            raise HTTPException(
                503, f"No healthy, valid replica for chunk {digest}"
            )

        assembled.extend(chunk_data)
        file_hash.update(chunk_data)

    actual_file_hash = file_hash.hexdigest()

    if actual_file_hash != item["sha256"]:
        raise HTTPException(500, "Reassembled object SHA-256 mismatch")

    return Response(
        content=bytes(assembled),
        media_type=item.get("content_type", "application/octet-stream"),
        headers={
            "X-Content-SHA256": actual_file_hash,
            "Content-Disposition": f'attachment; filename="{name}"',
        },
    )


@app.delete("/objects/{name}")
async def delete_object(name: str):
    meta = load_meta()
    item = meta.get(name)

    if not item:
        raise HTTPException(404, "Object not found")

    digests = list(dict.fromkeys(
        chunk["sha256"] for chunk in item_chunks(item)
    ))

    replica_map = {
        digest: referenced_replicas(meta, digest)
        for digest in digests
    }

    del meta[name]
    save_meta(meta)

    results = {}
    removed_digests = []

    for digest in digests:
        if digest_is_referenced(meta, digest):
            results[digest] = {
                "status": "retained",
                "reason": "still referenced",
            }
            continue

        node_results = {}

        for node in replica_map[digest]:
            try:
                response = await node_call("DELETE", f"{node}/chunks/{digest}")
                node_results[node] = response.status_code
            except Exception as exc:
                node_results[node] = str(exc)

        results[digest] = node_results
        removed_digests.append(digest)

    return {
        "deleted": name,
        "unreferenced_chunks": removed_digests,
        "chunk_delete_results": results,
    }


@app.get("/objects/{name}/metadata")
def object_metadata(name: str):
    item = load_meta().get(name)

    if not item:
        raise HTTPException(404, "Object not found")

    return {"name": name, **item}
