# Distributed Storage MVP (Milestone 1)

A small, runnable baseline for the distributed-storage lab. It starts one FastAPI gateway and four independent storage-node containers.

## Implemented in this milestone
- Four storage nodes with persistent Docker volumes.
- Consistent-hash ring with virtual nodes for selecting replica placement.
- SHA-256 content-addressed chunk files and integrity verification at write and read.
- Replication factor 3 (the gateway requires at least two successful writes).
- Object CRUD endpoints: upload/overwrite, list, download, metadata, delete.
- Basic health and cluster endpoints.
- Smoke test script.

## Not implemented yet (do not claim in the report)
- Automatic failure detection/repair and background scrubbing.
- Erasure coding (Reed-Solomon 4+2).
- Fixed-size chunk splitting for large files and reference-counted deduplication.
- Kubernetes deployment, authentication, and production-grade metadata consensus.
- A full benchmark harness for 5,000 small files and 10 large files.

## Run (Windows CMD or PowerShell)
From this folder:
```bash
docker compose up --build -d
docker compose ps
```
Open `http://localhost:8000/docs` for interactive API testing and `http://localhost:8000/health` to check node health.

## Smoke test
Install the test dependency in a local Python environment, then run:
```bash
python -m pip install -r tests/requirements.txt
python tests/smoke_test.py
```
Or test manually in Swagger UI at `/docs`:
1. `PUT /objects/{name}` and choose a file.
2. `GET /objects/{name}` and verify the returned bytes.
3. `GET /objects` to list objects.
4. `DELETE /objects/{name}`.

## Stop
```bash
docker compose down
```
Persistent volumes are retained. To erase all stored test data, use `docker compose down -v` (destructive).
