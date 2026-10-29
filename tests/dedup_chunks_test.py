
import hashlib
import uuid
import requests

BASE = "http://localhost:8001"
suffix = uuid.uuid4().hex
name_a = f"dedup-chunk-a-{suffix}.txt"
name_b = f"dedup-chunk-b-{suffix}.txt"

# Both objects contain identical data.
payload = (
    b"Distributed storage deduplication test.\n"
    * 100
)
expected_hash = hashlib.sha256(payload).hexdigest()

session = requests.Session()
created = []
success = False

try:
    # Upload the first object.
    r = session.post(
        f"{BASE}/objects/{name_a}",
        files={"file": (name_a, payload, "text/plain")},
        timeout=60,
    )
    print("UPLOAD A:", r.status_code, r.text)
    r.raise_for_status()
    created.append(name_a)

    # Upload the second object with identical content.
    r = session.post(
        f"{BASE}/objects/{name_b}",
        files={"file": (name_b, payload, "text/plain")},
        timeout=60,
    )
    print("UPLOAD B:", r.status_code, r.text)
    r.raise_for_status()
    created.append(name_b)

    # Compare object metadata.
    ma = session.get(
        f"{BASE}/objects/{name_a}/metadata", timeout=30
    )
    mb = session.get(
        f"{BASE}/objects/{name_b}/metadata", timeout=30
    )
    ma.raise_for_status()
    mb.raise_for_status()

    meta_a = ma.json()
    meta_b = mb.json()

    assert meta_a["sha256"] == expected_hash
    assert meta_b["sha256"] == expected_hash
    assert meta_a["sha256"] == meta_b["sha256"]

    chunks_a = [c["sha256"] for c in meta_a["chunks"]]
    chunks_b = [c["sha256"] for c in meta_b["chunks"]]
    assert chunks_a == chunks_b, "Chunk hashes differ"

    print("SAME FILE HASH: PASSED")
    print("SAME CHUNK HASHES: PASSED")

    # Delete A and verify B remains readable.
    r = session.delete(
        f"{BASE}/objects/{name_a}", timeout=60
    )
    print("DELETE A:", r.status_code, r.text)
    r.raise_for_status()
    created.remove(name_a)

    r = session.get(
        f"{BASE}/objects/{name_b}", timeout=60
    )
    print("GET B AFTER DELETE A:", r.status_code,
          "bytes:", len(r.content))
    r.raise_for_status()

    assert r.content == payload
    assert hashlib.sha256(r.content).hexdigest() == expected_hash

    print("SHARED CHUNKS SURVIVE DELETE: PASSED")
    success = True

except Exception as error:
    print("TEST FAILED:", repr(error))

finally:
    # Delete only test objects created by this script.
    for name in created:
        try:
            r = session.delete(
                f"{BASE}/objects/{name}", timeout=60
            )
            print("CLEANUP", name, ":", r.status_code, r.text)
        except Exception as error:
            print("CLEANUP FAILED:", name, repr(error))

if success:
    print("CHUNK DEDUPLICATION TEST PASSED")
