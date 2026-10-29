
import hashlib
import os
import uuid
import requests

BASE = "http://localhost:8001"
name = f"chunk-test-{uuid.uuid4().hex}.bin"

# Generate a 9 MiB file in memory.
data = os.urandom(9 * 1024 * 1024)
digest = hashlib.sha256(data).hexdigest()
created = False

try:
    response = requests.post(
        f"{BASE}/objects/{name}",
        files={"file": (name, data, "application/octet-stream")},
        timeout=180,
    )
    print("UPLOAD:", response.status_code, response.text)
    response.raise_for_status()
    created = True

    response = requests.get(
        f"{BASE}/objects/{name}/metadata", timeout=60
    )
    print("METADATA:", response.status_code, response.text)
    response.raise_for_status()

    info = response.json()

    assert info["chunk_count"] == 3, (
        f"Expected 3 chunks, got {info['chunk_count']}"
    )

    expected_sizes = [
        4 * 1024 * 1024,
        4 * 1024 * 1024,
        1 * 1024 * 1024,
    ]
    actual_sizes = [
        chunk["size_bytes"] for chunk in info["chunks"]
    ]
    assert actual_sizes == expected_sizes, (
        f"Unexpected chunk sizes: {actual_sizes}"
    )

    response = requests.get(
        f"{BASE}/objects/{name}", timeout=180
    )
    print("DOWNLOAD:", response.status_code,
          "bytes:", len(response.content))
    response.raise_for_status()

    assert response.content == data, (
        "Downloaded content differs from original"
    )
    assert hashlib.sha256(response.content).hexdigest() == digest, (
        "File SHA-256 mismatch"
    )

    print("LARGE FILE CHUNKING TEST PASSED")

except Exception as error:
    print("TEST FAILED:", repr(error))

finally:
    if created:
        try:
            response = requests.delete(
                f"{BASE}/objects/{name}", timeout=120
            )
            print("TEST CLEANUP:",
                  response.status_code, response.text)
        except Exception as error:
            print("CLEANUP FAILED:", repr(error))
