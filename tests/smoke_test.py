
import hashlib
import os
import uuid

import requests

BASE = os.getenv("GATEWAY_URL", "http://localhost:8001").rstrip("/")
name = f"smoke-test-{uuid.uuid4().hex}.txt"
payload = b"Distributed storage lab smoke test\n"
expected_digest = hashlib.sha256(payload).hexdigest()

try:
    # 1. Upload
    response = requests.post(
        f"{BASE}/objects/{name}",
        files={"file": (name, payload, "text/plain")},
        timeout=30,
    )
    print("PUT:", response.status_code, response.text)
    response.raise_for_status()

    upload_result = response.json()
    assert upload_result["sha256"] == expected_digest
    assert upload_result["size_bytes"] == len(payload)
    assert len(upload_result["replicas"]) >= 2

    # 2. Download and verify content
    response = requests.get(f"{BASE}/objects/{name}", timeout=30)
    print("GET:", response.status_code, len(response.content))
    response.raise_for_status()

    assert response.content == payload
    assert response.headers.get("X-Content-SHA256") == expected_digest

    # 3. Verify object appears in listing
    response = requests.get(f"{BASE}/objects", timeout=30)
    print("LIST:", response.status_code)
    response.raise_for_status()

    objects = response.json()["objects"]
    assert any(item["name"] == name for item in objects)

    # 4. Verify metadata
    response = requests.get(f"{BASE}/objects/{name}/metadata", timeout=30)
    print("METADATA:", response.status_code, response.text)
    response.raise_for_status()

    assert response.json()["sha256"] == expected_digest

    print("SMOKE TEST PASSED")

finally:
    # Delete only this test's uniquely named object.
    response = requests.delete(f"{BASE}/objects/{name}", timeout=30)
    if response.status_code not in (200, 404):
        print("WARNING: Test cleanup failed:", response.status_code, response.text)
    else:
        print("TEST OBJECT CLEANUP:", response.status_code)
