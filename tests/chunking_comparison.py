
import hashlib
import statistics
import tempfile
import time
from pathlib import Path

from fastcdc import fastcdc

MIB = 1024 * 1024
FIXED_SIZE = 4 * MIB

# FASTCDC: minimum 1 MiB, average target 4 MiB, maximum 8 MiB
MIN_SIZE = 1 * MIB
AVG_SIZE = 4 * MIB
MAX_SIZE = 8 * MIB

# Generate one test file and use the same bytes for both methods.
data = bytes((i * 31 + i // 251) % 256 for i in range(9 * MIB))
original_hash = hashlib.sha256(data).hexdigest()

# Method 1: Fixed-size chunking
start = time.perf_counter()
fixed_chunks = [
    data[i:i + FIXED_SIZE]
    for i in range(0, len(data), FIXED_SIZE)
]
fixed_time = time.perf_counter() - start

fixed_reconstructed = b"".join(fixed_chunks)
assert hashlib.sha256(fixed_reconstructed).hexdigest() == original_hash

# Method 2: FASTCDC content-defined chunking
with tempfile.TemporaryDirectory() as temp_dir:
    file_path = Path(temp_dir) / "comparison.bin"
    file_path.write_bytes(data)

    start = time.perf_counter()
    cdc_chunks = list(
        fastcdc(str(file_path), MIN_SIZE, AVG_SIZE, MAX_SIZE)
    )
    cdc_time = time.perf_counter() - start

    # Verify that all chunk ranges cover the original file in order.
    reconstructed = b"".join(
        data[chunk.offset:chunk.offset + chunk.length]
        for chunk in cdc_chunks
    )

assert hashlib.sha256(reconstructed).hexdigest() == original_hash

fixed_sizes = [len(chunk) for chunk in fixed_chunks]
cdc_sizes = [chunk.length for chunk in cdc_chunks]

print("=== CHUNKING COMPARISON ===")
print(f"Input size: {len(data) / MIB:.2f} MiB")
print(f"Original SHA-256: {original_hash}")
print()
print("FIXED-SIZE (4 MiB)")
print(f"Chunks: {len(fixed_chunks)}")
print(f"Chunk sizes (MiB): {[round(s / MIB, 2) for s in fixed_sizes]}")
print(f"Average chunk size: {statistics.mean(fixed_sizes) / MIB:.2f} MiB")
print(f"Time: {fixed_time:.6f} seconds")
print("Reconstruction SHA-256 verified: PASS")
print()
print("FASTCDC (min=1, avg=4, max=8 MiB)")
print(f"Chunks: {len(cdc_chunks)}")
print(f"Chunk sizes (MiB): {[round(s / MIB, 2) for s in cdc_sizes]}")
print(f"Average chunk size: {statistics.mean(cdc_sizes) / MIB:.2f} MiB")
print(f"Time: {cdc_time:.6f} seconds")
print("Reconstruction SHA-256 verified: PASS")
print()
print("Both methods reconstructed the original data successfully.")
