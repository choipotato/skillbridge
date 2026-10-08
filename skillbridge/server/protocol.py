from __future__ import annotations

# Payload bytes only; the 10-byte length header is not included.
MAX_FRAME_LENGTH = 1_000_000


def parse_frame_length(header: bytes) -> int:
    length = int(header)
    if not 0 <= length <= MAX_FRAME_LENGTH:
        raise ValueError(f'Invalid frame length {length}; expected 0..{MAX_FRAME_LENGTH} bytes')
    return length
