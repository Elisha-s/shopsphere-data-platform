"""Writes events as newline-delimited JSON files into a landing folder.

This is the no-infrastructure ingestion path: the local pipeline can read the
folder as a stream, so the whole project runs with just Python and Java.
Each ``close()`` (or every ``batch_size`` events) produces one new file, the
way an upstream system would drop files into cloud storage.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any

from ingestion import config


class FileEventProducer:
    def __init__(self, landing_dir: str = config.LANDING_DIR, batch_size: int = 500):
        self.landing_dir = landing_dir
        self.batch_size = batch_size
        self.failed = 0
        self._buffer: list[str] = []
        self._files_written = 0
        os.makedirs(landing_dir, exist_ok=True)

    def send(self, event: dict[str, Any]) -> None:
        self._buffer.append(json.dumps(event))
        if len(self._buffer) >= self.batch_size:
            self._write_file()

    def _write_file(self) -> None:
        if not self._buffer:
            return
        self._files_written += 1
        # Timestamp + sequence number keeps files in the order they were written.
        name = f"orders-{int(time.time() * 1000)}-{self._files_written:05d}-{uuid.uuid4().hex[:8]}.json"
        final_path = os.path.join(self.landing_dir, name)
        # Write under a hidden name and rename, so a reader never sees a
        # half-written file (Spark ignores files starting with a dot).
        temp_path = os.path.join(self.landing_dir, f".{name}.tmp")
        with open(temp_path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(self._buffer) + "\n")
        os.replace(temp_path, final_path)
        self._buffer = []

    def close(self) -> None:
        self._write_file()
