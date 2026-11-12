"""Where the local pipeline keeps its data, and how it reaches Kafka."""

from __future__ import annotations

import os
from dataclasses import dataclass

DELTA_VERSION = "3.2.0"
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6"
DELTA_PACKAGE = f"io.delta:delta-spark_2.12:{DELTA_VERSION}"


@dataclass(frozen=True)
class LocalConfig:
    data_dir: str = "data"
    source: str = "files"  # "files" or "kafka"
    kafka_bootstrap_servers: str = "localhost:9094"
    kafka_topic: str = "orders"

    @property
    def root(self) -> str:
        return os.path.abspath(self.data_dir)

    @property
    def landing_dir(self) -> str:
        return os.path.join(self.root, "landing", "orders")

    @property
    def warehouse_dir(self) -> str:
        return os.path.join(self.root, "warehouse")

    @property
    def metastore_dir(self) -> str:
        return os.path.join(self.root, "metastore_db")

    def checkpoint(self, name: str) -> str:
        return os.path.join(self.root, "checkpoints", name)
