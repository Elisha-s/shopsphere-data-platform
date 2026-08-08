from pathlib import Path
from datetime import datetime

METADATA_FILE = "data/metadata/gold_job/last_run.txt"


def get_last_run():
    file = Path(METADATA_FILE)

    if not file.exists():
        return None

    return file.read_text().strip()


def update_last_run():
    Path(METADATA_FILE).parent.mkdir(
        parents=True,
        exist_ok=True
    )

    Path(METADATA_FILE).write_text(
        datetime.utcnow().isoformat()
    )