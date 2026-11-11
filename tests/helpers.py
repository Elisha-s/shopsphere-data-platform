"""Shared helpers for the pipeline tests."""

from ingestion.run_producer import parse_args, run


def generate_landing_files(landing_dir: str, ticks: int, seed: int, minutes: int = 120) -> dict:
    """Write a reproducible backfill into ``landing_dir`` and return the generator's stats."""
    args = parse_args(
        [
            "--target",
            "files",
            "--max-events",
            str(ticks),
            "--backfill-minutes",
            str(minutes),
            "--seed",
            str(seed),
            "--landing-dir",
            landing_dir,
            "--invalid-rate",
            "0.05",
            "--duplicate-rate",
            "0.05",
            "--late-rate",
            "0.10",
        ]
    )
    return run(args)
