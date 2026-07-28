"""Checkpoint layout and health helpers."""

from __future__ import annotations

from pathlib import Path


class CheckpointManager:
    QUERY_NAMES = {"clean", "quarantine", "aggregates"}

    def __init__(self, root: Path) -> None:
        self.root = root

    def path_for(self, query_name: str) -> Path:
        if query_name not in self.QUERY_NAMES:
            raise ValueError(f"Unknown query checkpoint: {query_name}")
        path = self.root / query_name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def latest_commit(self, query_name: str) -> str | None:
        commits = self.path_for(query_name) / "commits"
        if not commits.exists():
            return None
        numeric = sorted((item.name for item in commits.iterdir() if item.name.isdigit()), key=int)
        return numeric[-1] if numeric else None
