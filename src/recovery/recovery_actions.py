"""Safe recovery actions and action audit persistence."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Callable

from src.utils.file_utils import atomic_write_json, read_json
from src.utils.time_utils import utc_now_iso


class RecoveryActions:
    def __init__(self, metadata_dir: Path) -> None:
        self.history_path = metadata_dir / "recovery_history.json"

    def record(self, action: str, success: bool, **details: Any) -> None:
        history = read_json(self.history_path, []) or []
        history.append(
            {
                "action": action,
                "success": success,
                "timestamp": utc_now_iso(),
                "details": details,
            }
        )
        atomic_write_json(self.history_path, history[-500:])

    def restart_query(self, start: Callable[[], Any]) -> Any:
        try:
            query = start()
            self.record("restart_streaming_query", True)
            return query
        except Exception as exc:
            self.record("restart_streaming_query", False, error=str(exc))
            raise

    def archive_corrupt_checkpoint(self, checkpoint: Path) -> Path:
        """Archive only an explicit query checkpoint, never the checkpoint root."""
        resolved = checkpoint.resolve()
        if resolved.name in {"", ".", "checkpoints"} or not checkpoint.exists():
            raise ValueError(f"Refusing to archive unsafe checkpoint target: {resolved}")
        destination = resolved.with_name(f"{resolved.name}.corrupt-{utc_now_iso().replace(':', '-')}")
        shutil.move(str(resolved), str(destination))
        resolved.mkdir(parents=True)
        self.record("archive_corrupt_checkpoint", True, source=str(resolved), archive=str(destination))
        return destination
