"""Atomic file publication shared by persistent state and report sinks."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

_WINDOWS = os.name == 'nt'


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_dir(p: Path) -> None:
    missing, current = [], p
    while not current.exists():
        missing.append(current)
        if current.parent == current:
            break
        current = current.parent
    p.mkdir(parents=True, exist_ok=True)
    if not _WINDOWS:
        # Persist newly created path entries as well as the eventual file name.
        for directory in reversed(missing):
            _sync_directory(directory.parent)


def _replace_file(source: Path, destination: Path) -> None:
    if not _WINDOWS:
        os.replace(source, destination)
        return
    import ctypes
    from ctypes import wintypes
    move = ctypes.WinDLL('kernel32', use_last_error=True).MoveFileExW
    move.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
    move.restype = wintypes.BOOL
    # Same-directory replacement; request write-through without following a
    # destination symlink through Path.resolve().
    if not move(str(source.absolute()), str(destination.absolute()), 0x1 | 0x8):
        raise ctypes.WinError(ctypes.get_last_error())


def atomic_write_text(path: Path, text: str) -> None:
    """Publish and sync a complete file; sync errors propagate after publication.

    Failures before replacement preserve the previous file. A failed directory
    sync can leave the new file visible without establishing crash durability.
    """
    path = Path(path)
    _ensure_dir(path.parent)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="",
                                         dir=path.parent, prefix=f".{path.name}.",
                                         suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_file(temporary, path)
        if not _WINDOWS:
            _sync_directory(path.parent)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
