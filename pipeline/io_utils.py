"""
Lecture / écriture de fichiers.

Écriture ATOMIQUE : on écrit d'abord dans un fichier temporaire situé dans le
même dossier, on force l'écriture disque (fsync), puis on le renomme avec
os.replace(). Le renommage est atomique sur un même système de fichiers :
le fichier final existe soit en entier, soit pas du tout. Une interruption
(Ctrl+C, coupure réseau, crash) ne laisse jamais de fichier à moitié écrit,
seulement un éventuel *.tmp ignoré par Git.
"""
from __future__ import annotations

import gzip
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Écrit des octets dans `path` de façon atomique."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())     # garantit que les octets sont sur le disque
        os.replace(tmp_name, path)   # renommage atomique
    except BaseException:
        # En cas d'erreur (y compris Ctrl+C), on supprime le temporaire.
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise


def write_json_atomic(path: Path, obj: Any, compress: bool = False) -> None:
    """Sérialise `obj` en JSON (gzip optionnel) et l'écrit atomiquement."""
    payload = json.dumps(obj, ensure_ascii=False, indent=None if compress else 2)
    data = payload.encode("utf-8")
    if compress:
        data = gzip.compress(data)
    atomic_write_bytes(path, data)


def read_json(path: Path) -> Any:
    """Lit un fichier .json ou .json.gz. Lève une erreur si le JSON est tronqué."""
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def write_csv_atomic(df, path: Path) -> None:
    """
    Écrit un DataFrame en CSV de façon atomique : même principe que
    atomic_write_bytes, mais pandas écrit directement dans le temporaire
    (pas de copie complète du CSV en mémoire, important pour ~3 M lignes).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            df.to_csv(f, index=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise
