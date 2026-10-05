"""
Configuration de la journalisation.

Chaque message est écrit à la fois :
  - dans la console (suivi en direct),
  - dans logs/pipeline_AAAA-MM.log (diagnostic après coup, sans relire le code).
"""
from __future__ import annotations

import logging
from pathlib import Path

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-18s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(logs_dir: Path, period_label: str, level: str = "INFO") -> logging.Logger:
    """Configure le logger racine du pipeline et renvoie-le."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"pipeline_{period_label}.log"

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    console = logging.StreamHandler()
    console.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger = logging.getLogger("pipeline")
    logger.setLevel(level.upper())
    logger.handlers.clear()          # évite les doublons si appelé deux fois
    logger.addHandler(console)
    logger.addHandler(file_handler)
    logger.propagate = False

    logger.info("Journal écrit dans %s", log_file)
    return logger
