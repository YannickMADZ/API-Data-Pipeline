"""
Chargement et validation de la configuration.

Deux sources :
  - config.yaml : paramètres non secrets (URL, pagination, réseau, seuils, chemins)
  - .env        : le token Socrata (secret, jamais versionné)

On valide tout au démarrage : mieux vaut échouer immédiatement avec un message
clair que découvrir une clé manquante après 20 minutes de téléchargement.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Racine du projet = dossier parent du package "pipeline".
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Sections obligatoires dans config.yaml.
REQUIRED_SECTIONS = ("api", "network", "storage", "paths", "schema", "cleaning", "output")


class ConfigError(RuntimeError):
    """Configuration invalide ou incomplète."""


@dataclass(frozen=True)
class Period:
    """Mois à traiter, avec ses bornes au format attendu par l'API."""

    year: int
    month: int

    @property
    def label(self) -> str:
        """Identifiant lisible, utilisé dans les chemins : '2023-01'."""
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def start(self) -> str:
        """Début du mois (inclus), format ISO sans fuseau comme dans l'API."""
        return f"{self.year:04d}-{self.month:02d}-01T00:00:00"

    @property
    def end(self) -> str:
        """Début du mois suivant (exclu) : évite les problèmes de 28/30/31 jours."""
        y, m = (self.year + 1, 1) if self.month == 12 else (self.year, self.month + 1)
        return f"{y:04d}-{m:02d}-01T00:00:00"


def make_period(year: int, month: int) -> Period:
    """Valide l'année et le mois fournis en ligne de commande."""
    if not 1 <= month <= 12:
        raise ConfigError(f"Mois invalide : {month} (attendu entre 1 et 12)")
    if not 1000 <= year <= 9999:
        raise ConfigError(f"Année invalide : {year} (attendu sur 4 chiffres)")
    return Period(year=year, month=month)


def load_config(config_path: str | Path = PROJECT_ROOT / "config.yaml") -> dict:
    """Lit config.yaml et vérifie la présence des sections obligatoires."""
    path = Path(config_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        raise ConfigError(f"Fichier de configuration introuvable : {path}")

    with path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    missing = [s for s in REQUIRED_SECTIONS if s not in cfg]
    if missing:
        raise ConfigError(f"Sections manquantes dans {path.name} : {missing}")

    page_size = cfg["api"].get("page_size")
    if not isinstance(page_size, int) or page_size <= 0:
        raise ConfigError(f"api.page_size doit être un entier > 0 (reçu : {page_size!r})")
    return cfg


def load_token(cfg: dict) -> str:
    """
    Lit le token depuis l'environnement (.env chargé par python-dotenv,
    ou variables injectées par Docker via env_file).
    """
    load_dotenv(PROJECT_ROOT / ".env")  # sans effet si le fichier n'existe pas
    var = cfg["api"]["token_env_var"]
    token = os.getenv(var, "").strip()
    if not token:
        raise ConfigError(
            f"Token absent : définis {var} dans le fichier .env "
            f"(voir .env.example)."
        )
    return token


def resolve_path(cfg: dict, key: str, period: Period) -> Path:
    """Construit un chemin absolu à partir de cfg['paths'][key] et du mois."""
    template = cfg["paths"][key]
    return PROJECT_ROOT / template.format(period=period.label)
