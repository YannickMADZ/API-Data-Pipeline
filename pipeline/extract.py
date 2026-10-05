"""
Étape 1 — EXTRACTION.

Télécharge l'intégralité des trajets d'un mois depuis l'API Socrata (SODA)
et les enregistre page par page dans data/raw/AAAA-MM/.

Garanties :
  1. Complétude : le nombre de lignes extraites doit être égal au count(*)
     officiel de l'API, calculé avec EXACTEMENT le même filtre $where.
  2. Pas de doublons : pagination triée sur ':id' (clé unique système),
     puis contrôle d'unicité des ':id' sur l'ensemble des pages.
  3. Robustesse réseau : timeout + tentatives avec attente exponentielle.
  4. Reprise : les pages déjà téléchargées (et valides) ne sont pas
     retéléchargées si le pipeline est relancé après une interruption.

En cas d'échec d'un de ces contrôles, une ExtractionError est levée :
l'orchestrateur arrête alors le pipeline.
"""
from __future__ import annotations

import logging
import math
import random
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from pipeline.config import PROJECT_ROOT, Period, load_token, resolve_path
from pipeline.io_utils import read_json, write_json_atomic

logger = logging.getLogger("pipeline.extract")

MANIFEST_NAME = "_manifest.json"


class ExtractionError(RuntimeError):
    """Échec de l'extraction : le pipeline doit s'arrêter."""


# ---------------------------------------------------------------------------
# Accès HTTP
# ---------------------------------------------------------------------------
def build_session(token: str) -> requests.Session:
    """
    Session HTTP réutilisée pour toutes les requêtes (connexion TCP gardée
    ouverte => plus rapide). Le token est envoyé dans l'en-tête X-App-Token,
    jamais dans l'URL (l'URL apparaît dans les logs, l'en-tête non).
    """
    session = requests.Session()
    session.headers.update({"X-App-Token": token, "Accept": "application/json"})
    return session


def request_with_retry(session: requests.Session, url: str, params: dict, net: dict) -> Any:
    """
    GET avec tentatives multiples. Renvoie le JSON décodé.

    - Erreurs TRANSITOIRES (coupure réseau, timeout, HTTP 429/5xx, JSON
      tronqué) : on réessaie après une attente exponentielle
      (2s, 4s, 8s, ... plafonnée), avec un peu d'aléa pour ne pas
      resolliciter l'API exactement au même instant.
      Si l'API envoie un en-tête Retry-After (HTTP 429), on le respecte.
    - Erreurs DÉFINITIVES (HTTP 400 requête invalide, 403 token refusé...) :
      réessayer ne servirait à rien, on échoue tout de suite.
    """
    timeout = (net["connect_timeout"], net["read_timeout"])
    max_retries = net["max_retries"]
    retry_statuses = set(net["retry_statuses"])

    for attempt in range(1, max_retries + 1):
        retry_after: float | None = None
        try:
            resp = session.get(url, params=params, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout) as exc:
            reason = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    reason = "réponse JSON invalide ou tronquée"
            elif resp.status_code in retry_statuses:
                reason = f"HTTP {resp.status_code} (transitoire)"
                header = resp.headers.get("Retry-After")
                if header and header.isdigit():
                    retry_after = float(header)
            else:
                raise ExtractionError(
                    f"HTTP {resp.status_code} non récupérable pour {url} "
                    f"params={params} : {resp.text[:300]}"
                )

        if attempt == max_retries:
            break
        wait = retry_after if retry_after is not None else min(
            net["backoff_base"] * 2 ** (attempt - 1), net["backoff_max"]
        )
        wait += random.uniform(0, 1)  # léger aléa (jitter)
        logger.warning(
            "Tentative %d/%d échouée (%s). Nouvel essai dans %.1fs",
            attempt, max_retries, reason, wait,
        )
        time.sleep(wait)

    raise ExtractionError(f"Abandon après {max_retries} tentatives : {reason}")


# ---------------------------------------------------------------------------
# Construction des requêtes SODA
# ---------------------------------------------------------------------------
def endpoint(cfg: dict) -> str:
    """URL de l'endpoint JSON du dataset."""
    return f"{cfg['api']['base_url']}/{cfg['api']['dataset_id']}.json"


def build_where(cfg: dict, period: Period) -> str:
    """
    Filtre du mois : [début du mois ; début du mois suivant[.
    La borne haute est EXCLUE (<) pour ne jamais compter deux fois un trajet
    situé à minuit pile entre deux mois.
    Ce même filtre sert au count(*) ET à l'extraction : c'est ce qui rend
    la comparaison des deux nombres valable.
    """
    col = cfg["api"]["date_column"]
    return f"{col} >= '{period.start}' AND {col} < '{period.end}'"


def fetch_official_count(session: requests.Session, cfg: dict, where: str) -> int:
    """Nombre officiel de lignes du mois selon l'API (référence de complétude)."""
    params = {"$select": "count(*) AS n", "$where": where}
    data = request_with_retry(session, endpoint(cfg), params, cfg["network"])
    try:
        return int(data[0]["n"])
    except (IndexError, KeyError, ValueError, TypeError) as exc:
        raise ExtractionError(f"Réponse inattendue pour count(*) : {data!r}") from exc


def fetch_page(session: requests.Session, cfg: dict, where: str, offset: int) -> list[dict]:
    """
    Télécharge une page de résultats.

    $select=*, :id -> ajoute l'identifiant système ':id' (absent par défaut),
                      indispensable pour vérifier l'absence de doublons.
    $order=:id     -> ordre total et stable : sans tri explicite, l'ordre
                      n'est pas garanti entre deux requêtes et la pagination
                      par $offset produirait doublons et trous.
    $limit         -> taille de page (config.yaml : api.page_size).
    $offset        -> nombre de lignes à sauter (page_index * page_size).
    """
    params = {
        "$select": "*, :id",
        "$where": where,
        "$order": cfg["api"]["order_by"],
        "$limit": cfg["api"]["page_size"],
        "$offset": offset,
    }
    data = request_with_retry(session, endpoint(cfg), params, cfg["network"])
    if not isinstance(data, list):
        raise ExtractionError(f"Page attendue sous forme de liste, reçu : {type(data).__name__}")
    return data


# ---------------------------------------------------------------------------
# Gestion des fichiers bruts (reprise après interruption)
# ---------------------------------------------------------------------------
def page_path(raw_dir: Path, page_index: int, compress: bool) -> Path:
    """data/raw/AAAA-MM/page_00000.json.gz"""
    ext = ".json.gz" if compress else ".json"
    return raw_dir / f"page_{page_index:05d}{ext}"


def expected_rows_in_page(page_index: int, n_pages: int, total: int, page_size: int) -> int:
    """Toutes les pages sont pleines sauf la dernière."""
    if page_index < n_pages - 1:
        return page_size
    return total - page_size * (n_pages - 1)


def prepare_raw_dir(raw_dir: Path, manifest: dict, force: bool) -> None:
    """
    Les pages déjà présentes ne sont réutilisables que si elles ont été
    produites avec les mêmes paramètres (filtre, taille de page, count).
    Sinon les offsets ne correspondraient plus et on mélangerait des pages
    incompatibles. Dans ce cas : erreur, sauf si --force (on repart de zéro).
    """
    manifest_file = raw_dir / MANIFEST_NAME
    if force and raw_dir.exists():
        logger.info("--force : suppression de %s", raw_dir)
        shutil.rmtree(raw_dir)

    if manifest_file.exists():
        previous = read_json(manifest_file)
        if previous != manifest:
            raise ExtractionError(
                f"{raw_dir} contient une extraction faite avec d'autres paramètres "
                f"(ancien : {previous}, nouveau : {manifest}). "
                "Relance avec --force pour la remplacer."
            )
        logger.info("Extraction existante compatible : reprise possible")
    else:
        raw_dir.mkdir(parents=True, exist_ok=True)
        write_json_atomic(manifest_file, manifest)


# ---------------------------------------------------------------------------
# Point d'entrée de l'étape
# ---------------------------------------------------------------------------
def run(cfg: dict, period: Period, force: bool = False) -> dict:
    """Exécute l'extraction complète du mois et renvoie un résumé chiffré."""
    started = time.monotonic()
    page_size: int = cfg["api"]["page_size"]
    compress: bool = cfg["storage"]["compress_raw"]
    raw_dir = resolve_path(cfg, "raw_dir", period)
    reports_dir = resolve_path(cfg, "reports_dir", period)

    session = build_session(load_token(cfg))
    where = build_where(cfg, period)
    logger.info("Filtre : %s", where)

    # 1) Référence : count(*) officiel
    expected_total = fetch_official_count(session, cfg, where)
    logger.info("count(*) officiel de l'API pour %s : %d lignes", period.label, expected_total)
    if expected_total == 0:
        raise ExtractionError(f"Aucun trajet pour {period.label} : mois absent du dataset ?")

    n_pages = math.ceil(expected_total / page_size)
    logger.info("Pagination : %d pages de %d lignes max ($limit)", n_pages, page_size)

    manifest = {
        "dataset_id": cfg["api"]["dataset_id"],
        "where": where,
        "order_by": cfg["api"]["order_by"],
        "page_size": page_size,
        "expected_total": expected_total,
        "compress_raw": compress,
    }
    prepare_raw_dir(raw_dir, manifest, force)

    # 2) Téléchargement page par page (ou réutilisation si déjà présente)
    seen_ids: set[str] = set()
    duplicates = 0
    total_rows = 0
    downloaded, reused = 0, 0

    for page_index in range(n_pages):
        path = page_path(raw_dir, page_index, compress)
        expected = expected_rows_in_page(page_index, n_pages, expected_total, page_size)

        if path.exists():
            # Fichier présent = forcément complet grâce à l'écriture atomique.
            rows = read_json(path)
            reused += 1
            logger.debug("Page %d réutilisée (%s)", page_index, path.name)
        else:
            rows = fetch_page(session, cfg, where, offset=page_index * page_size)
            # Contrôle AVANT écriture : une page incomplète n'est jamais enregistrée.
            if len(rows) != expected:
                raise ExtractionError(
                    f"Page {page_index} : {len(rows)} lignes reçues, {expected} attendues. "
                    "Le dataset a peut-être changé pendant l'extraction : relance avec --force."
                )
            write_json_atomic(path, rows, compress=compress)
            downloaded += 1

        if len(rows) != expected:
            raise ExtractionError(
                f"Page existante {path.name} : {len(rows)} lignes au lieu de {expected}. "
                "Relance avec --force."
            )

        # Contrôle des doublons sur la clé ':id'
        for row in rows:
            row_id = row.get(":id")
            if row_id is None:
                raise ExtractionError(f"Ligne sans ':id' dans {path.name} : $select mal formé ?")
            if row_id in seen_ids:
                duplicates += 1
            else:
                seen_ids.add(row_id)
        total_rows += len(rows)

        logger.info(
            "Page %d/%d OK (%d lignes, cumul %d/%d)",
            page_index + 1, n_pages, len(rows), total_rows, expected_total,
        )

    # 3) Contrôles finaux : complétude et unicité
    if duplicates:
        raise ExtractionError(f"{duplicates} doublons de ':id' détectés dans les fichiers bruts")
    if total_rows != expected_total:
        raise ExtractionError(f"{total_rows} lignes extraites au lieu de {expected_total}")

    summary = {
        "period": period.label,
        "extracted_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "api_where": where,
        "api_count": expected_total,
        "rows_extracted": total_rows,
        "unique_ids": len(seen_ids),
        "duplicates": duplicates,
        "page_size": page_size,
        "pages_total": n_pages,
        "pages_downloaded": downloaded,
        "pages_reused": reused,
        "raw_dir": str(raw_dir.relative_to(PROJECT_ROOT)),
        "duration_seconds": round(time.monotonic() - started, 1),
        "status": "OK",
    }
    # Le résumé (léger) va dans reports/ : il est versionné et sert de preuve.
    write_json_atomic(reports_dir / "extract_summary.json", summary)
    logger.info(
        "Extraction terminée : %d lignes = count(*) API, 0 doublon (%d pages téléchargées, %d réutilisées)",
        total_rows, downloaded, reused,
    )
    return summary
