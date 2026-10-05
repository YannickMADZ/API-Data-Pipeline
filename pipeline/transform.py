"""
Étape 2 — TRANSFORMATION.

Entrée  : pages brutes data/raw/AAAA-MM/page_*.json.gz (produites par extract)
Sortie  : data/processed/AAAA-MM/trips_clean.csv
          reports/AAAA-MM/transform_summary.json (lu par validate)

Déroulé :
  1. Chargement des pages et contrôle qu'elles correspondent à l'extraction.
  2. Typage explicite (l'API renvoie TOUT en chaînes : "1.0", "9.3"...).
  3. Colonnes dérivées : durée, vitesse implicite, écart sur total_amount.
  4. SUPPRESSION des trajets physiquement impossibles (règles + seuils de
     config.yaml). Chaque règle s'applique aux lignes restantes : les
     compteurs sont donc exclusifs et leur somme = lignes retirées.
  5. MARQUAGE (colonnes flag_*) des anomalies conservées volontairement :
     ce sont des transactions réelles, les supprimer fausserait les analyses.
  6. Écriture atomique du CSV et du résumé.

Les seuils viennent de config.yaml ; les percentiles observés sur le mois
traité sont recalculés et inscrits dans le résumé, pour que le rapport
qualité montre toujours sur quelles données le seuil s'appuie.
"""
from __future__ import annotations

import logging
import time

import numpy as np
import pandas as pd

from pipeline.config import PROJECT_ROOT, Period, resolve_path
from pipeline.io_utils import read_json, write_csv_atomic, write_json_atomic

logger = logging.getLogger("pipeline.transform")

PCTS = [0.5, 0.99, 0.999]


class TransformError(RuntimeError):
    """Échec de la transformation : le pipeline doit s'arrêter."""


# ---------------------------------------------------------------------------
# 1. Chargement
# ---------------------------------------------------------------------------
def load_extract_summary(cfg: dict, period: Period) -> dict:
    """L'extraction doit avoir réussi : sinon on refuse de transformer."""
    path = resolve_path(cfg, "reports_dir", period) / "extract_summary.json"
    if not path.exists():
        raise TransformError(f"{path} introuvable : lance d'abord l'étape extract.")
    summary = read_json(path)
    if summary.get("status") != "OK":
        raise TransformError(f"Extraction de {period.label} non valide : {summary.get('status')}")
    return summary


def load_raw(cfg: dict, period: Period, expected_rows: int) -> tuple[pd.DataFrame, dict]:
    """Charge et type les pages une par une (beaucoup moins de mémoire qu'en bloc)."""
    raw_dir = resolve_path(cfg, "raw_dir", period)
    pages = sorted(raw_dir.glob("page_*.json*"))
    if not pages:
        raise TransformError(f"Aucune page brute dans {raw_dir}")

    frames, failures = [], {}
    for i, page in enumerate(pages, start=1):
        frame, fail = cast_types(pd.DataFrame(read_json(page)), cfg["schema"])
        frames.append(frame)
        for k, v in fail.items():
            failures[k] = failures.get(k, 0) + v
        if i % 10 == 0 or i == len(pages):
            logger.info("Chargement et typage : %d/%d pages", i, len(pages))

    df = pd.concat(frames, ignore_index=True)
    if len(df) != expected_rows:
        raise TransformError(
            f"{len(df)} lignes chargées au lieu de {expected_rows} (extract_summary) : "
            "fichiers bruts modifiés ou incomplets."
        )
    return df, failures


# ---------------------------------------------------------------------------
# 2. Typage
# ---------------------------------------------------------------------------
def cast_types(df: pd.DataFrame, schema: dict) -> tuple[pd.DataFrame, dict]:
    """
    Convertit chaque colonne selon config.yaml (section schema).

    errors="coerce" : une valeur illisible devient NaN au lieu de faire
    planter le pipeline, MAIS chaque conversion ratée est comptée et remonte
    dans le rapport qualité (sinon ce serait une anomalie silencieuse).

    Les champs absents d'un objet JSON (valeur manquante) n'existent pas
    du tout dans la réponse : on crée la colonne si besoin.
    """
    failures: dict[str, int] = {}
    all_cols = [c for group in schema.values() for c in group]
    for col in all_cols:
        if col not in df:
            df[col] = None

    def convert(col: str, func) -> None:
        before = int(df[col].isna().sum())
        df[col] = func(df[col])
        failures[col] = int(df[col].isna().sum()) - before

    for col in schema["datetime"]:
        convert(col, lambda s: pd.to_datetime(s, format="%Y-%m-%dT%H:%M:%S.%f", errors="coerce"))
    for col in schema["float"]:
        convert(col, lambda s: pd.to_numeric(s, errors="coerce"))
    for col in schema["integer"]:
        num = pd.to_numeric(df[col], errors="coerce")
        # "1.0" -> 1 : on vérifie qu'aucune valeur n'a de vraie partie décimale.
        failures[f"{col}__non_entier"] = int((num.notna() & (num % 1 != 0)).sum())
        convert(col, lambda s, num=num: num.round().astype("Int64"))  # Int64 accepte les NaN
    for col in schema["category"]:
        df[col] = df[col].astype("category")
    return df, failures


# ---------------------------------------------------------------------------
# 3. Colonnes dérivées
# ---------------------------------------------------------------------------
def add_derived_columns(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    c = cfg["cleaning"]
    df["duration_min"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    # Vitesse moyenne implicite : définie seulement si durée > 0
    hours = df["duration_min"].where(df["duration_min"] > 0) / 60
    df["speed_mph"] = df["trip_distance"] / hours

    # Écart entre le total facturé et la somme de ses composantes.
    # min_count=1 : une composante manquante est ignorée dans la somme.
    components = df[c["amount_components"]].sum(axis=1, min_count=1)
    df["total_gap"] = (df["total_amount"] - components).round(2)
    return df


def describe(series: pd.Series) -> dict:
    """Percentiles observés, inscrits dans le rapport pour justifier un seuil."""
    s = series.dropna()
    q = s.quantile(PCTS)
    return {
        "p50": round(float(q[0.5]), 3),
        "p99": round(float(q[0.99]), 3),
        "p99.9": round(float(q[0.999]), 3),
        "max": round(float(s.max()), 3),
    }


# ---------------------------------------------------------------------------
# 4. Règles de suppression
# ---------------------------------------------------------------------------
def build_removal_rules(df: pd.DataFrame, cfg: dict, period: Period) -> list[dict]:
    """
    Chaque règle = un masque des lignes à supprimer + le seuil + la
    justification. L'ordre compte : les compteurs sont calculés sur les
    lignes restantes, une ligne n'est donc comptée que pour une seule raison.
    """
    c = cfg["cleaning"]
    start, end = pd.Timestamp(period.start), pd.Timestamp(period.end)
    pickup = df["tpep_pickup_datetime"]
    dur = describe(df["duration_min"])
    dist = describe(df["trip_distance"])
    speed = describe(df["speed_mph"])

    return [
        {
            "rule": "pickup_hors_du_mois",
            "mask": pickup.isna() | (pickup < start) | (pickup >= end),
            "threshold": f"[{period.start} ; {period.end}[",
            "observed": {},
            "justification": "Garde-fou : le filtre $where de l'extraction exclut déjà ces lignes ; "
                             "la règle garantit qu'un trajet n'appartient qu'à un seul mois.",
        },
        {
            "rule": "duree_non_positive",
            "mask": df["duration_min"].isna() | (df["duration_min"] <= c["min_duration_minutes"]),
            "threshold": f"duration_min <= {c['min_duration_minutes']}",
            "observed": {"negatives": int((df["duration_min"] < 0).sum()),
                         "nulles": int((df["duration_min"] == 0).sum())},
            "justification": "Un dépôt avant ou au même instant que la prise en charge est "
                             "physiquement impossible : horodatage erroné, trajet inexploitable.",
        },
        {
            "rule": "duree_trop_longue",
            "mask": df["duration_min"] > c["max_duration_minutes"],
            "threshold": f"duration_min > {c['max_duration_minutes']}",
            "observed": dur,
            "justification": f"99,9 % des trajets durent moins de {dur['p99.9']} min, puis la queue "
                             f"s'étire jusqu'à {dur['max']:.0f} min. Seuil arrondi à 3 h, juste au-dessus "
                             "du p99.9 : au-delà, compteur resté allumé plutôt que trajet réel.",
        },
        {
            "rule": "distance_trop_longue",
            "mask": df["trip_distance"] > c["max_trip_distance"],
            "threshold": f"trip_distance > {c['max_trip_distance']}",
            "observed": dist,
            "justification": f"p99.9 = {dist['p99.9']} mi mais max = {dist['max']:.0f} mi. Le seuil n'est "
                             "PAS le p99.9 : il garde les longues courses légitimes (aéroports, hors ville) "
                             "et n'exclut que des distances sans rapport avec un trajet en taxi.",
        },
        {
            "rule": "vitesse_impossible",
            "mask": df["speed_mph"] > c["max_speed_mph"],
            "threshold": f"speed_mph > {c['max_speed_mph']}",
            "observed": speed,
            "justification": f"99,9 % des vitesses moyennes sont sous {speed['p99.9']} mph, puis saut "
                             f"jusqu'à {speed['max']:.0f} mph. Au-delà de {c['max_speed_mph']} mph de "
                             "moyenne sur tout le trajet, la distance ou les horodatages sont faux.",
        },
    ]


def apply_removals(df: pd.DataFrame, rules: list[dict]) -> tuple[pd.DataFrame, list[dict]]:
    n_in = len(df)
    keep = pd.Series(True, index=df.index)
    report = []
    for r in rules:
        to_remove = keep & r["mask"].fillna(False).astype(bool)
        n = int(to_remove.sum())
        keep &= ~to_remove
        report.append({
            "rule": r["rule"], "threshold": r["threshold"], "rows_removed": n,
            "pct": round(100 * n / n_in, 4), "observed": r["observed"],
            "justification": r["justification"],
        })
        logger.info("Règle %-22s : %8d lignes retirées (%.3f %%)", r["rule"], n, 100 * n / n_in)
    return df[keep].copy(), report


# ---------------------------------------------------------------------------
# 5. Anomalies conservées (marquage)
# ---------------------------------------------------------------------------
def add_flags(df: pd.DataFrame, cfg: dict) -> list[dict]:
    """Ajoute les colonnes flag_* et renvoie leur description pour le rapport."""
    c = cfg["cleaning"]
    tol = c["amount_tolerance"]
    surcharges = df["congestion_surcharge"].fillna(0) + df["airport_fee"].fillna(0)

    df["flag_missing_metadata"] = df[c["metadata_columns"]].isna().any(axis=1)
    df["flag_extra_includes_surcharges"] = (
        df["vendorid"].isin(c["vendors_extra_includes_surcharges"]).fillna(False)
        & (df["total_gap"].abs() > tol)
        & ((df["total_gap"] + surcharges).abs() <= tol)
    )
    df["flag_total_mismatch_other"] = (
        (df["total_gap"].abs() > tol) & ~df["flag_extra_includes_surcharges"]
    ).fillna(False)
    df["flag_negative_amount"] = df["total_amount"] < 0
    df["flag_zero_distance"] = df["trip_distance"] == 0
    df["flag_zero_passenger"] = (df["passenger_count"] == 0).fillna(False)
    df["flag_unknown_zone"] = (
        df["pulocationid"].isin(c["unknown_zones"]) | df["dolocationid"].isin(c["unknown_zones"])
    ).fillna(False)

    n = len(df)
    pay = lambda m: {str(k): int(v) for k, v in df.loc[m, "payment_type"].value_counts().items()}
    vend = lambda m: {str(k): int(v) for k, v in df.loc[m, "vendorid"].value_counts().items()}

    specs = [
        ("flag_missing_metadata",
         "Les 5 colonnes de métadonnées sont manquantes ENSEMBLE, uniquement pour payment_type = 0. "
         "Montants valides : trajets réels dont seules les métadonnées du véhicule manquent. "
         "Pas d'imputation (une médiane inventerait des passagers, un 0 affirmerait l'absence de frais).",
         {"payment_type": pay(df["flag_missing_metadata"])}),
        ("flag_extra_includes_surcharges",
         "Anomalie piège : pour ces vendors, total_amount est JUSTE mais `extra` contient déjà "
         "congestion_surcharge et airport_fee, aussi répétés dans leurs colonnes. L'écart vaut "
         "exactement -(congestion + aéroport). Supprimer ces lignes retirerait ~25 % du mois.",
         {"vendorid": vend(df["flag_extra_includes_surcharges"])}),
        ("flag_total_mismatch_other",
         "Écart de total non expliqué par la règle précédente. Le détail donne les écarts les plus "
         "fréquents et la part de lignes à métadonnées manquantes (surcharge incluse au total mais "
         "absente des colonnes). Conservé : le total facturé reste la valeur de référence.",
         {"total_gap": {str(k): int(v) for k, v in
                        df.loc[df["flag_total_mismatch_other"], "total_gap"].value_counts().head(5).items()},
          "dont_metadata_manquantes": int(
              (df["flag_total_mismatch_other"] & df["flag_missing_metadata"]).sum())}),
        ("flag_negative_amount",
         "Contre-passations comptables (annulations, litiges) : concentrées sur payment_type 3 "
         "(sans frais) et 4 (litige). Conservées pour l'audit, à exclure d'une analyse de revenus.",
         {"payment_type": pay(df["flag_negative_amount"])}),
        ("flag_zero_distance",
         "Course payée mais distance nulle : défaillance GPS probable, transaction réelle. "
         "À exclure des analyses de distance ou de vitesse.", {}),
        ("flag_zero_passenger",
         "passenger_count = 0 presque exclusivement chez le vendor 1 : saisie manuelle non faite "
         "par le chauffeur, pas un trajet sans passager.",
         {"vendorid": vend(df["flag_zero_passenger"])}),
        ("flag_unknown_zone",
         "Zones 264/265 = 'Unknown'/'N/A' dans la table TLC. Montants valides : à exclure "
         "seulement des analyses géographiques.", {}),
    ]
    out = []
    for flag, why, breakdown in specs:
        k = int(df[flag].sum())
        out.append({"flag": flag, "rows": k, "pct": round(100 * k / n, 4),
                    "breakdown": breakdown, "justification": why})
        logger.info("Conservé et marqué %-31s : %8d lignes (%.2f %%)", flag, k, 100 * k / n)
    return out


# ---------------------------------------------------------------------------
# Point d'entrée de l'étape
# ---------------------------------------------------------------------------
def run(cfg: dict, period: Period, force: bool = False) -> dict:
    started = time.monotonic()
    extract_summary = load_extract_summary(cfg, period)
    rows_raw = extract_summary["rows_extracted"]

    df, failures = load_raw(cfg, period, rows_raw)
    n_fail = sum(v for v in failures.values())
    if n_fail:
        logger.warning("Conversions de type ratées : %s", {k: v for k, v in failures.items() if v})
    else:
        logger.info("Typage : 0 conversion ratée sur %d lignes", len(df))

    missing = {col: int(v) for col, v in df.isna().sum().items() if v}
    df = add_derived_columns(df, cfg)

    rules = build_removal_rules(df, cfg, period)
    df, removals = apply_removals(df, rules)
    flags = add_flags(df, cfg)

    # Format de sortie : identifiant renommé (":" pose problème dans certains outils).
    df = df.rename(columns={":id": "row_id"})
    df["speed_mph"] = df["speed_mph"].round(3)
    df["duration_min"] = df["duration_min"].round(3)
    base = cfg["output"]["column_order"]
    df = df[base + [col for col in df.columns if col not in base]]  # flags à la fin

    out_path = resolve_path(cfg, "processed_dir", period) / cfg["output"]["clean_filename"]
    logger.info("Écriture atomique de %s (%d lignes)...", out_path.name, len(df))
    write_csv_atomic(df, out_path)

    summary = {
        "period": period.label,
        "rows_in": rows_raw,
        "rows_removed": sum(r["rows_removed"] for r in removals),
        "rows_out": len(df),
        "output_file": str(out_path.relative_to(PROJECT_ROOT)),
        "columns": list(df.columns),
        "type_conversion_failures": failures,
        "missing_values_before_cleaning": missing,
        "removals": removals,
        "kept_despite_anomaly": flags,
        "duration_seconds": round(time.monotonic() - started, 1),
    }
    write_json_atomic(resolve_path(cfg, "reports_dir", period) / "transform_summary.json", summary)
    logger.info("Transformation : %d -> %d lignes (%d retirées)",
                rows_raw, len(df), summary["rows_removed"])
    return summary
