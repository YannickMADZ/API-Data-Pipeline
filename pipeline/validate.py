"""
Étape 3 — VALIDATION.

Prouve par des chiffres que le résultat est fiable et complet, en relisant
le CSV écrit sur disque (pas les données en mémoire : on valide ce qui sera
réellement livré).

Contrôles :
  1. Extraction complète : lignes extraites = count(*) officiel de l'API.
  2. Conservation des lignes : brut = final + somme des lignes retirées.
  3. Le CSV contient exactement le nombre de lignes annoncé.
  4. Aucun doublon de row_id dans le CSV.
  5. Aucune ligne du CSV ne viole un seuil de suppression.
  6. Toutes les prises en charge appartiennent au mois traité.
  7. Toutes les colonnes attendues sont présentes.

Produit reports/AAAA-MM/quality_report.json (écriture atomique), même en
cas d'échec, puis lève une exception si un contrôle échoue : le pipeline
s'arrête avec un code de sortie non nul.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import PROJECT_ROOT, Period, resolve_path
from pipeline.io_utils import read_json, write_json_atomic

logger = logging.getLogger("pipeline.validate")


class ValidationError(RuntimeError):
    """Au moins un contrôle qualité a échoué."""


def check(name: str, ok: bool, detail: str) -> dict:
    """Formate un contrôle et le journalise."""
    (logger.info if ok else logger.error)("[%s] %s : %s", "OK " if ok else "KO ", name, detail)
    return {"check": name, "passed": bool(ok), "detail": detail}


def run(cfg: dict, period: Period, force: bool = False) -> dict:
    reports_dir = resolve_path(cfg, "reports_dir", period)
    c = cfg["cleaning"]

    for name in ("extract_summary.json", "transform_summary.json"):
        if not (reports_dir / name).exists():
            raise ValidationError(f"{reports_dir / name} introuvable : étape précédente non exécutée.")
    ext = read_json(reports_dir / "extract_summary.json")
    tra = read_json(reports_dir / "transform_summary.json")

    csv_path = PROJECT_ROOT / tra["output_file"]
    if not csv_path.exists():
        raise ValidationError(f"Fichier final introuvable : {csv_path}")

    logger.info("Relecture de %s ...", csv_path.name)
    df = pd.read_csv(
        csv_path,
        usecols=["row_id", "tpep_pickup_datetime", "duration_min", "trip_distance", "speed_mph"],
        parse_dates=["tpep_pickup_datetime"],
    )
    header = pd.read_csv(csv_path, nrows=0).columns.tolist()
    start, end = pd.Timestamp(period.start), pd.Timestamp(period.end)

    checks = [
        check("extraction_complete",
              ext["rows_extracted"] == ext["api_count"] and ext["duplicates"] == 0,
              f"{ext['rows_extracted']} lignes extraites / count(*) API = {ext['api_count']}, "
              f"{ext['duplicates']} doublon"),
        check("conservation_des_lignes",
              tra["rows_in"] == tra["rows_out"] + tra["rows_removed"]
              and tra["rows_removed"] == sum(r["rows_removed"] for r in tra["removals"]),
              f"{tra['rows_in']} brutes = {tra['rows_out']} finales + {tra['rows_removed']} retirées"),
        check("taille_du_csv", len(df) == tra["rows_out"],
              f"{len(df)} lignes lues dans le CSV, {tra['rows_out']} attendues"),
        check("unicite_row_id", not df["row_id"].duplicated().any(),
              f"{int(df['row_id'].duplicated().sum())} doublon(s) de row_id"),
        check("seuils_respectes",
              bool(((df["duration_min"] > c["min_duration_minutes"])
                    & (df["duration_min"] <= c["max_duration_minutes"])
                    & (df["trip_distance"] <= c["max_trip_distance"])
                    & ~(df["speed_mph"] > c["max_speed_mph"])).all()),
              f"durée max = {df['duration_min'].max():.1f} min, distance max = "
              f"{df['trip_distance'].max():.1f} mi, vitesse max = {df['speed_mph'].max():.1f} mph"),
        check("pickup_dans_le_mois",
              bool(df["tpep_pickup_datetime"].between(start, end, inclusive="left").all()),
              f"de {df['tpep_pickup_datetime'].min()} à {df['tpep_pickup_datetime'].max()}"),
        check("colonnes_presentes", header == tra["columns"],
              f"{len(header)} colonnes, attendues {len(tra['columns'])}"),
    ]
    passed = all(ch["passed"] for ch in checks)

    report = {
        "period": period.label,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": "VALIDE" if passed else "ECHEC",
        "summary": {
            "api_count": ext["api_count"],
            "rows_extracted": ext["rows_extracted"],
            "rows_removed": tra["rows_removed"],
            "rows_removed_pct": round(100 * tra["rows_removed"] / tra["rows_in"], 4),
            "rows_final": tra["rows_out"],
            "output_file": tra["output_file"],
        },
        "thresholds": {k: c[k] for k in ("min_duration_minutes", "max_duration_minutes",
                                         "max_trip_distance", "max_speed_mph")},
        "removed_rows_by_rule": tra["removals"],
        "kept_despite_anomaly": tra["kept_despite_anomaly"],
        "type_conversion_failures": {k: v for k, v in tra["type_conversion_failures"].items() if v},
        "missing_values_before_cleaning": tra["missing_values_before_cleaning"],
        "checks": checks,
    }
    write_json_atomic(reports_dir / "quality_report.json", report)

    if not passed:
        failed = [ch["check"] for ch in checks if not ch["passed"]]
        raise ValidationError(f"Contrôles en échec : {failed} (voir quality_report.json)")
    logger.info("Validation : %d/%d contrôles OK -> quality_report.json", len(checks), len(checks))
    return report
