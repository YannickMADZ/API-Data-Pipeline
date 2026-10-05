"""
Orchestrateur : point d'entrée unique du pipeline.

    python -m pipeline --year 2023 --month 1
    python -m pipeline --year 2023 --month 1 --steps extract
    python -m pipeline --year 2023 --month 1 --force

Les étapes s'enchaînent automatiquement dans l'ordre extract -> transform ->
validate. Si une étape échoue (exception), le pipeline s'arrête IMMÉDIATEMENT
avec un code de sortie non nul : les étapes suivantes ne tournent jamais sur
des données incomplètes.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

from pipeline import extract, transform, validate
from pipeline.config import PROJECT_ROOT, ConfigError, load_config, make_period
from pipeline.logger import setup_logging

# Ordre officiel des étapes. Ajouter une étape = ajouter une ligne ici.
STEPS = {
    "extract": extract.run,
    "transform": transform.run,
    "validate": validate.run,
}

# Codes de sortie (utiles pour un ordonnanceur : cron, Airflow, CI...)
EXIT_OK, EXIT_STEP_FAILED, EXIT_CONFIG_ERROR, EXIT_INTERRUPTED = 0, 1, 2, 130


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline",
        description="Pipeline NYC Yellow Taxi : extraction, transformation, validation d'un mois.",
    )
    parser.add_argument("--year", type=int, required=True, help="Année à traiter, ex. 2023")
    parser.add_argument("--month", type=int, required=True, help="Mois à traiter (1-12)")
    parser.add_argument(
        "--steps", default=",".join(STEPS),
        help=f"Étapes à exécuter, séparées par des virgules (défaut : {','.join(STEPS)})",
    )
    parser.add_argument("--config", default="config.yaml", help="Chemin du fichier de configuration")
    parser.add_argument("--force", action="store_true",
                        help="Ignore les fichiers existants du mois et repart de zéro")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args(argv)


def select_steps(raw: str) -> list[str]:
    """Valide la liste d'étapes et la remet dans l'ordre officiel."""
    requested = [s.strip() for s in raw.split(",") if s.strip()]
    unknown = [s for s in requested if s not in STEPS]
    if unknown:
        raise ConfigError(f"Étapes inconnues : {unknown}. Disponibles : {list(STEPS)}")
    return [s for s in STEPS if s in requested]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    # Erreurs de configuration : détectées AVANT tout appel réseau.
    try:
        cfg = load_config(args.config)
        period = make_period(args.year, args.month)
        steps = select_steps(args.steps)
    except ConfigError as exc:
        print(f"Erreur de configuration : {exc}", file=sys.stderr)
        return EXIT_CONFIG_ERROR

    logger = setup_logging(PROJECT_ROOT / cfg["paths"]["logs_dir"], period.label, args.log_level)
    logger.info("=== Pipeline %s | étapes : %s ===", period.label, " -> ".join(steps))

    for step in steps:
        logger.info("--- Étape [%s] : début ---", step)
        t0 = time.monotonic()
        try:
            STEPS[step](cfg, period, force=args.force)
        except KeyboardInterrupt:
            logger.error("Interruption manuelle pendant [%s]. Relancer la même commande reprendra "
                         "l'extraction là où elle s'est arrêtée.", step)
            return EXIT_INTERRUPTED
        except NotImplementedError as exc:
            logger.error("Étape [%s] non implémentée : %s. Pipeline arrêté.", step, exc)
            return EXIT_STEP_FAILED
        except ConfigError as exc:
            logger.error("Étape [%s] : erreur de configuration : %s", step, exc)
            return EXIT_CONFIG_ERROR
        except Exception:
            # logger.exception ajoute la trace complète dans le fichier de log.
            logger.exception("Étape [%s] en échec. Pipeline arrêté, étapes suivantes annulées.", step)
            return EXIT_STEP_FAILED
        logger.info("--- Étape [%s] : terminée en %.1fs ---", step, time.monotonic() - t0)

    logger.info("=== Pipeline %s terminé avec succès ===", period.label)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
