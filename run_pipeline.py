#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PIPELINE] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("serverless_pipeline")


def _run_bronze(mode: str):
    logger.info("━" * 60)
    logger.info("[1/4] BRONZE — Scraping Gupy API → Delta Lake")
    logger.info("━" * 60)
    from src.bronze.gupy_scraper import coletar_vagas_bronze

    t0 = time.perf_counter()
    coletar_vagas_bronze(modo=mode)
    logger.info(f"[1/4] BRONZE completed in {time.perf_counter() - t0:.1f}s")


def _run_silver(full_reprocess: bool = False):
    logger.info("━" * 60)
    logger.info("[2/4] SILVER — Processing & Enriching data")
    logger.info("━" * 60)
    from src.silver.run_silver import main as silver_main

    t0 = time.perf_counter()
    # NOTA: Exige que run_silver.py seja refatorado para:
    # def main(process_date: str, full: bool = False):
    silver_main(process_date=datetime.now().strftime("%Y-%m-%d"), full=full_reprocess)
    logger.info(f"[2/4] SILVER completed in {time.perf_counter() - t0:.1f}s")


def _run_gold():
    logger.info("━" * 60)
    logger.info("[3/4] GOLD — Generating analytical aggregations")
    logger.info("━" * 60)
    from src.gold.run_gold import main as gold_main

    t0 = time.perf_counter()
    gold_main()
    logger.info(f"[3/4] GOLD completed in {time.perf_counter() - t0:.1f}s")


def _run_sync() -> bool:
    logger.info("━" * 60)
    logger.info("[4/4] SYNC — UPSERT into Neon PostgreSQL")
    logger.info("━" * 60)

    if not os.getenv("NEON_DATABASE_URL"):
        logger.warning("[4/4] SYNC skipped — NEON_DATABASE_URL not set")
        return False

    from scripts.sync_to_neon import main as sync_main

    t0 = time.perf_counter()
    try:
        sync_main()
    except SystemExit as e:
        if e.code != 0:
            logger.error(f"[4/4] SYNC failed with exit code {e.code}")
            return False

    logger.info(f"[4/4] SYNC completed in {time.perf_counter() - t0:.1f}s")
    return True


def main():
    parser = argparse.ArgumentParser(description="Serverless Medallion Pipeline")
    parser.add_argument("--mode", choices=["auto", "full", "incremental"], default="auto")
    parser.add_argument("--skip-sync", action="store_true")
    args = parser.parse_args()

    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║   SERVERLESS MEDALLION PIPELINE — GitHub Actions        ║")
    logger.info("╠══════════════════════════════════════════════════════════╣")
    logger.info(f"║ Mode: {args.mode:<48s} ║")
    logger.info(f"║ Sync: {'enabled' if not args.skip_sync else 'DISABLED':<45s} ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")

    pipeline_start = time.perf_counter()
    failures = 0

    steps = [
        ("[1/4] BRONZE FAILED", lambda: _run_bronze(mode=args.mode)),
        ("[2/4] SILVER FAILED", lambda: _run_silver(full_reprocess=(args.mode == "full"))),
        ("[3/4] GOLD FAILED", lambda: _run_gold()),
    ]

    for error_msg, step_func in steps:
        try:
            step_func()
        except Exception:
            logger.exception(f"{error_msg} — aborting pipeline")
            sys.exit(1)

    if not args.skip_sync:
        try:
            if not _run_sync():
                failures += 1
        except Exception:
            logger.exception("[4/4] SYNC FAILED (non-fatal)")
            failures += 1
    else:
        logger.info("[4/4] SYNC skipped (--skip-sync)")

    total_elapsed = time.perf_counter() - pipeline_start
    status = "COMPLETED" if failures == 0 else f"COMPLETED WITH {failures} WARNING(S)"

    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info(f"║   PIPELINE {status:<40s}║")
    logger.info(f"║   Total time: {total_elapsed:.1f}s{' ' * 35}║")
    logger.info("╚══════════════════════════════════════════════════════════╝")

    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    main()