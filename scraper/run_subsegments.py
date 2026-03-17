"""CLI to scrape national subsegment data (EV_PV, PV_CNG, PV_HYBRID, EV_2W, EV_3W).

Uses Selenium (headless Chrome) because subsegment queries require checkbox
filtering on the Vahan portal, which the HTTP scraper cannot handle.

Usage:
    # Scrape all subsegments for current FY
    python -m scraper.run_subsegments

    # Scrape specific subsegments
    python -m scraper.run_subsegments --types EV_PV PV_CNG

    # Scrape specific FY
    python -m scraper.run_subsegments --fy 2025

    # Scrape a single month
    python -m scraper.run_subsegments --fy 2025 --month 1

    # Non-headless (show browser window for debugging)
    python -m scraper.run_subsegments --visible
"""
import sys
import os
import argparse
import logging
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout.reconfigure(encoding="utf-8")

from config.settings import VAHAN_SCRAPE_CONFIGS

# All subsegment codes (those that need checkbox filters)
SUBSEGMENT_CODES = ["EV_PV", "EV_2W", "EV_3W", "PV_CNG", "PV_HYBRID"]

logger = logging.getLogger("scraper.run_subsegments")


def get_current_fy_start():
    """Get the FY start year for the current date."""
    today = date.today()
    return today.year if today.month >= 4 else today.year - 1


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Vahan subsegment data (Selenium)"
    )
    parser.add_argument(
        "--types", nargs="+", default=None,
        help=f"Subsegment codes to scrape (default: all). "
             f"Options: {', '.join(SUBSEGMENT_CODES)}",
    )
    parser.add_argument(
        "--fy", type=int, default=None,
        help="FY start year (e.g. 2025 for FY26). Default: current FY.",
    )
    parser.add_argument(
        "--month", type=int, default=None,
        help="Specific month number (1-12). Default: full FY.",
    )
    parser.add_argument(
        "--visible", action="store_true",
        help="Show the browser window (non-headless mode).",
    )
    parser.add_argument(
        "--delay", type=float, default=3.0,
        help="Delay between subsegment scrapes (seconds). Default: 3",
    )
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    types = args.types or SUBSEGMENT_CODES
    fy_start = args.fy or get_current_fy_start()

    # Validate types
    for t in types:
        if t not in VAHAN_SCRAPE_CONFIGS:
            logger.error(
                f"Unknown subsegment: '{t}'. "
                f"Available: {', '.join(SUBSEGMENT_CODES)}"
            )
            sys.exit(1)

    fy_label = f"FY{(fy_start + 1) % 100:02d}"
    month_str = f" month={args.month}" if args.month else " (full year)"
    logger.info(
        f"Subsegment scrape: {', '.join(types)} | "
        f"{fy_label} (start={fy_start}){month_str}"
    )

    # Initialize Selenium scraper
    from scraper.vahan_selenium_scraper import VahanSeleniumScraper
    scraper = VahanSeleniumScraper(headless=not args.visible)

    try:
        # Test connection
        ok, msg = scraper.test_connection()
        if not ok:
            logger.error(f"Cannot reach Vahan portal: {msg}")
            sys.exit(1)
        logger.info(f"Portal connected: {msg}")

        results = {}
        import time

        for sub_code in types:
            logger.info(f"\n{'='*50}")
            logger.info(f"Scraping {sub_code} for {fy_label}...")
            logger.info(f"{'='*50}")

            try:
                rows = scraper.scrape_subsegment(
                    sub_code, fy_start, month_num=args.month
                )
                results[sub_code] = rows
                logger.info(f"{sub_code}: {rows} rows stored")
            except Exception as e:
                logger.error(f"{sub_code}: FAILED - {e}")
                results[sub_code] = f"FAILED: {e}"

            time.sleep(args.delay)

        # Summary
        logger.info(f"\n{'='*50}")
        logger.info("SUMMARY")
        logger.info(f"{'='*50}")
        for code, result in results.items():
            status = f"{result} rows" if isinstance(result, int) else result
            logger.info(f"  {code:12s}: {status}")

    finally:
        scraper.close()
        logger.info("Browser closed")


if __name__ == "__main__":
    main()
