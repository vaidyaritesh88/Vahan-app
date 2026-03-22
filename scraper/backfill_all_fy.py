"""Batch backfill all subsegments for FY20-FY24 (FY25-FY26 already done).

Usage:
    python scraper/backfill_all_fy.py [--start-fy 2019] [--end-fy 2023]
    
FY naming: --start-fy 2019 means FY20 (Apr 2019 - Mar 2020)
"""
import argparse
import logging
import time
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper.vahan_selenium_scraper import VahanSeleniumScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Order: light subsegments first (single page), heavy last (many pages)
SUBSEGMENTS = ["EV_PV", "PV_CNG", "PV_HYBRID", "EV_2W", "EV_3W"]


def main():
    parser = argparse.ArgumentParser(description="Batch backfill subsegment data")
    parser.add_argument("--start-fy", type=int, default=2019,
                        help="Start FY year (2019 = FY20). Default: 2019")
    parser.add_argument("--end-fy", type=int, default=2023,
                        help="End FY year (2023 = FY24). Default: 2023")
    parser.add_argument("--types", nargs="+", default=SUBSEGMENTS,
                        help="Subsegment types to scrape")
    parser.add_argument("--visible", action="store_true",
                        help="Show browser window")
    args = parser.parse_args()

    fy_years = list(range(args.start_fy, args.end_fy + 1))
    total_jobs = len(fy_years) * len(args.types)
    
    logger.info(f"Backfill plan: {len(args.types)} subsegments x {len(fy_years)} FYs = {total_jobs} jobs")
    logger.info(f"FYs: {['FY' + str(y - 2000 + 1) for y in fy_years]}")
    logger.info(f"Types: {args.types}")

    completed = 0
    failed = []

    for sub in args.types:
        scraper = VahanSeleniumScraper(headless=not args.visible)
        try:
            for fy in fy_years:
                completed += 1
                fy_label = f"FY{fy - 2000 + 1}"
                logger.info(f"\n{'='*60}")
                logger.info(f"[{completed}/{total_jobs}] {sub} {fy_label} (fy_start={fy})")
                logger.info(f"{'='*60}")
                
                try:
                    rows = scraper.scrape_subsegment(sub, fy_start=fy)
                    logger.info(f"  Result: {rows} OEM-month rows stored")
                except Exception as e:
                    logger.error(f"  FAILED: {e}")
                    failed.append(f"{sub} {fy_label}: {e}")
                    # Reset page state for next attempt
                    scraper._page_loaded = False
                
                # Delay between FYs
                time.sleep(5)
        finally:
            scraper.close()
        
        # Delay between subsegment types
        time.sleep(10)

    logger.info(f"\n{'='*60}")
    logger.info(f"BACKFILL COMPLETE: {completed - len(failed)}/{total_jobs} succeeded")
    if failed:
        logger.info(f"FAILURES ({len(failed)}):")
        for f in failed:
            logger.info(f"  - {f}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
