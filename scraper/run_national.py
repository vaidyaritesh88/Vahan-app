"""National OEM data scraper -- scrapes Maker x (VehCat/Fuel/VehClass/Month) for All States.

Scraping strategy:
  Two kinds of national scrapes:
  1. PER-MONTH DATA: Y=Maker x X=VehCat/Fuel/VehClass (12 scrapes per FY per type)
     - Uses CY mode + in-table month dropdown (JAN-DEC)
     - Gives OEM x category/fuel/class breakdown for each month
     - For an FY (Apr-Mar), iterates months 4-12 then 1-3
  2. MONTHLY OEM TOTALS: Y=Maker x X=Month Wise (one scrape per FY)
     - Gives OEM x Month cross-category totals
     - Monthly columns (APR, MAY, ..., MAR) come from X-axis

Usage:
    # Backfill FY19-FY26 monthly data (all types including monthly totals)
    python -m scraper.run_national --backfill-all

    # Backfill FY19-FY26 per-month VehCat/Fuel/VehClass only
    python -m scraper.run_national --backfill-monthly-detail

    # Latest FY only (all types, per-month)
    python -m scraper.run_national --latest

    # Specific FY and types
    python -m scraper.run_national --fy 2024 --types vehcat fuel

    # Specific FY, specific months
    python -m scraper.run_national --fy 2025 --types vehcat --months 4 5 6

    # Annual aggregate mode (no per-month iteration)
    python -m scraper.run_national --fy 2024 --types vehcat --annual

    # Test with a single FY + single month
    python -m scraper.run_national --fy 2025 --types vehcat --months 4 --delay 1
"""
import argparse
import logging
import os
import pathlib
import subprocess
import sys
import time
from datetime import datetime, date

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import FY_DROPDOWN_VALUES, MONTH_DROPDOWN_MAP

logger = logging.getLogger("national_scraper")

# FY month order: Apr(4) through Dec(12) then Jan(1) through Mar(3)
FY_MONTH_ORDER = [4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3]

# Types that support per-month scraping via in-table month dropdown
DETAIL_TYPES = ('vehcat', 'fuel', 'vehclass')

# Subsegment types: use checkbox filters for cross-tabulated data
# These scrape Y=Maker x X=Month with VhClass+Fuel checkbox filters
SUBSEGMENT_TYPES = (
    'sub_ev_pv', 'sub_ev_2w', 'sub_ev_3w', 'sub_pv_cng', 'sub_pv_hybrid',
    'sub_pv', 'sub_2w', 'sub_3w', 'sub_tractors',
)

# All per-month types (detail + subsegment)
ALL_MONTHLY_TYPES = DETAIL_TYPES + SUBSEGMENT_TYPES

# All available FY start years
ALL_FY_YEARS = sorted(FY_DROPDOWN_VALUES.keys())


def _get_current_fy():
    """Return current FY start year (e.g. 2025 for FY26 = Apr 2025 - Mar 2026)."""
    today = date.today()
    return today.year if today.month >= 4 else today.year - 1


def _fy_label(fy_start):
    """Human-readable FY label, e.g. 2024 -> 'FY25'."""
    return f"FY{str(fy_start + 1)[-2:]}"


def _get_ytd_months():
    """Get months available for current FY (up to previous month).

    If today is Mar 16, 2026 (FY26), returns [4,5,6,7,8,9,10,11,12,1,2]
    (Apr 2025 through Feb 2026 -- March not yet complete).
    """
    today = date.today()
    current_fy = _get_current_fy()
    current_month = today.month

    months = []
    for m in FY_MONTH_ORDER:
        # Month is available if it's before the current month in FY order
        if m == current_month:
            break  # Current month not yet complete
        months.append(m)
    return months


def run_national_scrape(fy_years, scrape_types=('vehcat', 'fuel', 'vehclass', 'monthly'),
                        months=None, annual_only=False, delay=3):
    """Run national scrapes for given FY years.

    Args:
        fy_years: List of FY start years (e.g. [2024, 2025])
        scrape_types: Which scrapes to run
        months: Specific months to scrape (1-12). None = all 12 months.
                Only applies to detail types (vehcat/fuel/vehclass).
        annual_only: If True, scrape annual aggregates only (month_num=None).
        delay: Seconds between scrapes
    """
    from scraper.vahan_http_scraper import VahanHttpScraper

    current_fy = _get_current_fy()

    # Determine months for detail types
    if annual_only:
        detail_months = [None]  # Single annual scrape
    elif months:
        detail_months = months
    else:
        detail_months = FY_MONTH_ORDER  # All 12 months

    # Count total scrapes
    detail_count = sum(1 for t in scrape_types if t in DETAIL_TYPES or t in SUBSEGMENT_TYPES) * len(detail_months)
    monthly_count = 1 if 'monthly' in scrape_types else 0
    total_per_fy = detail_count + monthly_count
    total_scrapes = len(fy_years) * total_per_fy

    print("=" * 60)
    print("National OEM Data Scraper")
    print(f"FY years: {[_fy_label(fy) for fy in sorted(fy_years)]}")
    print(f"Types: {', '.join(scrape_types)}")
    if annual_only:
        print("Mode: Annual aggregates only")
    else:
        month_labels = [MONTH_DROPDOWN_MAP.get(m, str(m)) for m in detail_months] if detail_months[0] else ['ALL']
        print(f"Months: {', '.join(month_labels)}")
    print(f"Delay: {delay}s between scrapes")
    print(f"Total scrapes: ~{total_scrapes} ({total_per_fy} per FY x {len(fy_years)} FYs)")
    print("=" * 60)

    scraper = VahanHttpScraper()

    # Test connection first
    ok, msg = scraper.test_connection()
    if not ok:
        print(f"\nConnection test FAILED: {msg}")
        print("The Vahan portal may be blocking your IP. Try from a local machine.")
        return 0

    print(f"Connection: OK ({msg})")

    total_rows = 0
    success = 0
    failed = 0
    start_time = datetime.now()

    for fy in sorted(fy_years):
        fy_lbl = _fy_label(fy)
        print(f"\n{'='*40}")
        print(f"  {fy_lbl} ({fy}-{fy+1})")
        print(f"{'='*40}")

        # For current FY, limit to YTD months (previous month and before)
        fy_detail_months = detail_months
        if fy == current_fy and not annual_only and months is None:
            ytd = _get_ytd_months()
            fy_detail_months = [m for m in detail_months if m in ytd]
            print(f"  (Current FY: YTD months only -> {[MONTH_DROPDOWN_MAP.get(m) for m in fy_detail_months]})")

        for stype in scrape_types:
            if stype == 'monthly':
                # Monthly OEM totals: single scrape per FY
                label = f"{fy_lbl} monthly"
                print(f"\n  {label}...", end=" ", flush=True)
                try:
                    rows = scraper.scrape_and_store_national(
                        fy, scrape_types=('monthly',))
                    total_rows += rows
                    success += 1
                    print(f"OK ({rows} rows)")
                except Exception as e:
                    failed += 1
                    print(f"FAILED: {str(e)[:100]}")
                    logger.exception(f"Failed: {label}")
                time.sleep(delay)
            elif stype in DETAIL_TYPES or stype in SUBSEGMENT_TYPES:
                # Per-month scraping
                for month_num in fy_detail_months:
                    m_label = MONTH_DROPDOWN_MAP.get(month_num, 'ALL') if month_num else 'ALL'
                    label = f"{fy_lbl} {stype} {m_label}"
                    print(f"  {label}...", end=" ", flush=True)
                    try:
                        rows = scraper.scrape_and_store_national(
                            fy, month_num=month_num, scrape_types=(stype,))
                        total_rows += rows
                        success += 1
                        print(f"OK ({rows} rows)")
                    except Exception as e:
                        failed += 1
                        print(f"FAILED: {str(e)[:100]}")
                        logger.exception(f"Failed: {label}")
                    time.sleep(delay)

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n{'=' * 60}")
    print(f"Done! {success} succeeded, {failed} failed")
    print(f"Total rows: {total_rows:,}")
    print(f"Elapsed: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"{'=' * 60}")

    if total_rows > 0:
        print("\nPushing updated DB to remote...")
        _git_push_db()

    return total_rows


def _git_push_db():
    """Commit and push the updated database to keep the remote in sync."""
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    db_path = repo_root / "data" / "vahan_tracker.db"
    if not db_path.exists():
        print("  DB file not found, skipping git push.")
        return

    try:
        def _run(cmd):
            return subprocess.run(
                cmd, cwd=str(repo_root),
                capture_output=True, text=True, timeout=120,
            )

        status = _run(["git", "status", "--porcelain", str(db_path)])
        if not status.stdout.strip():
            print("  No DB changes detected, skipping git push.")
            return

        r1 = _run(["git", "add", str(db_path)])
        if r1.returncode != 0:
            print(f"  git add failed: {r1.stderr.strip()}")
            return

        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        msg = f"data: update national OEM data ({ts})"
        r2 = _run(["git", "commit", "-m", msg])
        if r2.returncode != 0:
            print(f"  git commit failed: {r2.stderr.strip()}")
            return

        print(f"  Committed: {msg}")
        r3 = _run(["git", "push"])
        if r3.returncode != 0:
            print(f"  git push failed: {r3.stderr.strip()}")
            return
        print("  Pushed to remote successfully.")

    except Exception as e:
        print(f"  Git push error (non-fatal): {e}")


def main():
    parser = argparse.ArgumentParser(
        description="National OEM data scraper (Maker x VehCat/Fuel/VehClass/Month)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m scraper.run_national --latest                       # Current FY, all types, per-month
  python -m scraper.run_national --backfill-all                 # FY19-FY26, all types, per-month
  python -m scraper.run_national --backfill-monthly-detail      # FY19-FY26 vehcat/fuel/vehclass per-month
  python -m scraper.run_national --fy 2024 --types vehcat       # FY25 vehcat per-month (all 12)
  python -m scraper.run_national --fy 2025 --types vehcat --months 4 5 6  # FY26 vehcat Apr-Jun only
  python -m scraper.run_national --fy 2024 --types vehcat --annual  # FY25 vehcat annual aggregate
  python -m scraper.run_national --fy 2025 --subsegments            # FY26 + all subsegment types
  python -m scraper.run_national --fy 2025 --types sub_ev_pv sub_pv_cng  # Specific subsegments only
        """,
    )
    parser.add_argument("--backfill-all", action="store_true",
                        help="Backfill FY19-FY26 with all types per-month")
    parser.add_argument("--backfill-monthly-detail", action="store_true",
                        help="Backfill FY19-FY26 vehcat/fuel/vehclass per-month (no monthly totals)")
    parser.add_argument("--latest", action="store_true",
                        help="Scrape the current FY only (all types, per-month YTD)")
    parser.add_argument("--fy", nargs="+", type=int,
                        help="Specific FY start years (e.g. 2024 for FY25)")
    parser.add_argument("--types", nargs="+",
                        default=["vehcat", "fuel", "vehclass", "monthly"],
                        choices=["vehcat", "fuel", "vehclass", "monthly",
                                 "sub_ev_pv", "sub_ev_2w", "sub_ev_3w",
                                 "sub_pv_cng", "sub_pv_hybrid",
                                 "sub_pv", "sub_2w", "sub_3w", "sub_tractors",
                                 "subsegments"],
                        help="Scrape types (default: all four)")
    parser.add_argument("--months", nargs="+", type=int,
                        help="Specific months (1-12) for detail types. Default: all 12")
    parser.add_argument("--subsegments", action="store_true",
                        help="Include all subsegment types (EV_PV, PV_CNG, etc.)")
    parser.add_argument("--annual", action="store_true",
                        help="Annual aggregates only (no per-month iteration)")
    parser.add_argument("--delay", type=int, default=3,
                        help="Delay between scrapes in seconds (default: 3)")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Handle 'subsegments' type alias and --subsegments flag
    types_list = list(args.types)
    if 'subsegments' in types_list:
        types_list.remove('subsegments')
        types_list.extend(SUBSEGMENT_TYPES)
    if hasattr(args, 'subsegments') and args.subsegments:
        types_list.extend(t for t in SUBSEGMENT_TYPES if t not in types_list)
    args.types = types_list

    if args.backfill_all:
        run_national_scrape(
            fy_years=ALL_FY_YEARS,
            scrape_types=('vehcat', 'fuel', 'vehclass', 'monthly'),
            delay=args.delay,
        )
    elif args.backfill_monthly_detail:
        run_national_scrape(
            fy_years=ALL_FY_YEARS,
            scrape_types=DETAIL_TYPES,
            delay=args.delay,
        )
    elif args.latest:
        current_fy = _get_current_fy()
        run_national_scrape(
            fy_years=[current_fy],
            scrape_types=tuple(args.types),
            delay=args.delay,
        )
    elif args.fy:
        run_national_scrape(
            fy_years=args.fy,
            scrape_types=tuple(args.types),
            months=args.months,
            annual_only=args.annual,
            delay=args.delay,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
