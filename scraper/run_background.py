"""Background scraper process — runs independently of Streamlit.

Usage:
    python -m scraper.run_background                    # scrape with defaults
    python -m scraper.run_background --categories PV 2W # specific categories
    python -m scraper.run_background --years 2025 2026  # specific years

Control:
    Stop gracefully by writing {"status": "stopping"} to data/.scraper_control.json,
    or use the Stop button in the Data Management page.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATA_DIR, ALL_STATES, VAHAN_SCRAPE_CONFIGS
from database.queries import aggregate_state_to_national

logger = logging.getLogger("vahan_scraper")

CONTROL_FILE = os.path.join(DATA_DIR, ".scraper_control.json")


def _write_control(status, pid=None, extra=None):
    """Write scraper control file."""
    data = {
        "status": status,
        "pid": pid or os.getpid(),
        "updated_at": datetime.now().isoformat(),
    }
    if extra:
        data.update(extra)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONTROL_FILE, "w") as f:
        json.dump(data, f, indent=2)


def _read_control():
    """Read scraper control file. Returns dict or None."""
    try:
        with open(CONTROL_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _should_stop():
    """Check if stop has been requested."""
    ctrl = _read_control()
    return ctrl is not None and ctrl.get("status") == "stopping"


def _cleanup_control():
    """Remove control file when done."""
    try:
        os.remove(CONTROL_FILE)
    except FileNotFoundError:
        pass


def is_scraper_running():
    """Check if a background scraper is currently running.

    Returns (running: bool, info: dict or None).
    """
    ctrl = _read_control()
    if ctrl is None:
        return False, None

    if ctrl.get("status") not in ("running", "stopping"):
        return False, ctrl

    # Check if the PID is still alive
    pid = ctrl.get("pid")
    if pid:
        try:
            # On Windows, os.kill with signal 0 checks existence
            os.kill(pid, 0)
            return True, ctrl
        except (OSError, ProcessLookupError):
            # Process died — stale control file
            _cleanup_control()
            return False, None

    return False, ctrl


def request_stop():
    """Signal the background scraper to stop gracefully."""
    ctrl = _read_control()
    if ctrl and ctrl.get("status") == "running":
        _write_control("stopping", pid=ctrl.get("pid"))
        return True
    return False


def run(categories, states, years, delay=2, skip_existing=True):
    """Main background scrape loop."""
    from scraper.vahan_http_scraper import VahanHttpScraper, get_pending_scrapes

    # Check if already running
    running, info = is_scraper_running()
    if running:
        print(f"Scraper already running (PID {info.get('pid')}). Stop it first.")
        sys.exit(1)

    # Determine jobs
    if skip_existing:
        jobs = get_pending_scrapes(categories, states, years)
    else:
        jobs = [(c, s, y) for c in categories for s in states for y in years]

    if not jobs:
        print("Nothing to scrape — all combinations already done.")
        return

    total = len(jobs)
    print(f"Starting background scrape: {total} combinations")
    print(f"Categories: {categories}")
    print(f"States: {len(states)} states")
    print(f"Years: {years}")
    print(f"Delay: {delay}s between requests")
    print(f"Control file: {CONTROL_FILE}")
    print(f"PID: {os.getpid()}")
    print("-" * 60)

    # Write control file
    _write_control("running", extra={
        "total_jobs": total,
        "categories": categories,
        "years": years,
        "states_count": len(states),
        "delay": delay,
        "started_at": datetime.now().isoformat(),
    })

    scraper = None
    success = 0
    failed = 0
    total_rows = 0

    try:
        scraper = VahanHttpScraper()

        for i, (cat, state, year) in enumerate(jobs):
            # Check stop signal
            if _should_stop():
                print(f"\nStop requested. Completed {success + failed}/{total} jobs.")
                break

            label = f"[{i+1}/{total}] {cat} / {state} / {year}"
            print(f"{label} ...", end=" ", flush=True)

            try:
                rows = scraper.scrape_and_store(cat, state, year)
                total_rows += rows
                success += 1
                print(f"OK ({rows} rows)")
            except Exception as e:
                failed += 1
                print(f"FAILED: {str(e)[:80]}")

            # Update control file with progress
            _write_control("running", extra={
                "total_jobs": total,
                "completed": success + failed,
                "success": success,
                "failed": failed,
                "total_rows": total_rows,
                "current_job": label,
                "categories": categories,
                "years": years,
                "states_count": len(states),
                "delay": delay,
                "started_at": _read_control().get("started_at", ""),
            })

            if i < total - 1:  # don't sleep after last job
                time.sleep(delay)

        # Aggregate if we scraped anything
        if success > 0:
            print("\nAggregating state data → national totals...")
            agg = aggregate_state_to_national()
            print(f"Aggregated {agg:,} national records.")

    except KeyboardInterrupt:
        print("\n\nKeyboard interrupt — stopping gracefully.")
    except Exception as e:
        print(f"\n\nFatal error: {e}")
    finally:
        _cleanup_control()

    print(f"\nDone! {success} succeeded, {failed} failed, {total_rows:,} rows upserted.")


def main():
    parser = argparse.ArgumentParser(description="Vahan background scraper")
    parser.add_argument(
        "--categories", nargs="+",
        default=["2W", "PV", "3W", "EV_2W", "EV_PV", "EV_3W", "TRACTORS"],
        help="Category codes to scrape",
    )
    parser.add_argument(
        "--states", nargs="+", default=None,
        help="States to scrape (default: top 15)",
    )
    parser.add_argument(
        "--years", nargs="+", type=int, default=list(range(2020, 2027)),
        help="Years to scrape",
    )
    parser.add_argument("--delay", type=int, default=2, help="Delay between requests (seconds)")
    parser.add_argument("--all-states", action="store_true", help="Scrape all states/UTs")
    parser.add_argument("--rescrape", action="store_true", help="Don't skip existing")
    parser.add_argument("--stop", action="store_true", help="Send stop signal to running scraper")

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.stop:
        if request_stop():
            print("Stop signal sent. Scraper will stop after current job.")
        else:
            print("No running scraper found.")
        return

    # Determine states
    if args.all_states:
        states = ALL_STATES
    elif args.states:
        states = args.states
    else:
        # Top 15 states by registrations
        states = [
            "Maharashtra", "Tamil Nadu", "Karnataka", "Gujarat", "Uttar Pradesh",
            "Rajasthan", "Delhi", "Haryana", "Kerala", "Madhya Pradesh",
            "Andhra Pradesh", "Telangana", "West Bengal", "Punjab", "Bihar",
        ]

    # Validate categories
    valid = list(VAHAN_SCRAPE_CONFIGS.keys())
    for cat in args.categories:
        if cat not in valid:
            print(f"Invalid category '{cat}'. Valid: {valid}")
            sys.exit(1)

    run(
        categories=args.categories,
        states=states,
        years=args.years,
        delay=args.delay,
        skip_existing=not args.rescrape,
    )


if __name__ == "__main__":
    main()
