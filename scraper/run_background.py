"""Background scraper process -- runs independently of Streamlit.

Usage (new Y-axis approach -- default):
    python -m scraper.run_background                     # scrape all states/years
    python -m scraper.run_background --years 2025 2026   # specific years
    python -m scraper.run_background --modes category fuel maker  # specific modes

Usage (legacy per-category approach):
    python -m scraper.run_background --legacy --categories PV 2W

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
import subprocess
import pathlib
from datetime import datetime

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATA_DIR, ALL_STATES, VAHAN_SCRAPE_CONFIGS
from database.queries import aggregate_state_to_national

logger = logging.getLogger("vahan_scraper")

CONTROL_FILE = os.path.join(DATA_DIR, ".scraper_control.json")


def _write_control(status, pid=None, extra=None):
    """Write scraper control file.

    IMPORTANT: If status is "running" but the file already says "stopping",
    we preserve "stopping" to avoid a race condition where the progress
    update clobbers the stop signal.
    """
    # Preserve stop signal — don't overwrite "stopping" with "running"
    if status == "running":
        existing = _read_control()
        if existing and existing.get("status") == "stopping":
            status = "stopping"

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
            os.kill(pid, 0)
            return True, ctrl
        except (OSError, ProcessLookupError):
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


def run_state_scrape(states, years, modes=("category", "fuel", "maker"),
                     delay=2, skip_existing=True):
    """New Y-axis based scrape loop: iterate over (state, year) pairs.

    Each state/year combo does 1-3 scrapes (Vehicle Class, Fuel, Maker)
    instead of the old approach of 7+ category-specific scrapes.
    """
    from scraper.vahan_http_scraper import (
        VahanHttpScraper, get_pending_state_scrapes,
    )

    running, info = is_scraper_running()
    if running:
        print(f"Scraper already running (PID {info.get('pid')}). Stop it first.")
        sys.exit(1)

    # Determine jobs
    if skip_existing:
        jobs = get_pending_state_scrapes(states, years, modes)
    else:
        jobs = [(s, y) for s in states for y in years]

    if not jobs:
        print("Nothing to scrape -- all state/year combinations already done.")
        return

    total = len(jobs)
    mode_str = "+".join(modes)
    print(f"Starting state scrape: {total} state/year combinations")
    print(f"Modes: {mode_str}")
    print(f"States: {len(states)} states")
    print(f"Years: {years}")
    print(f"Delay: {delay}s between scrapes")
    print(f"PID: {os.getpid()}")
    print("-" * 60)

    _write_control("running", extra={
        "total_jobs": total,
        "modes": list(modes),
        "years": list(years),
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

        consecutive_failures = 0
        MAX_CONSECUTIVE_FAILURES = 10

        for i, (state, year) in enumerate(jobs):
            if _should_stop():
                print(f"\nStop requested. Completed {success + failed}/{total}.")
                break

            label = f"[{i+1}/{total}] {state} / {year} ({mode_str})"
            print(f"{label} ...", end=" ", flush=True)

            try:
                rows = scraper.scrape_and_store_state(
                    state, year, modes=modes)
                total_rows += rows
                success += 1
                consecutive_failures = 0  # Reset on success
                print(f"OK ({rows} rows)")
            except Exception as e:
                failed += 1
                consecutive_failures += 1
                print(f"FAILED: {str(e)[:80]}")

                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"\n{MAX_CONSECUTIVE_FAILURES} consecutive failures "
                          f"-- stopping (portal may be unreachable).")
                    break

            _write_control("running", extra={
                "total_jobs": total,
                "completed": success + failed,
                "success": success,
                "failed": failed,
                "total_rows": total_rows,
                "current_job": label,
                "modes": list(modes),
                "years": list(years),
                "states_count": len(states),
                "delay": delay,
                "started_at": _read_control().get("started_at", ""),
            })

            if i < total - 1:
                time.sleep(delay)

    except KeyboardInterrupt:
        print("\n\nKeyboard interrupt -- stopping gracefully.")
    except Exception as e:
        print(f"\n\nFatal error: {e}")
    finally:
        _cleanup_control()

    if success > 0:
        print("\nAggregating state data to national totals...")
        agg = aggregate_state_to_national()
        print(f"Aggregated {agg:,} national records.")

    print(f"\nDone! {success} succeeded, {failed} failed, "
          f"{total_rows:,} rows upserted.")

    if success > 0:
        print("\nPushing updated DB to remote...")
        _git_push_db()


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

        # Check if there are actual changes to the DB
        status = _run(["git", "status", "--porcelain", str(db_path)])
        if not status.stdout.strip():
            print("  No DB changes detected, skipping git push.")
            return

        # Stage, commit, push
        r1 = _run(["git", "add", str(db_path)])
        if r1.returncode != 0:
            print(f"  git add failed: {r1.stderr.strip()}")
            return

        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        msg = f"data: update vahan_tracker.db ({ts})"
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
    parser = argparse.ArgumentParser(description="Vahan background scraper")
    parser.add_argument(
        "--modes", nargs="+", default=["category", "fuel", "maker"],
        help="Scrape modes: category (Vehicle Class axis), fuel (Fuel axis), "
             "maker (Maker axis). Default: category fuel",
    )
    parser.add_argument(
        "--states", nargs="+", default=None,
        help="States to scrape (default: top 15)",
    )
    parser.add_argument(
        "--years", nargs="+", type=int, default=list(range(2020, 2027)),
        help="Years to scrape",
    )
    parser.add_argument(
        "--delay", type=int, default=2,
        help="Delay between requests (seconds)",
    )
    parser.add_argument(
        "--all-states", action="store_true",
        help="Scrape all states/UTs",
    )
    parser.add_argument(
        "--rescrape", action="store_true",
        help="Don't skip existing data",
    )
    parser.add_argument(
        "--stop", action="store_true",
        help="Send stop signal to running scraper",
    )
    # Legacy mode
    parser.add_argument(
        "--legacy", action="store_true",
        help="Use old per-category scraping (broken VhClass filters)",
    )
    parser.add_argument(
        "--categories", nargs="+",
        default=["2W", "PV", "3W", "EV_2W", "EV_PV", "EV_3W", "TRACTORS"],
        help="Category codes for legacy mode",
    )

    args = parser.parse_args()

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
        states = [
            "Maharashtra", "Tamil Nadu", "Karnataka", "Gujarat",
            "Uttar Pradesh", "Rajasthan", "Delhi", "Haryana", "Kerala",
            "Madhya Pradesh", "Andhra Pradesh", "Telangana", "West Bengal",
            "Punjab", "Bihar",
        ]

    if args.legacy:
        # Legacy per-category approach
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
    else:
        # New Y-axis approach (default)
        valid_modes = ("category", "fuel", "maker")
        for m in args.modes:
            if m not in valid_modes:
                print(f"Invalid mode '{m}'. Valid: {list(valid_modes)}")
                sys.exit(1)
        run_state_scrape(
            states=states,
            years=args.years,
            modes=tuple(args.modes),
            delay=args.delay,
            skip_existing=not args.rescrape,
        )


if __name__ == "__main__":
    main()
