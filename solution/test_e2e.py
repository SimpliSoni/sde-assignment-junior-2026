"""End-to-end test runner for clean runs, chaos runs, and kill -9 crash recovery."""
import argparse
import asyncio
import csv
import logging
import os
import signal
import subprocess
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("e2e")


def run_full_simulation(
    speed: float = 1200.0,
    failure_scale: float = 1.0,
    seed: int = 7,
    simulate_crash: bool = False,
    crash_after_sec: float = 6.0,
    db_path: str = "solution/dispatcher.db",
    output_csv: str = "final_state.csv",
):
    # Clean previous db and output
    for path in (db_path, f"{db_path}-wal", f"{db_path}-shm", output_csv):
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass

    env = os.environ.copy()
    env["PYTHONPATH"] = f"assignment-b-workflow:{os.getcwd()}"

    # 1. Start mock telephony
    mock_cmd = [
        sys.executable,
        "-m",
        "mock_telephony",
        "--speed",
        str(speed),
        "--failure-scale",
        str(failure_scale),
        "--seed",
        str(seed),
        "--port",
        "8099",
    ]
    logger.info("Starting mock telephony: %s", " ".join(mock_cmd))
    mock_proc = subprocess.Popen(mock_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    try:
        # Wait for mock to be ready
        time.sleep(1.5)

        # 2. Start dispatcher
        disp_cmd = [
            sys.executable,
            "-m",
            "solution.runner",
            "--db",
            db_path,
            "--output",
            output_csv,
            "--exit-on-complete",
        ]

        if not simulate_crash:
            logger.info("Running dispatcher until completion...")
            disp_proc = subprocess.Popen(disp_cmd, env=env)
            disp_proc.wait()
            if disp_proc.returncode != 0:
                raise RuntimeError(f"Dispatcher exited with code {disp_proc.returncode}")
        else:
            logger.info("Running dispatcher with kill -9 simulation after %.1f seconds...", crash_after_sec)
            disp_proc = subprocess.Popen(disp_cmd, env=env)
            time.sleep(crash_after_sec)
            logger.warning("Simulating crash: sending SIGKILL (kill -9) to PID %d...", disp_proc.pid)
            os.kill(disp_proc.pid, signal.SIGKILL)
            disp_proc.wait()
            logger.info("Process killed! Waiting 2 seconds before restarting...")
            time.sleep(2.0)
            logger.info("Restarting dispatcher after crash...")
            disp_proc2 = subprocess.Popen(disp_cmd, env=env)
            disp_proc2.wait()
            if disp_proc2.returncode != 0:
                raise RuntimeError(f"Restarted dispatcher exited with code {disp_proc2.returncode}")

        # 3. Verification
        logger.info("Verifying output %s...", output_csv)
        assert os.path.exists(output_csv), f"Output file {output_csv} was not generated!"

        # Query provider admin endpoints
        import httpx
        with httpx.Client(base_url="http://127.0.0.1:8099", timeout=10.0) as client:
            rep = client.get("/admin/report").json()
            leads = client.get("/admin/leads").json()

        logger.info("=== Provider Final Report ===")
        logger.info("Violation count: %d", rep.get("violation_count", -1))
        logger.info("Violations: %s", rep.get("violations_by_type"))
        logger.info("Max Concurrency Seen: %s", rep.get("max_concurrency_seen"))
        logger.info("Callback Lateness (minutes): %s", rep.get("callback_lateness_minutes"))
        logger.info("Webhook stats: %s", rep.get("webhooks"))

        admin_map = {(l["campaign_id"], l["lead_id"]): l for l in leads}
        discrepancies = []
        with open(output_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        logger.info("Total rows in final_state.csv: %d (expected 200)", len(rows))
        for row in rows:
            key = (row["campaign_id"], row["lead_id"])
            expected = admin_map.get(key)
            if not expected:
                discrepancies.append(f"Missing lead {key}")
                continue
            if row["final_state"] != expected["expected_state"]:
                discrepancies.append(
                    f"Lead {key}: state {row['final_state']} != expected {expected['expected_state']}"
                )
            if int(row["attempts"]) != int(expected["attempts"]):
                discrepancies.append(
                    f"Lead {key}: attempts {row['attempts']} != expected {expected['attempts']}"
                )

        if discrepancies:
            logger.error("FAILED: Found %d discrepancies!", len(discrepancies))
            for d in discrepancies[:10]:
                logger.error("  %s", d)
            sys.exit(1)
        else:
            logger.info("SUCCESS: 0 discrepancies across all 200 leads!")

        if rep.get("violation_count", 0) > 0:
            logger.error("FAILED: Violation count is %d!", rep["violation_count"])
            for v in rep.get("violations", []):
                logger.error("  Violation: %s", v)
            sys.exit(1)
        else:
            logger.info("SUCCESS: 0 violations recorded by provider!")

    finally:
        logger.info("Stopping mock telephony...")
        mock_proc.terminate()
        try:
            mock_proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            mock_proc.kill()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed", type=float, default=1200.0)
    parser.add_argument("--failure-scale", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--crash", action="store_true")
    parser.add_argument("--crash-after", type=float, default=6.0)
    args = parser.parse_args()

    run_full_simulation(
        speed=args.speed,
        failure_scale=args.failure_scale,
        seed=args.seed,
        simulate_crash=args.crash,
        crash_after_sec=args.crash_after,
    )
