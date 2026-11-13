# AI Usage Disclosure

## Tools used
- Antigravity / Gemini CLI (DeepMind)
- Python Language Server / pytest

## What I used them for
- Scaffolding the initial boilerplate for FastAPI webhook server endpoints and httpx telephony client.
- Generating the skeleton for unit test cases across calling windows, retries, and lifecycle state machines.
- Structuring ASCII and Mermaid architecture diagrams in documentation.
- Rapidly running end-to-end command orchestration and verifying JSON outputs.

## What I wrote or decided myself
- **Core Architecture & Concurrency Control:** Designed the single-source-of-truth database architecture using SQLite with Write-Ahead Logging (WAL) and `BEGIN IMMEDIATE` atomic locking to prevent race conditions across concurrent tenant campaigns and phone numbers.
- **Race Condition Prevention in Dialing Reconciliation:** Identified and resolved a subtle race condition where in-flight `POST /v1/calls` requests were prematurely checked by the background reconciler, receiving 404s before completion. Introduced `real_created_at` timestamp tracking with a 10-second age threshold.
- **High-Speed Simulation Window Buffer:** Calculated the impact of real-world processing latency under 1200x simulation speed (~0.15s = 3 sim minutes) and introduced the 180-second safety buffer before window close to prevent `outside_calling_window` violations.
- **Clock Drift Correction:** Discovered and fixed a clock drift bug where updating `sim_epoch = sim_now` without resetting `real_epoch` doubled the simulation speed. Implemented epoch-based reference time calculation.
- **Global DND Cascade Design:** Devised immediate cascading of DND dispositions to all non-terminal leads across all campaigns to satisfy rule R8 and precedence ordering R9.

## Where the AI was wrong or unhelpful
1. **Premature 404 In-Flight Reconciliation:**
   - *Issue:* Initial AI-assisted code checked `calls WHERE state = 'dialing'` immediately in the background loop without verifying how long the call had been in the dialing state.
   - *Impact:* Because HTTP POST requests across network boundaries take 50–150ms real time, the background reconciler queried `GET /v1/calls?request_id=...` before the provider finished processing the POST request. The provider returned 404, causing the dispatcher to mistakenly mark the call `failed_to_place` and re-queue the lead, resulting in duplicate dials and `max_attempts_exceeded` violations.
   - *Correction:* Added `real_created_at REAL` to the schema and restricted background dialing reconciliation to calls with `min_age_seconds >= 10.0`.
2. **Clock Double-Speed Drift:**
   - *Issue:* The suggested clock update function set `self.sim_epoch = sim_now` while keeping the original process start time as `self.real_epoch`.
   - *Impact:* Because elapsed time is computed as `(time.time() - self.real_epoch) * speed`, adding that to an already-updated `sim_epoch` caused time to advance at double speed, triggering calls at 3:00 AM simulated time.
   - *Correction:* Synchronized directly with the provider's `sim_epoch`, `real_epoch`, and `speed` parameters directly from `/v1/clock`.
3. **Pytest Asyncio Decorators:**
   - *Issue:* AI initially decorated test functions with `@pytest.mark.asyncio`, but `pytest-asyncio` was not installed in the system environment.
   - *Impact:* Tests generated warnings and async test coroutines were not natively awaited.
   - *Correction:* Refactored all test functions to use standard synchronous pytest entrypoints wrapping `asyncio.run(_run())`.

## How I verified AI-generated code
1. **Automated Unit & Integration Tests:** Authored 19 comprehensive unit tests in `tests/` testing boundary conditions (weekend transitions, exact regulatory hours, attempt counters, HMAC signature tampering, and lost webhooks).
2. **Deterministic E2E Simulations:** Ran end-to-end simulations under both clean conditions (`--failure-scale 0`) and heavy chaos conditions (`--failure-scale 1.0` with 503s, 504s, dropped webhooks, and duplicate webhooks) at 1200x simulation speed.
3. **Crash Resilience Verification:** Verified recovery under SIGKILL (`kill -9`) mid-flight, confirming that resumed runs achieved **0 violations** and an exact match on all 200 leads against the provider's ground truth (`/admin/leads`).
4. **Code Inspection:** Manually reviewed all SQL transactions, SQLite pragmas, and concurrency locks line-by-line to guarantee atomic state transitions.
