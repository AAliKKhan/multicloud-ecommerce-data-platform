"""Capture the README screenshots from the RUNNING Grafana and Metabase (nothing is mocked).

Needs the monitoring and bi stacks up and their generated secrets (see docs/monitoring.md and
docs/bi-dashboard.md). Drives a real browser (Edge or Chrome that is already installed) and writes
PNG files to docs/images/. Playwright is only needed for this script, so it is not a project
dependency; run it as:

    uv run --with playwright python scripts/capture_screenshots.py

Logins are read from the git-ignored env files and never printed. Everything shown is synthetic
demo data from the local stack.
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeout

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "images"
GRAFANA = "http://127.0.0.1:3000"
METABASE = "http://127.0.0.1:3001"
GRAFANA_DASHBOARD = "/d/ecomm-pipeline-health"


def read_env(path: Path) -> dict[str, str]:
    if not path.is_file():
        sys.exit(f"missing {path}: generate the secrets first (see the docs)")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def grafana(page: Page, password: str) -> Path:
    page.goto(f"{GRAFANA}/login")
    page.locator("input[name='user']").fill("admin")
    page.locator("input[name='password']").fill(password)
    page.locator("button[type='submit']").click()
    page.wait_for_url(lambda url: "/login" not in url, timeout=30_000)
    page.goto(f"{GRAFANA}{GRAFANA_DASHBOARD}?orgId=1&kiosk", wait_until="domcontentloaded")
    # Grafana keeps polling, so "network idle" never happens. Panels query Prometheus after the
    # page loads: give them time to draw (the result is checked by eye).
    page.wait_for_timeout(15_000)
    target = OUT / "grafana-pipeline-health.png"
    # Crop the empty space below the last row of panels (height found by looking at the page).
    page.screenshot(
        path=target, full_page=True, clip={"x": 0, "y": 0, "width": 1600, "height": 1260}
    )
    return target


def metabase(page: Page, email: str, password: str) -> Path:
    page.goto(f"{METABASE}/auth/login")
    page.get_by_label("Email address").fill(email)
    page.get_by_label("Password").fill(password)
    page.get_by_role("button", name="Sign in").click()
    page.wait_for_url(lambda url: "/auth/login" not in url, timeout=30_000)
    # Dashboard id 2 is what `ecomm-bi provision` prints on a fresh instance.
    page.goto(f"{METABASE}/dashboard/2")
    page.wait_for_selector("[data-testid='dashboard-grid']", timeout=60_000)
    with contextlib.suppress(PlaywrightTimeout):  # nothing left loading, or settle time below
        page.wait_for_selector(
            "[data-testid='loading-indicator']", state="detached", timeout=30_000
        )
    page.wait_for_timeout(10_000)  # charts draw after the questions return
    target = OUT / "metabase-business-overview.png"
    page.screenshot(
        path=target, full_page=True, clip={"x": 0, "y": 0, "width": 1400, "height": 1920}
    )
    return target


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    grafana_password = read_env(ROOT / "docker" / "monitoring" / ".env")[
        "GF_SECURITY_ADMIN_PASSWORD"
    ]
    bi = read_env(ROOT / "docker" / "bi" / ".env")
    with sync_playwright() as p:
        browser = None
        for channel in ("msedge", "chrome"):
            try:
                browser = p.chromium.launch(channel=channel)
                break
            except PlaywrightError as exc:  # this browser is not installed: try the next one
                print(f"{channel}: not usable ({str(exc).splitlines()[0]})")
        if browser is None:
            sys.exit("no Edge or Chrome found; install one or run `playwright install chromium`")
        try:
            wide = browser.new_context(
                viewport={"width": 1600, "height": 1000}, device_scale_factor=1
            )
            print("wrote", grafana(wide.new_page(), grafana_password))
            tall = browser.new_context(
                viewport={"width": 1400, "height": 2300}, device_scale_factor=1
            )
            print(
                "wrote",
                metabase(
                    tall.new_page(), bi["METABASE_ADMIN_EMAIL"], bi["METABASE_ADMIN_PASSWORD"]
                ),
            )
        finally:
            browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
