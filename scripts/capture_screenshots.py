"""Capture the README screenshots from the RUNNING services (nothing is mocked).

Each target needs its own stack up (and its generated secrets, see the docs); name the targets you
want, or give none to capture every one whose service answers:

    uv run --with playwright python scripts/capture_screenshots.py kafka minio grafana

Targets: kafka, minio, airflow, dbt, grafana, metabase, ci. Drives a real browser (Edge or Chrome
that is already installed) and writes PNG files to docs/images/. Playwright is only needed for this
script, so it is not a project dependency. Logins are read from the git-ignored env files and never
printed. Everything shown is synthetic demo data from the local stack.
"""

from __future__ import annotations

import contextlib
import sys
import time
from collections.abc import Callable
from pathlib import Path

from playwright.sync_api import Browser, Page, sync_playwright
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeout

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "images"
GRAFANA = "http://127.0.0.1:3000"
METABASE = "http://127.0.0.1:3001"
KAFKA_UI = "http://127.0.0.1:8080"
GRAFANA_DASHBOARD = "/d/ecomm-pipeline-health"
WIDE = {"width": 1600, "height": 1000}
# A page load took 33 s on the 5 GB development machine while Kafka was busy, so be generous.
LOAD_MS = 180_000


def read_env(path: Path) -> dict[str, str]:
    if not path.is_file():
        sys.exit(f"missing {path}: generate the secrets first (see the docs)")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def step(message: str) -> None:
    """Progress line (flushed): a capture that stalls then says where."""
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def new_page(browser: Browser, viewport: dict[str, int] | None = None) -> Page:
    page = browser.new_context(viewport=viewport or WIDE, device_scale_factor=1).new_page()
    page.set_default_navigation_timeout(LOAD_MS)
    page.set_default_timeout(LOAD_MS)
    return page


# --- Grafana and Metabase ------------------------------------------------------------------


def grafana(browser: Browser) -> list[Path]:
    password = read_env(ROOT / "docker" / "monitoring" / ".env")["GF_SECURITY_ADMIN_PASSWORD"]
    page = new_page(browser)
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
    return [target]


def metabase(browser: Browser) -> list[Path]:
    bi = read_env(ROOT / "docker" / "bi" / ".env")
    page = new_page(browser, {"width": 1400, "height": 2300})
    page.goto(f"{METABASE}/auth/login")
    page.get_by_label("Email address").fill(bi["METABASE_ADMIN_EMAIL"])
    page.get_by_label("Password").fill(bi["METABASE_ADMIN_PASSWORD"])
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
    return [target]


# --- Kafka UI ------------------------------------------------------------------------------


def kafka(browser: Browser) -> list[Path]:
    page = new_page(browser)
    step("kafka: opening the topics page")
    page.goto(f"{KAFKA_UI}/ui/clusters/local/all-topics?perPage=25", wait_until="domcontentloaded")
    page.wait_for_selector("table tbody tr")
    step("kafka: topics table is there")
    with contextlib.suppress(PlaywrightTimeout):  # hide Kafka's internal topics: they are noise
        page.get_by_text("Show Internal Topics").click(timeout=5_000)
    page.wait_for_timeout(2_500)
    topics = OUT / "kafka-ui-topics.png"
    page.screenshot(path=topics, clip={"x": 0, "y": 0, "width": 1600, "height": 620})

    step("kafka: opening the messages page")
    page.goto(
        f"{KAFKA_UI}/ui/clusters/local/all-topics/commerce.orders/messages",
        wait_until="domcontentloaded",
    )
    # A loading placeholder also matches "table tbody tr", so wait for what only a real message has:
    # the event envelope in the value preview. If it never shows, fail: never capture a spinner.
    try:
        page.get_by_text('"event_id"').first.wait_for(state="visible")
    except PlaywrightTimeout:
        sys.exit("kafka: the messages never appeared (is the broker healthy?); not capturing")
    step("kafka: messages are there")
    page.wait_for_timeout(3_000)  # let the list finish streaming
    with contextlib.suppress(PlaywrightTimeout):  # expand one message to show its JSON envelope
        page.locator("table tbody tr").first.locator("td").first.click(timeout=5_000)
    page.wait_for_timeout(1_500)
    messages = OUT / "kafka-ui-messages.png"
    page.screenshot(path=messages, clip={"x": 0, "y": 0, "width": 1600, "height": 980})
    return [topics, messages]


TARGETS: dict[str, Callable[[Browser], list[Path]]] = {
    "kafka": kafka,
    "grafana": grafana,
    "metabase": metabase,
}


def launch(p: object) -> Browser:
    for channel in ("msedge", "chrome"):
        try:
            return p.chromium.launch(channel=channel)  # type: ignore[attr-defined, no-any-return]
        except PlaywrightError as exc:  # this browser is not installed: try the next one
            print(f"{channel}: not usable ({str(exc).splitlines()[0]})")
    sys.exit("no Edge or Chrome found; install one or run `playwright install chromium`")


def main(argv: list[str]) -> int:
    unknown = [name for name in argv if name not in TARGETS]
    if unknown:
        sys.exit(f"unknown target(s) {unknown}; choose from {sorted(TARGETS)}")
    names = argv or list(TARGETS)
    OUT.mkdir(parents=True, exist_ok=True)
    step("starting the browser")
    with sync_playwright() as p:
        browser = launch(p)
        step("browser started")
        try:
            for name in names:
                for path in TARGETS[name](browser):
                    print(f"{name}: wrote {path}")
        finally:
            browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
