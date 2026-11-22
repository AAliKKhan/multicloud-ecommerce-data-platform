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
import urllib.error
import urllib.request
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
MINIO_CONSOLE = "http://127.0.0.1:9001"
AIRFLOW_UI = "http://127.0.0.1:8081"
DBT_DOCS = "http://127.0.0.1:8082"
GRAFANA_DASHBOARD = "/d/ecomm-pipeline-health"
ROOT_ENV = ROOT / "docker" / "minio" / ".env.root"
MINIO_BUCKET = "ecomm-raw-minio"
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


def wait_for_service(url: str, name: str, *, timeout_s: int = 180) -> None:
    """Wait for a live service before navigating; fail with its URL instead of a vague timeout."""
    deadline = time.monotonic() + timeout_s
    last: str = "no response"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 - loopback
                if response.status < 500:
                    step(f"{name}: ready ({response.status})")
                    return
                last = f"HTTP {response.status}"
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last = str(exc)[:160]
        time.sleep(3)
    sys.exit(f"{name} is not ready at {url} after {timeout_s}s ({last})")


def new_page(browser: Browser, viewport: dict[str, int] | None = None) -> Page:
    page = browser.new_context(viewport=viewport or WIDE, device_scale_factor=1).new_page()
    page.set_default_navigation_timeout(LOAD_MS)
    page.set_default_timeout(LOAD_MS)
    return page


# --- Grafana and Metabase ------------------------------------------------------------------


def grafana(browser: Browser) -> list[Path]:
    wait_for_service(f"{GRAFANA}/api/health", "Grafana")
    password = read_env(ROOT / "docker" / "monitoring" / ".env")["GF_SECURITY_ADMIN_PASSWORD"]
    page = new_page(browser, {"width": 2000, "height": 1200})
    page.goto(f"{GRAFANA}/login")
    page.locator("input[name='user']").fill("admin")
    page.locator("input[name='password']").fill(password)
    page.locator("button[type='submit']").click()
    page.wait_for_url(lambda url: "/login" not in url, timeout=30_000)
    page.goto(f"{GRAFANA}{GRAFANA_DASHBOARD}?orgId=1&kiosk", wait_until="domcontentloaded")
    # Grafana keeps polling, so "network idle" never happens. Wait for a real panel title, not
    # just the time picker, or the capture is an empty canvas.
    try:
        page.get_by_text("Pipeline").first.wait_for(state="visible", timeout=60_000)
    except PlaywrightTimeout:
        sys.exit("grafana: the dashboard panels never appeared; not capturing an empty canvas")
    page.get_by_text("Seconds since last successful load").first.wait_for(state="visible")
    page.wait_for_timeout(4_000)
    target = OUT / "grafana-pipeline-health.png"
    page.screenshot(path=target, clip={"x": 0, "y": 0, "width": 2000, "height": 1180})
    return [target]


def metabase(browser: Browser) -> list[Path]:
    wait_for_service(f"{METABASE}/api/health", "Metabase")
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
    # The chrome appears before the questions return. Wait for the published total
    # ($50,186.26 net sales) so a skeleton dashboard is not saved as proof.
    # Cards below the first row stay skeletons until they have been on screen.
    for _ in range(6):
        page.mouse.wheel(0, 500)
        page.wait_for_timeout(1_000)
    page.evaluate("window.scrollTo(0, 0)")
    try:
        page.wait_for_function(
            """() => {
                const text = document.body.innerText;
                return text.includes('50,186.26')
                    && text.includes('Urban Charger')
                    && text.includes('out_of_stock');
            }""",
            timeout=120_000,
        )
    except PlaywrightTimeout:
        debug = OUT / "_metabase-debug.png"
        page.screenshot(path=debug, full_page=True)
        sys.exit(f"metabase: the dashboard numbers never appeared; debug shot at {debug}")
    with contextlib.suppress(PlaywrightTimeout):
        page.get_by_label("Close").click(timeout=2_000)
    page.wait_for_timeout(2_000)
    target = OUT / "metabase-business-overview.png"
    page.screenshot(path=target, full_page=True)
    return [target]


# --- MinIO console ------------------------------------------------------------------------


def dismiss_minio_notice(page: Page) -> None:
    """The console covers the object list with a license dialog."""
    clicked = page.evaluate(
        """() => {
            const nodes = [...document.querySelectorAll('button, [role=button], a')];
            const hit = nodes.find(n => /acknowledge|i understand/i.test(n.innerText || ''));
            if (!hit) return false;
            hit.click();
            return true;
        }"""
    )
    if clicked:
        page.wait_for_timeout(1_500)
        step("minio: dismissed the license dialog")


def minio(browser: Browser) -> list[Path]:
    wait_for_service(f"{MINIO_CONSOLE}/minio/health/live", "MinIO")
    root = read_env(ROOT_ENV)
    page = new_page(browser)
    step("minio: opening the console")
    page.goto(f"{MINIO_CONSOLE}/login", wait_until="domcontentloaded")
    page.locator("input[name='accessKey']").fill(root["MINIO_ROOT_USER"])
    page.locator("input[name='secretKey']").fill(root["MINIO_ROOT_PASSWORD"])
    login = page.locator("button#login")
    if login.count():
        login.click()
    else:
        page.get_by_role("button", name="Login").click()
    page.wait_for_url(lambda url: "/login" not in url, timeout=LOAD_MS)
    dismiss_minio_notice(page)
    page.goto(f"{MINIO_CONSOLE}/browser/{MINIO_BUCKET}", wait_until="domcontentloaded")
    dismiss_minio_notice(page)
    if page.get_by_role("heading", name="License").count():
        dismiss_minio_notice(page)
    if page.get_by_role("heading", name="License").is_visible():
        sys.exit("minio: the license dialog is still covering the bucket; not capturing")
    try:
        page.get_by_text("manifests", exact=True).first.wait_for(state="visible")
        page.get_by_text("raw", exact=True).first.wait_for(state="visible")
    except PlaywrightTimeout:
        sys.exit("minio: the bucket folders never appeared; not capturing a login or overlay")
    step("minio: bucket folders are there")
    dismiss_minio_notice(page)
    page.wait_for_timeout(1_000)
    if page.get_by_text("What is the GNU AGPL").count():
        dismiss_minio_notice(page)
        page.wait_for_timeout(1_000)
    if page.get_by_text("What is the GNU AGPL").count():
        sys.exit("minio: the license dialog is still covering the bucket; not capturing")
    target = OUT / "minio-raw-bucket.png"
    page.screenshot(path=target, clip={"x": 0, "y": 0, "width": 1600, "height": 900})
    return [target]


def airflow(browser: Browser) -> list[Path]:
    wait_for_service(f"{AIRFLOW_UI}/health", "Airflow")
    env = read_env(ROOT / "docker" / "airflow" / ".env")
    page = new_page(browser)
    step("airflow: opening the real Airflow UI")
    page.goto(f"{AIRFLOW_UI}/login/", wait_until="domcontentloaded")
    # The login form is a Flask-AppBuilder submit input, not a labeled button.
    # The generated admin user is "admin" (see docker/airflow/init.sh).
    page.locator("#username").fill("admin", timeout=30_000)
    page.locator("#password").fill(env["AIRFLOW_ADMIN_PASSWORD"])
    page.locator("input[type='submit']").click()
    page.wait_for_url(lambda url: "/login" not in url, timeout=60_000)
    page.goto(
        f"{AIRFLOW_UI}/dags/raw_to_warehouse_daily/grid",
        wait_until="domcontentloaded",
    )
    try:
        page.get_by_text("raw_to_warehouse_daily").first.wait_for(state="visible", timeout=60_000)
    except PlaywrightTimeout:
        debug = OUT / "_airflow-debug.png"
        page.screenshot(path=debug, full_page=True)
        sys.exit(f"airflow: the DAG page never appeared; debug shot at {debug}")
    page.wait_for_timeout(2_000)
    target = OUT / "airflow-dag-runs.png"
    page.screenshot(path=target, clip={"x": 0, "y": 0, "width": 1600, "height": 900})
    return [target]


def dbt(browser: Browser) -> list[Path]:
    wait_for_service(DBT_DOCS, "dbt docs server")
    page = new_page(browser)
    page.goto(f"{DBT_DOCS}/#!/model/model.ecomm.fct_orders", wait_until="domcontentloaded")
    try:
        page.get_by_text("fct_orders", exact=True).first.wait_for(state="visible")
    except PlaywrightTimeout:
        sys.exit("dbt: the docs page never rendered; not capturing a blank server")
    lineage = page.get_by_title("View lineage graph")
    if not lineage.count():
        lineage = page.get_by_role("button", name="Lineage")
    if lineage.count():
        lineage.first.click()
        page.wait_for_timeout(2_000)
    step("dbt: lineage page is open")
    target = OUT / "dbt-lineage.png"
    page.screenshot(path=target, clip={"x": 0, "y": 0, "width": 1600, "height": 1000})
    return [target]


def ci(browser: Browser) -> list[Path]:
    page = new_page(browser)
    page.goto(
        "https://github.com/AAliKKhan/multicloud-ecommerce-data-platform/actions",
        wait_until="domcontentloaded",
    )
    try:
        page.get_by_text("All workflows").first.wait_for(state="visible")
    except PlaywrightTimeout:
        sys.exit("ci: the Actions page never rendered; not capturing a blank or login wall")
    page.wait_for_timeout(3_000)
    target = OUT / "github-actions-ci.png"
    page.screenshot(path=target, clip={"x": 0, "y": 0, "width": 1600, "height": 1000})
    return [target]


# --- Kafka UI ------------------------------------------------------------------------------


def kafka(browser: Browser) -> list[Path]:
    wait_for_service(f"{KAFKA_UI}/actuator/health", "Kafka UI")
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
    "minio": minio,
    "airflow": airflow,
    "dbt": dbt,
    "grafana": grafana,
    "metabase": metabase,
    "ci": ci,
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
