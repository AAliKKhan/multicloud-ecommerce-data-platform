"""Record short segments of the live local UIs.

Each segment is its own file under docs/demo/, so a later Docker failure does
not delete an earlier recording. Frames come only from pages that answer; this
script does not draw title cards or reuse screenshots as video.

    uv run --with playwright python scripts/record_demo.py metabase

Playwright is a one-off dependency, same as scripts/capture_screenshots.py.
The clip is a screencast started only after the page has rendered, then encoded
with ffmpeg (PATH, imageio-ffmpeg, or Playwright's own copy). Playwright's
built-in video mode is not used: on this machine it makes Metabase drop a script chunk.
Logins are read from the git-ignored env files and are never printed.
"""

from __future__ import annotations

import base64
import subprocess
import sys
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
from capture_screenshots import (  # noqa: E402
    AIRFLOW_UI,
    DBT_DOCS,
    GRAFANA,
    GRAFANA_DASHBOARD,
    KAFKA_UI,
    METABASE,
    MINIO_BUCKET,
    MINIO_CONSOLE,
    ROOT,
    ROOT_ENV,
    dismiss_minio_notice,
    launch,
    read_env,
    step,
)

DEMO = ROOT / "docs" / "demo"
VIEW = {"width": 1400, "height": 900}
_FILM: dict[str, object] = {}


def start_film(page: Page) -> None:
    """Start a live screencast. Call this only after the page has rendered."""
    if _FILM.get("cdp") is not None:
        return
    frames = _FILM.get("frames")
    if not isinstance(frames, list):
        frames = []
    cdp = page.context.new_cdp_session(page)

    def on_frame(params: dict) -> None:
        now = time.monotonic()
        if not frames or now - frames[-1][0] >= 0.2:
            frames.append((now, base64.b64decode(params["data"])))
        try:
            cdp.send("Page.screencastFrameAck", {"sessionId": params["sessionId"]})
        except Exception:
            return

    cdp.on("Page.screencastFrame", on_frame)
    cdp.send(
        "Page.startScreencast",
        {
            "format": "jpeg",
            "quality": 65,
            "maxWidth": VIEW["width"],
            "maxHeight": VIEW["height"],
            "everyNthFrame": 1,
        },
    )
    _FILM["cdp"] = cdp
    _FILM["frames"] = frames
    step("filming the live page")


def pause_film(page: Page) -> None:
    """Stop the screencast but keep frames already captured."""
    cdp = _FILM.pop("cdp", None)
    if cdp is None:
        return
    try:
        cdp.send("Page.stopScreencast")
        page.wait_for_timeout(300)
    except Exception:
        return


def stop_film(page: Page) -> list[tuple[float, bytes]]:
    cdp = _FILM.get("cdp")
    frames = _FILM.get("frames")
    if cdp is not None:
        cdp.send("Page.stopScreencast")
        page.wait_for_timeout(400)
    _FILM.clear()
    return frames if isinstance(frames, list) else []


def drift(page: Page, pixels: int, label: str) -> None:
    step(label)
    moved = 0
    while moved < pixels:
        page.mouse.wheel(0, 120)
        page.wait_for_timeout(300)
        moved += 120


def ffmpeg_exe() -> str:
    """Prefer a local binary. Do not download one during a recording."""
    import shutil

    found = shutil.which("ffmpeg")
    if found:
        return found
    try:
        import imageio_ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except ImportError:
        pass
    root = Path.home() / "AppData" / "Local" / "ms-playwright"
    copies = sorted(root.glob("ffmpeg-*/ffmpeg-win64.exe"))
    if copies:
        return str(copies[-1])
    sys.exit("ffmpeg was not found; not writing a clip from unsaved frames")


def encode(frames: list[tuple[float, bytes]], target: Path) -> None:
    """Hold each captured frame for the time it was on screen. Do not draw new frames."""
    if len(frames) < 8:
        sys.exit(f"only {len(frames)} live frames; not writing a clip")
    ffmpeg = ffmpeg_exe()
    fps = 4
    pieces: list[bytes] = []
    for index, (stamp, blob) in enumerate(frames):
        gap = frames[index + 1][0] - stamp if index + 1 < len(frames) else 0.5
        gap = min(max(gap, 0.2), 20.0)
        pieces.append(blob * max(1, round(gap * fps)))
    payload = b"".join(pieces)
    step(f"encoding {len(payload) // max(len(frames[0][1]), 1)} held frames to {target.name}")
    subprocess.run(  # noqa: S603 - argv is this script's own local ffmpeg path
        [
            ffmpeg,
            "-y",
            "-f",
            "image2pipe",
            "-framerate",
            str(fps),
            "-c:v",
            "mjpeg",
            "-i",
            "pipe:0",
            "-c:v",
            "libvpx-vp9",
            "-b:v",
            "1M",
            "-deadline",
            "realtime",
            "-cpu-used",
            "8",
            "-an",
            str(target),
        ],
        input=payload,
        check=True,
    )


def grab(page: Page) -> None:
    """Capture the live page when the screencast stays quiet. Do not draw a new image."""
    cdp = _FILM.get("cdp")
    frames = _FILM.get("frames")
    if cdp is None or not isinstance(frames, list):
        return
    now = time.monotonic()
    if frames and now - frames[-1][0] < 0.4:
        return
    try:
        shot = cdp.send("Page.captureScreenshot", {"format": "jpeg", "quality": 65})
    except Exception:
        return
    frames.append((now, base64.b64decode(shot["data"])))


def pause(page: Page, ms: int, label: str) -> None:
    step(label)
    elapsed = 0
    while elapsed < ms:
        page.wait_for_timeout(500)
        elapsed += 500
        grab(page)


def sign_in_metabase(page: Page) -> None:
    """Use the browser's own session call so every cookie is stored. Never print them."""
    bi = read_env(ROOT / "docker" / "bi" / ".env")
    response = page.request.post(
        f"{METABASE}/api/session",
        data={"username": bi["METABASE_ADMIN_EMAIL"], "password": bi["METABASE_ADMIN_PASSWORD"]},
        timeout=60_000,
    )
    if not response.ok:
        sys.exit(f"metabase: session API returned {response.status}; not recording a login page")


def page_text(page: Page) -> str:
    return page.evaluate("() => (document.body && document.body.innerText) || ''")


def wait_for_kpi(page: Page, timeout_s: int = 180) -> None:
    """Dashboard metadata alone can take a minute on this machine. Scroll so cards render."""
    page.goto(f"{METABASE}/dashboard/2", wait_until="domcontentloaded", timeout=60_000)
    step(f"metabase: landed on {page.url}")
    if "/auth/login" in page.url:
        sys.exit("metabase: the session was not accepted; not recording a login page")
    deadline = time.monotonic() + timeout_s
    reloads = 0
    while time.monotonic() < deadline:
        text = page_text(page)
        if "50,186.26" in text and "Urban Charger" in text:
            step("metabase: published numbers are on screen")
            return
        if "ChunkLoadError" in text or "Unexpected Application Error" in text:
            reloads += 1
            if reloads > 2:
                sys.exit(
                    "metabase: the app chunk did not load; not keeping an error-page recording"
                )
            step(f"metabase: app chunk failed; reloading ({reloads})")
            page.reload(wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(3_000)
            continue
        page.mouse.wheel(0, 450)
        page.wait_for_timeout(4_000)
    snippet = " ".join(page_text(page).split())[:180]
    sys.exit(f"metabase: KPI number not visible at {page.url}: {snippet!r}")


def metabase(page: Page) -> None:
    sign_in_metabase(page)
    wait_for_kpi(page)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(500)
    start_film(page)
    pause(page, 12_000, "metabase: KPI row")
    drift(page, 650, "metabase: scrolling to daily trends")
    pause(page, 12_000, "metabase: daily trends")
    drift(page, 650, "metabase: scrolling to payments")
    pause(page, 12_000, "metabase: payments and freshness")
    drift(page, 700, "metabase: scrolling to products")
    pause(page, 12_000, "metabase: products and categories")
    drift(page, 700, "metabase: scrolling to stock")
    pause(page, 12_000, "metabase: low stock")
    page.evaluate("window.scrollTo({top: 0, behavior: 'smooth'})")
    pause(page, 6_000, "metabase: back to the top")


def dbt(page: Page) -> None:
    page.goto(f"{DBT_DOCS}/#!/model/model.ecomm.fct_orders", wait_until="domcontentloaded")
    page.get_by_text("fct_orders", exact=True).first.wait_for(state="visible", timeout=30_000)
    start_film(page)
    pause(page, 8_000, "dbt: fct_orders")
    lineage = page.get_by_title("View lineage graph")
    if not lineage.count():
        lineage = page.get_by_role("button", name="Lineage")
    if lineage.count():
        lineage.first.click()
        pause(page, 12_000, "dbt: lineage")


def airflow(page: Page) -> None:
    env = read_env(ROOT / "docker" / "airflow" / ".env")
    page.goto(f"{AIRFLOW_UI}/login/", wait_until="domcontentloaded")
    page.locator("#username").fill("admin")
    page.locator("#password").fill(env["AIRFLOW_ADMIN_PASSWORD"])
    page.locator("input[type='submit']").click()
    page.wait_for_url(lambda url: "/login" not in url, timeout=60_000)
    page.goto(f"{AIRFLOW_UI}/dags/raw_to_warehouse_daily/grid", wait_until="domcontentloaded")
    page.get_by_text("Total success").first.wait_for(state="visible", timeout=60_000)
    start_film(page)
    pause(page, 18_000, "airflow: successful runs")


def grafana(page: Page) -> None:
    password = read_env(ROOT / "docker" / "monitoring" / ".env")["GF_SECURITY_ADMIN_PASSWORD"]
    page.goto(f"{GRAFANA}/login", wait_until="domcontentloaded")
    page.locator("input[name='user']").fill("admin")
    page.locator("input[name='password']").fill(password)
    page.locator("button[type='submit']").click()
    page.wait_for_url(lambda url: "/login" not in url, timeout=30_000)
    page.goto(f"{GRAFANA}{GRAFANA_DASHBOARD}?orgId=1&kiosk", wait_until="domcontentloaded")
    page.get_by_text("Seconds since last successful load").first.wait_for(
        state="visible", timeout=60_000
    )
    page.get_by_text("1430").first.wait_for(state="visible", timeout=60_000)
    start_film(page)
    pause(page, 12_000, "grafana: top panels")
    page.mouse.wheel(0, 350)
    pause(page, 12_000, "grafana: lower panels")


def minio(page: Page) -> None:
    root = read_env(ROOT_ENV)
    page.goto(f"{MINIO_CONSOLE}/login", wait_until="domcontentloaded")
    page.locator("input[name='accessKey']").fill(root["MINIO_ROOT_USER"], timeout=30_000)
    page.locator("input[name='secretKey']").fill(root["MINIO_ROOT_PASSWORD"])
    login = page.locator("button#login")
    if login.count():
        login.click()
    else:
        page.get_by_role("button", name="Login").click()
    page.wait_for_url(lambda url: "/login" not in url, timeout=60_000)
    dismiss_minio_notice(page)
    page.goto(f"{MINIO_CONSOLE}/browser/{MINIO_BUCKET}", wait_until="domcontentloaded")
    dismiss_minio_notice(page)
    page.get_by_text("manifests", exact=True).first.wait_for(state="visible", timeout=60_000)
    dismiss_minio_notice(page)
    if page.get_by_text("What is the GNU AGPL").count():
        dismiss_minio_notice(page)
    notice = page.get_by_text("What is the GNU AGPL")
    if notice.count() and notice.first.is_visible():
        sys.exit("minio: the license dialog is still covering the bucket; not recording")
    start_film(page)
    pause(page, 15_000, "minio: raw bucket")


def kafka(page: Page) -> None:
    page.goto(f"{KAFKA_UI}/ui/clusters/local/all-topics?perPage=25", wait_until="domcontentloaded")
    page.wait_for_selector("table tbody tr", timeout=60_000)
    start_film(page)
    pause(page, 12_000, "kafka: topics")
    pause_film(page)
    page.goto(
        f"{KAFKA_UI}/ui/clusters/local/all-topics/commerce.orders/messages",
        wait_until="domcontentloaded",
    )
    page.get_by_text("messages consumed").first.wait_for(state="visible", timeout=60_000)
    page.get_by_text('"event_id"').first.wait_for(state="visible", timeout=30_000)
    start_film(page)
    pause(page, 12_000, "kafka: an order message")


PROBES = {
    "metabase": (f"{METABASE}/api/health", metabase),
    "dbt": (DBT_DOCS, dbt),
    "airflow": (f"{AIRFLOW_UI}/health", airflow),
    "grafana": (f"{GRAFANA}/api/health", grafana),
    "minio": (f"{MINIO_CONSOLE}/minio/health/live", minio),
    "kafka": (KAFKA_UI, kafka),
}


def record(name: str) -> Path:
    _url, action = PROBES[name]
    DEMO.mkdir(parents=True, exist_ok=True)
    _FILM.clear()
    step(f"{name}: opening")
    with sync_playwright() as p:
        browser = launch(p)
        context = browser.new_context(viewport=VIEW)
        page = context.new_page()
        page.set_default_navigation_timeout(60_000)
        page.set_default_timeout(60_000)
        try:
            action(page)
            frames = stop_film(page)
        finally:
            context.close()
            browser.close()
    if len(frames) < 8:
        sys.exit(f"{name}: only {len(frames)} live frames; not writing a clip")
    check = DEMO / f"_{name}-check.jpg"
    check.write_bytes(frames[len(frames) // 2][1])
    target = DEMO / f"{name}.webm"
    encode(frames, target)
    elapsed = frames[-1][0] - frames[0][0]
    step(f"{name}: {len(frames)} frames over {elapsed:.0f}s")
    return target


def main(argv: list[str]) -> int:
    names = argv or ["metabase"]
    unknown = [name for name in names if name not in PROBES]
    if unknown:
        sys.exit(f"unknown segment(s) {unknown}; choose from {sorted(PROBES)}")
    for name in names:
        print(f"wrote {record(name)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
