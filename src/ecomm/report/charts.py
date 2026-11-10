"""Tiny dependency-free SVG chart helpers (escaped, responsive, no JavaScript)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from html import escape

Formatter = Callable[[float], str]

WIDTH, HEIGHT = 640, 230
PAD_LEFT, PAD_BOTTOM, PAD_TOP, PAD_RIGHT = 58, 34, 12, 10


def _frame(inner: str, label: str) -> str:
    return (
        f'<svg viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-label="{escape(label)}" '
        f'xmlns="http://www.w3.org/2000/svg">{inner}</svg>'
    )


def _axis(max_value: float, fmt: Formatter) -> str:
    base = HEIGHT - PAD_BOTTOM
    return (
        f'<line x1="{PAD_LEFT}" y1="{base}" x2="{WIDTH - PAD_RIGHT}" y2="{base}" stroke="#cbd5e1"/>'
        f'<text x="{PAD_LEFT - 6}" y="{base}" text-anchor="end" font-size="10" fill="#64748b">{escape(fmt(0))}</text>'
        f'<text x="{PAD_LEFT - 6}" y="{PAD_TOP + 8}" text-anchor="end" font-size="10" fill="#64748b">'
        f"{escape(fmt(max_value))}</text>"
    )


def _x_labels(labels: Sequence[str], x_of: Callable[[int], float]) -> str:
    step = max(1, len(labels) // 8)
    parts = []
    for i, label in enumerate(labels):
        if i % step == 0:
            parts.append(
                f'<text x="{x_of(i):.1f}" y="{HEIGHT - 14}" text-anchor="middle" font-size="10" '
                f'fill="#64748b">{escape(label)}</text>'
            )
    return "".join(parts)


def bar_chart(
    labels: Sequence[str],
    values: Sequence[float],
    fmt: Formatter,
    title: str,
    color: str = "#2563eb",
) -> str:
    if not values:
        return empty_chart(title)
    top = max(values) or 1.0
    span = WIDTH - PAD_LEFT - PAD_RIGHT
    slot = span / len(values)
    plot_h = HEIGHT - PAD_BOTTOM - PAD_TOP
    bars = []
    for i, (label, value) in enumerate(zip(labels, values, strict=True)):
        h = plot_h * value / top
        x = PAD_LEFT + i * slot + slot * 0.15
        bars.append(
            f'<rect x="{x:.1f}" y="{HEIGHT - PAD_BOTTOM - h:.1f}" width="{slot * 0.7:.1f}" '
            f'height="{h:.1f}" fill="{color}" rx="2"><title>{escape(label)}: {escape(fmt(value))}</title></rect>'
        )
    centre = lambda i: PAD_LEFT + i * slot + slot / 2  # noqa: E731
    return _frame(_axis(top, fmt) + "".join(bars) + _x_labels(labels, centre), title)


def line_chart(
    labels: Sequence[str],
    values: Sequence[float],
    fmt: Formatter,
    title: str,
    color: str = "#059669",
) -> str:
    if not values:
        return empty_chart(title)
    top = max(values) or 1.0
    span = WIDTH - PAD_LEFT - PAD_RIGHT
    plot_h = HEIGHT - PAD_BOTTOM - PAD_TOP
    step = span / max(len(values) - 1, 1)

    def x_of(i: int) -> float:
        return PAD_LEFT + (i * step if len(values) > 1 else span / 2)

    def y_of(v: float) -> float:
        return HEIGHT - PAD_BOTTOM - plot_h * v / top

    points = " ".join(f"{x_of(i):.1f},{y_of(v):.1f}" for i, v in enumerate(values))
    dots = "".join(
        f'<circle cx="{x_of(i):.1f}" cy="{y_of(v):.1f}" r="3" fill="{color}">'
        f"<title>{escape(labels[i])}: {escape(fmt(v))}</title></circle>"
        for i, v in enumerate(values)
    )
    line = f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2"/>'
    return _frame(_axis(top, fmt) + line + dots + _x_labels(labels, x_of), title)


def hbar_chart(
    labels: Sequence[str],
    values: Sequence[float],
    fmt: Formatter,
    title: str,
    color: str = "#7c3aed",
) -> str:
    if not values:
        return empty_chart(title)
    top = max(values) or 1.0
    row_h = 22
    height = row_h * len(values) + 10
    label_w = 190
    bar_w = WIDTH - label_w - 90
    rows = []
    for i, (label, value) in enumerate(zip(labels, values, strict=True)):
        y = 6 + i * row_h
        w = bar_w * value / top
        rows.append(
            f'<text x="{label_w - 6}" y="{y + 13}" text-anchor="end" font-size="11" fill="#334155">'
            f"{escape(label[:30])}</text>"
            f'<rect x="{label_w}" y="{y + 2}" width="{w:.1f}" height="14" fill="{color}" rx="2">'
            f"<title>{escape(label)}: {escape(fmt(value))}</title></rect>"
            f'<text x="{label_w + w + 6:.1f}" y="{y + 13}" font-size="11" fill="#64748b">{escape(fmt(value))}</text>'
        )
    return (
        f'<svg viewBox="0 0 {WIDTH} {height}" role="img" aria-label="{escape(title)}" '
        f'xmlns="http://www.w3.org/2000/svg">{"".join(rows)}</svg>'
    )


def empty_chart(title: str) -> str:
    return _frame(
        f'<text x="{WIDTH / 2}" y="{HEIGHT / 2}" text-anchor="middle" fill="#94a3b8">No data</text>',
        title,
    )
