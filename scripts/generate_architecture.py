#!/usr/bin/env python
"""Generate docs/images/architecture.png — the project's visual architecture diagram.

Reproducible asset: edit and re-run `python scripts/generate_architecture.py`.
Drawn with Pillow so there are no extra diagram-tool dependencies.
"""
from __future__ import annotations

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

W, H = 1600, 1330
BG = "#FFFFFF"
INK = "#1F2328"
MUTED = "#57606A"

FONT_DIR = Path("C:/Windows/Fonts")


def font(name: str, size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(str(FONT_DIR / name), size)
    except OSError:  # non-Windows fallback
        return ImageFont.load_default(size)


F_TITLE = font("segoeuib.ttf", 34)
F_BOXTITLE = font("segoeuib.ttf", 25)
F_BODY = font("segoeuil.ttf", 19)
F_BADGE = font("segoeuib.ttf", 15)


def box(d: ImageDraw.ImageDraw, x0, y0, x1, y1, border, fill, title, lines, badge=None):
    d.rounded_rectangle([x0, y0, x1, y1], radius=16, fill=fill, outline=border, width=3)
    cx = (x0 + x1) / 2
    d.text((cx, y0 + 26), title, font=F_BOXTITLE, fill=border, anchor="mm")
    y = y0 + 62
    for line in lines:
        d.text((cx, y), line, font=F_BODY, fill=INK, anchor="mm")
        y += 28
    if badge:
        bw = d.textlength(badge, font=F_BADGE) + 28
        bx0 = x1 - bw - 14
        by1 = y1 - 14
        d.rounded_rectangle([bx0, by1 - 30, x1 - 14, by1], radius=14, fill=border)
        d.text((bx0 + bw / 2, by1 - 15), badge, font=F_BADGE, fill="#FFFFFF", anchor="mm")


def arrow(d: ImageDraw.ImageDraw, x0, y0, x1, y1, color="#8B949E"):
    d.line([x0, y0, x1, y1], fill=color, width=4)
    # arrowhead
    s = 11
    if abs(y1 - y0) >= abs(x1 - x0):  # vertical-ish
        sign = 1 if y1 > y0 else -1
        d.polygon([(x1, y1), (x1 - s, y1 - sign * s * 1.6), (x1 + s, y1 - sign * s * 1.6)], fill=color)
    else:
        sign = 1 if x1 > x0 else -1
        d.polygon([(x1, y1), (x1 - sign * s * 1.6, y1 - s), (x1 - sign * s * 1.6, y1 + s)], fill=color)


def main() -> None:
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)

    d.text((W / 2, 48), "Data Processing Engine — architecture", font=F_TITLE, fill=INK, anchor="mm")

    BLUE, GREEN, PURPLE, AMBER, ORANGE, SLATE = ("#2F6FEB", "#EAF2FE"), ("#2EA043", "#E9F9EE"), \
        ("#8957E5", "#F3EDFD"), ("#BF8700", "#FFF6E5"), ("#E8590C", "#FFF0EA"), ("#57606A", "#F0F3F6")

    # 1. ingestion
    box(d, 440, 105, 1160, 245, **{
        "border": BLUE[0], "fill": BLUE[1], "title": "INGESTION",
        "lines": ["EventGenerator (paced bursts)  ·  Kafka producer", "JSON events  →  asyncio queue (drop-oldest backpressure)"]})

    # 2. stream processing
    box(d, 380, 325, 1220, 545, **{
        "border": GREEN[0], "fill": GREEN[1], "title": "STREAM PROCESSING",
        "lines": ["200 ms micro-batches  ·  validation  ·  enrichment",
                  "parallel transform — process pool under load",
                  "in-memory hot layer: last 60 min of window aggregates"],
        "badge": "p50 ≈ 6 ms  ·  p95 < 1 s"})

    # 3a insights / 3b storage
    box(d, 120, 630, 760, 830, **{
        "border": PURPLE[0], "fill": PURPLE[1], "title": "INSIGHTS ENGINE",
        "lines": ["rolling z-score anomalies  ·  trend detection",
                  "revenue-share movers  ·  rate thresholds",
                  "per-entity cooldowns → signal-dense feed"],
        "badge": "auto-generated"})
    box(d, 840, 630, 1480, 830, **{
        "border": AMBER[0], "fill": AMBER[1], "title": "STORAGE (incremental)",
        "lines": ["PostgreSQL primary  ·  SQLite fallback",
                  "events  ·  1-min aggregates  ·  hourly summary",
                  "insights  ·  composite indexes + BRIN + materialized view"],
        "badge": "upserts, batched"})

    # 4. router
    box(d, 380, 915, 1220, 1105, **{
        "border": ORANGE[0], "fill": ORANGE[1], "title": "INTELLIGENT QUERY ROUTER",
        "lines": ["cache  →  hot layer  →  pre-agg summary  →  indexed  →  raw scan",
                  "cheapest accurate backend per query  ·  decisions recorded"],
        "badge": "87% latency reduction"})

    # 5. api
    box(d, 440, 1185, 1160, 1310, **{
        "border": SLATE[0], "fill": SLATE[1], "title": "FASTAPI  +  LIVE DASHBOARD",
        "lines": ["/stats  /insights  /route  /docs      real-time HTML dashboard"]})

    # arrows
    arrow(d, 800, 245, 800, 320)          # ingestion -> stream
    arrow(d, 590, 545, 440, 625)          # stream -> insights
    arrow(d, 1010, 545, 1160, 625)        # stream -> storage
    arrow(d, 440, 830, 590, 910)          # insights -> router
    arrow(d, 1160, 830, 1010, 910)        # storage -> router
    arrow(d, 800, 1105, 800, 1180)        # router -> api

    out = Path(__file__).resolve().parents[1] / "docs" / "images" / "architecture.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    img.save(out)
    print(f"wrote {out} ({out.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
