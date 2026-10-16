#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["reportlab>=4"]
# ///
"""Synthetic test PDF for docling benches: N pages of heading + paragraphs, a table every 2nd page, a drawn chart every 3rd.
No real data. Usage: bench/make-synthetic-pdf.py OUT.pdf [PAGES=200]"""
import random, sys
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.shapes import Drawing
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Table, TableStyle

out, pages = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 200
rnd = random.Random(42)
WORDS = ("orbit lattice vector shader voxel mesh texture camera isometric palette sprite tile render buffer "
         "frame kernel cache queue thread model layer token batch schedule").split()
ss = getSampleStyleSheet()


def sentence():
    return " ".join(rnd.choice(WORDS) for _ in range(rnd.randint(8, 16))).capitalize() + "."


story = []
for p in range(1, pages + 1):
    story.append(Paragraph(f"Section {p}: {rnd.choice(WORDS).title()} {rnd.choice(WORDS)}", ss["Heading2"]))
    for _ in range(3):
        story.append(Paragraph(" ".join(sentence() for _ in range(5)), ss["BodyText"]))
    if p % 2 == 0:
        rows = [["Item", "Count", "Ratio", "Label"]] + [
            [f"I-{p}-{i}", str(rnd.randint(1, 999)), f"{rnd.random():.3f}", rnd.choice(WORDS)] for i in range(5)]
        t = Table(rows)
        t.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                               ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey)]))
        story.append(t)
    if p % 3 == 0:
        d = Drawing(400, 180)
        c = VerticalBarChart()
        c.x, c.y, c.width, c.height = 40, 20, 340, 140
        c.data = [[rnd.randint(1, 50) for _ in range(6)]]
        c.categoryAxis.categoryNames = [w[:5] for w in rnd.sample(WORDS, 6)]
        d.add(c)
        story.append(d)
    story.append(PageBreak())
SimpleDocTemplate(out, pagesize=A4).build(story)
print(f"{out}: {pages} pages")
