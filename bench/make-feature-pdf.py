#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["reportlab>=4", "matplotlib>=3.8", "pillow>=10"]
# ///
"""Synthetic docling feature-test PDF (no real data), 3 pages:
  1. heading, paragraph, Python code block, display formula (matplotlib mathtext image), small logo (~1.5 % of the page)
  2. "scanned" page: Swedish text rendered to a bitmap only (no text layer), so it needs OCR (å ä ö)
  3. raster bar chart with known values + a table
Usage: bench/make-feature-pdf.py OUT.pdf   (prints the ground truth for the OCR text and the chart)"""
import io, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from PIL import Image, ImageDraw, ImageFont
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Image as RLImage, PageBreak, Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle

out = sys.argv[1]
ss = getSampleStyleSheet()
SV = ["Årsrapport för lagret i Västerås",
      "Under fjärde kvartalet ökade antalet sändningar med 18 procent.",
      "Kylaggregatet på våning två byttes ut och förbrukningen minskade.",
      "Åtgärdslista: kontrollera säkringar, smörj fläktar, töm avfallskärl.",
      "Nästa översyn är planerad till vecka 47."]
CHART = {"Norr": 42, "Söder": 17, "Öst": 33, "Väst": 8}


def png(fig):
    b = io.BytesIO(); fig.savefig(b, format="png", dpi=200, bbox_inches="tight"); plt.close(fig); b.seek(0); return b


# formula image
fig = plt.figure(figsize=(4, 0.8)); fig.text(0.5, 0.5, r"$I = P\,\left(1 + \frac{r}{n}\right)^{n t} - P$", ha="center", va="center", fontsize=20)
formula = png(fig)
# small logo
logo = Image.new("RGB", (200, 200), "white"); d = ImageDraw.Draw(logo)
d.ellipse((20, 20, 180, 180), fill="navy"); d.text((70, 85), "ACME", fill="white", font=ImageFont.load_default(28))
lb = io.BytesIO(); logo.save(lb, "PNG"); lb.seek(0)
# scanned Swedish page
scan = Image.new("L", (1654, 900), 255); d = ImageDraw.Draw(scan)
try:
    font = ImageFont.truetype("DejaVuSans.ttf", 40)
except OSError:
    font = ImageFont.load_default(40)
for i, line in enumerate(SV):
    d.text((60, 60 + i * 120), line, fill=0, font=font)
sb = io.BytesIO(); scan.save(sb, "PNG"); sb.seek(0)
# raster chart
fig, ax = plt.subplots(figsize=(6, 3.5)); ax.bar(list(CHART), list(CHART.values()), color="teal")
ax.set_title("Sändningar per region (tusental)"); ax.set_ylabel("Antal")
chart = png(fig)

code = '''def compound_interest(principal: float, rate: float, n: int, years: float) -> float:
    """Interest earned with n compounding periods per year."""
    return principal * (1 + rate / n) ** (n * years) - principal'''
story = [RLImage(lb, 2.5 * cm, 2.5 * cm, hAlign="RIGHT"),
         Paragraph("Feature test document", ss["Heading1"]),
         Paragraph("The interest earned with periodic compounding is given by the formula below, and the function after it computes it.", ss["BodyText"]),
         Spacer(1, 12), RLImage(formula, 9 * cm, 1.8 * cm), Spacer(1, 12),
         Preformatted(code, ss["Code"]), PageBreak(),
         RLImage(sb, 18 * cm, 9.8 * cm), PageBreak(),
         Paragraph("Regional summary", ss["Heading2"]), RLImage(chart, 14 * cm, 8.2 * cm)]
t = Table([["Region", "Sändningar"]] + [[k, str(v)] for k, v in CHART.items()])
t.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.5, colors.black)]))
story.append(t)
SimpleDocTemplate(out, pagesize=A4).build(story)
print("OCR truth:", " | ".join(SV))
print("chart truth:", CHART)
