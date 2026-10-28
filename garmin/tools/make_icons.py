"""Erzeugt die Symbol-Schrift icons.fnt/.png (BMFont-Textformat) aus den SVG-Symbolen in tools/icons/.

Aufruf: python3 garmin/tools/make_icons.py [Größe in Pixel] [edge=N]
Ergebnis: garmin/resources/fonts/icons.fnt und icons.png. Zeichen: r = Laufen, b = Rad, s = Schwimmen. Wie bei den Schriften
steht die Deckkraft in allen vier Kanälen, die Farbe setzt die Uhr beim Zeichnen (setColor).
Die Symbole sind Tabler Icons (MIT-Lizenz, tabler.io/icons), Stil "outline": run, bike, swimming. Braucht cairosvg und Pillow.
Größe 40 passt zu 416 Pixel (Forerunner 265); Bitmap-Schriften skalieren nicht mit.
"""
import io
import sys
from pathlib import Path

import cairosvg
from PIL import Image

args = sys.argv[1:]
size = int(args[0]) if args and args[0].isdigit() else 40
opt = dict(a.split("=", 1) for a in args if "=" in a)
edge = int(opt.get("edge", 40))
here = Path(__file__).resolve().parent
out = here.parent / "resources" / "fonts"
glyphs = [("r", "run"), ("b", "bike"), ("s", "swim")]

cov = Image.new("L", (256, 64), 0)
placed = []
x = 0
for ch, name in glyphs:
    svg = (here / "icons" / f"{name}.svg").read_text().replace("currentColor", "#fff")
    png = cairosvg.svg2png(bytestring=svg.encode(), output_width=size, output_height=size)
    img = Image.open(io.BytesIO(png)).convert("RGBA").getchannel("A")
    cov.paste(img, (x, 0))
    placed.append((ord(ch), x))
    x += size + 1
if edge:
    lo, hi = 255 * edge / 400, 255 - 255 * edge / 400
    cov = cov.point(lambda v: 0 if v <= lo else (255 if v >= hi else round((v - lo) * 255 / (hi - lo))))
cov = cov.crop((0, 0, 256, 64))
Image.merge("RGBA", (cov, cov, cov, cov)).save(out / "icons.png")

lines = [
    f'info face="Tabler Icons" size={size} bold=0 italic=0 charset="" unicode=1 stretchH=100 smooth=1 aa=1 padding=0,0,0,0 spacing=1,1',
    f"common lineHeight={size} base={size} scaleW=256 scaleH=64 pages=1 packed=0",
    'page id=0 file="icons.png"',
    f"chars count={len(placed)}",
]
for c, px in placed:
    lines.append(f"char id={c} x={px} y=0 width={size} height={size} xoffset=0 yoffset=0 xadvance={size} page=0 chnl=15")
(out / "icons.fnt").write_text("\n".join(lines) + "\n")
print(f"{len(placed)} Symbole, {size} Pixel")
