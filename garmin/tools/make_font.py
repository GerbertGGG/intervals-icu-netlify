"""Erzeugt die kleine Bitmap-Schrift (BMFont-Textformat) für das Watchface.

Aufruf: python3 garmin/tools/make_font.py [Pfad zur .ttf] [Größe in Pixel]
Ergebnis: garmin/resources/fonts/small.fnt und small.png
Die eingebauten Schriften der Uhr sind für Statuszeile, Sticker und Beschriftungen zu groß.
"""
import sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ttf = sys.argv[1] if len(sys.argv) > 1 else "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
size = int(sys.argv[2]) if len(sys.argv) > 2 else 18
out = Path(__file__).resolve().parent.parent / "resources" / "fonts"
font = ImageFont.truetype(ttf, size)
ascent, descent = font.getmetrics()
codes = list(range(32, 127)) + [0xB0, 0xB7]  # ASCII, Grad, Mittelpunkt

glyphs = []
for c in codes:
    ch = chr(c)
    l, t, r, b = font.getbbox(ch, anchor="la")
    w, h = max(r - l, 0), max(b - t, 0)
    if h == 0:
        w = 0  # Leerzeichen: nur Vorschub
    glyphs.append((c, ch, l, t, w, h, int(round(font.getlength(ch)))))

W = 256
x = y = row_h = 0
placed = []
for c, ch, l, t, w, h, adv in glyphs:
    if x + w + 1 > W:
        x, y, row_h = 0, y + row_h + 1, 0
    placed.append((c, ch, l, t, w, h, adv, x, y))
    x += w + 1
    row_h = max(row_h, h)
H = 1
while H < y + row_h + 1:
    H *= 2

# Weiß mit Alpha: die Deckkraft trägt die Glyphenform
img = Image.new("RGBA", (W, H), (255, 255, 255, 0))
for c, ch, l, t, w, h, adv, px, py in placed:
    if w == 0 or h == 0:
        continue
    mask = Image.new("L", (w, h), 0)
    ImageDraw.Draw(mask).text((-l, -t), ch, font=font, fill=255, anchor="la")
    img.paste((255, 255, 255, 255), (px, py), mask)
img.save(out / "small.png")

lines = [
    f'info face="{Path(ttf).stem}" size={size} bold=1 italic=0 charset="" unicode=1 stretchH=100 smooth=1 aa=1 padding=0,0,0,0 spacing=1,1',
    f"common lineHeight={ascent + descent} base={ascent} scaleW={W} scaleH={H} pages=1 packed=0",
    'page id=0 file="small.png"',
    f"chars count={len(placed)}",
]
for c, ch, l, t, w, h, adv, px, py in placed:
    lines.append(f"char id={c} x={px} y={py} width={w} height={h} xoffset={l} yoffset={t} xadvance={adv} page=0 chnl=15")
(out / "small.fnt").write_text("\n".join(lines) + "\n")
print(f"{len(placed)} Zeichen, Atlas {W}x{H}, Zeilenhöhe {ascent + descent}")
