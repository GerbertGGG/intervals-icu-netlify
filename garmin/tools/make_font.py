"""Erzeugt Bitmap-Schriften (BMFont-Textformat) für das Watchface.

Aufruf: python3 garmin/tools/make_font.py [Pfad zur .ttf/.otf] [Größe in Pixel] [Name] [digits]
Ergebnis: garmin/resources/fonts/<Name>.fnt und <Name>.png (Name ist standardmäßig "small")
Mit "digits" nur Ziffern und Doppelpunkt, alle Ziffern gleich breit (Uhrzeit springt nicht).
Die Schriften des Zifferblatts sind Nunito (SIL Open Font License, fonts.google.com/specimen/Nunito),
Bold und ExtraBold als .ttf herunterladen:
  python3 garmin/tools/make_font.py Nunito-Bold.ttf 138 time digits
  python3 garmin/tools/make_font.py Nunito-Bold.ttf 25 small
  python3 garmin/tools/make_font.py Nunito-ExtraBold.ttf 22 mid
  python3 garmin/tools/make_font.py Nunito-Bold.ttf 18 cap
  python3 garmin/tools/make_font.py Nunito-Bold.ttf 15 tiny
Die Größen sind für 416 Pixel (Forerunner 265); Bitmap-Schriften skalieren nicht mit.
"""
import sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ttf = sys.argv[1] if len(sys.argv) > 1 else "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
size = int(sys.argv[2]) if len(sys.argv) > 2 else 18
name = sys.argv[3] if len(sys.argv) > 3 else "small"
digits = len(sys.argv) > 4 and sys.argv[4] == "digits"
out = Path(__file__).resolve().parent.parent / "resources" / "fonts"
font = ImageFont.truetype(ttf, size)
ascent, descent = font.getmetrics()
# ASCII, Grad, Mittelpunkt, Umlaute, Minutenzeichen (′)
codes = [ord(c) for c in "0123456789:"] if digits else list(range(32, 127)) + [0xB0, 0xB7, 0xC4, 0xD6, 0xDC, 0xE4, 0xF6, 0xFC, 0xDF, 0x2032]

glyphs = []
for c in codes:
    ch = chr(c)
    l, t, r, b = font.getbbox(ch, anchor="la")
    w, h = max(r - l, 0), max(b - t, 0)
    if h == 0:
        w = 0  # Leerzeichen: nur Vorschub
    glyphs.append((c, ch, l, t, w, h, int(round(font.getlength(ch)))))
xoff, top, line_h, base = {}, 0, ascent + descent, ascent
if digits:
    # Gleiche Breite für alle Ziffern, Glyphe mittig im Vorschub
    adv = max(g[6] for g in glyphs if g[1].isdigit())
    xoff = {c: (adv - w) // 2 for c, ch, l, t, w, h, a in glyphs if ch.isdigit()}
    glyphs = [(c, ch, l, t, w, h, adv if ch.isdigit() else a) for c, ch, l, t, w, h, a in glyphs]
    # Zeilenhöhe = Ziffernhöhe, damit TEXT_JUSTIFY_VCENTER die Ziffern wirklich mittig setzt
    top = min(g[3] for g in glyphs)
    line_h = max(g[3] + g[5] for g in glyphs) - top
    base = ascent - top

W = 256 if size <= 40 else 1024
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
img.save(out / f"{name}.png")

lines = [
    f'info face="{Path(ttf).stem}" size={size} bold=1 italic=0 charset="" unicode=1 stretchH=100 smooth=1 aa=1 padding=0,0,0,0 spacing=1,1',
    f"common lineHeight={line_h} base={base} scaleW={W} scaleH={H} pages=1 packed=0",
    f'page id=0 file="{name}.png"',
    f"chars count={len(placed)}",
]
for c, ch, l, t, w, h, adv, px, py in placed:
    lines.append(f"char id={c} x={px} y={py} width={w} height={h} xoffset={xoff.get(c, l)} yoffset={t - top} xadvance={adv} page=0 chnl=15")
(out / f"{name}.fnt").write_text("\n".join(lines) + "\n")
print(f"{len(placed)} Zeichen, Atlas {W}x{H}, Zeilenhöhe {line_h}")
