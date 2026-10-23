import Toybox.Application;
import Toybox.Graphics;
import Toybox.Lang;
import Toybox.Math;
import Toybox.System;
import Toybox.Time;
import Toybox.Time.Gregorian;
import Toybox.WatchUi;

// Zifferblatt nach Tageszeit (Mockup "Modular", schwarz mit Sportfarben). Fest: oben Datum, Akku und das Urteil des Coaches
// als Wort, darunter die große Uhrzeit, unten die offenen Wochenminuten je Sportart mit Balken. Dazwischen eine Karte, die
// mit dem Tag wechselt: morgens (bis 12 Uhr) Schlaf und HRV der letzten 7 Tage, tagsüber die Woche Tag für Tag, bis 3 Stunden
// nach einer Einheit deren Werte. Ganz unten eine Zeile: morgens der Coach, sonst die nächste Einheit.
// Alle Maße stammen aus dem Mockup (454 x 454) und werden mit _s auf die Displaygröße skaliert.
// Daten kommen aus dem Speicher (vom Hintergrunddienst geschrieben), gezeichnet wird nur.
class IntervalsFaceView extends WatchUi.WatchFace {

    const FG = 0xF5F5F7;
    const SEC = 0x8E8E93;
    const TER = 0x48484A;
    const CARD = 0x1C1C1E;
    const BAR = 0x3A3A3C;
    const DASH = 0x636366;
    const OK = 0x30D158;
    const WARN = 0xFF9F0A;
    const BAD = 0xFF453A;
    const ACCENT = 0x64A8FF; // Coach ohne Urteil (wie im Widget)
    const SLEEP = 0x7D7AFF;
    const STALE_SECS = 3600;
    const AFTER_SECS = 3 * 3600; // so lange nach dem Ende einer Einheit steht ihre Karte
    const MORNING_END = 12;      // bis zu dieser Stunde die Morgenkarte
    const BAT_LOW = 20;          // Akku in Prozent, ab dem er orange wird (ab der Hälfte davon rot)
    // Grundlinien und Mitten (Mockup-Pixel)
    const TOP_Y = 76;
    const TIME_Y = 140;
    const CARD_X = 40;
    const CARD_Y = 194;
    const CARD_W = 374;
    const CARD_H = 128;
    const PAD = 20;
    const HEAD_Y = 226;
    const BARS_TOP = 238;
    const BARS_BOTTOM = 288;
    const DAYS_Y = 312;
    const CELL_Y = 272;
    const CELL_TAG_Y = 300;
    const ROW_TAG_Y = 346;
    const ROW_VAL_Y = 378;
    const ROW_BAR_Y = 386;
    const ROW_DX = 108;
    const NEXT_Y = 420;

    private var _s as Float = 1.0;
    private var _cx as Number = 0;
    private var _cy as Number = 0;
    private var _sleep as Boolean = false;
    private var _time as Graphics.FontType = Graphics.FONT_NUMBER_HOT;
    private var _val as Graphics.FontType = Graphics.FONT_SMALL;
    private var _cap as Graphics.FontType = Graphics.FONT_XTINY;
    private var _tag as Graphics.FontType = Graphics.FONT_XTINY;

    function initialize() {
        WatchFace.initialize();
        // Bitmap-Schriften aus Inter Tight (erzeugt mit tools/make_font.py, Größen für 416 Pixel; Beschriftungen nicht unter
        // 17 Pixel, sonst auf der Uhr schlecht lesbar)
        _time = WatchUi.loadResource(Rez.Fonts.Time) as Graphics.FontType;
        _val = WatchUi.loadResource(Rez.Fonts.Val) as Graphics.FontType;
        _cap = WatchUi.loadResource(Rez.Fonts.Cap) as Graphics.FontType;
        _tag = WatchUi.loadResource(Rez.Fonts.Tag) as Graphics.FontType;
    }

    function onEnterSleep() as Void {
        _sleep = true;
        WatchUi.requestUpdate();
    }

    function onExitSleep() as Void {
        _sleep = false;
        WatchUi.requestUpdate();
    }

    function onUpdate(dc as Dc) as Void {
        _s = dc.getWidth() / 454.0;
        _cx = dc.getWidth() / 2;
        _cy = dc.getHeight() / 2;
        dc.setColor(FG, Graphics.COLOR_BLACK);
        dc.clear();
        if (dc has :setAntiAlias) {
            dc.setAntiAlias(true);
        }

        var clock = System.getClockTime();
        // Always-on-Modus (AMOLED): nur die Uhrzeit, grau und klein, damit wenig Pixel leuchten
        if (_sleep) {
            dc.setColor(SEC, Graphics.COLOR_BLACK);
            dc.drawText(_cx, _cy, Graphics.FONT_NUMBER_MILD, timeText(clock), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
            return;
        }
        var data = Application.Storage.getValue("d") as Dictionary?;
        drawTopLine(dc, data);
        dc.setColor(FG, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx, px(TIME_Y), _time, timeText(clock), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
        if (data == null) {
            dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
            dc.drawText(_cx, px(CARD_Y + CARD_H / 2), _cap, "Keine Daten" + errSuffix(), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
            return;
        }

        var la = data.get("la") as Dictionary?;
        var end = la != null ? numOrNull(la.get("e")) : null;
        var since = end != null ? Time.now().value() - end.toNumber() : -1;
        if (la != null && since >= 0 && since < AFTER_SECS) {
            drawLastCard(dc, la);
            drawNext(dc, data);
        } else if (clock.hour < MORNING_END) {
            drawMorningCard(dc, data);
            drawCoachLine(dc, data);
        } else {
            drawWeekCard(dc, data);
            drawNext(dc, data);
        }
        drawWeekRow(dc, data.get("rz") as Array?);
    }

    // Uhrzeit ohne führende Null bei den Stunden ("6:48"), im 12-Stunden-Modus 1 bis 12
    private function timeText(clock as System.ClockTime) as String {
        var h = clock.hour;
        if (!System.getDeviceSettings().is24Hour) {
            h = h % 12;
            if (h == 0) {
                h = 12;
            }
        }
        return h.toString() + ":" + clock.min.format("%02d");
    }

    // ---- Oben: Datum, Akku, Urteil des Coaches ---------------------------------------------
    // "DO 23. · 78 % · LOCKER": Akku grau (orange ab BAT_LOW, rot ab der Hälfte davon, grün beim Laden), das Urteil (go aus
    // save_coach) als Wort in seiner Farbe. Eine eigene Ampel hat die Uhr nicht. Daten älter als eine Stunde: statt des Urteils
    // ein oranges "!" mit dem Fehlercode des letzten Abrufs.

    private function drawTopLine(dc as Dc, data as Dictionary?) as Void {
        var info = Gregorian.info(Time.now(), Time.FORMAT_SHORT);
        var days = ["SO", "MO", "DI", "MI", "DO", "FR", "SA"];
        var items = [["t", days[(info.day_of_week as Number) - 1] + " " + info.day + ".", FG, _cap]] as Array<Array>;
        var stats = System.getSystemStats();
        var pct = Math.round(stats.battery).toNumber();
        var bat = stats.charging ? OK : (pct <= BAT_LOW / 2 ? BAD : (pct <= BAT_LOW ? WARN : SEC));
        items.add(["t", " · ", SEC, _cap]);
        items.add(["t", pct.toString() + " %", bat, _cap]);
        if (data != null && isStale(data)) {
            items.add(["t", " · !" + errSuffix(), WARN, _cap]);
        } else if (data != null) {
            var rd = data.get("rd");
            var word = null;
            var color = SEC;
            if ("ok".equals(rd)) { word = "HART"; color = OK; }
            else if ("warn".equals(rd)) { word = "LOCKER"; color = WARN; }
            else if ("bad".equals(rd)) { word = "FREI"; color = BAD; }
            else if (numOrNull(data.get("co")) != null && (data.get("co") as Number) == 1) { word = "COACH"; color = ACCENT; }
            if (word != null) {
                items.add(["t", " · ", SEC, _cap]);
                items.add(["t", word, color, _cap]);
            }
        }
        drawItems(dc, _cx, px(TOP_Y), items, 0);
    }

    // ---- Karte morgens: Schlaf und HRV -----------------------------------------------------
    // Kopf: Schlaf in Stunden (lila), HRV heute mit der eigenen Basis (Ø). Darunter die HRV der letzten 7 Tage als Balken,
    // heute hervorgehoben (grün, wenn über der Basis), die Basis als gestrichelte Linie. Vergleich nur mit der eigenen Basis.

    private function drawMorningCard(dc as Dc, data as Dictionary) as Void {
        drawCard(dc, CARD);
        var sl = numOrNull(data.get("sl"));
        var hv = data.get("hv") as Array?;
        var hm = numOrNull(data.get("hm"));
        var today = hv != null && hv.size() > 0 ? numOrNull(hv[hv.size() - 1]) : null;
        var hi = today != null && hm != null && today >= hm ? OK : FG;
        var y = px(HEAD_Y);
        drawItems(dc, px(CARD_X + PAD), y, [["t", "SCHLAF ", SEC, _tag], ["t", sl != null ? hours((sl * 60).toNumber()) : "--", SLEEP, _cap]], -1);
        var right = [["t", "HRV ", SEC, _tag], ["t", today != null ? today.toNumber().toString() : "--", hi, _cap]] as Array<Array>;
        if (hm != null) {
            right.add(["t", " Ø " + hm.toNumber().toString(), SEC, _tag]);
        }
        drawItems(dc, px(CARD_X + CARD_W - PAD), y, right, 1);
        if (hv == null || hv.size() == 0) {
            return;
        }
        // Skala: vom 0,7-Fachen des kleinsten Werts bis zum größten (Basis eingeschlossen), damit Unterschiede sichtbar sind
        var lo = null;
        var top = hm;
        for (var i = 0; i < hv.size(); i++) {
            var v = numOrNull(hv[i]);
            if (v != null) {
                lo = lo == null || v < lo ? v : lo;
                top = top == null || v > top ? v : top;
            }
        }
        if (lo == null || top == null) {
            return;
        }
        var base = lo * 0.7;
        var span = top - base > 0 ? (top - base).toFloat() : 1.0;
        var n = hv.size();
        for (var i = 0; i < n; i++) {
            var v = numOrNull(hv[i]);
            if (v != null) {
                bar(dc, i, n, ((v - base) / span).toFloat(), i == n - 1 ? hi : BAR);
            }
        }
        if (hm != null) {
            dashLine(dc, px(BARS_BOTTOM) - ((px(BARS_BOTTOM) - px(BARS_TOP)) * ((hm - base) / span)).toNumber());
        }
        // Wochentage der letzten 7 Tage, heute zuletzt
        var dow = (Gregorian.info(Time.now(), Time.FORMAT_SHORT).day_of_week as Number) - 1;
        var names = ["SO", "MO", "DI", "MI", "DO", "FR", "SA"];
        for (var i = 0; i < n; i++) {
            dayLabel(dc, i, n, names[(dow - (n - 1 - i) + 70) % 7] as String, i == n - 1);
        }
    }

    // ---- Karte tagsüber: die Woche Tag für Tag --------------------------------------------------
    // Kopf: Ausdauer der Woche gegen das Ziel des Coaches (wm). Je Tag Mo bis So die Minuten je Sportart übereinander,
    // erledigt gefüllt, geplant als Umriss in gedämpfter Sportfarbe (wd: [[Sportart, Minuten, 1 = geplant], ...]).

    private function drawWeekCard(dc as Dc, data as Dictionary) as Void {
        drawCard(dc, CARD);
        var y = px(HEAD_Y);
        drawItems(dc, px(CARD_X + PAD), y, [["t", "WOCHE", SEC, _tag]], -1);
        var wm = data.get("wm") as Array?;
        if (wm != null && wm.size() == 2 && numOrNull(wm[0]) != null) {
            var right = [["t", hours((wm[0] as Numeric).toNumber()), FG, _cap]] as Array<Array>;
            if (numOrNull(wm[1]) != null && (wm[1] as Numeric) > 0) {
                right.add(["t", " / " + hours((wm[1] as Numeric).toNumber()), SEC, _tag]);
            }
            drawItems(dc, px(CARD_X + CARD_W - PAD), y, right, 1);
        }
        var wd = data.get("wd") as Array?;
        if (wd == null || wd.size() == 0) {
            return;
        }
        var n = wd.size();
        var most = 60;
        for (var i = 0; i < n; i++) {
            var t = dayTotal(wd[i] as Array);
            most = t > most ? t : most;
        }
        var h = px(BARS_BOTTOM) - px(BARS_TOP);
        var gap = px(2) > 1 ? px(2) : 1;
        for (var i = 0; i < n; i++) {
            var x = barX(i, n);
            var w = barW(n);
            var yb = px(BARS_BOTTOM);
            var segs = wd[i] as Array;
            if (segs.size() == 0) {
                // Freier Tag: dünne Linie am Boden
                dc.setColor(BAR, Graphics.COLOR_TRANSPARENT);
                dc.fillRectangle(x, yb - px(3), w, px(3) > 1 ? px(3) : 1);
            }
            for (var k = 0; k < segs.size(); k++) {
                var e = segs[k] as Array;
                var sh = (h * (e[1] as Numeric).toFloat() / most).toNumber();
                sh = sh < px(4) ? px(4) : sh;
                var color = sportColor(e[0]);
                if ((e[2] as Numeric).toNumber() == 1) {
                    dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.55), Graphics.COLOR_TRANSPARENT);
                    dc.setPenWidth(px(2) > 1 ? px(2) : 1);
                    dc.drawRoundedRectangle(x, yb - sh, w, sh, px(4));
                } else {
                    dc.setColor(color, Graphics.COLOR_TRANSPARENT);
                    dc.fillRoundedRectangle(x, yb - sh, w, sh, px(4));
                }
                yb -= sh + gap;
            }
        }
        var names = ["MO", "DI", "MI", "DO", "FR", "SA", "SO"];
        var ti = data.get("ti");
        for (var i = 0; i < n && i < 7; i++) {
            dayLabel(dc, i, n, names[i] as String, ti instanceof Lang.Number && i == (ti as Number));
        }
    }

    // Minuten eines Tages (erledigt und geplant)
    private function dayTotal(segs as Array) as Number {
        var t = 0;
        for (var k = 0; k < segs.size(); k++) {
            t += ((segs[k] as Array)[1] as Numeric).toNumber();
        }
        return t;
    }

    // ---- Karte nach einer Einheit -----------------------------------------------------------
    // Kopf in Sportfarbe: Sportart mit Startzeit, rechts "LÄNGSTE SEIT N WO." (Vergleich mit den eigenen Einheiten derselben
    // Sportart). Darunter bis zu vier Werte (c: [[Text, Beschriftung], ...]), die Karte leicht in Sportfarbe getönt.

    private function drawLastCard(dc as Dc, la as Dictionary) as Void {
        var color = sportColor(la.get("s"));
        drawCard(dc, blend(CARD, color, 0.08));
        var y = px(HEAD_Y);
        var t = la.get("t");
        var head = sportName(la.get("s")).toUpper() + (t instanceof Lang.String ? " " + t : "");
        drawItems(dc, px(CARD_X + PAD), y, [["t", head, color, _tag]], -1);
        var lw = numOrNull(la.get("lw"));
        if (lw != null) {
            drawItems(dc, px(CARD_X + CARD_W - PAD), y, [["t", "LÄNGSTE SEIT " + lw.toNumber().toString() + " WO.", color, _tag]], 1);
        }
        var c = la.get("c") as Array?;
        if (c == null) {
            return;
        }
        var colW = px(CARD_W - 2 * PAD) / 4;
        for (var i = 0; i < c.size() && i < 4; i++) {
            var cell = c[i] as Array;
            var x = px(CARD_X + PAD) + i * colW;
            drawItems(dc, x, px(CELL_Y), [["t", cell[0] as String, FG, _val]], -1);
            drawItems(dc, x, px(CELL_TAG_Y), [["t", cell[1] as String, SEC, _tag]], -1);
        }
    }

    // ---- Untere Reihe: offene Wochenminuten -----------------------------------------------------
    // Je Sportart groß in Sportfarbe die Minuten bis zum Zeitziel des Coaches (rz: [[Sportart, erledigt, Ziel], ...]),
    // darunter ein Balken, der sich zum Ziel füllt. Ziel erreicht: Haken; ohne Ziel "--" in Grau.

    private function drawWeekRow(dc as Dc, rz as Array?) as Void {
        var sports = ["run", "bike", "swim"];
        var labels = ["LAUF", "RAD", "SCHWIMM"];
        for (var k = 0; k < sports.size(); k++) {
            var x = _cx + px(ROW_DX * (k - 1));
            var color = sportColor(sports[k]);
            drawItems(dc, x, px(ROW_TAG_Y), [["t", labels[k] as String, SEC, _tag]], 0);
            var e = goalOf(rz, sports[k]);
            if (e == null) {
                drawItems(dc, x, px(ROW_VAL_Y), [["t", "--", TER, _val]], 0);
                continue;
            }
            var done = (e[1] as Numeric).toNumber();
            var goal = (e[2] as Numeric).toNumber();
            var open = goal - done;
            if (open > 0) {
                drawItems(dc, x, px(ROW_VAL_Y), [["t", open.toString(), color, _val], ["t", "'", SEC, _val]], 0);
            } else {
                drawCheck(dc, x, px(ROW_VAL_Y) - px(8), color);
            }
            var bw = px(64);
            var bh = px(5) > 2 ? px(5) : 2;
            dc.setColor(BAR, Graphics.COLOR_TRANSPARENT);
            dc.fillRoundedRectangle(x - bw / 2, px(ROW_BAR_Y), bw, bh, bh / 2);
            var fw = (bw * frac(done, goal)).toNumber();
            if (fw > 0) {
                dc.setColor(color, Graphics.COLOR_TRANSPARENT);
                dc.fillRoundedRectangle(x - bw / 2, px(ROW_BAR_Y), fw, bh, bh / 2);
            }
        }
    }

    // ---- Zeile ganz unten ----------------------------------------------------------------------
    // Morgens die Kopfzeile des Coaches (cl) in Blau, ohne sie die nächste Einheit.

    private function drawCoachLine(dc as Dc, data as Dictionary) as Void {
        var cl = data.get("cl");
        if (cl instanceof Lang.String && cl.length() > 0) {
            drawItems(dc, _cx, px(NEXT_Y), [["t", cl, ACCENT, _cap]], 0);
        } else {
            drawNext(dc, data);
        }
    }

    // Nächste Einheit (next: d Tage ab heute, s Sportart, m Minuten, km): "Heute Lauf 45'", "Morgen Rad 60'", sonst mit
    // Wochentag ("Sa Rad 90'")
    private function drawNext(dc as Dc, data as Dictionary) as Void {
        var nx = data.get("next") as Dictionary?;
        if (nx == null) {
            return;
        }
        var d = numOrNull(nx.get("d"));
        var when = "Heute ";
        if (d != null && d.toNumber() == 1) {
            when = "Morgen ";
        } else if (d != null && d.toNumber() > 1) {
            var names = ["So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"];
            var dow = (Gregorian.info(Time.now(), Time.FORMAT_SHORT).day_of_week as Number) - 1;
            when = names[(dow + d.toNumber()) % 7] + " ";
        }
        var what = sportName(nx.get("s"));
        var m = numOrNull(nx.get("m"));
        var km = numOrNull(nx.get("km"));
        if (m != null) {
            what += " " + m.toNumber().toString() + "'";
        } else if (km != null) {
            what += " " + km.format("%.1f") + " km";
        }
        drawItems(dc, _cx, px(NEXT_Y), [["t", when, SEC, _cap], ["t", what, sportColor(nx.get("s")), _cap]], 0);
    }

    // ---- Bausteine ------------------------------------------------------------------------------

    private function drawCard(dc as Dc, color as Number) as Void {
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(px(CARD_X), px(CARD_Y), px(CARD_W), px(CARD_H), px(24));
    }

    // Balken i von n in der Karte, f = Höhe als Anteil (0 bis 1)
    private function bar(dc as Dc, i as Number, n as Number, f as Float, color as Number) as Void {
        var h = ((px(BARS_BOTTOM) - px(BARS_TOP)) * clamp01(f)).toNumber();
        h = h < px(4) ? px(4) : h;
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(barX(i, n), px(BARS_BOTTOM) - h, barW(n), h, px(4));
    }

    private function barW(n as Number) as Number {
        return (px(CARD_W - 2 * PAD) - (n - 1) * px(7)) / n;
    }

    private function barX(i as Number, n as Number) as Number {
        return px(CARD_X + PAD) + i * (barW(n) + px(7));
    }

    private function dayLabel(dc as Dc, i as Number, n as Number, text as String, today as Boolean) as Void {
        drawItems(dc, barX(i, n) + barW(n) / 2, px(DAYS_Y), [["t", text, today ? FG : SEC, _tag]], 0);
    }

    // Gestrichelte Linie quer über die Balken (Basis der HRV)
    private function dashLine(dc as Dc, y as Number) as Void {
        dc.setColor(DASH, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(2) > 1 ? px(2) : 1);
        var x0 = px(CARD_X + PAD);
        var x1 = px(CARD_X + CARD_W - PAD);
        for (var x = x0; x < x1; x += px(8)) {
            dc.drawLine(x, y, x + px(4) < x1 ? x + px(4) : x1, y);
        }
    }

    // Haken als Linien um die Mitte x, y, die Bitmap-Schriften haben kein ✓
    private function drawCheck(dc as Dc, x as Number, y as Number, color as Number) as Void {
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(5));
        dc.drawLine(x - px(12), y, x - px(4), y + px(8));
        dc.drawLine(x - px(4), y + px(8), x + px(13), y - px(9));
    }

    // Eintrag [Sportart, erledigt, Ziel] aus rz, ohne Ziel null
    private function goalOf(rz as Array?, sport) as Array? {
        if (rz == null) {
            return null;
        }
        for (var i = 0; i < rz.size(); i++) {
            var e = rz[i] as Array;
            if (sport.equals(e[0])) {
                return e;
            }
        }
        return null;
    }

    private function sportColor(sport) as Number {
        if ("run".equals(sport)) { return 0xFFD60A; }        // gelb
        if ("bike".equals(sport)) { return 0x30D158; }       // grün
        if ("swim".equals(sport)) { return 0x409CFF; }       // blau
        return FG;
    }

    private function sportName(sport) as String {
        if ("run".equals(sport)) { return "Lauf"; }
        if ("bike".equals(sport)) { return "Rad"; }
        if ("swim".equals(sport)) { return "Schwimmen"; }
        if ("strength".equals(sport)) { return "Kraft"; }
        return "Einheit";
    }

    // Minuten als h:mm
    private function hours(min as Number) as String {
        return (min / 60).toString() + ":" + (min % 60).format("%02d");
    }

    // Farbe zwischen a (f = 0) und b (f = 1)
    private function blend(a as Number, b as Number, f as Float) as Number {
        var r = ((a >> 16) & 0xFF) + ((((b >> 16) & 0xFF) - ((a >> 16) & 0xFF)) * f).toNumber();
        var g = ((a >> 8) & 0xFF) + ((((b >> 8) & 0xFF) - ((a >> 8) & 0xFF)) * f).toNumber();
        var bl = (a & 0xFF) + (((b & 0xFF) - (a & 0xFF)) * f).toNumber();
        return (r << 16) | (g << 8) | bl;
    }

    // ---- Zeile aus Texten verschiedener Schriften auf einer gemeinsamen Grundlinie y -----------------
    // Elemente: ["t", Text, Farbe, Schrift]; align -1 = ab x nach rechts, 0 = um x zentriert, 1 = bis x

    private function drawItems(dc as Dc, x as Number, y as Number, items as Array<Array>, align as Number) as Void {
        var total = 0;
        for (var i = 0; i < items.size(); i++) {
            total += textWidth(dc, items[i]);
        }
        var left = align < 0 ? x : (align == 0 ? x - total / 2 : x - total);
        for (var i = 0; i < items.size(); i++) {
            var it = items[i];
            var font = it[3] as Graphics.FontType;
            dc.setColor(it[2] as Number, Graphics.COLOR_TRANSPARENT);
            dc.drawText(left, y - Graphics.getFontAscent(font), font, it[1] as String, Graphics.TEXT_JUSTIFY_LEFT);
            left += textWidth(dc, it);
        }
    }

    private function textWidth(dc as Dc, it as Array) as Number {
        var text = it[1] as String;
        return text.length() > 0 ? dc.getTextWidthInPixels(text, it[3] as Graphics.FontType) : 0;
    }

    // ---- Helfer -------------------------------------------------------------------

    private function px(v as Numeric) as Number {
        return (v * _s).toNumber();
    }

    private function clamp01(v as Float) as Float {
        return v < 0 ? 0.0 : (v > 1 ? 1.0 : v);
    }

    // Anteil v / of zwischen 0 und 1; ohne Werte 0
    private function frac(v as Numeric?, of as Numeric?) as Float {
        return (v != null && of != null && of > 0) ? clamp01(v.toFloat() / of) : 0.0;
    }

    private function numOrNull(v) as Numeric? {
        if (v instanceof Lang.Number || v instanceof Lang.Float || v instanceof Lang.Long || v instanceof Lang.Double) {
            return v;
        }
        return null;
    }

    // Letzter Fehlercode des Abrufs (" -104"), leer ohne Fehler
    private function errSuffix() as String {
        var e = Application.Storage.getValue("e");
        return e instanceof Lang.Number ? " " + e.toString() : "";
    }

    private function isStale(data as Dictionary?) as Boolean {
        var at = data != null ? numOrNull(data.get("at")) : null;
        return at == null || Time.now().value() - at.toNumber() > STALE_SECS;
    }
}
