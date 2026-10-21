import Toybox.Application;
import Toybox.Graphics;
import Toybox.Lang;
import Toybox.Math;
import Toybox.System;
import Toybox.Time;
import Toybox.Time.Gregorian;
import Toybox.WatchUi;

// Zifferblatt "Regenbogen" (Mockup 5d): schmale Bögen am Rand, oben Essen (außen Kalorien, innen Eiweiß), unten Sport (außen Lauf,
// dann Rad, innen Schwimmen). Innen von oben nach unten: übrige Kalorien und Eiweiß, Datum mit Akku, große Uhrzeit (der Doppelpunkt
// trägt das Urteil des Coaches) und die offenen Minuten je Sportart mit grauer Beschriftung.
// Alle Maße stammen aus dem Mockup (454 x 454) und werden mit _s auf die Displaygröße skaliert.
// Daten kommen aus dem Speicher (vom Hintergrunddienst geschrieben), gezeichnet wird nur.
class IntervalsFaceView extends WatchUi.WatchFace {

    const FG = 0xF5F5F7;
    const SEC = 0x8E8E93;
    const GREY = 0xAEAEB2;
    const TER = 0x48484A;
    const OK = 0x30D158;
    const WARN = 0xFF9F0A;
    const BAD = 0xFF453A;
    const ACCENT = 0x64A8FF; // Coach (wie im Widget)
    const PROT = 0x00C7BE;   // Eiweiß: türkis, damit es sich von Kalorien (weiß) und den Sportfarben abhebt
    const KCAL_TRACK = 0x2C2C2E;
    const PROT_TRACK = 0x0F3A38;
    const STALE_SECS = 3600;
    const BAT_LOW = 20;      // Akku in Prozent, ab dem er orange wird (ab der Hälfte davon rot)
    const NUT_Y = 117;
    const KCAL_X = 160;
    const PROT_X = 292;
    const DATE_Y = 161;
    const TIME_Y = 226;
    const OPEN_Y = 306;
    const TAG_Y = 335;
    // Bögen: Radien von außen nach innen, Strichstärke und Länge; oben von 10 Grad über 9 Uhr bis 10 Grad über 3 Uhr,
    // unten von 10 Grad unter 3 Uhr bis 10 Grad unter 9 Uhr (Winkel ab 12 Uhr im Uhrzeigersinn)
    const ARC_R1 = 204;
    const ARC_R2 = 190;
    const ARC_R3 = 176;
    const ARC_PEN = 10;
    const ARC_SPAN = 160;
    const TOP_START = 280;
    const BOTTOM_START = 100;

    private var _s as Float = 1.0;
    private var _cx as Number = 0;
    private var _cy as Number = 0;
    private var _sleep as Boolean = false;
    private var _time as Graphics.FontType = Graphics.FONT_NUMBER_HOT;
    private var _open as Graphics.FontType = Graphics.FONT_XTINY;
    private var _cap as Graphics.FontType = Graphics.FONT_XTINY;
    private var _tag as Graphics.FontType = Graphics.FONT_XTINY;

    function initialize() {
        WatchFace.initialize();
        // Bitmap-Schriften aus Roboto Medium (erzeugt mit tools/make_font.py, Größen für 416 Pixel)
        _time = WatchUi.loadResource(Rez.Fonts.Time) as Graphics.FontType;
        _open = WatchUi.loadResource(Rez.Fonts.Open) as Graphics.FontType;
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

        var data = Application.Storage.getValue("d") as Dictionary?;
        drawTime(dc, data);
        // Always-on-Modus (AMOLED): nur die Uhrzeit, damit wenig Pixel leuchten
        if (_sleep) {
            return;
        }
        var rz = null as Array?;
        if (data != null) {
            rz = data.get("rz") as Array?;
        }
        drawSportArcs(dc, rz);
        drawNutrition(dc, data);
        drawDateLine(dc, data);
        if (data == null) {
            dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
            dc.drawText(_cx, px(OPEN_Y), _cap, "Keine Daten" + errSuffix(), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
        } else {
            drawOpen(dc, rz);
        }
    }

    // ---- Zeit ----------------------------------------------------------------
    // Der Doppelpunkt trägt das Urteil des Coaches (rd, siehe coachColon); im Always-on-Modus nur grau.

    private function drawTime(dc as Dc, data as Dictionary?) as Void {
        var clock = System.getClockTime();
        var h = clock.hour;
        if (!System.getDeviceSettings().is24Hour) {
            h = h % 12;
            if (h == 0) {
                h = 12;
            }
        }
        var hs = h.format("%02d");
        var ms = clock.min.format("%02d");
        if (_sleep) {
            dc.setColor(SEC, Graphics.COLOR_BLACK);
            dc.drawText(_cx, _cy, Graphics.FONT_NUMBER_MILD, hs + ":" + ms, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
            return;
        }
        var y = px(TIME_Y);
        var gap = px(16);
        dc.setColor(FG, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx - gap, y, _time, hs, Graphics.TEXT_JUSTIFY_RIGHT | Graphics.TEXT_JUSTIFY_VCENTER);
        dc.drawText(_cx + gap, y, _time, ms, Graphics.TEXT_JUSTIFY_LEFT | Graphics.TEXT_JUSTIFY_VCENTER);
        coachColon(dc, data, y);
    }

    // Coach-Urteil als Doppelpunkt: Kreise grün = hart erlaubt, Dreiecke orange = nur locker, Quadrate rot = frei,
    // Kreise blau = Coach hat geschrieben (ohne Urteil), grau = noch nichts oder Daten älter als eine Stunde. Eigene Regeln gibt es nicht.
    private function coachColon(dc as Dc, data as Dictionary?, y as Number) as Void {
        var rd = data != null ? data.get("rd") : null;
        var co = data != null ? numOrNull(data.get("co")) : null;
        var color = co != null && co.toNumber() == 1 ? ACCENT : TER;
        if ("ok".equals(rd)) { color = OK; }
        else if ("warn".equals(rd)) { color = WARN; }
        else if ("bad".equals(rd)) { color = BAD; }
        if (isStale(data)) {
            color = TER;
            rd = null;
        }
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        var d = px(18);
        for (var i = -1; i <= 1; i += 2) {
            var cy = y + i * d;
            if ("warn".equals(rd)) {
                var t = px(10);
                dc.fillPolygon([[_cx - t, cy + t * 4 / 5], [_cx, cy - t], [_cx + t, cy + t * 4 / 5]]);
            } else if ("bad".equals(rd)) {
                var q = px(7);
                dc.fillRectangle(_cx - q, cy - q, 2 * q, 2 * q);
            } else {
                dc.fillCircle(_cx, cy, px(8));
            }
        }
    }

    // ---- Untere Bögen: Wochenziele des Coaches ----------------------------------------
    // Grundbogen in gedämpfter Sportfarbe, darauf ab dem rechten Ende die Minuten der Woche gegen das Zeitziel
    // (rz: [[Sportart, erledigt, Ziel], ...]); voller Bogen = Ziel erreicht. Ohne Ziel bleibt nur der Grundbogen.

    private function drawSportArcs(dc as Dc, rz as Array?) as Void {
        var sports = ["run", "bike", "swim"];
        var radii = [ARC_R1, ARC_R2, ARC_R3];
        for (var k = 0; k < sports.size(); k++) {
            var color = sportColor(sports[k]);
            var e = goalOf(rz, sports[k]);
            var f = e != null ? frac(e[1] as Numeric, e[2] as Numeric) : 0.0;
            arc(dc, px(radii[k] as Number), BOTTOM_START, f, color, blend(Graphics.COLOR_BLACK, color, 0.2));
        }
    }

    // Bogen mit runden Enden ab start (Grad ab 12 Uhr im Uhrzeigersinn) über ARC_SPAN Grad: erst der Grundbogen, darauf der Anteil f
    private function arc(dc as Dc, r as Number, start as Number, f as Float, color as Number, track as Number) as Void {
        dc.setPenWidth(px(ARC_PEN));
        capArc(dc, r, start, ARC_SPAN.toFloat(), track);
        if (f > 0) {
            capArc(dc, r, start, ARC_SPAN * f, color);
        }
    }

    // drawArc endet gerade, die runden Enden kommen als Kreise dazu
    private function capArc(dc as Dc, r as Number, start as Number, len as Float, color as Number) as Void {
        dc.setColor(color, Graphics.COLOR_BLACK);
        if (len >= 1) {
            dc.drawArc(_cx, _cy, r, Graphics.ARC_CLOCKWISE, norm(90 - start), norm(90 - start - len));
        }
        var cap = px(ARC_PEN) / 2;
        for (var i = 0; i < 2; i++) {
            var t = Math.toRadians(start + i * len);
            dc.fillCircle(_cx + r * Math.sin(t), _cy - r * Math.cos(t), cap);
        }
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
        return SEC;
    }

    // ---- Offene Minuten je Sportart ---------------------------------------------------
    // Groß in Sportfarbe die Minuten bis zum Zeitziel, darunter klein und grau die Sportart. Ziel erreicht: Haken in Sportfarbe,
    // ohne Ziel "--" in Grau.

    private function drawOpen(dc as Dc, rz as Array?) as Void {
        var sports = ["run", "bike", "swim"];
        var labels = ["LAUF", "RAD", "SCHWIMM."];
        var just = Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER;
        var y = px(OPEN_Y);
        for (var k = 0; k < sports.size(); k++) {
            var x = _cx + px(72 * (k - 1));
            var color = sportColor(sports[k]);
            var e = goalOf(rz, sports[k]);
            if (e == null) {
                dc.setColor(TER, Graphics.COLOR_TRANSPARENT);
                dc.drawText(x, y, _open, "--", just);
            } else {
                var open = (e[2] as Numeric).toNumber() - (e[1] as Numeric).toNumber();
                dc.setColor(color, Graphics.COLOR_TRANSPARENT);
                if (open > 0) {
                    dc.drawText(x, y, _open, open.toString() + "′", just);
                } else {
                    drawCheck(dc, x, y, color);
                }
            }
            dc.setColor(GREY, Graphics.COLOR_TRANSPARENT);
            dc.drawText(x, px(TAG_Y), _tag, labels[k] as String, just);
        }
    }

    // Haken als Linien um die Mitte x, y, die Bitmap-Schriften haben kein ✓
    private function drawCheck(dc as Dc, x as Number, y as Number, color as Number) as Void {
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(5));
        dc.drawLine(x - px(12), y, x - px(4), y + px(8));
        dc.drawLine(x - px(4), y + px(8), x + px(13), y - px(9));
    }

    // ---- Obere Bögen und Zahlen: Essen ------------------------------------------------
    // Außen Kalorien (weiß) gegen das Tagesziel, der Bogen füllt sich mit dem Gegessenen; innen Eiweiß (türkis) gegen das
    // Yazio-Ziel. Darunter, was noch übrig ist: kcal bis zum Ziel ("+N" darüber) mit kleinem grauem "kcal" und Eiweiß in Gramm
    // mit "g Eiweiß" (Ziel erreicht: Haken). Ohne Ziel jeweils das Gegessene.

    private function drawNutrition(dc as Dc, data as Dictionary?) as Void {
        var kcal = null as Dictionary?;
        var pr = null as Dictionary?;
        if (data != null) {
            kcal = data.get("kcal") as Dictionary?;
            pr = data.get("pr") as Dictionary?;
        }
        var kv = kcal != null ? numOrNull(kcal.get("v")) : null;
        var kg = kcal != null ? numOrNull(kcal.get("g")) : null;
        var pv = pr != null ? numOrNull(pr.get("v")) : null;
        var pg = pr != null ? numOrNull(pr.get("g")) : null;
        arc(dc, px(ARC_R1), TOP_START, frac(kv, kg), FG, KCAL_TRACK);
        arc(dc, px(ARC_R2), TOP_START, frac(pv, pg), PROT, PROT_TRACK);

        var kText = kv != null ? kv.toNumber().toString() : "--";
        if (kv != null && kg != null && kg > 0) {
            var rest = (kg - kv).toNumber();
            kText = rest >= 0 ? rest.toString() : "+" + (-rest).toString();
        }
        var y = px(NUT_Y);
        drawLine(dc, px(KCAL_X), y, [["t", kText, FG, _open], ["t", " kcal", GREY, _tag]]);
        var pItem = ["t", pv != null ? pv.toNumber().toString() : "--", PROT, _open];
        if (pv != null && pg != null && pg > 0) {
            var left = (pg - pv).toNumber();
            pItem = left > 0 ? ["t", left.toString(), PROT, _open] : ["check", px(24), PROT];
        }
        drawLine(dc, px(PROT_X), y, [pItem, ["t", " g Eiweiß", GREY, _tag]]);
    }

    // ---- Datum und Akku ------------------------------------------------------------
    // "MI 21. OKT" in Grau, dahinter das Akkusymbol mit Prozentzahl (weiß, orange ab BAT_LOW, rot ab der Hälfte davon, grün
    // beim Laden). Daten älter als eine Stunde: hinter dem Datum ein oranges "!" und der Fehlercode des letzten Abrufs.

    private function drawDateLine(dc as Dc, data as Dictionary?) as Void {
        var info = Gregorian.info(Time.now(), Time.FORMAT_SHORT);
        var days = ["SO", "MO", "DI", "MI", "DO", "FR", "SA"];
        var months = ["JAN", "FEB", "MÄR", "APR", "MAI", "JUN", "JUL", "AUG", "SEP", "OKT", "NOV", "DEZ"];
        var text = days[(info.day_of_week as Number) - 1] + " " + info.day + ". " + months[(info.month as Number) - 1];
        var items = [["t", text, GREY, _cap]] as Array<Array>;
        if (data != null && isStale(data)) {
            items.add(["t", " !" + errSuffix(), WARN, _cap]);
        }
        var stats = System.getSystemStats();
        var pct = Math.round(stats.battery).toNumber();
        var color = stats.charging ? OK : (pct <= BAT_LOW / 2 ? BAD : (pct <= BAT_LOW ? WARN : FG));
        items.add(["gap", px(12)]);
        items.add(["bat", px(26), color, pct]);
        items.add(["gap", px(6)]);
        items.add(["t", pct.toString(), color == FG ? GREY : color, _cap]);
        drawLine(dc, _cx, px(DATE_Y), items);
    }

    // Akkusymbol: Rahmen mit Pol rechts, gefüllt nach Ladestand; x = linke Kante, y = Mitte, w = Breite samt Pol
    private function drawBattery(dc as Dc, x as Number, y as Number, w as Number, color as Number, pct as Number) as Void {
        var nub = px(3) > 1 ? px(3) : 1;
        var bw = w - nub;
        var h = px(13);
        var pen = px(2) > 1 ? px(2) : 1;
        dc.setColor(GREY, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(pen);
        dc.drawRoundedRectangle(x, y - h / 2, bw, h, px(3));
        dc.fillRectangle(x + bw, y - h / 4, nub, h / 2);
        var inner = bw - 2 * pen - 2;
        var fill = (inner * (pct < 0 ? 0 : (pct > 100 ? 100 : pct)) / 100).toNumber();
        if (fill > 0) {
            dc.setColor(color, Graphics.COLOR_TRANSPARENT);
            dc.fillRectangle(x + pen + 1, y - h / 2 + pen + 1, fill, h - 2 * pen - 2);
        }
    }

    // Farbe zwischen a (f = 0) und b (f = 1)
    private function blend(a as Number, b as Number, f as Float) as Number {
        var r = ((a >> 16) & 0xFF) + ((((b >> 16) & 0xFF) - ((a >> 16) & 0xFF)) * f).toNumber();
        var g = ((a >> 8) & 0xFF) + ((((b >> 8) & 0xFF) - ((a >> 8) & 0xFF)) * f).toNumber();
        var bl = (a & 0xFF) + (((b & 0xFF) - (a & 0xFF)) * f).toNumber();
        return (r << 16) | (g << 8) | bl;
    }

    // ---- Zeile aus Text und Formen, als Ganzes um cx zentriert ---------------------------
    // Elemente: ["t", Text, Farbe, Schrift], ["bat", Breite, Farbe, Prozent], ["check", Breite, Farbe], ["gap", Breite]

    private function drawLine(dc as Dc, cx as Number, y as Number, items as Array<Array>) as Void {
        var total = 0;
        for (var i = 0; i < items.size(); i++) {
            total += itemWidth(dc, items[i]);
        }
        var x = cx - total / 2;
        for (var i = 0; i < items.size(); i++) {
            var it = items[i];
            var kind = it[0] as String;
            var w = itemWidth(dc, it);
            if (kind.equals("t")) {
                dc.setColor(it[2] as Number, Graphics.COLOR_TRANSPARENT);
                dc.drawText(x, y, it[3] as Graphics.FontType, it[1] as String, Graphics.TEXT_JUSTIFY_LEFT | Graphics.TEXT_JUSTIFY_VCENTER);
            } else if (kind.equals("bat")) {
                drawBattery(dc, x, y, w, it[2] as Number, it[3] as Number);
            } else if (kind.equals("check")) {
                drawCheck(dc, x + w / 2, y, it[2] as Number);
            }
            x += w;
        }
    }

    private function itemWidth(dc as Dc, it as Array) as Number {
        var kind = it[0] as String;
        if (kind.equals("t")) {
            var text = it[1] as String;
            return text.length() > 0 ? dc.getTextWidthInPixels(text, it[3] as Graphics.FontType) : 0;
        }
        return it[1] as Number;
    }

    // ---- Helfer -------------------------------------------------------------------

    private function px(v as Numeric) as Number {
        return (v * _s).toNumber();
    }

    private function norm(a as Numeric) as Number {
        var n = a.toNumber() % 360;
        return n < 0 ? n + 360 : n;
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
