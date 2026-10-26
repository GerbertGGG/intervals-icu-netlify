import Toybox.Application;
import Toybox.Graphics;
import Toybox.Lang;
import Toybox.Math;
import Toybox.System;
import Toybox.Time;
import Toybox.Time.Gregorian;
import Toybox.WatchUi;

// Zifferblatt "Ultra" nach Apples Modular Ultra, rund umgesetzt. Fest: oben Datum und das Urteil des Coaches als Kapsel, die
// große Uhrzeit in einer Zeile, am Rand zwei Skalen (links die Kalorien von heute gegen das Tagesziel, rechts der Akku),
// unten drei runde Anzeigen mit den offenen Wochenminuten je Sportart und ganz unten die nächste Einheit. Der Bereich unter
// der Uhrzeit wechselt mit dem Tag: morgens (bis 10 Uhr) Schlaf, HRV und Ruhepuls, sonst der Wochenvergleich.
// Alle Maße sind Mockup-Pixel (454 x 454) und werden mit _s auf die Displaygröße skaliert.
// Daten kommen aus dem Speicher (vom Hintergrunddienst geschrieben), gezeichnet wird nur.
class IntervalsFaceView extends WatchUi.WatchFace {

    const FG = 0xF5F5F7;
    const SEC = 0xA1A1A6;     // Grau für Beschriftungen, heller als bei Apple: auf dem AMOLED sonst matt
    const TER = 0x48484A;
    const CARD = 0x1C1C1E;
    const GRID = 0x2C2C2E;    // Tageslinien hinter der Kurve
    const TICK = 0x323234;    // kurze Striche der Skalen
    const RING_BG = 0x2C2C2E; // Spur der Ringe unten
    const ACCENT = 0x64D2FF;  // die eine Akzentfarbe (Woche, Kalorien), wie beim Ultra
    const OK = 0x30D158;
    const WARN = 0xFF9F0A;
    const BAD = 0xFF453A;
    const COACH = 0x64A8FF;   // Coach ohne Urteil (wie im Widget)
    const STALE_SECS = 3600;
    const MORNING_END = 10;      // bis zu dieser Stunde die Erholungswerte
    const BAT_LOW = 20;          // Akku in Prozent, ab dem er orange wird (ab der Hälfte davon rot)
    // Grundlinien und Mitten (Mockup-Pixel)
    const DATE_Y = 66;
    const PILL_Y = 76;
    const PILL_H = 24;
    const TIME_Y = 155;
    const HEAD_Y = 224;
    // Skalen am Rand: Radius außen, Länge der langen und kurzen Striche, Winkel (Grad gegen den Uhrzeigersinn ab 3 Uhr)
    const SCALE_R = 218;
    const SCALE_LONG = 17;
    const SCALE_SHORT = 10;
    const SCALE_N = 25;
    // Wochenkurve
    const G_X0 = 94;
    const G_X1 = 360;
    const G_TOP = 244;
    const G_BOTTOM = 312;
    const DAYS_Y = 334;
    const GRAD = 8;           // Streifen für den Verlauf der Fläche
    // Kacheln morgens (Oberkante, Höhe, Breite, Abstand der Mitten, Eckenradius) und Grundlinien darin ab Oberkante
    const K_Y = 234;
    const K_H = 106;
    const K_W = 108;
    const K_DX = 112;
    const K_R = 20;
    const K_TAG = 25;
    const K_VAL = 67;
    const K_BAND = 79;
    const K_BASE = 101;
    // Runde Anzeigen unten (Mitte, Abstand, Radius, Ringstärke)
    const RING_Y = 378;
    const RING_DX = 82;
    const RING_R = 34;
    const RING_W = 6;
    const NEXT_Y = 436;

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
        // Bitmap-Schriften aus Inter (erzeugt mit tools/make_font.py, Größen für 416 Pixel; Beschriftungen nicht unter 17 Pixel,
        // sonst auf der Uhr schlecht lesbar)
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
        drawScales(dc, data);
        drawTopLine(dc, data);
        dc.setColor(FG, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx, px(TIME_Y), _time, timeText(clock), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
        if (data == null) {
            dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
            dc.drawText(_cx, px(K_Y + K_H / 2), _cap, "Keine Daten" + errSuffix(), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
            return;
        }

        if (clock.hour < MORNING_END) {
            drawMorning(dc, data);
        } else {
            drawWeek(dc, data);
        }
        drawWeekRow(dc, data.get("rz") as Array?);
        drawNext(dc, data, NEXT_Y);
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

    // ---- Skalen am Rand ----------------------------------------------------------------------
    // Wie die Seitenskalen des Ultra: je 25 Striche, jeder sechste lang. Links die Kalorien von heute gegen das Tagesziel
    // (kcal: {v, g}, live aus Yazio), von unten gefüllt in der Akzentfarbe, über dem Ziel voll; ohne Ziel bleibt sie leer. Rechts der Akku
    // (grau, orange ab BAT_LOW, rot ab der Hälfte davon, grün beim Laden).

    private function drawScales(dc as Dc, data as Dictionary?) as Void {
        var kcal = data != null ? data.get("kcal") as Dictionary? : null;
        var food = kcal != null ? frac(numOrNull(kcal.get("v")), numOrNull(kcal.get("g"))) : 0.0;
        drawScale(dc, 215, 145, food, ACCENT);
        var stats = System.getSystemStats();
        var pct = Math.round(stats.battery).toNumber();
        var bat = stats.charging ? OK : (pct <= BAT_LOW / 2 ? BAD : (pct <= BAT_LOW ? WARN : SEC));
        drawScale(dc, 325, 395, pct / 100.0, bat);
    }

    // Striche von Winkel a0 (unten) nach a1 (oben), bis zum Anteil f in color
    private function drawScale(dc as Dc, a0 as Number, a1 as Number, f as Float, color as Number) as Void {
        var r1 = px(SCALE_R);
        for (var i = 0; i < SCALE_N; i++) {
            var a = Math.toRadians(a0 + (a1 - a0) * i / (SCALE_N - 1.0));
            var long = i % 6 == 0;
            var r2 = r1 - px(long ? SCALE_LONG : SCALE_SHORT);
            var on = f > 0 && i <= Math.round(f * (SCALE_N - 1));
            dc.setColor(on ? color : (long ? TER : TICK), Graphics.COLOR_TRANSPARENT);
            dc.setPenWidth(long ? px(3) : px(2));
            var c = Math.cos(a);
            var sn = Math.sin(a);
            dc.drawLine(_cx + (r1 * c).toNumber(), _cy - (r1 * sn).toNumber(), _cx + (r2 * c).toNumber(), _cy - (r2 * sn).toNumber());
        }
    }

    // ---- Oben: Datum, Urteil des Coaches ----------------------------------------------------
    // "SO, 25. OKT.", darunter das Urteil (go aus save_coach) als Wort in einer Kapsel seiner Farbe. Eine eigene Ampel hat die Uhr
    // nicht. Daten älter als eine Stunde: statt des Urteils ein oranges "!" mit dem Fehlercode des letzten Abrufs.

    private function drawTopLine(dc as Dc, data as Dictionary?) as Void {
        var info = Gregorian.info(Time.now(), Time.FORMAT_SHORT);
        var days = ["SO", "MO", "DI", "MI", "DO", "FR", "SA"];
        var months = ["JAN", "FEB", "MÄRZ", "APR", "MAI", "JUNI", "JULI", "AUG", "SEPT", "OKT", "NOV", "DEZ"];
        var date = days[(info.day_of_week as Number) - 1] + ", " + info.day + ". " + months[(info.month as Number) - 1] + ".";
        drawItems(dc, _cx, px(DATE_Y), [["t", date, FG, _tag]], 0);
        if (data == null) {
            return;
        }
        var word = null;
        var color = SEC;
        if (isStale(data)) {
            word = "!" + errSuffix();
            color = WARN;
        } else {
            var rd = data.get("rd");
            if ("ok".equals(rd)) { word = "HART"; color = OK; }
            else if ("warn".equals(rd)) { word = "LOCKER"; color = WARN; }
            else if ("bad".equals(rd)) { word = "FREI"; color = BAD; }
            else if (numOrNull(data.get("co")) != null && (data.get("co") as Number) == 1) { word = "COACH"; color = COACH; }
        }
        if (word == null) {
            return;
        }
        var w = dc.getTextWidthInPixels(word, _tag) + px(20);
        var h = px(PILL_H);
        dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.28), Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(_cx - w / 2, px(PILL_Y), w, h, h / 2);
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx, px(PILL_Y) + h / 2, _tag, word, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
    }

    // ---- Morgens: Erholung --------------------------------------------------------------------
    // Drei Kacheln mit Rahmen: Schlaf, HRV und Ruhepuls von heute (rv: [[Wert, Basis, Vergleich, [p10, p25, p75, p90]], ...]).
    // Rahmen, Tönung und Wert in der Farbe des Vergleichs mit dem eigenen üblichen Bereich (grün im Bereich oder besser, orange
    // schlechter, rot schlechter als 9 von 10 Tagen). Darunter ein Band: heller Streifen = dein üblicher Bereich (p25 bis p75),
    // Punkt = heute; ganz unten die Basis (Ø). Ein Gesamturteil gibt es nicht, das ist das Wort des Coaches oben.

    private function drawMorning(dc as Dc, data as Dictionary) as Void {
        drawItems(dc, _cx, px(HEAD_Y), [["t", "ERHOLUNG", SEC, _tag]], 0);
        var rv = data.get("rv") as Array?;
        var labels = ["SCHLAF", "HRV", "RUHEPULS"];
        for (var k = 0; k < 3; k++) {
            var mid = _cx + px(K_DX * (k - 1));
            var e = rv != null && rv.size() > k ? rv[k] as Array : null;
            var v = e != null ? numOrNull(e[0]) : null;
            var base = e != null ? numOrNull(e[1]) : null;
            var cls = e != null ? numOrNull(e[2]) : null;
            var band = e != null && e.size() > 3 && e[3] instanceof Lang.Array ? e[3] as Array : null;
            var color = SEC;
            if (cls != null && cls.toNumber() == 1) { color = OK; }
            else if (cls != null && cls.toNumber() == 2) { color = WARN; }
            else if (cls != null && cls.toNumber() == 3) { color = BAD; }
            var x = mid - px(K_W) / 2;
            var y = px(K_Y);
            var w = px(K_W);
            var h = px(K_H);
            dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.1), Graphics.COLOR_TRANSPARENT);
            dc.fillRoundedRectangle(x, y, w, h, px(K_R));
            dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.55), Graphics.COLOR_TRANSPARENT);
            dc.setPenWidth(px(2) > 1 ? px(2) : 1);
            dc.drawRoundedRectangle(x, y, w, h, px(K_R));
            drawItems(dc, mid, y + px(K_TAG), [["t", labels[k] as String, SEC, _tag]], 0);
            var text = v == null ? "--" : (k == 0 ? hours((v * 60).toNumber()) : v.toNumber().toString());
            drawItems(dc, mid, y + px(K_VAL), [["t", text, v == null ? TER : (color == SEC ? FG : color), _val]], 0);
            if (v != null && band != null && band.size() == 4) {
                drawBand(dc, x + px(14), x + w - px(14), y + px(K_BAND), v, band, color);
            }
            if (base != null) {
                drawItems(dc, mid, y + px(K_BASE), [["t", "Ø " + (k == 0 ? hours((base * 60).toNumber()) : base.toNumber().toString()), SEC, _tag]], 0);
            }
        }
    }

    // Band von x0 bis x1: Spur von p10 bis p90, darauf der übliche Bereich (p25 bis p75) heller, der Punkt für v (am Rand begrenzt)
    private function drawBand(dc as Dc, x0 as Number, x1 as Number, y as Number, v as Numeric, band as Array, color as Number) as Void {
        var p10 = numOrNull(band[0]);
        var p25 = numOrNull(band[1]);
        var p75 = numOrNull(band[2]);
        var p90 = numOrNull(band[3]);
        if (p10 == null || p25 == null || p75 == null || p90 == null || p90 <= p10) {
            return;
        }
        var h = px(5) > 2 ? px(5) : 2;
        var w = x1 - x0;
        dc.setColor(RING_BG, Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(x0, y, w, h, h / 2);
        var a = x0 + (w * frac(p25 - p10, p90 - p10)).toNumber();
        var b = x0 + (w * frac(p75 - p10, p90 - p10)).toNumber();
        dc.setColor(blend(RING_BG, color, 0.5), Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(a, y, b - a > h ? b - a : h, h, h / 2);
        var dx = x0 + (w * frac(v - p10, p90 - p10)).toNumber();
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(dx, y + h / 2, px(6));
    }

    // ---- Tagsüber: Wochenvergleich -------------------------------------------------------------
    // Ausdauerminuten aufsummiert Mo bis So (wv: [diese Woche bis heute, Vorwoche], Minuten je Tag): diese Woche als Fläche mit
    // Linie in der Akzentfarbe bis zum Punkt auf heute, die Vorwoche als graue Linie, das Zeitziel des Coaches (wm[1])
    // gestrichelt. Kopf: Stand der Woche und der Abstand zur Vorwoche am selben Wochentag. Ein Urteil ist das nicht.

    private function drawWeek(dc as Dc, data as Dictionary) as Void {
        var wv = data.get("wv") as Array?;
        var wm = data.get("wm") as Array?;
        if (wv == null || wv.size() != 2) {
            return;
        }
        var cur = cumulate(wv[0] as Array);
        var last = cumulate(wv[1] as Array);
        var today = cur.size() - 1;
        if (today < 0 || last.size() != 7) {
            return;
        }
        var goal = wm != null && wm.size() == 2 ? numOrNull(wm[1]) : null;
        var now = cur[today] as Number;
        var diff = now - (last[today < 6 ? today : 6] as Number);
        drawItems(dc, _cx, px(HEAD_Y), [
            ["t", hours(now), ACCENT, _cap],
            ["t", "  " + (diff < 0 ? "−" : "+") + hours(diff < 0 ? -diff : diff), FG, _cap],
            ["t", "  ZUR VORWOCHE", SEC, _tag]
        ], 0);

        var most = 60.0;
        for (var i = 0; i < 7; i++) {
            most = (last[i] as Number) > most ? (last[i] as Number).toFloat() : most;
        }
        most = now > most ? now.toFloat() : most;
        if (goal != null && goal > most) {
            most = goal.toFloat();
        }
        most *= 1.05;
        var x0 = px(G_X0);
        var x1 = px(G_X1);
        var yb = px(G_BOTTOM);
        var h = yb - px(G_TOP);
        dc.setColor(GRID, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(1);
        for (var i = 0; i < 7; i++) {
            var x = gx(i, x0, x1);
            dc.drawLine(x, px(G_TOP) - px(6), x, yb);
        }
        // Ziel des Coaches gestrichelt, rechts daneben der Wert
        if (goal != null && goal > 0) {
            var y = yb - (h * goal / most).toNumber();
            dc.setColor(0x6E6E73, Graphics.COLOR_TRANSPARENT);
            dc.setPenWidth(px(2));
            for (var x = x0; x < x1; x += px(9)) {
                dc.drawLine(x, y, (x + px(5) < x1 ? x + px(5) : x1), y);
            }
            drawItems(dc, x1 + px(6), y + px(6), [["t", hours(goal.toNumber()), SEC, _tag]], -1);
        }
        // Diese Woche: Fläche bis heute, nach unten ausblendend (Verlauf in GRAD Streifen, je Streifen die Fläche beschnitten)
        if (today > 0) {
            var poly = new [today + 3];
            var top = yb;
            for (var i = 0; i <= today; i++) {
                var y = yb - (h * (cur[i] as Number) / most).toNumber();
                poly[i] = [gx(i, x0, x1), y];
                top = y < top ? y : top;
            }
            poly[today + 1] = [gx(today, x0, x1), yb];
            poly[today + 2] = [x0, yb];
            var band = (yb - top) / GRAD + 1;
            for (var k = 0; k < GRAD; k++) {
                dc.setClip(x0, top + k * band, x1 - x0 + 1, band);
                dc.setColor(blend(Graphics.COLOR_BLACK, ACCENT, 0.42 - 0.36 * k / (GRAD - 1)), Graphics.COLOR_TRANSPARENT);
                dc.fillPolygon(poly);
            }
            dc.clearClip();
        }
        // Vorwoche grau, darüber diese Woche
        dc.setColor(0x8E8E93, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(2));
        for (var i = 0; i < 6; i++) {
            dc.drawLine(gx(i, x0, x1), yb - (h * (last[i] as Number) / most).toNumber(), gx(i + 1, x0, x1), yb - (h * (last[i + 1] as Number) / most).toNumber());
        }
        dc.setColor(ACCENT, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(3));
        for (var i = 0; i < today; i++) {
            dc.drawLine(gx(i, x0, x1), yb - (h * (cur[i] as Number) / most).toNumber(), gx(i + 1, x0, x1), yb - (h * (cur[i + 1] as Number) / most).toNumber());
        }
        var tx = gx(today, x0, x1);
        var ty = yb - (h * now / most).toNumber();
        // Punkt auf heute mit Schein
        dc.setColor(blend(Graphics.COLOR_BLACK, ACCENT, 0.25), Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(tx, ty, px(13));
        dc.setColor(blend(Graphics.COLOR_BLACK, ACCENT, 0.5), Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(tx, ty, px(10));
        dc.setColor(ACCENT, Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(tx, ty, px(7));
        dc.setColor(blend(ACCENT, FG, 0.6), Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(tx, ty, px(3));
        var names = ["MO", "DI", "MI", "DO", "FR", "SA", "SO"];
        for (var i = 0; i < 7; i++) {
            drawItems(dc, gx(i, x0, x1), px(DAYS_Y), [["t", names[i] as String, i == today ? FG : SEC, _tag]], 0);
        }
    }

    // x des Tages i zwischen x0 und x1
    private function gx(i as Number, x0 as Number, x1 as Number) as Number {
        return x0 + (x1 - x0) * i / 6;
    }

    // Minuten je Tag aufsummiert ([45, 0, 30] -> [45, 45, 75])
    private function cumulate(mins as Array) as Array<Number> {
        var out = [] as Array<Number>;
        var sum = 0;
        for (var i = 0; i < mins.size() && i < 7; i++) {
            var v = numOrNull(mins[i]);
            sum += v != null ? v.toNumber() : 0;
            out.add(sum);
        }
        return out;
    }

    // ---- Untere Reihe: offene Wochenminuten -----------------------------------------------------
    // Drei runde Anzeigen wie bei Apple: je Sportart in der Mitte groß in Sportfarbe die Minuten bis zum Zeitziel des Coaches
    // (rz: [[Sportart, erledigt, Ziel], ...]), außen ein Ring mit Lücke unten, der sich zum Ziel füllt. Keine Beschriftung, die
    // Farbe sagt die Sportart (Lauf, Rad, Schwimmen von links). Ziel erreicht: Haken; ohne Ziel "--" in Grau.

    private function drawWeekRow(dc as Dc, rz as Array?) as Void {
        var sports = ["run", "bike", "swim"];
        var y = px(RING_Y);
        var r = px(RING_R);
        var pen = px(RING_W) > 2 ? px(RING_W) : 2;
        var rr = r - pen / 2 - px(3); // Mitte des Rings, innen im Kreis
        for (var k = 0; k < sports.size(); k++) {
            var x = _cx + px(RING_DX * (k - 1));
            var color = sportColor(sports[k]);
            dc.setColor(CARD, Graphics.COLOR_TRANSPARENT);
            dc.fillCircle(x, y, r);
            drawGauge(dc, x, y, rr, pen, RING_BG, 1.0);
            var e = goalOf(rz, sports[k]);
            if (e == null) {
                drawItems(dc, x, y + px(8), [["t", "--", TER, _cap]], 0);
                continue;
            }
            var done = (e[1] as Numeric).toNumber();
            var goal = (e[2] as Numeric).toNumber();
            var open = goal - done;
            drawGauge(dc, x, y, rr, pen, color, frac(done, goal));
            if (open > 0) {
                drawItems(dc, x, y + px(8), [["t", open.toString(), color, _cap]], 0);
            } else {
                drawCheck(dc, x, y, color);
            }
        }
    }

    // Ring mit Lücke unten (270 Grad, im Uhrzeigersinn von links unten), zum Anteil f gefüllt, mit runden Enden
    private function drawGauge(dc as Dc, x as Number, y as Number, r as Number, pen as Number, color as Number, f as Float) as Void {
        if (f <= 0.0) {
            return;
        }
        var start = 225;
        var sweep = (270 * f).toNumber();
        var stop = start - sweep;
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(pen);
        if (sweep > 0) {
            dc.drawArc(x, y, r, Graphics.ARC_CLOCKWISE, start, stop < 0 ? stop + 360 : stop);
        }
        var a1 = Math.toRadians(start);
        var a2 = Math.toRadians(stop);
        dc.fillCircle(x + (r * Math.cos(a1)).toNumber(), y - (r * Math.sin(a1)).toNumber(), pen / 2);
        dc.fillCircle(x + (r * Math.cos(a2)).toNumber(), y - (r * Math.sin(a2)).toNumber(), pen / 2);
    }

    // ---- Zeile ganz unten ----------------------------------------------------------------------

    // Nächste Einheit (next: d Tage ab heute, s Sportart, m Minuten, km): "Heute Lauf 45'", "Morgen Rad 60'", sonst mit
    // Wochentag ("Sa Rad 90'")
    private function drawNext(dc as Dc, data as Dictionary, y as Number) as Void {
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
        drawItems(dc, _cx, px(y), [["t", when, SEC, _tag], ["t", what, sportColor(nx.get("s")), _tag]], 0);
    }

    // ---- Bausteine ------------------------------------------------------------------------------

    // Haken als Linien um die Mitte x, y, die Bitmap-Schriften haben kein ✓
    private function drawCheck(dc as Dc, x as Number, y as Number, color as Number) as Void {
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(4));
        dc.drawLine(x - px(9), y + px(1), x - px(2), y + px(8));
        dc.drawLine(x - px(2), y + px(8), x + px(11), y - px(7));
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
