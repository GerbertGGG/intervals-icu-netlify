import Toybox.Application;
import Toybox.Graphics;
import Toybox.Lang;
import Toybox.Math;
import Toybox.System;
import Toybox.Time;
import Toybox.Time.Gregorian;
import Toybox.WatchUi;

// Zifferblatt "Ultra" nach Apples Modular Ultra, rund umgesetzt. Fest: oben Datum und das Urteil des Coaches als Kapsel, die
// große Uhrzeit in einer Zeile, am Rand zwei Skalen (links die Ausdauer der Woche gegen das Ziel des Coaches, rechts der Akku),
// unten drei runde Anzeigen mit den offenen Wochenminuten je Sportart und ganz unten die nächste Einheit. Der Bereich unter
// der Uhrzeit wechselt mit dem Tag: morgens (bis 10 Uhr) Schlaf, HRV und Ruhepuls, tagsüber die Woche als Kurve, bis 3 Stunden
// nach einer Einheit deren Werte.
// Alle Maße sind Mockup-Pixel (454 x 454) und werden mit _s auf die Displaygröße skaliert.
// Daten kommen aus dem Speicher (vom Hintergrunddienst geschrieben), gezeichnet wird nur.
class IntervalsFaceView extends WatchUi.WatchFace {

    const FG = 0xF5F5F7;
    const SEC = 0x8E8E93;
    const TER = 0x48484A;
    const CARD = 0x1C1C1E;
    const GRID = 0x262628;    // Tageslinien hinter der Kurve
    const TICK = 0x323234;    // kurze Striche der Skalen
    const RING_BG = 0x2C2C2E; // Spur der Ringe unten
    const ACCENT = 0x64D2FF;  // die eine Akzentfarbe (Woche), wie beim Ultra
    const OK = 0x30D158;
    const WARN = 0xFF9F0A;
    const BAD = 0xFF453A;
    const COACH = 0x64A8FF;   // Coach ohne Urteil (wie im Widget)
    const STALE_SECS = 3600;
    const AFTER_SECS = 3 * 3600; // so lange nach dem Ende einer Einheit stehen ihre Werte
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
    const G_STEPS = 6;        // Zwischenpunkte je Tag für die weiche Kurve
    const DAYS_Y = 334;
    // Werte morgens und nach einer Einheit
    const V_Y = 278;
    const V_TAG_Y = 302;
    const V_BASE_Y = 322;
    const V_DX = 112;
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
            dc.drawText(_cx, px(V_Y), _cap, "Keine Daten" + errSuffix(), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
            return;
        }

        var la = data.get("la") as Dictionary?;
        var end = la != null ? numOrNull(la.get("e")) : null;
        var since = end != null ? Time.now().value() - end.toNumber() : -1;
        if (la != null && since >= 0 && since < AFTER_SECS) {
            drawLast(dc, la);
        } else if (clock.hour < MORNING_END) {
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
    // Wie die Seitenskalen des Ultra: je 25 Striche, jeder sechste lang. Links die Ausdauer der Woche gegen das Zeitziel des
    // Coaches (wm: [erledigt, Ziel] in Minuten), von unten gefüllt in der Akzentfarbe; ohne Ziel bleibt sie leer. Rechts der Akku
    // (grau, orange ab BAT_LOW, rot ab der Hälfte davon, grün beim Laden).

    private function drawScales(dc as Dc, data as Dictionary?) as Void {
        var wm = data != null ? data.get("wm") as Array? : null;
        var week = 0.0;
        if (wm != null && wm.size() == 2) {
            week = frac(numOrNull(wm[0]), numOrNull(wm[1]));
        }
        drawScale(dc, 215, 145, week, ACCENT);
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
    // "SO. 25. OKT.", darunter das Urteil (go aus save_coach) als Wort in einer Kapsel seiner Farbe. Eine eigene Ampel hat die Uhr
    // nicht. Daten älter als eine Stunde: statt des Urteils ein oranges "!" mit dem Fehlercode des letzten Abrufs.

    private function drawTopLine(dc as Dc, data as Dictionary?) as Void {
        var info = Gregorian.info(Time.now(), Time.FORMAT_SHORT);
        var days = ["SO", "MO", "DI", "MI", "DO", "FR", "SA"];
        var months = ["JAN", "FEB", "MÄRZ", "APR", "MAI", "JUNI", "JULI", "AUG", "SEPT", "OKT", "NOV", "DEZ"];
        var date = days[(info.day_of_week as Number) - 1] + ". " + info.day + ". " + months[(info.month as Number) - 1] + ".";
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
    // Schlaf, HRV und Ruhepuls von heute (rv: [[Wert, Basis, Vergleich], ...]), jeder Wert in der Farbe seines Vergleichs mit dem
    // eigenen üblichen Bereich (grün im Bereich oder besser, orange schlechter, rot schlechter als 9 von 10 Tagen), darunter die
    // Basis (Ø). Ein Gesamturteil gibt es nicht, das ist das Wort des Coaches oben.

    private function drawMorning(dc as Dc, data as Dictionary) as Void {
        drawItems(dc, _cx, px(HEAD_Y), [["t", "ERHOLUNG", SEC, _tag]], 0);
        var rv = data.get("rv") as Array?;
        var labels = ["SCHLAF", "HRV", "RUHEPULS"];
        for (var k = 0; k < 3; k++) {
            var x = _cx + px(V_DX * (k - 1));
            var e = rv != null && rv.size() > k ? rv[k] as Array : null;
            var v = e != null ? numOrNull(e[0]) : null;
            var base = e != null ? numOrNull(e[1]) : null;
            var cls = e != null ? numOrNull(e[2]) : null;
            var color = FG;
            if (cls != null && cls.toNumber() == 1) { color = OK; }
            else if (cls != null && cls.toNumber() == 2) { color = WARN; }
            else if (cls != null && cls.toNumber() == 3) { color = BAD; }
            var text = v == null ? "--" : (k == 0 ? hours((v * 60).toNumber()) : v.toNumber().toString());
            drawItems(dc, x, px(V_Y), [["t", text, v == null ? TER : color, _val]], 0);
            drawItems(dc, x, px(V_TAG_Y), [["t", labels[k] as String, SEC, _tag]], 0);
            if (base != null) {
                drawItems(dc, x, px(V_BASE_Y), [["t", "Ø " + (k == 0 ? hours((base * 60).toNumber()) : base.toNumber().toString()), TER, _tag]], 0);
            }
        }
    }

    // ---- Tagsüber: die Woche als Kurve ------------------------------------------------------------
    // Kopf: Ausdauer der Woche gegen das Ziel des Coaches (wm). Darunter wie die Gezeitenkurve des Ultra die Minuten je Tag Mo bis
    // So (wd: [[Sportart, Minuten, 1 = geplant], ...]): bis heute das Erledigte als Fläche mit Linie, ab heute das Geplante
    // gestrichelt, ein Punkt auf heute (ti).

    private function drawWeek(dc as Dc, data as Dictionary) as Void {
        var wm = data.get("wm") as Array?;
        if (wm != null && wm.size() == 2 && numOrNull(wm[0]) != null) {
            var head = [["t", hours((wm[0] as Numeric).toNumber()), ACCENT, _cap]] as Array<Array>;
            if (numOrNull(wm[1]) != null && (wm[1] as Numeric) > 0) {
                head.add(["t", " / " + hours((wm[1] as Numeric).toNumber()), SEC, _cap]);
            }
            head.add(["t", "  WOCHE", SEC, _tag]);
            drawItems(dc, _cx, px(HEAD_Y), head, 0);
        }
        var wd = data.get("wd") as Array?;
        if (wd == null || wd.size() != 7) {
            return;
        }
        var ti = data.get("ti");
        var today = ti instanceof Lang.Number ? ti as Number : 6;
        // Minuten je Tag: bis heute erledigt, danach geplant
        var mins = new [7];
        var most = 60;
        for (var i = 0; i < 7; i++) {
            var m = dayMinutes(wd[i] as Array, i > today);
            mins[i] = m;
            most = m > most ? m : most;
        }
        var x0 = px(G_X0);
        var x1 = px(G_X1);
        var yb = px(G_BOTTOM);
        var h = yb - px(G_TOP);
        dc.setColor(GRID, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(1);
        for (var i = 0; i < 7; i++) {
            var x = x0 + (x1 - x0) * i / 6;
            dc.drawLine(x, px(G_TOP) - px(6), x, yb);
        }
        // Weiche Kurve: zwischen zwei Tagen mit Kosinus übergeblendet
        var n = 6 * G_STEPS + 1;
        var xs = new [n];
        var ys = new [n];
        for (var k = 0; k < n; k++) {
            var i = k / G_STEPS;
            var t = (k % G_STEPS) / G_STEPS.toFloat();
            var a = mins[i] as Number;
            var b = i < 6 ? mins[i + 1] as Number : a;
            var m = a + (b - a) * (1 - Math.cos(Math.PI * t)) / 2;
            xs[k] = x0 + (x1 - x0) * k / (n - 1);
            ys[k] = yb - (h * m / most).toNumber();
        }
        var cut = today * G_STEPS;
        if (cut > 0) {
            var poly = new [cut + 3];
            for (var k = 0; k <= cut; k++) {
                poly[k] = [xs[k], ys[k]];
            }
            poly[cut + 1] = [xs[cut], yb];
            poly[cut + 2] = [x0, yb];
            dc.setColor(blend(Graphics.COLOR_BLACK, ACCENT, 0.28), Graphics.COLOR_TRANSPARENT);
            dc.fillPolygon(poly);
            dc.setColor(ACCENT, Graphics.COLOR_TRANSPARENT);
            dc.setPenWidth(px(3));
            for (var k = 0; k < cut; k++) {
                dc.drawLine(xs[k], ys[k], xs[k + 1], ys[k + 1]);
            }
        }
        dc.setColor(blend(Graphics.COLOR_BLACK, ACCENT, 0.5), Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(2));
        for (var k = cut; k < n - 1; k += 2) {
            dc.drawLine(xs[k], ys[k], xs[k + 1], ys[k + 1]);
        }
        var tx = xs[cut] as Number;
        var ty = ys[cut] as Number;
        dc.setColor(FG, Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(tx, ty, px(8));
        dc.setColor(Graphics.COLOR_BLACK, Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(tx, ty, px(4));
        var names = ["MO", "DI", "MI", "DO", "FR", "SA", "SO"];
        for (var i = 0; i < 7; i++) {
            drawItems(dc, x0 + (x1 - x0) * i / 6, px(DAYS_Y), [["t", names[i] as String, i == today ? FG : SEC, _tag]], 0);
        }
    }

    // Minuten eines Tages, erledigt (planned false) oder geplant (planned true)
    private function dayMinutes(segs as Array, planned as Boolean) as Number {
        var t = 0;
        for (var k = 0; k < segs.size(); k++) {
            var e = segs[k] as Array;
            if (((e[2] as Numeric).toNumber() == 1) == planned) {
                t += (e[1] as Numeric).toNumber();
            }
        }
        return t;
    }

    // ---- Nach einer Einheit -------------------------------------------------------------------
    // Kopf in Sportfarbe: Sportart mit Startzeit, daneben "LÄNGSTE SEIT N WO." (Vergleich mit den eigenen Einheiten derselben
    // Sportart). Darunter bis zu vier Werte (c: [[Text, Beschriftung], ...]) nebeneinander.

    private function drawLast(dc as Dc, la as Dictionary) as Void {
        var color = sportColor(la.get("s"));
        var t = la.get("t");
        var head = [["t", sportName(la.get("s")).toUpper() + (t instanceof Lang.String ? " " + t : ""), color, _tag]] as Array<Array>;
        var lw = numOrNull(la.get("lw"));
        if (lw != null) {
            head.add(["t", "  LÄNGSTE SEIT " + lw.toNumber().toString() + " WO.", SEC, _tag]);
        }
        drawItems(dc, _cx, px(HEAD_Y), head, 0);
        var c = la.get("c") as Array?;
        if (c == null) {
            return;
        }
        var n = c.size() < 4 ? c.size() : 4;
        var dx = n == 4 ? 92 : V_DX;
        for (var i = 0; i < n; i++) {
            var cell = c[i] as Array;
            var x = _cx + px(dx * (2 * i - (n - 1)) / 2);
            drawItems(dc, x, px(V_Y), [["t", cell[0] as String, FG, _val]], 0);
            drawItems(dc, x, px(V_TAG_Y), [["t", (cell[1] as String).toUpper(), SEC, _tag]], 0);
        }
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
