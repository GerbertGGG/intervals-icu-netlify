import Toybox.Application;
import Toybox.Graphics;
import Toybox.Lang;
import Toybox.Math;
import Toybox.System;
import Toybox.Time;
import Toybox.Time.Gregorian;
import Toybox.WatchUi;

// Zifferblatt "Intervals Ultimate & Readiness"
// Angepasst für 454x454 Displays. Symmetrisch, mit Glow-Effekten & optimiertem Readiness-Bereich.
class IntervalsFaceView extends WatchUi.WatchFace {

    const FG = 0xF5F5F7;
    const SEC = 0xA1A1A6;     // Grau für Beschriftungen
    const TER = 0x48484A;
    const CARD = 0x1C1C1E;
    const GRID = 0x2C2C2E;    // Rasterlinien
    const TICK = 0x323234;    
    const RING_BG = 0x2C2C2E; // Inaktive Ringbahnen
    
    // Leuchtende/Glow-Farben
    const ACCENT = 0x64D2FF;  // Cyan (Woche)
    const PROTEIN = 0xC4F0FF; // Kalorien und Eiweiß in derselben hellen Cyan-Farbe!
    const OK = 0x30D158;      // Leuchtendes Grün
    const WARN = 0xFF9F0A;     // Orange/Gelb
    const BAD = 0xFF453A;      // Rot
    const COACH = 0x64A8FF;   // Blau
    
    const STALE_SECS = 3600;
    const MORNING_END = 10;   // Ab 10 Uhr wird zum Wochenverlauf gewechselt
    const BAT_LOW = 20;

    // Koordinaten-Skalierungen (basierend auf 454x454)
    const DATE_Y = 66;
    const PILL_Y = 76;
    const PILL_H = 24;
    const TIME_Y = 145;       // Uhrzeit
    const HEAD_Y = 224;       // Bereichs-Kopf

    // Neue Readiness-Kacheln (Morgens)
    const K_Y = 236;
    const K_H = 92;           // Kompakter
    const K_W = 112;          // Breiter für Pillen-Optik
    const K_DX = 120;         // Abstand
    const K_R = 18;           // Eckenradius

    // Skalen
    const SCALE_R = 218;
    const SCALE_LONG = 17;
    const SCALE_SHORT = 10;
    const SCALE_N = 25;

    // Wochenverlauf (Tagsüber)
    const G_X0 = 94;
    const G_X1 = 360;
    const G_TOP = 240;
    const G_BOTTOM = 308;
    const DAYS_Y = 328;
    const GRAD = 6;

    // Ringe unten
    const RING_Y = 378;
    const RING_DX = 82;
    const RING_R = 34;
    const RING_W = 6;
    const NEXT_Y = 434;

    private var _s as Float = 1.0;
    private var _cx as Number = 0;
    private var _cy as Number = 0;
    private var _sleep as Boolean = false;

    // Fonts
    private var _time as Graphics.FontType = Graphics.FONT_NUMBER_HOT;
    private var _val as Graphics.FontType = Graphics.FONT_SMALL;
    private var _cap as Graphics.FontType = Graphics.FONT_XTINY;
    private var _tag as Graphics.FontType = Graphics.FONT_XTINY;

    function initialize() {
        WatchFace.initialize();
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
        if (_sleep) {
            dc.setColor(SEC, Graphics.COLOR_BLACK);
            dc.drawText(_cx, _cy, Graphics.FONT_NUMBER_MILD, timeText(clock), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
            return;
        }

        var data = Application.Storage.getValue("d") as Dictionary?;
        drawScales(dc, data);
        drawTopLine(dc, data);
        
        // Glimmen / Leuchten der Uhrzeit (simulierter Schattenwurf über 2 kleine Offset-Zeichnungen)
        dc.setColor(blend(Graphics.COLOR_BLACK, FG, 0.15), Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx, px(TIME_Y) + 1, _time, timeText(clock), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
        dc.setColor(FG, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx, px(TIME_Y), _time, timeText(clock), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);

        if (data == null) {
            dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
            dc.drawText(_cx, px(K_Y + K_H / 2), _cap, "Keine Daten" + errSuffix(), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
            return;
        }

        if (clock.hour < MORNING_END) {
            drawMorningReady(dc, data);
        } else {
            drawWeekGraph(dc, data);
        }
        drawWeekRow(dc, data.get("rz") as Array?);
        drawNext(dc, data, NEXT_Y);
    }

    private function timeText(clock as System.ClockTime) as String {
        var h = clock.hour;
        if (!System.getDeviceSettings().is24Hour) {
            h = h % 12;
            if (h == 0) { h = 12; }
        }
        return h.toString() + ":" + clock.min.format("%02d");
    }

    // ---- Skalen am Rand (Jetzt BEIDE in PROTEIN/Cyan-Farbe!) -------------------------------
    private function drawScales(dc as Dc, data as Dictionary?) as Void {
        var kcal = data != null ? data.get("kcal") as Dictionary? : null;
        var food = kcal != null ? frac(numOrNull(kcal.get("v")), numOrNull(kcal.get("g"))) : 0.0;
        // Kalorien-Anzeige auf PROTEIN (helles Cyan) geändert
        drawScale(dc, 215, 145, food, PROTEIN, px(SCALE_R), SCALE_LONG, SCALE_SHORT);
        
        var pr = data != null ? data.get("pr") as Dictionary? : null;
        var protein = pr != null ? frac(numOrNull(pr.get("v")), numOrNull(pr.get("g"))) : 0.0;
        drawScale(dc, 325, 395, protein, PROTEIN, px(SCALE_R), SCALE_LONG, SCALE_SHORT);
    }

    private function drawScale(dc as Dc, a0 as Number, a1 as Number, f as Float, color as Number, r1 as Number, longLen as Number, shortLen as Number) as Void {
        for (var i = 0; i < SCALE_N; i++) {
            var a = Math.toRadians(a0 + (a1 - a0) * i / (SCALE_N - 1.0));
            var long = i % 6 == 0;
            var r2 = r1 - px(long ? longLen : shortLen);
            var on = f > 0 && i <= Math.round(f * (SCALE_N - 1));
            dc.setColor(on ? color : (long ? TER : TICK), Graphics.COLOR_TRANSPARENT);
            dc.setPenWidth(long ? px(3) : px(2));
            var c = Math.cos(a);
            var sn = Math.sin(a);
            dc.drawLine(_cx + (r1 * c).toNumber(), _cy - (r1 * sn).toNumber(), _cx + (r2 * c).toNumber(), _cy - (r2 * sn).toNumber());
        }
    }

    // ---- Oben: Datum & Coach ----------------------------------------------------
    private function drawTopLine(dc as Dc, data as Dictionary?) as Void {
        var info = Gregorian.info(Time.now(), Time.FORMAT_SHORT);
        var days = ["SO", "MO", "DI", "MI", "DO", "FR", "SA"];
        var months = ["JAN", "FEB", "MÄRZ", "APR", "MAI", "JUNI", "JULI", "AUG", "SEPT", "OKT", "NOV", "DEZ"];
        var date = days[(info.day_of_week as Number) - 1] + ", " + info.day + ". " + months[(info.month as Number) - 1] + ".";
        drawDateAndBattery(dc, date);
        if (data == null) { return; }
        
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
        if (word == null) { return; }
        
        var w = dc.getTextWidthInPixels(word, _tag) + px(20);
        var h = px(PILL_H);
        dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.28), Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(_cx - w / 2, px(PILL_Y), w, h, h / 2);
        
        // Simulierter Glow-Rand
        dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.6), Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(1);
        dc.drawRoundedRectangle(_cx - w / 2, px(PILL_Y), w, h, h / 2);
        
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx, px(PILL_Y) + h / 2, _tag, word, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
    }

    private function drawDateAndBattery(dc as Dc, date as String) as Void {
        var stats = System.getSystemStats();
        var pct = Math.round(stats.battery).toNumber();
        var color = stats.charging ? OK : (pct <= BAT_LOW / 2 ? BAD : (pct <= BAT_LOW ? WARN : SEC));
        var text = pct.toString() + " %";
        var dateW = dc.getTextWidthInPixels(date, _tag);
        var textW = dc.getTextWidthInPixels(text, _tag);
        var iconW = px(26);
        var iconH = px(13);
        var gap = px(10);
        var total = dateW + gap + iconW + px(6) + textW;
        var x = _cx - total / 2;
        var base = px(DATE_Y);
        drawItems(dc, x, base, [["t", date, FG, _tag]], -1);
        
        var ix = x + dateW + gap;
        var iy = base - iconH - px(1);
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(2) > 1 ? px(2) : 1);
        dc.drawRoundedRectangle(ix, iy, iconW, iconH, px(3));
        var inner = (iconW - px(6)) * pct / 100;
        if (inner > 0) {
            dc.fillRectangle(ix + px(3), iy + px(3), inner, iconH - px(6));
        }
        dc.fillRectangle(ix + iconW + 1, iy + iconH / 4, px(2), iconH / 2);
        drawItems(dc, ix + iconW + px(6), base, [["t", text, color, _tag]], -1);
    }

    // ---- NEU: Readiness / Erholung am Morgen im eleganten Glow-Layout -----------------------
    private function drawMorningReady(dc as Dc, data as Dictionary) as Void {
        drawItems(dc, _cx, px(HEAD_Y), [["t", "ERHOLUNG", SEC, _tag]], 0);
        var rv = data.get("rv") as Array?;
        var labels = ["SCHLAF", "HRV", "PULS"];
        var symbols = ["S", "H", "P"]; // ☾ Schlaf als "S", "H" für HRV, "P" für Puls
        
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

            // Subtiler, tönender Glassmorphism & Glow im Hintergrund
            dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.08), Graphics.COLOR_TRANSPARENT);
            dc.fillRoundedRectangle(x, y, w, h, px(K_R));
            
            // Leuchtender Rahmen
            dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.45), Graphics.COLOR_TRANSPARENT);
            dc.setPenWidth(px(2) > 1 ? px(2) : 1);
            dc.drawRoundedRectangle(x, y, w, h, px(K_R));

            // Name & Symbol
            var subColor = blend(color, FG, 0.5);
            drawItems(dc, mid, y + px(22), [["t", symbols[k] + " " + labels[k], subColor, _tag]], 0);

            // Wert-Text
            var text = v == null ? "--" : (k == 0 ? hoursReady((v * 60).toNumber()) : v.toNumber().toString() + (k == 1 ? " ms" : ""));
            drawItems(dc, mid, y + px(50), [["t", text, v == null ? TER : color, _val]], 0);

            // Werteband / Schieberegler (nur für HRV & Ruhepuls (k > 0) sowie Schlaf bei vorhandenen Daten)
            if (v != null && band != null && band.size() == 4) {
                drawCompactBand(dc, x + px(10), x + w - px(10), y + px(65), v, band, color);
            }

            // Durchschnitt (Ø)
            if (base != null) {
                var baseText = "Ø " + (k == 0 ? hoursReady((base * 60).toNumber()) : base.toNumber().toString());
                drawItems(dc, mid, y + px(80), [["t", baseText, SEC, _tag]], 0);
            }
        }
    }

    // Wandelt Minuten in kompaktes "7h 45m" für Readiness-Ansicht um
    private function hoursReady(min as Number) as String {
        return (min / 60).toString() + "h " + (min % 60).format("%02d") + "m";
    }

    // Reduziertes, feines Band mit Glow-Punkt für die Erholungs-Pillen
    private function drawCompactBand(dc as Dc, x0 as Number, x1 as Number, y as Number, v as Numeric, band as Array, color as Number) as Void {
        var p10 = numOrNull(band[0]);
        var p25 = numOrNull(band[1]);
        var p75 = numOrNull(band[2]);
        var p90 = numOrNull(band[3]);
        if (p10 == null || p25 == null || p75 == null || p90 == null || p90 <= p10) { return; }
        
        var h = px(4) > 1 ? px(4) : 1;
        var w = x1 - x0;
        dc.setColor(RING_BG, Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(x0, y, w, h, h / 2);
        
        var a = x0 + (w * frac(p25 - p10, p90 - p10)).toNumber();
        var b = x0 + (w * frac(p75 - p10, p90 - p10)).toNumber();
        dc.setColor(blend(RING_BG, color, 0.4), Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(a, y, b - a > h ? b - a : h, h, h / 2);
        
        // Glow-Punkt
        var dx = x0 + (w * frac(v - p10, p90 - p10)).toNumber();
        dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.3), Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(dx, y + h / 2, px(5));
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(dx, y + h / 2, px(3));
    }

    // ---- Wochenverlauf (Behobener "Leere Tage"-Look durch Schattenvorlauf) ---------------------
    private function drawWeekGraph(dc as Dc, data as Dictionary) as Void {
        var wv = data.get("wv") as Array?;
        var wm = data.get("wm") as Array?;
        if (wv == null || wv.size() != 2) { return; }
        
        var cur = cumulate(wv[0] as Array);
        var today = cur.size() - 1;
        if (today < 0) { return; }
        
        var goal = wm != null && wm.size() == 2 ? numOrNull(wm[1]) : null;
        var now = cur[today] as Number;
        var items = [["t", hours(now), ACCENT, _cap]] as Array<Array>;
        
        if (goal != null && goal > 0) {
            var diff = now - (goal * (today < 6 ? today : 6) / 6).toNumber();
            items.add(["t", "  " + (diff < 0 ? "−" : "+") + hours(diff < 0 ? -diff : diff), FG, _cap]);
            items.add(["t", "  ZUM SOLL", SEC, _tag]);
        }
        drawItems(dc, _cx, px(HEAD_Y), items, 0);

        var most = 60.0;
        most = now > most ? now.toFloat() : most;
        if (goal != null && goal > most) { most = goal.toFloat(); }
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

        // 1. NEU: Schattierte Schattenvorlauf-/Zielzone im Hintergrund, damit Mo/Di nicht leer aussehen
        if (goal != null && goal > 0) {
            var ghostPoly = new [9];
            for (var i = 0; i < 7; i++) {
                // Ein theoretischer linearer Verlauf zum Ziel als Hintergrundfüllung
                var gVal = (goal * i / 6.0).toNumber();
                ghostPoly[i] = [gx(i, x0, x1), yb - (h * gVal / most).toNumber()];
            }
            ghostPoly[7] = [x1, yb];
            ghostPoly[8] = [x0, yb];
            dc.setColor(blend(Graphics.COLOR_BLACK, ACCENT, 0.05), Graphics.COLOR_TRANSPARENT);
            dc.fillPolygon(ghostPoly);
            
            // Faint dotted target outline
            dc.setColor(blend(Graphics.COLOR_BLACK, ACCENT, 0.15), Graphics.COLOR_TRANSPARENT);
            dc.setPenWidth(1);
            for (var i = 0; i < 6; i++) {
                var xa = gx(i, x0, x1);
                var ya = yb - (h * (goal * i / 6.0) / most).toNumber();
                var xb = gx(i+1, x0, x1);
                var yb_t = yb - (h * (goal * (i+1) / 6.0) / most).toNumber();
                dc.drawLine(xa, ya, xb, yb_t);
            }
        }

        // Soll gestrichelt zum Sonntag
        if (goal != null && goal > 0) {
            var yg = yb - (h * goal / most).toNumber();
            dc.setColor(0x8E8E93, Graphics.COLOR_TRANSPARENT);
            dc.setPenWidth(px(2));
            drawDashed(dc, x0, yb, x1, yg);
            drawItems(dc, x1 + px(6), yg + px(6), [["t", hours(goal.toNumber()), SEC, _tag]], -1);
        }

        // Aktivitäts-Gradient bis heute
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
                dc.setColor(blend(Graphics.COLOR_BLACK, ACCENT, 0.40 - 0.33 * k / (GRAD - 1)), Graphics.COLOR_TRANSPARENT);
                dc.fillPolygon(poly);
            }
            dc.clearClip();
        }

        // Aktive Woche als dicke, glühende Linie
        dc.setColor(ACCENT, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(3));
        for (var i = 0; i < today; i++) {
            dc.drawLine(gx(i, x0, x1), yb - (h * (cur[i] as Number) / most).toNumber(), gx(i + 1, x0, x1), yb - (h * (cur[i + 1] as Number) / most).toNumber());
        }

        // Glow-Punkt auf heute
        var tx = gx(today, x0, x1);
        var ty = yb - (h * now / most).toNumber();
        dc.setColor(blend(Graphics.COLOR_BLACK, ACCENT, 0.25), Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(tx, ty, px(13));
        dc.setColor(ACCENT, Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(tx, ty, px(7));
        dc.setColor(blend(ACCENT, FG, 0.7), Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(tx, ty, px(3));

        var names = ["MO", "DI", "MI", "DO", "FR", "SA", "SO"];
        for (var i = 0; i < 7; i++) {
            drawItems(dc, gx(i, x0, x1), px(DAYS_Y), [["t", names[i] as String, i == today ? FG : SEC, _tag]], 0);
        }
    }

    private function drawDashed(dc as Dc, xa as Number, ya as Number, xb as Number, yb as Number) as Void {
        var dx = (xb - xa).toFloat();
        var dy = (yb - ya).toFloat();
        var len = Math.sqrt(dx * dx + dy * dy);
        var dash = px(6) > 2 ? px(6) : 2;
        var gap = px(4) > 2 ? px(4) : 2;
        if (len < 1) { return; }
        for (var t = 0.0; t < len; t += dash + gap) {
            var t2 = t + dash < len ? t + dash : len;
            dc.drawLine(xa + (dx * t / len).toNumber(), ya + (dy * t / len).toNumber(), xa + (dx * t2 / len).toNumber(), ya + (dy * t2 / len).toNumber());
        }
    }

    private function gx(i as Number, x0 as Number, x1 as Number) as Number {
        return x0 + (x1 - x0) * i / 6;
    }

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

    // ---- Untere Reihe: offene Wochenminuten ----------------------------------------
    private function drawWeekRow(dc as Dc, rz as Array?) as Void {
        var sports = ["run", "bike", "swim"];
        var y = px(RING_Y);
        var r = px(RING_R);
        var pen = px(RING_W) > 2 ? px(RING_W) : 2;
        var rr = r - pen / 2 - px(3); 
        
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

    private function drawGauge(dc as Dc, x as Number, y as Number, r as Number, pen as Number, color as Number, f as Float) as Void {
        if (f <= 0.0) { return; }
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
    private function drawNext(dc as Dc, data as Dictionary, y as Number) as Void {
        var nx = data.get("next") as Dictionary?;
        if (nx == null) { return; }
        
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

    // ---- Haken-Zeichnung ----------------------------------------------------------------------
    private function drawCheck(dc as Dc, x as Number, y as Number, color as Number) as Void {
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(4));
        dc.drawLine(x - px(9), y + px(1), x - px(2), y + px(8));
        dc.drawLine(x - px(2), y + px(8), x + px(11), y - px(7));
    }

    private function goalOf(rz as Array?, sport) as Array? {
        if (rz == null) { return null; }
        for (var i = 0; i < rz.size(); i++) {
            var e = rz[i] as Array;
            if (sport.equals(e[0])) { return e; }
        }
        return null;
    }

    private function sportColor(sport) as Number {
        if ("run".equals(sport)) { return 0xFFD60A; }        // Gelb
        if ("bike".equals(sport)) { return 0x30D158; }       // Grün
        if ("swim".equals(sport)) { return 0x409CFF; }       // Blau
        return FG;
    }

    private function sportName(sport) as String {
        if ("run".equals(sport)) { return "Lauf"; }
        if ("bike".equals(sport)) { return "Rad"; }
        if ("swim".equals(sport)) { return "Schwimmen"; }
        if ("strength".equals(sport)) { return "Kraft"; }
        return "Einheit";
    }

    private function hours(min as Number) as String {
        return (min / 60).toString() + ":" + (min % 60).format("%02d");
    }

    private function blend(a as Number, b as Number, f as Float) as Number {
        var r = ((a >> 16) & 0xFF) + ((((b >> 16) & 0xFF) - ((a >> 16) & 0xFF)) * f).toNumber();
        var g = ((a >> 8) & 0xFF) + ((((b >> 8) & 0xFF) - ((a >> 8) & 0xFF)) * f).toNumber();
        var bl = (a & 0xFF) + (((b & 0xFF) - (a & 0xFF)) * f).toNumber();
        return (r << 16) | (g << 8) | bl;
    }

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

    private function px(v as Numeric) as Number {
        return (v * _s).toNumber();
    }

    private function clamp01(v as Float) as Float {
        return v < 0 ? 0.0 : (v > 1 ? 1.0 : v);
    }

    private function frac(v as Numeric?, of as Numeric?) as Float {
        return (v != null && of != null && of > 0) ? clamp01(v.toFloat() / of) : 0.0;
    }

    private function numOrNull(v) as Numeric? {
        if (v instanceof Lang.Number || v instanceof Lang.Float || v instanceof Lang.Long || v instanceof Lang.Double) {
            return v;
        }
        return null;
    }

    private function errSuffix() as String {
        var e = Application.Storage.getValue("e");
        return e instanceof Lang.Number ? " " + e.toString() : "";
    }

    private function isStale(data as Dictionary?) as Boolean {
        var at = data != null ? numOrNull(data.get("at")) : null;
        return at == null || Time.now().value() - at.toNumber() > STALE_SECS;
    }
}
