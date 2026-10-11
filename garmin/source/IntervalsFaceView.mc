import Toybox.Application;
import Toybox.Graphics;
import Toybox.Lang;
import Toybox.Math;
import Toybox.System;
import Toybox.Time;
import Toybox.Weather;
import Toybox.WatchUi;

// Zifferblatt "Bubble". Alle Maße stammen aus dem Mockup (454 x 454) und werden mit _s auf die Displaygröße skaliert.
// Daten kommen aus dem Speicher (vom Hintergrunddienst geschrieben), gezeichnet wird nur.
class IntervalsFaceView extends WatchUi.WatchFace {

    const PURPLE = 0xC77DFF;
    const CORAL = 0xFF6A4D;
    const AMBER = 0xFFB43A;
    const SKY = 0x6AB7FF;
    const GREEN = 0x4CAF6A;
    const RED = 0xE5534B;
    const GREY = 0x8A9196;
    const DIM = 0x2B3038;
    const BAR = 0x5A606C;
    const BAR_NOW = 0x4A7BD0;
    const GAUGE = 0xA8AEB8;
    const STALE_SECS = 3600;

    private var _s as Float = 1.0;
    private var _cx as Number = 0;
    private var _cy as Number = 0;
    private var _sleep as Boolean = false;

    function initialize() {
        WatchFace.initialize();
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
        dc.setColor(Graphics.COLOR_WHITE, Graphics.COLOR_BLACK);
        dc.clear();
        if (dc has :setAntiAlias) {
            dc.setAntiAlias(true);
        }

        drawTime(dc);
        // Always-on-Modus (AMOLED): nur die Uhrzeit, damit wenig Pixel leuchten
        if (_sleep) {
            return;
        }

        var data = Application.Storage.getValue("d") as Dictionary?;
        drawStatusLine(dc, data);
        if (data == null) {
            return;
        }
        drawWeekDots(dc, data);
        drawRace(dc, data);
        drawGauges(dc, data);
        drawWeekBars(dc, data);
        drawSideArcs(dc, data);
    }

    // ---- Zeit ----------------------------------------------------------------

    private function drawTime(dc as Dc) as Void {
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
        var font = Graphics.FONT_NUMBER_HOT;
        var just = Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER;
        if (_sleep) {
            dc.setColor(GREY, Graphics.COLOR_BLACK);
            dc.drawText(_cx, _cy, font, hs + ":" + ms, just);
            return;
        }
        var y = px(170);
        dc.setColor(PURPLE, Graphics.COLOR_BLACK);
        dc.drawText(_cx - px(67), y, font, hs, just);
        dc.setColor(Graphics.COLOR_WHITE, Graphics.COLOR_BLACK);
        dc.drawText(_cx + px(69), y, font, ms, just);
    }

    // ---- Kopf: Block, Akku -----------------------------------------------------

    private function drawStatusLine(dc as Dc, data as Dictionary?) as Void {
        var parts = [] as Array<String>;
        var block = data != null ? data.get("block") as Dictionary? : null;
        if (block != null) {
            var name = (block.get("n") as String?);
            var st = block.get("st");
            if (name != null) {
                parts.add(name.toUpper());
            }
            if ("upcoming".equals(st) && block.get("d") != null) {
                parts.add("START IN " + block.get("d") + " T");
            } else if ("active".equals(st) && block.get("w") != null) {
                parts.add("WOCHE " + block.get("w") + (block.get("of") != null ? "/" + block.get("of") : ""));
            }
        }
        parts.add(System.getSystemStats().battery.format("%d") + " %");
        var text = join(parts, " · ");
        if (isStale(data)) {
            text = text + " !";
        }
        dc.setColor(GREY, Graphics.COLOR_BLACK);
        dc.drawText(_cx, px(66), Graphics.FONT_XTINY, text, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
    }

    // ---- Wochentage oben -------------------------------------------------------
    // Gefüllt = erledigt, Ring = geplant, grau = frei, weißer Rahmen = heute. Farbe = Sportart.

    private function drawWeekDots(dc as Dc, data as Dictionary) as Void {
        var days = data.get("days") as Array?;
        if (days == null) {
            return;
        }
        var letters = ["M", "D", "M", "D", "F", "S", "S"];
        var today = numOrNull(data.get("ti"));
        for (var i = 0; i < 7 && i < days.size(); i++) {
            var p = pt(px(211), -30 + 10 * i);
            var day = days[i] as Dictionary?;
            var color = day != null ? sportColor(day.get("s")) : DIM;
            var planned = day != null && numOrNull(day.get("p")) == 1;
            dc.setPenWidth(2);
            if (today != null && today == i) {
                dc.setColor(Graphics.COLOR_WHITE, Graphics.COLOR_BLACK);
                dc.drawCircle(p[0], p[1], px(15.5));
            }
            if (planned) {
                dc.setPenWidth(px(3));
                dc.setColor(color, Graphics.COLOR_BLACK);
                dc.drawCircle(p[0], p[1], px(11));
            } else {
                dc.setColor(color, Graphics.COLOR_BLACK);
                dc.fillCircle(p[0], p[1], px(11));
            }
            dc.setColor(planned ? color : (day != null ? Graphics.COLOR_BLACK : GREY), Graphics.COLOR_TRANSPARENT);
            dc.drawText(p[0], p[1], Graphics.FONT_XTINY, letters[i], Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
        }
    }

    private function sportColor(sport) as Number {
        if ("run".equals(sport)) { return 0x2FD1B0; }
        if ("bike".equals(sport)) { return 0xE6C229; }
        if ("strength".equals(sport)) { return 0xFF7EB6; }
        if ("swim".equals(sport)) { return 0x22D3EE; }
        return GREY;
    }

    // ---- Rennziel (Sticker) ----------------------------------------------------

    private function drawRace(dc as Dc, data as Dictionary) as Void {
        var race = data.get("race") as Dictionary?;
        var days = race != null ? numOrNull(race.get("d")) : null;
        if (race == null || days == null || days < 0) {
            return;
        }
        var name = race.get("n") as String?;
        var text = (name != null && name.length() > 0 ? name.toUpper() + " · " : "") + days + " T";
        var w = px(174);
        var h = px(36);
        dc.setColor(CORAL, Graphics.COLOR_BLACK);
        dc.fillRoundedRectangle(_cx - w / 2, px(80), w, h, h / 2);
        dc.setColor(0x2A0A04, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx, px(80) + h / 2, Graphics.FONT_TINY, text, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
    }

    // ---- Drei Rundanzeigen: Decoupling, Kraft, ACWR ------------------------------

    private function drawGauges(dc as Dc, data as Dictionary) as Void {
        var block = data.get("block") as Dictionary?;
        if (block == null) {
            return;
        }
        var dec = block.get("dec") as Dictionary?;
        var str = block.get("str") as Dictionary?;
        var acwr = block.get("acwr") as Dictionary?;
        var strV = str != null ? numOrNull(str.get("v")) : null;
        var strOf = str != null ? numOrNull(str.get("of")) : null;
        var strFrac = (strV != null && strOf != null && strOf > 0) ? strV.toFloat() / strOf : 0.0;
        var acwrV = acwr != null ? numOrNull(acwr.get("v")) : null;

        gauge(dc, 143, 268, "DEC", dec != null ? fmtPct(dec.get("v"), true) : "--", 0.0, dec);
        gauge(dc, 227, 276, "KRAFT", strV != null ? strV.toNumber() + "/" + (strOf != null ? strOf.toNumber() : "?") : "--", strFrac, str);
        gauge(dc, 311, 268, "ACWR", acwr != null ? fmtPct(acwr.get("v"), false) : "--", acwrV != null ? acwrV.toFloat() / 100 : 0.0, acwr);
    }

    private function gauge(dc as Dc, x as Number, y as Number, label as String, value as String, frac as Float, goal as Dictionary?) as Void {
        var cx = px(x);
        var cy = px(y);
        var r = px(28);
        var color = statusColor(goal != null ? goal.get("ok") : null);
        dc.setColor(0x12161B, Graphics.COLOR_BLACK);
        dc.fillCircle(cx, cy, r + px(5));
        ring(dc, cx, cy, r, px(6), frac, color, DIM);
        dc.setColor(GREY, Graphics.COLOR_TRANSPARENT);
        dc.drawText(cx, cy - px(11), Graphics.FONT_XTINY, label, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.drawText(cx, cy + px(11), Graphics.FONT_XTINY, value, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
    }

    // ok = im Ziel, warn = knapp, bad = verfehlt; base und none (Ausgangswert, noch keine Daten) bleiben grau
    private function statusColor(status) as Number {
        if ("ok".equals(status)) { return GREEN; }
        if ("warn".equals(status)) { return AMBER; }
        if ("bad".equals(status)) { return RED; }
        return GAUGE;
    }

    // ---- TSS: Textzeile und 6 Wochenbalken -----------------------------------------

    private function drawWeekBars(dc as Dc, data as Dictionary) as Void {
        var tss = numOrNull(data.get("tss"));
        var plan = numOrNull(data.get("plan"));
        var goal = numOrNull(data.get("goal"));
        var runs = [] as Array<Array>;
        runs.add([tss != null ? tss.toNumber().toString() : "--", BAR_NOW]);
        runs.add([" TSS", GREY]);
        if (plan != null) {
            runs.add([" · Plan ", GREY]);
            runs.add([plan.toNumber().toString(), Graphics.COLOR_WHITE]);
        }
        if (goal != null) {
            runs.add([" · Ziel ", GREY]);
            runs.add([goal.toNumber().toString(), Graphics.COLOR_WHITE]);
        }
        drawRuns(dc, px(327), Graphics.FONT_XTINY, runs);

        var wk = data.get("wk") as Array?;
        if (wk == null) {
            return;
        }
        var max = 100.0;
        for (var i = 0; i < wk.size(); i++) {
            var pair = wk[i] as Array;
            for (var k = 0; k < 2; k++) {
                var v = numOrNull(pair[k]);
                if (v != null && v > max) {
                    max = v.toFloat();
                }
            }
        }
        var base = px(408);
        var hmax = px(62);
        for (var i = 0; i < wk.size() && i < 6; i++) {
            var pair = wk[i] as Array;
            var x = px(107 + i * 42);
            var t = numOrNull(pair[0]);
            var g = numOrNull(pair[1]);
            var bh = t != null ? (t * hmax / max).toNumber() : 0;
            if (bh < px(4)) {
                bh = px(4);
            }
            dc.setColor(i == wk.size() - 1 ? BAR_NOW : BAR, Graphics.COLOR_BLACK);
            dc.fillRoundedRectangle(x, base - bh, px(30), bh, px(4));
            if (g != null) {
                dc.setColor(GREEN, Graphics.COLOR_BLACK);
                dc.fillRoundedRectangle(x - px(3), base - (g * hmax / max).toNumber() - px(1), px(36), px(3), px(1));
            }
        }
    }

    // ---- Randbögen: Kalorien links, Regenwahrscheinlichkeit rechts ---------------------

    private function drawSideArcs(dc as Dc, data as Dictionary) as Void {
        var r = px(204);
        var pen = px(10);

        // Kalorien: Bogen von unten links nach oben links (Winkel: 0 = oben, im Uhrzeigersinn)
        var kcal = data.get("kcal") as Dictionary?;
        var kv = kcal != null ? numOrNull(kcal.get("v")) : null;
        var kg = kcal != null ? numOrNull(kcal.get("g")) : null;
        var kf = (kv != null && kg != null && kg > 0) ? clamp01(kv.toFloat() / kg) : 0.0;
        sideArc(dc, r, pen, 238, 292, kf, AMBER, 0x2A2010);
        dc.setColor(AMBER, Graphics.COLOR_TRANSPARENT);
        dc.drawText(px(66), _cy - px(12), Graphics.FONT_XTINY, kv != null ? kv.toNumber().toString() : "--", Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
        dc.setColor(GREY, Graphics.COLOR_TRANSPARENT);
        dc.drawText(px(66), _cy + px(12), Graphics.FONT_XTINY, "kcal", Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);

        // Wetter kommt von der Uhr
        var temp = "--";
        var rain = 0.0;
        var rainText = "";
        var cond = Weather.getCurrentConditions();
        if (cond != null) {
            if (cond.temperature != null) {
                var c = cond.temperature as Number;
                var imperial = System.getDeviceSettings().temperatureUnits == System.UNIT_STATUTE;
                temp = (imperial ? (c * 9 / 5 + 32).toNumber() : c).toString() + "°";
            }
            if (cond.precipitationChance != null) {
                rain = clamp01((cond.precipitationChance as Number).toFloat() / 100);
                rainText = cond.precipitationChance.toString() + "%";
            }
        }
        sideArc(dc, r, pen, 68, 122, rain, SKY, 0x12263A);
        dc.setColor(Graphics.COLOR_WHITE, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx * 2 - px(66), _cy - px(12), Graphics.FONT_XTINY, temp, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
        dc.setColor(SKY, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx * 2 - px(66), _cy + px(12), Graphics.FONT_XTINY, rainText, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
    }

    // Bogen von a1 nach a2 (Grad, 0 = oben, im Uhrzeigersinn), Füllung ab a1
    private function sideArc(dc as Dc, r as Number, pen as Number, a1 as Number, a2 as Number, frac as Float, color as Number, track as Number) as Void {
        dc.setPenWidth(pen);
        dc.setColor(track, Graphics.COLOR_BLACK);
        dc.drawArc(_cx, _cy, r, Graphics.ARC_CLOCKWISE, norm(90 - a1), norm(90 - a2));
        if (frac > 0) {
            dc.setColor(color, Graphics.COLOR_BLACK);
            dc.drawArc(_cx, _cy, r, Graphics.ARC_CLOCKWISE, norm(90 - a1), norm(90 - (a1 + (a2 - a1) * frac)));
        }
    }

    // Vollkreis mit Fortschritt ab 12 Uhr im Uhrzeigersinn
    private function ring(dc as Dc, cx as Number, cy as Number, r as Number, pen as Number, frac as Float, color as Number, track as Number) as Void {
        dc.setPenWidth(pen);
        dc.setColor(track, Graphics.COLOR_BLACK);
        dc.drawCircle(cx, cy, r);
        if (frac >= 1.0) {
            dc.setColor(color, Graphics.COLOR_BLACK);
            dc.drawCircle(cx, cy, r);
        } else if (frac > 0) {
            dc.setColor(color, Graphics.COLOR_BLACK);
            dc.drawArc(cx, cy, r, Graphics.ARC_CLOCKWISE, 90, norm(90 - 360 * frac));
        }
    }

    // ---- Helfer -------------------------------------------------------------------

    private function px(v as Numeric) as Number {
        return (v * _s).toNumber();
    }

    // Punkt auf dem Kreis um die Mitte; Winkel in Grad, 0 = oben, im Uhrzeigersinn
    private function pt(r as Numeric, a as Numeric) as Array<Number> {
        var t = Math.toRadians(a);
        return [ (_cx + r * Math.sin(t)).toNumber(), (_cy - r * Math.cos(t)).toNumber() ];
    }

    private function norm(a as Numeric) as Number {
        var n = a.toNumber() % 360;
        return n < 0 ? n + 360 : n;
    }

    private function clamp01(v as Float) as Float {
        return v < 0 ? 0.0 : (v > 1 ? 1.0 : v);
    }

    private function numOrNull(v) as Numeric? {
        if (v instanceof Lang.Number || v instanceof Lang.Float || v instanceof Lang.Long || v instanceof Lang.Double) {
            return v;
        }
        return null;
    }

    private function fmtPct(v, decimal as Boolean) as String {
        var n = numOrNull(v);
        if (n == null) {
            return "--";
        }
        if (!decimal) {
            return n.toNumber().toString() + "%";
        }
        var t = n.toFloat().format("%.1f");
        var i = t.find(".");
        if (i != null) {
            t = t.substring(0, i) + "," + t.substring(i + 1, t.length());
        }
        return t + "%";
    }

    private function join(parts as Array<String>, sep as String) as String {
        var out = "";
        for (var i = 0; i < parts.size(); i++) {
            out = out + (i > 0 ? sep : "") + parts[i];
        }
        return out;
    }

    // Mehrfarbige Zeile, als Ganzes zentriert
    private function drawRuns(dc as Dc, y as Number, font as Graphics.FontType, runs as Array<Array>) as Void {
        var total = 0;
        for (var i = 0; i < runs.size(); i++) {
            total += dc.getTextWidthInPixels(runs[i][0] as String, font);
        }
        var x = _cx - total / 2;
        for (var i = 0; i < runs.size(); i++) {
            var text = runs[i][0] as String;
            dc.setColor(runs[i][1] as Number, Graphics.COLOR_TRANSPARENT);
            dc.drawText(x, y, font, text, Graphics.TEXT_JUSTIFY_LEFT | Graphics.TEXT_JUSTIFY_VCENTER);
            x += dc.getTextWidthInPixels(text, font);
        }
    }

    private function isStale(data as Dictionary?) as Boolean {
        var at = data != null ? numOrNull(data.get("at")) : null;
        return at == null || Time.now().value() - at.toNumber() > STALE_SECS;
    }
}
