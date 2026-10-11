import Toybox.Application;
import Toybox.Graphics;
import Toybox.Lang;
import Toybox.Math;
import Toybox.System;
import Toybox.Time;
import Toybox.Weather;
import Toybox.WatchUi;

// Zifferblatt im Apple-Stil: Schwarz und Weiß, Farbe nur, wo sie etwas bedeutet. Oben die Woche am Rand,
// dann nächste Einheit, große Uhrzeit, Bereitschaft (mit Warnung) und vier runde Komplikationen.
// Alle Maße stammen aus dem Mockup (454 x 454) und werden mit _s auf die Displaygröße skaliert.
// Daten kommen aus dem Speicher (vom Hintergrunddienst geschrieben), gezeichnet wird nur.
class IntervalsFaceView extends WatchUi.WatchFace {

    // true = jede Komplikation in eigener, dezenter Farbe; false = alle Ringe weiß (Mono)
    const COLORFUL = true;

    const FG = 0xF5F5F7;
    const SEC = 0x8E8E93;
    const TER = 0x48484A;
    const TRACK = 0x2C2C2E;
    const FREE = 0x1C1C1E;
    const OK = 0x30D158;
    const WARN = 0xFF9F0A;
    const BAD = 0xFF453A;
    const KCAL = 0xFF9F0A;
    const TSS = 0x0A84FF;
    const STR = 0xFF6482;
    const RAIN = 0x64D2FF;
    const STALE_SECS = 3600;
    const TIME_Y = 206;
    const READY_Y = 280;
    const COMP_Y = 352;

    private var _s as Float = 1.0;
    private var _cx as Number = 0;
    private var _cy as Number = 0;
    private var _sleep as Boolean = false;
    private var _time as Graphics.FontType = Graphics.FONT_NUMBER_HOT;
    private var _small as Graphics.FontType = Graphics.FONT_XTINY;
    private var _mid as Graphics.FontType = Graphics.FONT_XTINY;
    private var _cap as Graphics.FontType = Graphics.FONT_XTINY;
    private var _tiny as Graphics.FontType = Graphics.FONT_XTINY;

    function initialize() {
        WatchFace.initialize();
        // Bitmap-Schriften aus Inter (erzeugt mit tools/make_font.py, Größen für 416 Pixel)
        _time = WatchUi.loadResource(Rez.Fonts.Time) as Graphics.FontType;
        _small = WatchUi.loadResource(Rez.Fonts.Small) as Graphics.FontType;
        _mid = WatchUi.loadResource(Rez.Fonts.Mid) as Graphics.FontType;
        _cap = WatchUi.loadResource(Rez.Fonts.Cap) as Graphics.FontType;
        _tiny = WatchUi.loadResource(Rez.Fonts.Tiny) as Graphics.FontType;
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

        drawTime(dc);
        // Always-on-Modus (AMOLED): nur die Uhrzeit, damit wenig Pixel leuchten
        if (_sleep) {
            return;
        }

        var data = Application.Storage.getValue("d") as Dictionary?;
        if (data == null) {
            drawLine(dc, px(READY_Y), [["dot", px(6), TER], ["gap", px(8)], ["t", "Keine Daten", SEC, _small]]);
            return;
        }
        drawWeek(dc, data);
        drawNext(dc, data);
        drawReady(dc, data);
        drawComplications(dc, data);
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
        if (_sleep) {
            dc.setColor(SEC, Graphics.COLOR_BLACK);
            dc.drawText(_cx, _cy, Graphics.FONT_NUMBER_MILD, hs + ":" + ms, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
            return;
        }
        var y = px(TIME_Y);
        var gap = px(14);
        dc.setColor(FG, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx - gap, y, _time, hs, Graphics.TEXT_JUSTIFY_RIGHT | Graphics.TEXT_JUSTIFY_VCENTER);
        dc.drawText(_cx + gap, y, _time, ms, Graphics.TEXT_JUSTIFY_LEFT | Graphics.TEXT_JUSTIFY_VCENTER);
        // Doppelpunkt zurückgenommen grau
        dc.setColor(TER, Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(_cx, y - px(16), px(6));
        dc.fillCircle(_cx, y + px(16), px(6));
    }

    // ---- Oberer Rand: Woche Mo bis So ------------------------------------------
    // Gefüllt in Sportfarbe = erledigt, Ring = geplant, dunkel = frei, weißer Ring = heute.

    private function drawWeek(dc as Dc, data as Dictionary) as Void {
        var days = data.get("days") as Array?;
        if (days == null) {
            return;
        }
        var letters = ["M", "D", "M", "D", "F", "S", "S"];
        var today = numOrNull(data.get("ti"));
        var just = Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER;
        for (var i = 0; i < 7 && i < days.size(); i++) {
            var p = pt(px(196), -36 + 12 * i);
            var day = days[i] as Dictionary?;
            if (today != null && today == i) {
                dc.setPenWidth(px(2));
                dc.setColor(FG, Graphics.COLOR_BLACK);
                dc.drawCircle(p[0], p[1], px(17));
            }
            if (day == null) {
                dc.setColor(FREE, Graphics.COLOR_BLACK);
                dc.fillCircle(p[0], p[1], px(13));
                dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
            } else {
                var color = sportColor(day.get("s"));
                dc.setColor(color, Graphics.COLOR_BLACK);
                if (numOrNull(day.get("p")) == 1) {
                    dc.setPenWidth(px(3));
                    dc.drawCircle(p[0], p[1], px(12));
                    dc.setColor(color, Graphics.COLOR_TRANSPARENT);
                } else {
                    dc.fillCircle(p[0], p[1], px(14));
                    dc.setColor(Graphics.COLOR_BLACK, Graphics.COLOR_TRANSPARENT);
                }
            }
            dc.drawText(p[0], p[1], _tiny, letters[i], just);
        }
    }

    private function sportColor(sport) as Number {
        if ("run".equals(sport)) { return 0x63E6BE; }
        if ("bike".equals(sport)) { return 0xFFD60A; }
        if ("swim".equals(sport)) { return 0x64D2FF; }
        if ("strength".equals(sport)) { return 0xFF6482; }
        return SEC;
    }

    // ---- Nächste Einheit: Sportpunkt, Tag (grau), Einheit (weiß) ----------------

    private function drawNext(dc as Dc, data as Dictionary) as Void {
        var next = data.get("next") as Dictionary?;
        if (next == null) {
            return;
        }
        var offset = numOrNull(next.get("d"));
        var today = numOrNull(data.get("ti"));
        var dayText = "";
        if (offset != null) {
            var names = ["MO", "DI", "MI", "DO", "FR", "SA", "SO"];
            dayText = offset == 0 ? "HEUTE" : (offset == 1 ? "MORGEN" : (today != null ? names[(today.toNumber() + offset.toNumber()) % 7] : ""));
        }
        var sport = next.get("s");
        var label = "Training";
        if ("run".equals(sport)) { label = "Lauf"; }
        else if ("bike".equals(sport)) { label = "Rad"; }
        else if ("swim".equals(sport)) { label = "Schwimmen"; }
        else if ("strength".equals(sport)) { label = "Kraft"; }
        var m = numOrNull(next.get("m"));
        var km = numOrNull(next.get("km"));
        if (m != null) {
            label = label + " · " + m.toNumber() + "′";
        } else if (km != null) {
            label = label + " · " + km.toFloat().format("%.0f") + " km";
        }
        drawLine(dc, px(122), [["dot", px(5), sportColor(sport)], ["gap", px(8)], ["t", dayText, SEC, _cap], ["gap", px(8)], ["t", label, FG, _small]]);
    }

    // ---- Bereitschaft, dahinter die Warnung ------------------------------------------
    // rd: ok, warn, bad, none (wie im Dashboard). w: { k: "acwr", v } oder { k: "hint" }.

    private function drawReady(dc as Dc, data as Dictionary) as Void {
        var rd = data.get("rd");
        var word = "Ohne Eintrag";
        var color = SEC;
        if ("ok".equals(rd)) { word = "Bereit"; color = OK; }
        else if ("warn".equals(rd)) { word = "Mit Vorsicht"; color = WARN; }
        else if ("bad".equals(rd)) { word = "Ruhig angehen"; color = BAD; }
        // Älter als eine Stunde: alles grau mit "!"
        var stale = isStale(data);
        var items = [["dot", px(6), stale ? TER : color], ["gap", px(8)], ["t", stale ? word + " !" : word, stale ? SEC : FG, _small]] as Array<Array>;
        var w = data.get("w") as Dictionary?;
        if (w != null) {
            var text = "Hinweis";
            var v = numOrNull(w.get("v"));
            if ("acwr".equals(w.get("k")) && v != null) {
                text = "ACWR " + decimal(v, "%.2f");
            }
            items.add(["gap", px(14)]);
            items.add(["bar", px(18), TER]);
            items.add(["gap", px(14)]);
            items.add(["tri", px(15), WARN]);
            items.add(["gap", px(7)]);
            items.add(["t", text, WARN, _small]);
        }
        drawLine(dc, px(READY_Y), items);
    }

    // ---- Vier Komplikationen: Essen, TSS, Kraft, Wetter --------------------------

    private function drawComplications(dc as Dc, data as Dictionary) as Void {
        // Essen: Kalorien gegen das Tagesziel
        var kcal = data.get("kcal") as Dictionary?;
        var kv = kcal != null ? numOrNull(kcal.get("v")) : null;
        var kg = kcal != null ? numOrNull(kcal.get("g")) : null;
        complication(dc, 110, "ESSEN", kv != null ? kv.toNumber().toString() : "--", "KCAL", frac(kv, kg), accent(KCAL), FG);

        // TSS der Woche gegen den Plan (ohne Plan gegen WEEKLY_TSS_GOAL); grün, sobald erreicht
        var tss = numOrNull(data.get("tss"));
        var target = numOrNull(data.get("plan"));
        if (target == null) {
            target = numOrNull(data.get("goal"));
        }
        var tf = frac(tss, target);
        complication(dc, 188, "TSS", tss != null ? tss.toNumber().toString() : "--", target != null ? "/ " + target.toNumber() : "", tf, tf >= 1.0 ? OK : accent(TSS), FG);

        // Kraft: Sätze der Woche gegen das Ziel aus dem Trainingsblock; knapp/verfehlt in Ampelfarbe
        var str = data.get("str") as Dictionary?;
        var sv = str != null ? numOrNull(str.get("v")) : null;
        var so = str != null ? numOrNull(str.get("of")) : null;
        var st = str != null ? str.get("ok") : null;
        var sc = "warn".equals(st) ? WARN : ("bad".equals(st) ? BAD : accent(STR));
        complication(dc, 266, "KRAFT", sv != null ? sv.toNumber().toString() : "--", so != null ? "/ " + so.toNumber() : "", frac(sv, so), sc, sc == accent(STR) ? FG : sc);

        // Wetter kommt von der Uhr: Temperatur in der Mitte, Regenwahrscheinlichkeit als Ring
        var temp = "--";
        var rain = 0.0;
        var rainText = "";
        var cond = Weather.getCurrentConditions();
        if (cond != null) {
            if (cond.temperature != null) {
                var c = (cond.temperature as Numeric).toFloat();
                var imperial = System.getDeviceSettings().temperatureUnits == System.UNIT_STATUTE;
                temp = Math.round(imperial ? c * 9 / 5 + 32 : c).toNumber().toString() + "°";
            }
            if (cond.precipitationChance != null) {
                var chance = (cond.precipitationChance as Numeric).toFloat();
                rain = clamp01(chance / 100);
                rainText = Math.round(chance).toNumber().toString() + " %";
            }
        }
        complication(dc, 344, "WETTER", temp, rainText, rain, accent(RAIN), FG);
    }

    private function complication(dc as Dc, x as Number, label as String, value as String, sub as String, f as Float, color as Number, valueColor as Number) as Void {
        var cx = px(x);
        var cy = px(COMP_Y);
        var just = Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER;
        ring(dc, cx, cy, px(29), px(5), f, color, TRACK);
        dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
        dc.drawText(cx, px(COMP_Y - 42), _tiny, label, just);
        dc.setColor(valueColor, Graphics.COLOR_TRANSPARENT);
        dc.drawText(cx, cy - px(5), _mid, value, just);
        if (sub.length() > 0) {
            dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
            dc.drawText(cx, cy + px(13), _tiny, sub, just);
        }
    }

    // Farbe einer Komplikation: dezent farbig oder weiß (Mono)
    private function accent(color as Number) as Number {
        return COLORFUL ? color : FG;
    }

    // Vollkreis mit Fortschritt ab 12 Uhr im Uhrzeigersinn
    private function ring(dc as Dc, cx as Number, cy as Number, r as Number, pen as Number, f as Float, color as Number, track as Number) as Void {
        dc.setPenWidth(pen);
        dc.setColor(track, Graphics.COLOR_BLACK);
        dc.drawCircle(cx, cy, r);
        if (f >= 1.0) {
            dc.setColor(color, Graphics.COLOR_BLACK);
            dc.drawCircle(cx, cy, r);
        } else if (f > 0) {
            dc.setColor(color, Graphics.COLOR_BLACK);
            dc.drawArc(cx, cy, r, Graphics.ARC_CLOCKWISE, 90, norm(90 - 360 * f));
        }
    }

    // ---- Zeile aus Text und Formen, als Ganzes zentriert -----------------------------
    // Elemente: ["t", Text, Farbe, Schrift], ["dot", Radius, Farbe], ["tri", Größe, Farbe], ["bar", Höhe, Farbe], ["gap", Breite]

    private function drawLine(dc as Dc, y as Number, items as Array<Array>) as Void {
        var total = 0;
        for (var i = 0; i < items.size(); i++) {
            total += itemWidth(dc, items[i]);
        }
        var x = _cx - total / 2;
        for (var i = 0; i < items.size(); i++) {
            var it = items[i];
            var kind = it[0] as String;
            var w = itemWidth(dc, it);
            if (kind.equals("t")) {
                dc.setColor(it[2] as Number, Graphics.COLOR_TRANSPARENT);
                dc.drawText(x, y, it[3] as Graphics.FontType, it[1] as String, Graphics.TEXT_JUSTIFY_LEFT | Graphics.TEXT_JUSTIFY_VCENTER);
            } else if (kind.equals("dot")) {
                dc.setColor(it[2] as Number, Graphics.COLOR_TRANSPARENT);
                dc.fillCircle(x + w / 2, y, it[1] as Number);
            } else if (kind.equals("tri")) {
                var sz = it[1] as Number;
                dc.setColor(it[2] as Number, Graphics.COLOR_TRANSPARENT);
                dc.fillPolygon([[x, y + sz / 2], [x + sz / 2, y - sz / 2], [x + sz, y + sz / 2]]);
                dc.setColor(Graphics.COLOR_BLACK, Graphics.COLOR_TRANSPARENT);
                dc.fillRectangle(x + sz / 2 - 1, y - sz / 6, 2, sz / 3);
                dc.fillRectangle(x + sz / 2 - 1, y + sz / 4, 2, 2);
            } else if (kind.equals("bar")) {
                var bh = it[1] as Number;
                dc.setColor(it[2] as Number, Graphics.COLOR_TRANSPARENT);
                dc.fillRectangle(x, y - bh / 2, w, bh);
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
        if (kind.equals("dot")) {
            return (it[1] as Number) * 2;
        }
        if (kind.equals("tri")) {
            return it[1] as Number;
        }
        if (kind.equals("bar")) {
            return px(2) > 1 ? px(2) : 1;
        }
        return it[1] as Number;
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

    // Zahl mit Dezimalkomma
    private function decimal(v as Numeric, fmt as String) as String {
        var t = v.toFloat().format(fmt);
        var i = t.find(".");
        return i != null ? t.substring(0, i) + "," + t.substring(i + 1, t.length()) : t;
    }

    private function isStale(data as Dictionary?) as Boolean {
        var at = data != null ? numOrNull(data.get("at")) : null;
        return at == null || Time.now().value() - at.toNumber() > STALE_SECS;
    }
}
