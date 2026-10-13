import Toybox.Application;
import Toybox.Graphics;
import Toybox.Lang;
import Toybox.Math;
import Toybox.System;
import Toybox.Time;
import Toybox.Time.Gregorian;
import Toybox.Weather;
import Toybox.WatchUi;

// Zifferblatt im Apple-Stil: Weiß ist der Normalfall. Farbe heißt entweder erledigte Sportart (Woche, TSS) oder
// "hier stimmt etwas nicht" (orange/rot), dazu Blau für Regen. Geplantes, Grundringe und alles im Soll bleiben grau/weiß. Oben die Woche
// als gerade Reihe, dann nächste Einheit, große Uhrzeit, Bereitschaft (mit Warnung), vier runde Komplikationen und
// unten Renn-Countdown und Blockwoche als gerade Zeile.
// Alle Maße stammen aus dem Mockup (454 x 454) und werden mit _s auf die Displaygröße skaliert.
// Daten kommen aus dem Speicher (vom Hintergrunddienst geschrieben), gezeichnet wird nur.
class IntervalsFaceView extends WatchUi.WatchFace {

    const FG = 0xF5F5F7;
    const SEC = 0x8E8E93;
    const BOTTOM = 0xAEAEB2;
    const TER = 0x48484A;
    const TRACK = 0x3A3A3C;
    const FREE = 0x2C2C2E;
    const OK = 0x30D158;
    const WARN = 0xFF9F0A;
    const BAD = 0xFF453A;
    const KCAL_OVER = 150;   // so viele kcal über dem Ziel, bevor Essen orange wird
    const PROT_MIN = 0.8;    // Eiweiß-Anteil am Ziel, unter dem Essen ab PROT_HOUR orange wird
    const PROT_HOUR = 18;
    const RAIN = 0x64D2FF;
    const STALE_SECS = 3600;
    const TIME_Y = 192;
    const READY_Y = 268;
    const COMP_Y = 346;

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
        // Bitmap-Schriften aus Nunito (erzeugt mit tools/make_font.py, Größen für 416 Pixel)
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
            drawLine(dc, px(READY_Y), [["dot", px(7), TER], ["gap", px(9)], ["t", "Keine Daten", SEC, _small]]);
            return;
        }
        drawWeek(dc, data);
        drawNext(dc, data);
        drawReady(dc, data);
        drawComplications(dc, data);
        drawStatusLine(dc, data);
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

    // ---- Oben: Woche Mo bis So als gerade Reihe ------------------------------------
    // Gefüllt in Sportfarbe = erledigt, graue Scheibe mit Buchstaben in Sportfarbe = geplant, dunkel = frei, weißer Ring = heute.
    // Zwei Sportarten an einem Tag: geteilter Kreis, links die erste (erledigt vor geplant), rechts die zweite;
    // sind beide nur geplant, zwei Punkte in den Sportfarben statt des Buchstabens.

    private function drawWeek(dc as Dc, data as Dictionary) as Void {
        var days = data.get("days") as Array?;
        if (days == null) {
            return;
        }
        var letters = ["M", "D", "M", "D", "F", "S", "S"];
        var today = numOrNull(data.get("ti"));
        var just = Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER;
        for (var i = 0; i < 7 && i < days.size(); i++) {
            // Gerade Reihe mit 44 Pixel Abstand; Radius 19, damit Mo und So samt Heute-Ring noch im runden Display liegen
            var p = [_cx + px(44 * (i - 3)), px(78)];
            var day = days[i] as Dictionary?;
            if (today != null && today == i) {
                dc.setPenWidth(px(2));
                dc.setColor(FG, Graphics.COLOR_BLACK);
                dc.drawCircle(p[0], p[1], px(22));
            }
            var textColor = SEC;
            if (day == null) {
                dc.setColor(FREE, Graphics.COLOR_BLACK);
                dc.fillCircle(p[0], p[1], px(18));
            } else if (day.get("s2") == null) {
                // Eine Sportart: voll = erledigt, grau mit farbigem Buchstaben = geplant
                var color = sportColor(day.get("s"));
                var planned = numOrNull(day.get("p")) == 1;
                dayPart(dc, p, color, planned, 0);
                textColor = planned ? color : Graphics.COLOR_BLACK;
            } else {
                // Doppeltag: links die erste, rechts die zweite Sportart, getrennt durch eine schwarze Linie
                var planned1 = numOrNull(day.get("p")) == 1;
                var planned2 = numOrNull(day.get("p2")) == 1;
                dayPart(dc, p, sportColor(day.get("s")), planned1, -1);
                dayPart(dc, p, sportColor(day.get("s2")), planned2, 1);
                // Breiter schwarzer Spalt, sonst verschwimmen die beiden Hälften auf der Uhr zu einem Kreis
                dc.setColor(Graphics.COLOR_BLACK, Graphics.COLOR_BLACK);
                dc.fillRectangle(p[0] - px(2), p[1] - px(20), px(4), px(40));
                textColor = planned1 || planned2 ? FG : Graphics.COLOR_BLACK;
                if (planned1 && planned2) {
                    dc.setColor(sportColor(day.get("s")), Graphics.COLOR_TRANSPARENT);
                    dc.fillCircle(p[0] - px(8), p[1], px(5));
                    dc.setColor(sportColor(day.get("s2")), Graphics.COLOR_TRANSPARENT);
                    dc.fillCircle(p[0] + px(8), p[1], px(5));
                    continue;
                }
            }
            dc.setColor(textColor, Graphics.COLOR_TRANSPARENT);
            dc.drawText(p[0], p[1], _cap, letters[i], just);
        }
    }

    // Kreis oder Halbkreis eines Tages: side 0 = ganz, -1 = linke Hälfte, 1 = rechte Hälfte.
    // Erledigt = volle Sportfarbe, geplant = graue Scheibe mit einem Hauch der Sportfarbe; die Sportart zeigt dort der Buchstabe.
    private function dayPart(dc as Dc, p as Array<Number>, color as Number, planned as Boolean, side as Number) as Void {
        dc.setColor(planned ? blend(FREE, color, 0.18) : color, Graphics.COLOR_BLACK);
        if (side == 0) {
            dc.fillCircle(p[0], p[1], px(19));
        } else {
            // Gefüllter Halbkreis als Vieleck
            var r = px(19);
            var pts = [] as Array<Graphics.Point2D>;
            for (var k = 0; k <= 12; k++) {
                var a = Math.toRadians(90 + 180 * k / 12.0);
                pts.add([p[0] - side * r * Math.cos(a), p[1] - r * Math.sin(a)]);
            }
            dc.fillPolygon(pts);
        }
    }

    private function sportColor(sport) as Number {
        if ("run".equals(sport)) { return 0xFFD60A; }        // gelb
        if ("bike".equals(sport)) { return 0x30D158; }       // grün
        if ("swim".equals(sport)) { return 0x409CFF; }       // blau
        if ("strength".equals(sport)) { return 0xFF7A00; }   // orange, kräftiger als das Warn-Orange, damit es sich vom Gelb vom Laufen abhebt
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
        drawLine(dc, px(119), [["dot", px(6), sportColor(sport)], ["gap", px(9)], ["t", dayText, SEC, _cap], ["gap", px(10)], ["t", label, FG, _small]]);
    }

    // ---- Bereitschaft, dahinter die Warnung ------------------------------------------
    // rd: ok, warn, bad, none (wie im Dashboard). w: { k: "acwr", v } oder { k: "hint", t: Stichwort }.

    private function drawReady(dc as Dc, data as Dictionary) as Void {
        var rd = data.get("rd");
        var word = "Ohne Eintrag";
        var color = SEC;
        if ("ok".equals(rd)) { word = "Bereit"; color = OK; }
        else if ("warn".equals(rd)) { word = "Mit Vorsicht"; color = WARN; }
        else if ("bad".equals(rd)) { word = "Ruhig angehen"; color = BAD; }
        // Älter als eine Stunde: alles grau mit "!"
        var stale = isStale(data);
        var items = [["dot", px(7), stale ? TER : color], ["gap", px(9)], ["t", stale ? word + " !" : word, stale ? SEC : FG, _small]] as Array<Array>;
        var w = data.get("w") as Dictionary?;
        if (w != null) {
            // Stichwort aus "Was auffällt" (z. B. "Eiweiß"); ohne Stichwort (ältere Worker-Version) "Hinweis"
            var tag = w.get("t");
            var text = tag instanceof Lang.String && tag.length() > 0 ? tag : "Hinweis";
            var v = numOrNull(w.get("v"));
            if ("acwr".equals(w.get("k")) && v != null) {
                text = "ACWR " + decimal(v, "%.2f");
            }
            items.add(["gap", px(14)]);
            items.add(["bar", px(22), TER]);
            items.add(["gap", px(14)]);
            items.add(["tri", px(19), WARN]);
            items.add(["gap", px(8)]);
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
        // In der Mitte der Rest bis zum Ziel, darüber hinaus "+N"; ohne Ziel die gegessenen kcal mit "kcal".
        // Der Ring füllt sich mit dem Gegessenen.
        var kText = kv != null ? kv.toNumber().toString() : "--";
        var kSub = "kcal";
        if (kv != null && kg != null && kg > 0) {
            var rest = (kg - kv).toNumber();
            kText = rest >= 0 ? rest.toString() : "+" + (-rest).toString();
            kSub = "";
        }
        // Zwei Ringe: außen Kalorien (weiß), innen Eiweiß (hellgrau) gegen das Yazio-Ziel; ohne Eiweißziel nur der Kalorienring.
        // Orange nur bei einem Problem: mehr als KCAL_OVER über dem Ziel (Ring und Zahl) oder ab PROT_HOUR Uhr unter PROT_MIN Eiweiß.
        var over = kv != null && kg != null && kg > 0 && kv - kg > KCAL_OVER;
        var kColor = over ? WARN : FG;
        var pr = data.get("pr") as Dictionary?;
        var pv = pr != null ? numOrNull(pr.get("v")) : null;
        var pg = pr != null ? numOrNull(pr.get("g")) : null;
        if (pg != null && pg > 0 && kSub.length() == 0) {
            var pf = frac(pv, pg);
            var low = pf < PROT_MIN && System.getClockTime().hour >= PROT_HOUR;
            ring(dc, px(104), px(COMP_Y), px(34), px(5), frac(kv, kg), kColor, TRACK);
            ring(dc, px(104), px(COMP_Y), px(26), px(5), pf, low ? WARN : BOTTOM, FREE);
            labels(dc, 104, "ESSEN", kText, "", kColor);
        } else {
            ring(dc, px(104), px(COMP_Y), px(34), px(6), frac(kv, kg), kColor, TRACK);
            labels(dc, 104, "ESSEN", kText, kSub, kColor);
        }

        // TSS der Woche gegen den Plan (ohne Plan gegen WEEKLY_TSS_GOAL)
        var tss = numOrNull(data.get("tss"));
        var target = numOrNull(data.get("plan"));
        if (target == null) {
            target = numOrNull(data.get("goal"));
        }
        var tss2 = data.get("ts") as Array?;
        if (tss2 != null && tss2.size() > 0) {
            tssRing(dc, px(185), px(COMP_Y), px(34), px(6), tss2, target);
            labels(dc, 185, "TSS", tss != null ? tss.toNumber().toString() : "--", target != null ? "/" + target.toNumber() : "", FG);
        } else {
            // Älterer Worker ohne TSS je Sportart: ein weißer Ring
            complication(dc, 185, "TSS", tss != null ? tss.toNumber().toString() : "--", target != null ? "/" + target.toNumber() : "", frac(tss, target), FG, TRACK);
        }

        // Kraft: Sätze der Woche gegen das Ziel aus dem Trainingsblock; Ring in Ampelfarbe, solange es nicht im Ziel ist
        // (base und none, also Ausgangswert oder noch keine Daten, grau)
        var str = data.get("str") as Dictionary?;
        var sv = str != null ? numOrNull(str.get("v")) : null;
        var so = str != null ? numOrNull(str.get("of")) : null;
        var st = str != null ? str.get("ok") : null;
        // Im Ziel weiß, hinterher orange, deutlich hinterher rot (Ring und Zahl)
        var sc = "ok".equals(st) ? FG : ("warn".equals(st) ? WARN : ("bad".equals(st) ? BAD : SEC));
        ring(dc, px(269), px(COMP_Y), px(34), px(6), frac(sv, so), sc, TRACK);
        labels(dc, 269, "KRAFT", sv != null ? sv.toNumber().toString() : "--", so != null ? "/" + so.toNumber() : "", "warn".equals(st) || "bad".equals(st) ? sc : FG);

        // Wetter kommt von der Uhr: Temperatur in der Mitte, darunter die höchste Regenwahrscheinlichkeit der nächsten 12 Stunden.
        // Der Ring ist ein 12-Stunden-Zifferblatt: jede Stunde ein Bogen an ihrer Uhrposition, blau, wenn es dann regnen kann,
        // je kräftiger, desto wahrscheinlicher. Blau auch im Mono-Modus, denn hier bedeutet die Farbe etwas.
        var temp = "--";
        var chances = new Array<Number?>[12];
        var now = Time.now().value();
        var cond = Weather.getCurrentConditions();
        if (cond != null) {
            if (cond.temperature != null) {
                var c = (cond.temperature as Numeric).toFloat();
                var imperial = System.getDeviceSettings().temperatureUnits == System.UNIT_STATUTE;
                temp = Math.round(imperial ? c * 9 / 5 + 32 : c).toNumber().toString() + "°";
            }
            if (cond.precipitationChance != null) {
                chances[System.getClockTime().hour % 12] = Math.round((cond.precipitationChance as Numeric).toFloat()).toNumber();
            }
        }
        if (Weather has :getHourlyForecast) {
            var hourly = Weather.getHourlyForecast();
            if (hourly != null) {
                for (var i = 0; i < hourly.size(); i++) {
                    var h = hourly[i];
                    if (h == null || h.forecastTime == null || h.precipitationChance == null) {
                        continue;
                    }
                    var m = h.forecastTime as Time.Moment;
                    // Laufende Stunde plus die nächsten 11; die Stunde in 12 Stunden läge wieder auf dem Segment von jetzt
                    var dt = m.value() - now;
                    if (dt <= -3600 || dt > 11 * 3600) {
                        continue;
                    }
                    var slot = Gregorian.info(m, Time.FORMAT_SHORT).hour % 12;
                    if (chances[slot] == null) {
                        chances[slot] = Math.round((h.precipitationChance as Numeric).toFloat()).toNumber();
                    }
                }
            }
        }
        // -1 = keine Regenwahrscheinlichkeit bekannt
        var maxChance = -1;
        for (var i = 0; i < 12; i++) {
            var hc = chances[i];
            if (hc != null && (hc as Number) > maxChance) {
                maxChance = hc as Number;
            }
        }
        rainRing(dc, px(350), px(COMP_Y), px(34), px(6), chances, RAIN);
        // Prozentzahl blau ab 30 %, darunter weiß
        labels(dc, 350, "WETTER", temp, maxChance >= 0 ? maxChance.toString() + " %" : "", maxChance >= 30 ? RAIN : FG);
    }

    // Ring mit Fortschritt; der Grundring ist die eigene Farbe abgedunkelt, damit auch ein leerer Ring farbig ist
    private function complication(dc as Dc, x as Number, label as String, value as String, sub as String, f as Float, color as Number, track as Number) as Void {
        ring(dc, px(x), px(COMP_Y), px(34), px(6), f, color, track);
        labels(dc, x, label, value, sub, FG);
    }

    // TSS-Ring wie die Wochenkreise: ab 12 Uhr das Erledigte je Sportart in Sportfarbe, das noch Offene bleibt grauer
    // Grundring. Volle Runde = Plan (ohne Plan das Wochenziel, ohne beides die Summe). ts: [[Sportart, erledigt, offen], ...]
    private function tssRing(dc as Dc, cx as Number, cy as Number, r as Number, pen as Number, ts as Array, target as Numeric?) as Void {
        var sum = 0.0;
        for (var i = 0; i < ts.size(); i++) {
            var e = ts[i] as Array;
            sum += (e[1] as Numeric).toFloat() + (e[2] as Numeric).toFloat();
        }
        var full = (target != null && target > 0) ? target.toFloat() : sum;
        dc.setPenWidth(pen);
        dc.setColor(TRACK, Graphics.COLOR_BLACK);
        dc.drawCircle(cx, cy, r);
        if (full <= 0) {
            return;
        }
        var a = 0.0;
        for (var i = 0; i < ts.size(); i++) {
            var e = ts[i] as Array;
            var v = (e[1] as Numeric).toFloat();
            if (v <= 0 || a >= 360) {
                continue;
            }
            var d = 360 * v / full;
            var end = a + d > 360 ? 360.0 : a + d;
            // 3° Lücke zwischen den Abschnitten, sehr kleine Abschnitte ohne Lücke
            var gap = end - a > 8 ? 3 : 0;
            dc.setColor(sportColor(e[0]), Graphics.COLOR_BLACK);
            if (end - a >= 359) {
                dc.drawCircle(cx, cy, r);
            } else if (end - gap - a >= 1) {
                dc.drawArc(cx, cy, r, Graphics.ARC_CLOCKWISE, norm(90 - a), norm(90 - end + gap));
            }
            a = end;
        }
    }

    private function labels(dc as Dc, x as Number, label as String, value as String, sub as String, valueColor as Number) as Void {
        var cx = px(x);
        var cy = px(COMP_Y);
        var just = Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER;
        dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
        dc.drawText(cx, px(COMP_Y - 48), _tiny, label, just);
        dc.setColor(valueColor, Graphics.COLOR_TRANSPARENT);
        dc.drawText(cx, sub.length() > 0 ? cy - px(9) : cy, _mid, value, just);
        if (sub.length() > 0) {
            // Unterzeile ("/60", "kcal") eine Stufe größer und heller, sonst sind 6 und 8 kaum zu unterscheiden
            dc.setColor(BOTTOM, Graphics.COLOR_TRANSPARENT);
            dc.drawText(cx, cy + px(15), _cap, sub, just);
        }
    }

    // Regenring: grauer Grundring wie bei den anderen Komplikationen, darauf ein blauer Bogen für jede Stunde
    // mit mindestens 10 % Regenwahrscheinlichkeit (Bogen i liegt zwischen i und i+1 Uhr). Ein weißer Strich markiert
    // die aktuelle Uhrzeit, dort beginnt und endet die Zeitleiste. Ohne Regen bleibt der Ring grau.
    private function rainRing(dc as Dc, cx as Number, cy as Number, r as Number, pen as Number, chances as Array<Number?>, color as Number) as Void {
        dc.setPenWidth(pen);
        dc.setColor(TRACK, Graphics.COLOR_BLACK);
        dc.drawCircle(cx, cy, r);
        for (var i = 0; i < 12; i++) {
            var c = chances[i];
            if (c == null || c < 10) {
                continue;
            }
            dc.setColor(blend(TRACK, color, 0.35 + 0.65 * clamp01(c / 100.0)), Graphics.COLOR_BLACK);
            dc.drawArc(cx, cy, r, Graphics.ARC_CLOCKWISE, norm(90 - i * 30), norm(90 - (i + 1) * 30));
        }
        var clock = System.getClockTime();
        var t = Math.toRadians((clock.hour % 12 + clock.min / 60.0) * 30);
        var sin = Math.sin(t);
        var cos = Math.cos(t);
        var r0 = r - pen / 2 - px(3);
        var r1 = r + pen / 2 + px(3);
        // Schwarz unterlegt, damit der Strich auf hellen und dunklen Segmenten gleich gut zu sehen ist
        dc.setColor(Graphics.COLOR_BLACK, Graphics.COLOR_BLACK);
        dc.setPenWidth(px(6));
        dc.drawLine(cx + r0 * sin, cy - r0 * cos, cx + r1 * sin, cy - r1 * cos);
        dc.setColor(FG, Graphics.COLOR_BLACK);
        dc.setPenWidth(px(2));
        dc.drawLine(cx + r0 * sin, cy - r0 * cos, cx + r1 * sin, cy - r1 * cos);
    }

    // Farbe zwischen a (f = 0) und b (f = 1)
    private function blend(a as Number, b as Number, f as Float) as Number {
        var r = ((a >> 16) & 0xFF) + ((((b >> 16) & 0xFF) - ((a >> 16) & 0xFF)) * f).toNumber();
        var g = ((a >> 8) & 0xFF) + ((((b >> 8) & 0xFF) - ((a >> 8) & 0xFF)) * f).toNumber();
        var bl = (a & 0xFF) + (((b & 0xFF) - (a & 0xFF)) * f).toNumber();
        return (r << 16) | (g << 8) | bl;
    }

    // ---- Unten: Renn-Countdown und Blockwoche als gerade Zeile -----------------------

    private function drawStatusLine(dc as Dc, data as Dictionary) as Void {
        var race = data.get("race") as Dictionary?;
        var raceDays = race != null ? numOrNull(race.get("d")) : null;
        var raceText = (raceDays != null && raceDays >= 0 && raceDays <= 100) ? "RENNEN " + raceDays.toNumber() + " T" : "";
        var blockText = "";
        var block = data.get("block") as Dictionary?;
        if (block != null) {
            var st = block.get("st");
            var bd = numOrNull(block.get("d"));
            var bw = numOrNull(block.get("w"));
            var of = numOrNull(block.get("of"));
            if ("upcoming".equals(st) && bd != null) {
                blockText = "BLOCK IN " + bd.toNumber() + " T";
            } else if ("active".equals(st) && bw != null) {
                blockText = "WOCHE " + bw.toNumber() + (of != null ? "/" + of.toNumber() : "");
            }
        }
        var text = raceText.length() > 0 && blockText.length() > 0 ? raceText + "  ·  " + blockText : raceText + blockText;
        if (text.length() == 0) {
            return;
        }
        // Heller als die übrigen Beschriftungen, sonst ist die Zeile am Rand kaum lesbar.
        // Die große Schrift, wenn sie in die Breite des Kreises an dieser Stelle passt, sonst die kleinere.
        dc.setColor(BOTTOM, Graphics.COLOR_TRANSPARENT);
        var y = px(404);
        var dy = y - _cy;
        var room = 2 * Math.sqrt(_cx * _cx - dy * dy) - px(40);
        var f = dc.getTextWidthInPixels(text, _small) <= room ? _small : _cap;
        dc.drawText(_cx, y, f, text, Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
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
