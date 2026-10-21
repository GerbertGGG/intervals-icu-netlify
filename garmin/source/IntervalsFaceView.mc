import Toybox.Activity;
import Toybox.Application;
import Toybox.Graphics;
import Toybox.Lang;
import Toybox.Math;
import Toybox.System;
import Toybox.Time;
import Toybox.Time.Gregorian;
import Toybox.Weather;
import Toybox.WatchUi;

// Zifferblatt im Apple-Stil, möglichst ohne Text: Weiß ist der Normalfall. Farbe heißt entweder erledigte Sportart (Woche, TSS) oder
// "hier stimmt etwas nicht" (orange/rot), dazu Blau für Regen und gedämpftes Gold für die hellen Stunden. Geplantes, Grundringe und alles im Soll bleiben grau/weiß.
// Ganz oben der Akku, darunter mit Wochenzielen die offenen Minuten je Sportart und Kraft (sonst die Woche als Reihe und die nächste
// Einheit), große Uhrzeit, deren Doppelpunkt das Urteil des Coaches trägt, darunter das Datum und unten vier große Ringe im Bogen
// (Essen, Zeit, TSS, Wetter) ohne Überschriften.
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
    const ACCENT = 0x64A8FF; // Coach (wie im Widget)
    const KCAL_OVER = 150;   // so viele kcal über dem Ziel, bevor Essen orange wird
    const PROT_MIN = 0.8;    // Eiweiß-Anteil am Ziel, unter dem Essen ab PROT_HOUR orange wird
    const PROT_HOUR = 18;
    // Regenwahrscheinlichkeit in vier Stufen von sehr hellem bis dunklem Blau: je dunkler, desto sicherer Regen
    const RAIN_1 = 0xCDEEFF; // 30-49 %
    const RAIN_2 = 0x64D2FF; // 50-69 %
    const RAIN_3 = 0x2F8CFF; // 70-84 %
    const RAIN_4 = 0x1E4FE6; // ab 85 %
    const SUN = 0x7F702A;    // gedämpftes Gold für die hellen Stunden im Wetterring, kräftiges Gelb ist Laufen
    const STALE_SECS = 3600;
    const BAT_LOW = 20;      // Akku in Prozent, ab dem er orange wird (ab der Hälfte davon rot)
    const PROT = 0xC77DFF;   // Eiweiß im Essen-Ring: lila, damit es sich von Kalorien (weiß) und den Sportfarben abhebt
    const BAT_Y = 40;
    const TIME_Y = 200;
    const DATE_Y = 290;
    // Vier Ringe im Bogen: außen etwas höher (Essen links, Wetter rechts), innen tiefer (Zeit, TSS)
    const OUTER_Y = 312;
    const INNER_Y = 378;
    const RING_R = 44;
    const RING_PEN = 8;

    private var _s as Float = 1.0;
    private var _cx as Number = 0;
    private var _cy as Number = 0;
    private var _sleep as Boolean = false;
    private var _time as Graphics.FontType = Graphics.FONT_NUMBER_HOT;
    private var _big as Graphics.FontType = Graphics.FONT_XTINY;
    private var _small as Graphics.FontType = Graphics.FONT_XTINY;
    private var _mid as Graphics.FontType = Graphics.FONT_XTINY;
    private var _cap as Graphics.FontType = Graphics.FONT_XTINY;

    function initialize() {
        WatchFace.initialize();
        // Bitmap-Schriften aus Nunito (erzeugt mit tools/make_font.py, Größen für 416 Pixel)
        _time = WatchUi.loadResource(Rez.Fonts.Time) as Graphics.FontType;
        _big = WatchUi.loadResource(Rez.Fonts.Big) as Graphics.FontType;
        _small = WatchUi.loadResource(Rez.Fonts.Small) as Graphics.FontType;
        _mid = WatchUi.loadResource(Rez.Fonts.Mid) as Graphics.FontType;
        _cap = WatchUi.loadResource(Rez.Fonts.Cap) as Graphics.FontType;
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
        drawBatteryLine(dc);
        drawDate(dc, data);

        if (data == null) {
            dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
            dc.drawText(_cx, px(INNER_Y), _small, "Keine Daten" + errSuffix(), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
            return;
        }
        // Mit Wochenzielen des Coaches oben, was noch offen ist, statt der Wochenreihe und der nächsten Einheit
        var rz = data.get("rz") as Array?;
        if (rz != null && rz.size() > 0) {
            drawOpen(dc, data, rz);
        } else {
            drawWeek(dc, data);
            drawNext(dc, data);
        }
        drawComplications(dc, data);
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
        var gap = px(14);
        dc.setColor(FG, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx - gap, y, _time, hs, Graphics.TEXT_JUSTIFY_RIGHT | Graphics.TEXT_JUSTIFY_VCENTER);
        dc.drawText(_cx + gap, y, _time, ms, Graphics.TEXT_JUSTIFY_LEFT | Graphics.TEXT_JUSTIFY_VCENTER);
        coachColon(dc, data, y);
    }

    // Coach-Urteil als Doppelpunkt, Form und Farbe wie bisher in der Coach-Zeile: Kreise grün = hart erlaubt,
    // Dreiecke orange = nur locker, Quadrate rot = frei, Kreise blau = Coach hat geschrieben (ohne Urteil), grau = noch nichts
    // oder Daten älter als eine Stunde. Eigene Regeln gibt es nicht.
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
        var d = px(17);
        for (var i = -1; i <= 1; i += 2) {
            var cy = y + i * d;
            if ("warn".equals(rd)) {
                var t = px(10);
                dc.fillPolygon([[_cx - t, cy + t * 4 / 5], [_cx, cy - t], [_cx + t, cy + t * 4 / 5]]);
            } else if ("bad".equals(rd)) {
                var q = px(8);
                dc.fillRectangle(_cx - q, cy - q, 2 * q, 2 * q);
            } else {
                dc.fillCircle(_cx, cy, px(8));
            }
        }
    }

    // ---- Ganz oben: Akku ---------------------------------------------------------------
    // Akkusymbol mit Prozent. Weiß im Normalfall, orange ab BAT_LOW, rot ab der Hälfte davon, grün beim Laden.

    private function drawBatteryLine(dc as Dc) as Void {
        var stats = System.getSystemStats();
        var pct = Math.round(stats.battery).toNumber();
        var color = stats.charging ? OK : (pct <= BAT_LOW / 2 ? BAD : (pct <= BAT_LOW ? WARN : FG));
        drawLine(dc, px(BAT_Y), [["bat", px(32), color, pct], ["gap", px(8)], ["t", pct.toString() + " %", color == FG ? BOTTOM : color, _mid]]);
    }

    // ---- Unter der Uhrzeit: Datum ----------------------------------------------------------
    // "DI 21. OKT" zwischen den äußeren Ringen. Daten älter als eine Stunde: grau mit "!" und Fehlercode.
    // Daneben die Zahl der Blocker des Coaches (bl) als orangefarbenes Dreieck mit Zahl.

    private function drawDate(dc as Dc, data as Dictionary?) as Void {
        var info = Gregorian.info(Time.now(), Time.FORMAT_SHORT);
        var days = ["SO", "MO", "DI", "MI", "DO", "FR", "SA"];
        var months = ["JAN", "FEB", "MÄR", "APR", "MAI", "JUN", "JUL", "AUG", "SEP", "OKT", "NOV", "DEZ"];
        var text = days[(info.day_of_week as Number) - 1] + " " + info.day + ". " + months[(info.month as Number) - 1];
        var stale = data != null && isStale(data);
        var items = [["t", stale ? text + " !" + errSuffix() : text, stale ? SEC : FG, _small]] as Array<Array>;
        var bl = data != null ? numOrNull(data.get("bl")) : null;
        if (!stale && bl != null && bl.toNumber() > 0) {
            items.add(["gap", px(12)]);
            items.add(["tri", px(17), WARN]);
            items.add(["gap", px(5)]);
            items.add(["t", bl.toNumber().toString(), WARN, _small]);
        }
        drawLine(dc, px(DATE_Y), items);
    }

    // Akkusymbol: Rahmen mit Pol rechts, gefüllt nach Ladestand; x = linke Kante, y = Mitte, w = Breite samt Pol
    private function drawBattery(dc as Dc, x as Number, y as Number, w as Number, color as Number, pct as Number) as Void {
        var nub = px(3) > 1 ? px(3) : 1;
        var bw = w - nub;
        var h = px(14);
        var pen = px(2) > 1 ? px(2) : 1;
        dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
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

    // ---- Oben: Woche Mo bis So als gerade Reihe ------------------------------------
    // Abgerundete Quadrate: gefüllt in Sportfarbe = erledigt, graues Feld mit Buchstaben in Sportfarbe = geplant, dunkel = frei, weißer Rahmen = heute.
    // Zwei Sportarten an einem Tag: geteiltes Feld, links die erste (erledigt vor geplant), rechts die zweite;
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
            // Gerade Reihe mit 44 Pixel Abstand; Felder 36 Pixel, damit Mo und So samt Heute-Rahmen noch im runden Display liegen
            var p = [_cx + px(44 * (i - 3)), px(80)];
            var day = days[i] as Dictionary?;
            if (today != null && today == i) {
                dc.setPenWidth(px(2));
                dc.setColor(FG, Graphics.COLOR_BLACK);
                dc.drawRoundedRectangle(p[0] - px(22), p[1] - px(22), px(44), px(44), px(13));
            }
            var textColor = SEC;
            if (day == null) {
                dc.setColor(FREE, Graphics.COLOR_BLACK);
                dc.fillRoundedRectangle(p[0] - px(17), p[1] - px(17), px(34), px(34), px(8));
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
                // Breiter schwarzer Spalt, sonst verschwimmen die beiden Hälften auf der Uhr zu einem Feld
                dc.setColor(Graphics.COLOR_BLACK, Graphics.COLOR_BLACK);
                dc.fillRectangle(p[0] - px(2), p[1] - px(19), px(4), px(38));
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

    // Feld eines Tages (abgerundetes Quadrat): side 0 = ganz, -1 = linke Hälfte, 1 = rechte Hälfte.
    // Erledigt = volle Sportfarbe, geplant = graues Feld mit einem Hauch der Sportfarbe; die Sportart zeigt dort der Buchstabe.
    private function dayPart(dc as Dc, p as Array<Number>, color as Number, planned as Boolean, side as Number) as Void {
        dc.setColor(planned ? blend(FREE, color, 0.18) : color, Graphics.COLOR_BLACK);
        var h = px(18);
        if (side != 0) {
            // Halbes Feld: ganzes Feld zeichnen, aber nur die eine Hälfte freigeben
            dc.setClip(side < 0 ? p[0] - h : p[0], p[1] - h, h, 2 * h);
        }
        dc.fillRoundedRectangle(p[0] - h, p[1] - h, 2 * h, 2 * h, px(9));
        if (side != 0) {
            dc.clearClip();
        }
    }

    private function sportColor(sport) as Number {
        if ("run".equals(sport)) { return 0xFFD60A; }        // gelb
        if ("bike".equals(sport)) { return 0x30D158; }       // grün
        if ("swim".equals(sport)) { return 0x409CFF; }       // blau
        if ("strength".equals(sport)) { return 0xFF7A00; }   // orange, kräftiger als das Warn-Orange, damit es sich vom Gelb vom Laufen abhebt
        return SEC;
    }

    // ---- Oben mit Wochenzielen: was diese Woche noch offen ist ------------------------
    // Je Sportart eine Kapsel mit den offenen Minuten bis zum Zeitziel (Lauf, Rad, Schwimmen), dazu Kraft mit den offenen
    // Sätzen bis zum Wochenziel. Dunkel mit Zahl in Sportfarbe = noch offen, voll in Sportfarbe mit Haken = erreicht.

    private function drawOpen(dc as Dc, data as Dictionary, rz as Array) as Void {
        var sports = [] as Array;
        var values = [] as Array;
        var order = ["run", "bike", "swim"];
        for (var k = 0; k < order.size(); k++) {
            for (var i = 0; i < rz.size(); i++) {
                var e = rz[i] as Array;
                if (order[k].equals(e[0])) {
                    var open = (e[2] as Numeric).toNumber() - (e[1] as Numeric).toNumber();
                    sports.add(order[k]);
                    values.add(open > 0 ? open.toString() + "′" : "");
                }
            }
        }
        var str = data.get("str") as Dictionary?;
        var sv = str != null ? numOrNull(str.get("v")) : null;
        var so = str != null ? numOrNull(str.get("of")) : null;
        if (sv != null && so != null && so > 0) {
            var openSets = so.toNumber() - sv.toNumber();
            sports.add("strength");
            values.add(openSets > 0 ? openSets.toString() : "");
        }
        var n = sports.size();
        var w = px(76);
        var gap = px(8);
        var h = px(52);
        var y = px(96);
        var x = _cx - (n * w + (n - 1) * gap) / 2;
        var just = Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER;
        for (var i = 0; i < n; i++) {
            var color = sportColor(sports[i]);
            var v = values[i] as String;
            var mx = x + w / 2;
            dc.setColor(v.length() > 0 ? blend(Graphics.COLOR_BLACK, color, 0.25) : color, Graphics.COLOR_BLACK);
            dc.fillRoundedRectangle(x, y - h / 2, w, h, h / 2);
            if (v.length() > 0) {
                dc.setColor(color, Graphics.COLOR_TRANSPARENT);
                dc.drawText(mx, y, _big, v, just);
            } else {
                // Haken als Linien, die Bitmap-Schriften haben kein ✓
                dc.setColor(Graphics.COLOR_BLACK, Graphics.COLOR_TRANSPARENT);
                dc.setPenWidth(px(5));
                dc.drawLine(mx - px(13), y, mx - px(4), y + px(9));
                dc.drawLine(mx - px(4), y + px(9), mx + px(14), y - px(9));
            }
            x += w + gap;
        }
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

    // ---- Vier Ringe im Bogen: Essen, Zeit und TSS (mit Wochenzielen, sonst TSS und Kraft), Wetter ----------
    // Ohne Überschriften, die Plätze sind fest; in der Mitte jeweils nur die Zahl in großer Schrift.

    private function drawComplications(dc as Dc, data as Dictionary) as Void {
        var ex = px(79);
        var ey = px(OUTER_Y);
        var r = px(RING_R);
        var pen = px(RING_PEN);
        // Essen: Kalorien gegen das Tagesziel
        var kcal = data.get("kcal") as Dictionary?;
        var kv = kcal != null ? numOrNull(kcal.get("v")) : null;
        var kg = kcal != null ? numOrNull(kcal.get("g")) : null;
        // In der Mitte der Rest bis zum Ziel, darüber hinaus "+N"; ohne Ziel die gegessenen kcal.
        // Der Ring füllt sich mit dem Gegessenen.
        var kText = kv != null ? kv.toNumber().toString() : "--";
        var hasGoal = kv != null && kg != null && kg > 0;
        if (hasGoal) {
            var rest = (kg - kv).toNumber();
            kText = rest >= 0 ? rest.toString() : "+" + (-rest).toString();
        }
        // Zwei Ringe: außen Kalorien (weiß), innen Eiweiß (lila) gegen das Yazio-Ziel; ohne Eiweißziel nur der Kalorienring.
        // Orange nur bei einem Problem: mehr als KCAL_OVER über dem Ziel (Ring und Zahl) oder ab PROT_HOUR Uhr unter PROT_MIN Eiweiß.
        var over = hasGoal && kv - kg > KCAL_OVER;
        var kColor = over ? WARN : FG;
        var pr = data.get("pr") as Dictionary?;
        var pv = pr != null ? numOrNull(pr.get("v")) : null;
        var pg = pr != null ? numOrNull(pr.get("g")) : null;
        ring(dc, ex, ey, r, pen, frac(kv, kg), kColor, TRACK);
        if (pg != null && pg > 0 && hasGoal) {
            var pf = frac(pv, pg);
            var low = pf < PROT_MIN && System.getClockTime().hour >= PROT_HOUR;
            ring(dc, ex, ey, px(RING_R - 11), px(7), pf, low ? WARN : PROT, FREE);
        }
        centerText(dc, ex, ey, kText, kColor, "", FG);

        // Wochenziele des Coaches: ZEIT (Minuten gegen die Summe der Zeitziele) und TSS (gegen die Summe der Obergrenzen) als je
        // ein Ring mit einem Abschnitt pro Sportart in Sportfarbe; wie viel je Sportart noch offen ist, zeigen die Kapseln oben.
        // In der Mitte die Summe; TSS orange, wenn eine Sportart über ihrer Grenze liegt.
        // Ohne Wochenziele wie bisher: TSS gegen Plan bzw. WEEKLY_TSS_GOAL und Kraft.
        var rz = data.get("rz") as Array?;
        if (rz != null && rz.size() > 0) {
            goalRing(dc, px(177), px(INNER_Y), rz);
            centerText(dc, px(177), px(INNER_Y), ringSum(rz).toString(), FG, "", FG);
            var rt = data.get("rt") as Array?;
            var tssOver = false;
            var tssText = "--";
            if (rt != null && rt.size() > 0) {
                tssOver = goalRing(dc, px(277), px(INNER_Y), rt);
                tssText = ringSum(rt).toString();
            } else {
                ring(dc, px(277), px(INNER_Y), r, pen, 0.0, FG, TRACK);
                var tssAll = numOrNull(data.get("tss"));
                tssText = tssAll != null ? tssAll.toNumber().toString() : "--";
            }
            centerText(dc, px(277), px(INNER_Y), tssText, tssOver ? WARN : FG, "", FG);
        } else {
            drawTssAndStrength(dc, data);
        }

        // Wetter kommt von der Uhr: Temperatur in der Mitte, darunter die höchste Regenwahrscheinlichkeit der nächsten 12 Stunden.
        // Der Ring ist ein 12-Stunden-Zifferblatt: jede Stunde ein Bogen an ihrer Uhrposition, blau, wenn es dann regnen kann,
        // von sehr hell (30 %) bis dunkel (ab 85 %), siehe rainColor. Blau auch im Mono-Modus, denn hier bedeutet die Farbe etwas.
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
        // Die laufende Stunde kommt aus der Stundenvorhersage. Die Wahrscheinlichkeit aus dem aktuellen Wetter passt oft
        // nicht zu ihr (eher ein Tageswert) und trieb die Prozentzahl hoch, deshalb nur, wenn die Stunde keinen Wert hat.
        var nowSlot = System.getClockTime().hour % 12;
        if (cond != null && chances[nowSlot] == null && cond.precipitationChance != null) {
            chances[nowSlot] = Math.round((cond.precipitationChance as Numeric).toFloat()).toNumber();
        }
        // Die Vorhersage hinkt oft hinterher: meldet die Wetterlage schon Niederschlag, regnet es jetzt,
        // also zählt die laufende Stunde als 100 %
        if (cond != null && cond.condition != null && isPrecipitation(cond.condition as Number)) {
            chances[nowSlot] = 100;
        }
        // -1 = keine Regenwahrscheinlichkeit bekannt
        var maxChance = -1;
        for (var i = 0; i < 12; i++) {
            var hc = chances[i];
            if (hc != null && (hc as Number) > maxChance) {
                maxChance = hc as Number;
            }
        }
        // Sonnenauf- und -untergänge von heute und morgen, damit auch abends der Morgen im 12-Stunden-Fenster liegt.
        // Selbst gerechnet aus dem Standort (siehe sunNow), Weather.getSunrise und der Standort der Wetterdaten fehlen auf der Uhr oft.
        var sun = sunNow(cond, now);
        rainRing(dc, px(375), px(OUTER_Y), px(RING_R), px(RING_PEN), chances, sun, now);
        // Prozentzahl ab 30 % im Blau ihrer Stufe, ab da zeigt auch der Ring Regen. Darunter gilt es als trocken und dort steht der
        // nächste Sonnenauf- oder -untergang, passend zum goldenen Bogen
        var sub = maxChance >= 0 ? maxChance.toString() + " %" : "";
        if (maxChance < 30) {
            for (var i = 0; i < sun.size(); i++) {
                if (sun[i] > now) {
                    var si = Gregorian.info(new Time.Moment(sun[i]), Time.FORMAT_SHORT);
                    sub = si.hour.format("%02d") + ":" + si.min.format("%02d");
                    break;
                }
            }
        }
        centerText(dc, px(375), px(OUTER_Y), temp, FG, sub, maxChance >= 30 ? rainColor(maxChance) : BOTTOM);
    }

    // Ohne Wochenziele: TSS der Woche gegen den Plan (ohne Plan gegen WEEKLY_TSS_GOAL) und Kraft daneben
    private function drawTssAndStrength(dc as Dc, data as Dictionary) as Void {
        var x1 = px(177);
        var x2 = px(277);
        var y = px(INNER_Y);
        var r = px(RING_R);
        var pen = px(RING_PEN);
        var tss = numOrNull(data.get("tss"));
        var target = numOrNull(data.get("plan"));
        if (target == null) {
            target = numOrNull(data.get("goal"));
        }
        var tss2 = data.get("ts") as Array?;
        if (tss2 != null && tss2.size() > 0) {
            tssRing(dc, x1, y, r, pen, tss2, target);
        } else {
            // Älterer Worker ohne TSS je Sportart: ein weißer Ring
            ring(dc, x1, y, r, pen, frac(tss, target), FG, TRACK);
        }
        centerText(dc, x1, y, tss != null ? tss.toNumber().toString() : "--", FG, "", FG);

        // Kraft: Sätze der Woche gegen das Ziel aus dem Trainingsblock; im Ziel weiß, hinterher orange, deutlich hinterher rot
        // (base und none, also Ausgangswert oder noch keine Daten, grau)
        var str = data.get("str") as Dictionary?;
        var sv = str != null ? numOrNull(str.get("v")) : null;
        var so = str != null ? numOrNull(str.get("of")) : null;
        var st = str != null ? str.get("ok") : null;
        var sc = "ok".equals(st) ? FG : ("warn".equals(st) ? WARN : ("bad".equals(st) ? BAD : SEC));
        ring(dc, x2, y, r, pen, frac(sv, so), sc, TRACK);
        centerText(dc, x2, y, sv != null ? sv.toNumber().toString() : "--", "warn".equals(st) || "bad".equals(st) ? sc : FG, "", FG);
    }

    // Ring gegen Wochenziele: ab 12 Uhr das Erledigte je Sportart in Sportfarbe (Lauf, Rad, Schwimmen), volle Runde = Summe der
    // Vorgaben. rings: [[Sportart, erledigt, Vorgabe], ...]. Gibt zurück, ob eine Sportart über ihrer Vorgabe liegt.
    private function goalRing(dc as Dc, cx as Number, cy as Number, rings as Array) as Boolean {
        var over = false;
        var goal = 0.0;
        var parts = [] as Array;
        var order = ["run", "bike", "swim"];
        for (var k = 0; k < order.size(); k++) {
            for (var i = 0; i < rings.size(); i++) {
                var e = rings[i] as Array;
                if (order[k].equals(e[0])) {
                    var v = (e[1] as Numeric).toFloat();
                    var g = (e[2] as Numeric).toFloat();
                    goal += g;
                    parts.add([e[0], v, 0]);
                    if (g > 0 && v > g) {
                        over = true;
                    }
                }
            }
        }
        tssRing(dc, cx, cy, px(RING_R), px(RING_PEN), parts, goal);
        return over;
    }

    private function ringSum(rings as Array) as Number {
        var sum = 0;
        for (var i = 0; i < rings.size(); i++) {
            sum += ((rings[i] as Array)[1] as Numeric).toNumber();
        }
        return sum;
    }

    // TSS-Ring wie die Wochenkreise: ab 12 Uhr das Erledigte je Sportart in Sportfarbe, das noch Offene bleibt grauer
    // Grundring. Volle Runde = Plan (ohne Plan das Wochenziel, ohne beides die Summe). ts: [[Sportart, erledigt, offen], ...]
    // Mit Wochenzielen zeichnet goalRing damit die Ringe für Zeit und TSS.
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

    // Zahl groß in der Mitte eines Rings, darunter optional eine kleine Zeile (Wetter: Regen-% oder Sonnenzeit)
    private function centerText(dc as Dc, cx as Number, cy as Number, value as String, valueColor as Number, sub as String, subColor as Number) as Void {
        var just = Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER;
        // Vier Zeichen ("+250", "1850") passen nicht in den Eiweißring, dann eine Stufe kleiner
        var f = dc.getTextWidthInPixels(value, _big) > px(56) ? _small : _big;
        dc.setColor(valueColor, Graphics.COLOR_TRANSPARENT);
        dc.drawText(cx, sub.length() > 0 ? cy - px(10) : cy, f, value, just);
        if (sub.length() > 0) {
            dc.setColor(subColor, Graphics.COLOR_TRANSPARENT);
            dc.drawText(cx, cy + px(17), _cap, sub, just);
        }
    }

    // Wetterring: grauer Grundring wie bei den anderen Komplikationen, darauf in gedämpftem Gold die hellen Stunden
    // (Sonnenaufgang bis -untergang) der nächsten 12 Stunden und darüber ein blauer Bogen für jede Stunde mit mindestens
    // 30 % Regenwahrscheinlichkeit in der Farbe ihrer Stufe (Bogen i liegt zwischen i und i+1 Uhr). Ein weißer Strich markiert die aktuelle
    // Uhrzeit, dort beginnt und endet die Zeitleiste. sun: [Aufgang, Untergang, Aufgang, Untergang] in Sekunden.
    private function rainRing(dc as Dc, cx as Number, cy as Number, r as Number, pen as Number, chances as Array<Number?>, sun as Array<Number>, now as Number) as Void {
        dc.setPenWidth(pen);
        dc.setColor(TRACK, Graphics.COLOR_BLACK);
        dc.drawCircle(cx, cy, r);
        dc.setColor(SUN, Graphics.COLOR_BLACK);
        for (var i = 0; i + 1 < sun.size(); i += 2) {
            var a = sun[i] > now ? sun[i] : now;
            var b = sun[i + 1] < now + 12 * 3600 ? sun[i + 1] : now + 12 * 3600;
            if (b - a < 120) {
                continue;
            }
            if (b - a >= 12 * 3600 - 120) {
                dc.drawCircle(cx, cy, r);
            } else {
                dc.drawArc(cx, cy, r, Graphics.ARC_CLOCKWISE, norm(90 - clockAngle(a)), norm(90 - clockAngle(b)));
            }
        }
        for (var i = 0; i < 12; i++) {
            var c = chances[i];
            if (c == null || c < 30) {
                continue;
            }
            dc.setColor(rainColor(c), Graphics.COLOR_BLACK);
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

    // Feste Stufen statt eines Verlaufs, damit man auf dem kleinen Ring 40 % und 80 % sicher auseinanderhält
    private function rainColor(c as Number) as Number {
        if (c >= 85) {
            return RAIN_4;
        }
        if (c >= 70) {
            return RAIN_3;
        }
        if (c >= 50) {
            return RAIN_2;
        }
        return RAIN_1;
    }

    // Wetterlagen, bei denen es gerade regnet, schneit oder hagelt
    private function isPrecipitation(c as Number) as Boolean {
        return c == Weather.CONDITION_RAIN || c == Weather.CONDITION_LIGHT_RAIN || c == Weather.CONDITION_HEAVY_RAIN
            || c == Weather.CONDITION_SHOWERS || c == Weather.CONDITION_HEAVY_SHOWERS || c == Weather.CONDITION_SCATTERED_SHOWERS
            || c == Weather.CONDITION_DRIZZLE || c == Weather.CONDITION_THUNDERSTORMS || c == Weather.CONDITION_SCATTERED_THUNDERSTORMS
            || c == Weather.CONDITION_SNOW || c == Weather.CONDITION_LIGHT_SNOW || c == Weather.CONDITION_HEAVY_SNOW
            || c == Weather.CONDITION_RAIN_SNOW || c == Weather.CONDITION_LIGHT_RAIN_SNOW || c == Weather.CONDITION_HEAVY_RAIN_SNOW
            || c == Weather.CONDITION_WINTRY_MIX || c == Weather.CONDITION_FREEZING_RAIN || c == Weather.CONDITION_SLEET
            || c == Weather.CONDITION_HAIL || c == Weather.CONDITION_UNKNOWN_PRECIPITATION;
    }

    // Sonnenzeiten für den aktuellen Standort: vom Wetter, sonst von der letzten Aktivität; der letzte bekannte Standort
    // bleibt gespeichert, damit die Zeiten auch ohne frische Position da sind. Ohne jeden Standort leer.
    private function sunNow(cond as Weather.CurrentConditions?, now as Number) as Array<Number> {
        var pos = cond != null ? cond.observationLocationPosition : null;
        if (pos == null) {
            var ai = Activity.getActivityInfo();
            pos = ai != null ? ai.currentLocation : null;
        }
        if (pos != null) {
            var d = pos.toDegrees();
            Application.Storage.setValue("loc", d);
            return sunTimes(now, d[0].toDouble(), d[1].toDouble());
        }
        var saved = Application.Storage.getValue("loc");
        if (saved instanceof Array && saved.size() == 2) {
            return sunTimes(now, (saved[0] as Numeric).toDouble(), (saved[1] as Numeric).toDouble());
        }
        return [] as Array<Number>;
    }

    // Sonnenauf- und -untergang (Unix-Sekunden) für den heutigen und den morgigen Tag nach der Sonnenaufgangsgleichung,
    // auf etwa eine Minute genau. Rechnet in Tagen ab J2000 (Unix 946728000), mit Double, weil Float zu ungenau ist.
    // Ergebnis: [Aufgang, Untergang, Aufgang, Untergang]; Tage ohne Auf- oder Untergang (Polartag/-nacht) fehlen.
    private function sunTimes(t as Number, lat as Double, lon as Double) as Array<Number> {
        var out = [] as Array<Number>;
        var rad = Math.PI / 180.0d;
        var n0 = (t / 86400.0d - 10957.5d + lon / 360.0d + 0.5d).toLong();
        for (var k = 0; k < 2; k++) {
            var js = (n0 + k).toDouble() - lon / 360.0d;
            var m = 357.5291d + 0.98560028d * js;
            m = m - 360.0d * (m / 360.0d).toLong();
            var c = 1.9148d * Math.sin(m * rad) + 0.02d * Math.sin(2 * m * rad) + 0.0003d * Math.sin(3 * m * rad);
            var l = m + c + 282.9372d;
            l = l - 360.0d * (l / 360.0d).toLong();
            var jt = js + 0.0053d * Math.sin(m * rad) - 0.0069d * Math.sin(2 * l * rad);
            var sd = Math.sin(l * rad) * Math.sin(23.4397d * rad);
            var cd = Math.sqrt(1 - sd * sd);
            var cw = (Math.sin(-0.833d * rad) - Math.sin(lat * rad) * sd) / (Math.cos(lat * rad) * cd);
            if (cw < -1 || cw > 1) {
                continue;
            }
            var w = Math.acos(cw) / rad / 360.0d;
            out.add(((jt - w + 10957.5d) * 86400).toNumber());
            out.add(((jt + w + 10957.5d) * 86400).toNumber());
        }
        return out;
    }

    // Winkel eines Zeitpunkts auf dem 12-Stunden-Zifferblatt in Grad ab 12 Uhr im Uhrzeigersinn
    private function clockAngle(t as Number) as Float {
        var info = Gregorian.info(new Time.Moment(t), Time.FORMAT_SHORT);
        return (info.hour % 12 + info.min / 60.0) * 30;
    }

    // Farbe zwischen a (f = 0) und b (f = 1)
    private function blend(a as Number, b as Number, f as Float) as Number {
        var r = ((a >> 16) & 0xFF) + ((((b >> 16) & 0xFF) - ((a >> 16) & 0xFF)) * f).toNumber();
        var g = ((a >> 8) & 0xFF) + ((((b >> 8) & 0xFF) - ((a >> 8) & 0xFF)) * f).toNumber();
        var bl = (a & 0xFF) + (((b & 0xFF) - (a & 0xFF)) * f).toNumber();
        return (r << 16) | (g << 8) | bl;
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
    // Elemente: ["t", Text, Farbe, Schrift], ["dot", Radius, Farbe], ["tri", Größe, Farbe], ["sq", Größe, Farbe], ["bat", Breite, Farbe, Prozent],
    // ["bar", Höhe, Farbe], ["gap", Breite]

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
                drawTri(dc, x, y, it[1] as Number, it[2] as Number);
            } else if (kind.equals("sq")) {
                drawSq(dc, x, y, it[1] as Number, it[2] as Number);
            } else if (kind.equals("bat")) {
                drawBattery(dc, x, y, w, it[2] as Number, it[3] as Number);
            } else if (kind.equals("bar")) {
                var bh = it[1] as Number;
                dc.setColor(it[2] as Number, Graphics.COLOR_TRANSPARENT);
                dc.fillRectangle(x, y - bh / 2, w, bh);
            }
            x += w;
        }
    }

    // Warn-Dreieck mit schwarzem Ausrufezeichen; x = linke Kante, y = Mitte
    private function drawTri(dc as Dc, x as Number, y as Number, sz as Number, color as Number) as Void {
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.fillPolygon([[x, y + sz / 2], [x + sz / 2, y - sz / 2], [x + sz, y + sz / 2]]);
        dc.setColor(Graphics.COLOR_BLACK, Graphics.COLOR_TRANSPARENT);
        dc.fillRectangle(x + sz / 2 - 1, y - sz / 6, 2, sz / 3);
        dc.fillRectangle(x + sz / 2 - 1, y + sz / 4, 2, 2);
    }

    // Quadrat mit schwarzem Kreuz (deutlich schlechter / verfehlt); x = linke Kante, y = Mitte
    private function drawSq(dc as Dc, x as Number, y as Number, sz as Number, color as Number) as Void {
        var h = sz / 2;
        var k = sz / 4;
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(x, y - h, sz, sz, px(2));
        dc.setColor(Graphics.COLOR_BLACK, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(2) > 1 ? px(2) : 1);
        dc.drawLine(x + k, y - h + k, x + sz - k, y + h - k);
        dc.drawLine(x + sz - k, y - h + k, x + k, y + h - k);
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
        if (kind.equals("tri") || kind.equals("sq") || kind.equals("bat")) {
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
