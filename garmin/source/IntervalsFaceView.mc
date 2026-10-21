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

// Zifferblatt "Kombi" (Mockup 2f): drei volle Ringe für die Wochenziele des Coaches, außen Lauf, in der Mitte Rad, innen Schwimmen.
// Innen von oben nach unten: Akku, Datum, große Uhrzeit (der Doppelpunkt trägt das Urteil des Coaches), die offenen Minuten je
// Sportart mit grauer Beschriftung und das Wetter. Farbe heißt Sportart, Blau Regen, Orange/Rot nur beim Akku.
// Alle Maße stammen aus dem Mockup (454 x 454) und werden mit _s auf die Displaygröße skaliert.
// Daten kommen aus dem Speicher (vom Hintergrunddienst geschrieben), gezeichnet wird nur.
class IntervalsFaceView extends WatchUi.WatchFace {

    const FG = 0xF5F5F7;
    const SEC = 0x8E8E93;
    const BOTTOM = 0xAEAEB2;
    const TER = 0x48484A;
    const OK = 0x30D158;
    const WARN = 0xFF9F0A;
    const BAD = 0xFF453A;
    const ACCENT = 0x64A8FF; // Coach (wie im Widget)
    // Regenwahrscheinlichkeit in vier Stufen von sehr hellem bis dunklem Blau: je dunkler, desto sicherer Regen
    const RAIN_1 = 0xCDEEFF; // 30-49 %
    const RAIN_2 = 0x64D2FF; // 50-69 %
    const RAIN_3 = 0x2F8CFF; // 70-84 %
    const RAIN_4 = 0x1E4FE6; // ab 85 %
    const STALE_SECS = 3600;
    const BAT_LOW = 20;      // Akku in Prozent, ab dem er orange wird (ab der Hälfte davon rot)
    const BAT_Y = 107;
    const DATE_Y = 141;
    const TIME_Y = 194;
    const OPEN_Y = 265;
    const TAG_Y = 288;
    const WEATHER_Y = 322;
    // Ringe: Radien außen (Lauf), Mitte (Rad), innen (Schwimmen) und Strichstärke
    const RING_RUN = 200;
    const RING_BIKE = 174;
    const RING_SWIM = 148;
    const RING_PEN = 22;

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
        // Bitmap-Schriften aus Nunito (erzeugt mit tools/make_font.py, Größen für 416 Pixel)
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
        drawRings(dc, rz);
        drawBatteryLine(dc);
        drawDate(dc, data);
        if (data == null) {
            dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
            dc.drawText(_cx, px(OPEN_Y), _cap, "Keine Daten" + errSuffix(), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
        } else {
            drawOpen(dc, rz);
        }
        drawWeather(dc);
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
        var gap = px(13);
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
        var d = px(15);
        for (var i = -1; i <= 1; i += 2) {
            var cy = y + i * d;
            if ("warn".equals(rd)) {
                var t = px(8);
                dc.fillPolygon([[_cx - t, cy + t * 4 / 5], [_cx, cy - t], [_cx + t, cy + t * 4 / 5]]);
            } else if ("bad".equals(rd)) {
                var q = px(6);
                dc.fillRectangle(_cx - q, cy - q, 2 * q, 2 * q);
            } else {
                dc.fillCircle(_cx, cy, px(6.5));
            }
        }
    }

    // ---- Drei Ringe: Wochenziele des Coaches -------------------------------------------
    // Grundring in gedämpfter Sportfarbe, darauf ab 12 Uhr im Uhrzeigersinn die Minuten der Woche gegen das Zeitziel
    // (rz: [[Sportart, erledigt, Ziel], ...]); volle Runde = Ziel erreicht. Ohne Ziel bleibt nur der Grundring.

    private function drawRings(dc as Dc, rz as Array?) as Void {
        var sports = ["run", "bike", "swim"];
        var radii = [RING_RUN, RING_BIKE, RING_SWIM];
        for (var k = 0; k < sports.size(); k++) {
            var color = sportColor(sports[k]);
            var e = goalOf(rz, sports[k]);
            var f = e != null ? frac(e[1] as Numeric, e[2] as Numeric) : 0.0;
            ring(dc, px(radii[k] as Number), px(RING_PEN), f, color, blend(Graphics.COLOR_BLACK, color, 0.2));
        }
    }

    // Vollkreis um die Mitte mit Fortschritt ab 12 Uhr im Uhrzeigersinn und runden Enden
    private function ring(dc as Dc, r as Number, pen as Number, f as Float, color as Number, track as Number) as Void {
        dc.setPenWidth(pen);
        dc.setColor(track, Graphics.COLOR_BLACK);
        dc.drawCircle(_cx, _cy, r);
        if (f <= 0) {
            return;
        }
        dc.setColor(color, Graphics.COLOR_BLACK);
        if (f >= 1.0) {
            dc.drawCircle(_cx, _cy, r);
            return;
        }
        var end = 360 * f;
        if (end >= 1) {
            dc.drawArc(_cx, _cy, r, Graphics.ARC_CLOCKWISE, 90, norm(90 - end));
        }
        // drawArc endet gerade, die runden Enden kommen als Kreise dazu
        var t = Math.toRadians(end);
        dc.fillCircle(_cx, _cy - r, pen / 2);
        dc.fillCircle(_cx + r * Math.sin(t), _cy - r * Math.cos(t), pen / 2);
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
    // Groß in Sportfarbe die Minuten bis zum Zeitziel, darunter grau die Sportart. Ziel erreicht: Haken in Sportfarbe,
    // ohne Ziel "--" in Grau.

    private function drawOpen(dc as Dc, rz as Array?) as Void {
        var sports = ["run", "bike", "swim"];
        var labels = ["LAUF", "RAD", "SCHWIMM."];
        var just = Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER;
        var y = px(OPEN_Y);
        for (var k = 0; k < sports.size(); k++) {
            var x = _cx + px(62 * (k - 1));
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
                    // Haken als Linien, die Bitmap-Schriften haben kein ✓
                    dc.setPenWidth(px(4));
                    dc.drawLine(x - px(10), y, x - px(3), y + px(7));
                    dc.drawLine(x - px(3), y + px(7), x + px(11), y - px(7));
                }
            }
            dc.setColor(SEC, Graphics.COLOR_TRANSPARENT);
            dc.drawText(x, px(TAG_Y), _tag, labels[k] as String, just);
        }
    }

    // ---- Oben: Akku ---------------------------------------------------------------
    // Akkusymbol mit Prozentzahl. Weiß im Normalfall, orange ab BAT_LOW, rot ab der Hälfte davon, grün beim Laden.

    private function drawBatteryLine(dc as Dc) as Void {
        var stats = System.getSystemStats();
        var pct = Math.round(stats.battery).toNumber();
        var color = stats.charging ? OK : (pct <= BAT_LOW / 2 ? BAD : (pct <= BAT_LOW ? WARN : FG));
        drawLine(dc, px(BAT_Y), [["bat", px(34), color, pct], ["gap", px(6)], ["t", pct.toString(), color == FG ? SEC : color, _cap]]);
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

    // ---- Datum ----------------------------------------------------------------------
    // "MI 21. OKT" in Grau. Daten älter als eine Stunde: dahinter ein oranges "!" und der Fehlercode des letzten Abrufs.

    private function drawDate(dc as Dc, data as Dictionary?) as Void {
        var info = Gregorian.info(Time.now(), Time.FORMAT_SHORT);
        var days = ["SO", "MO", "DI", "MI", "DO", "FR", "SA"];
        var months = ["JAN", "FEB", "MÄR", "APR", "MAI", "JUN", "JUL", "AUG", "SEP", "OKT", "NOV", "DEZ"];
        var text = days[(info.day_of_week as Number) - 1] + " " + info.day + ". " + months[(info.month as Number) - 1];
        var items = [["t", text, SEC, _cap]] as Array<Array>;
        if (data != null && isStale(data)) {
            items.add(["t", " !" + errSuffix(), WARN, _cap]);
        }
        drawLine(dc, px(DATE_Y), items);
    }

    // ---- Wetter ---------------------------------------------------------------------
    // Kommt von der Uhr: Temperatur, dahinter die höchste Regenwahrscheinlichkeit der nächsten 12 Stunden mit der Stunde, ab der
    // sie gilt ("70 % 14 h", in der laufenden Stunde "jetzt"), davor ein Tropfen, beides im Blau ihrer Stufe (siehe rainColor).
    // Unter 30 % gilt es als trocken, dann steht dort der nächste Sonnenauf- oder -untergang.

    private function drawWeather(dc as Dc) as Void {
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
                    // Laufende Stunde plus die nächsten 11; die Stunde in 12 Stunden läge wieder auf dem Platz von jetzt
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
        var hour = System.getClockTime().hour;
        var nowSlot = hour % 12;
        if (cond != null && chances[nowSlot] == null && cond.precipitationChance != null) {
            chances[nowSlot] = Math.round((cond.precipitationChance as Numeric).toFloat()).toNumber();
        }
        // Die Vorhersage hinkt oft hinterher: meldet die Wetterlage schon Niederschlag, regnet es jetzt,
        // also zählt die laufende Stunde als 100 %
        if (cond != null && cond.condition != null && isPrecipitation(cond.condition as Number)) {
            chances[nowSlot] = 100;
        }
        // Höchste Wahrscheinlichkeit und die erste Stunde ab jetzt, in der sie erreicht wird (-1 = nichts bekannt)
        var maxChance = -1;
        var maxIn = 0;
        for (var i = 0; i < 12; i++) {
            var hc = chances[(nowSlot + i) % 12];
            if (hc != null && (hc as Number) > maxChance) {
                maxChance = hc as Number;
                maxIn = i;
            }
        }
        var items = [] as Array<Array>;
        if (maxChance >= 30) {
            var rc = rainColor(maxChance);
            var when = maxIn == 0 ? "jetzt" : ((hour + maxIn) % 24).toString() + " h";
            items.add(["drop", px(14), rc]);
            items.add(["gap", px(7)]);
            items.add(["t", temp, FG, _cap]);
            items.add(["t", " · " + maxChance.toString() + " % " + when, rc, _cap]);
        } else {
            items.add(["t", temp, FG, _cap]);
            // Sonnenauf- und -untergänge von heute und morgen, selbst gerechnet aus dem Standort (siehe sunNow),
            // Weather.getSunrise und der Standort der Wetterdaten fehlen auf der Uhr oft
            var sun = sunNow(cond, now);
            for (var i = 0; i < sun.size(); i++) {
                if (sun[i] > now) {
                    var si = Gregorian.info(new Time.Moment(sun[i]), Time.FORMAT_SHORT);
                    items.add(["t", " · " + si.hour.format("%02d") + ":" + si.min.format("%02d"), BOTTOM, _cap]);
                    break;
                }
            }
        }
        drawLine(dc, px(WEATHER_Y), items);
    }

    // Feste Stufen statt eines Verlaufs, damit man 40 % und 80 % sicher auseinanderhält
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

    // Farbe zwischen a (f = 0) und b (f = 1)
    private function blend(a as Number, b as Number, f as Float) as Number {
        var r = ((a >> 16) & 0xFF) + ((((b >> 16) & 0xFF) - ((a >> 16) & 0xFF)) * f).toNumber();
        var g = ((a >> 8) & 0xFF) + ((((b >> 8) & 0xFF) - ((a >> 8) & 0xFF)) * f).toNumber();
        var bl = (a & 0xFF) + (((b & 0xFF) - (a & 0xFF)) * f).toNumber();
        return (r << 16) | (g << 8) | bl;
    }

    // ---- Zeile aus Text und Formen, als Ganzes zentriert -----------------------------
    // Elemente: ["t", Text, Farbe, Schrift], ["bat", Breite, Farbe, Prozent], ["drop", Breite, Farbe], ["gap", Breite]

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
            } else if (kind.equals("bat")) {
                drawBattery(dc, x, y, w, it[2] as Number, it[3] as Number);
            } else if (kind.equals("drop")) {
                drawDrop(dc, x, y, w, it[2] as Number);
            }
            x += w;
        }
    }

    // Regentropfen: Kreis unten, Spitze oben; x = linke Kante, y = Mitte, w = Breite
    private function drawDrop(dc as Dc, x as Number, y as Number, w as Number, color as Number) as Void {
        var r = w / 2;
        var cy = y + r / 2;
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(x + r, cy, r);
        dc.fillPolygon([[x + 1, cy - r / 3], [x + r, cy - 2 * r], [x + w - 1, cy - r / 3]]);
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
