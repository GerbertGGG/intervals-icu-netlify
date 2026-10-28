import Toybox.Application;
import Toybox.Graphics;
import Toybox.Lang;
import Toybox.Math;
import Toybox.System;
import Toybox.Time;
import Toybox.Time.Gregorian;
import Toybox.WatchUi;

class IntervalsFaceView extends WatchUi.WatchFace {

    const FG = 0xF5F5F7;
    const SEC = 0xA1A1A6;     
    const TER = 0x48484A;
    const CARD = 0x1C1C1E;
    const TICK = 0x444446;    
    const RING_BG = 0x2C2C2E; 
    
    const ACCENT = 0x64D2FF;  
    const PROTEIN = 0xC4F0FF; 
    const OK = 0x30D158;      
    const WARN = 0xFF9F0A;     
    const BAD = 0xFF453A;      
    const COACH = 0x64A8FF;   
    
    const STALE_SECS = 3600;
    const MORNING_END = 10;   
    const BAT_LOW = 20;

    const DATE_Y = 66;
    const PILL_Y = 76;
    const PILL_H = 24;
    const TIME_Y = 170;

    const K_Y = 236;
    const K_H = 96;
    const K_W = 106;
    const K_DX = 114;
    const K_R = 18;           

    const SCALE_R = 218;
    const SCALE_LONG = 17;
    const SCALE_SHORT = 10;
    const SCALE_N = 25;

    // Woche gegen Plan (tagsüber): Seitenwerte, Zeile mit Stand, Balken
    const SIDE_Y = 246;
    const WEEK_Y = 302;
    const BAR_X = 82;
    const BAR_W = 290;
    const BAR_Y = 318;
    const BAR_H = 14;

    // Untere Reihe: Symbol, offene Zeit, Trennstriche
    const ICON_Y = 370;
    const OPEN_Y = 406;
    const ICON_DX = 86;
    const NEXT_Y = 434;

    private var _s as Float = 1.0;
    private var _cx as Number = 0;
    private var _cy as Number = 0;
    private var _sleep as Boolean = false;

    private var _time as Graphics.FontType = Graphics.FONT_NUMBER_HOT;
    private var _big as Graphics.FontType = Graphics.FONT_MEDIUM;
    private var _icons as Graphics.FontType = Graphics.FONT_XTINY;
    private var _val as Graphics.FontType = Graphics.FONT_SMALL;
    private var _cap as Graphics.FontType = Graphics.FONT_XTINY;
    private var _tag as Graphics.FontType = Graphics.FONT_XTINY;

    function initialize() {
        WatchFace.initialize();
        _time = WatchUi.loadResource(Rez.Fonts.Time) as Graphics.FontType;
        _big = WatchUi.loadResource(Rez.Fonts.Big) as Graphics.FontType;
        _icons = WatchUi.loadResource(Rez.Fonts.Icons) as Graphics.FontType;
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
        if (data == null) {
            data = {
                "wv" => [[0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]],
                "wm" => [300, 300],
                "kcal" => {"v" => 1500, "g" => 2500},
                "pr" => {"v" => 100, "g" => 160}
            };
        }
        
        drawScales(dc, data);
        drawTopLine(dc, data);
        
        dc.setColor(blend(Graphics.COLOR_BLACK, FG, 0.15), Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx, px(TIME_Y) + 1, _time, timeText(clock), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
        dc.setColor(FG, Graphics.COLOR_TRANSPARENT);
        dc.drawText(_cx, px(TIME_Y), _time, timeText(clock), Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);

        if (clock.hour < MORNING_END) {
            drawMorningReady(dc, data);
        } else {
            drawWeek(dc, data, data.get("rz") as Array?);
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

    // Skalen am Rand wie bei der Apple Ultra: je 25 feine Striche, jeder sechste länger, von unten nach oben gefüllt mit dem, was
    // heute schon gegessen ist (kcal, pr: {v, g}), über dem Ziel voll, ohne Ziel leer. Links Kalorien in Cyan, rechts Eiweiß in
    // hellerem Cyan. Die Zahlen daneben zeigen, was bis zum Tagesziel noch fehlt (drawSideValues).

    private function drawScales(dc as Dc, data as Dictionary?) as Void {
        var food = 0.0;
        var protein = 0.0;
        if (data != null) {
            var kcal = data.get("kcal");
            if (kcal instanceof Dictionary) {
                food = frac(numOrNull(kcal.get("v")), numOrNull(kcal.get("g")));
            }
            var pr = data.get("pr");
            if (pr instanceof Dictionary) {
                protein = frac(numOrNull(pr.get("v")), numOrNull(pr.get("g")));
            }
        }
        drawScale(dc, 215, 145, food, ACCENT, px(SCALE_R), SCALE_LONG, SCALE_SHORT);
        drawScale(dc, 325, 395, protein, PROTEIN, px(SCALE_R), SCALE_LONG, SCALE_SHORT);
    }

    // Striche von Winkel a0 (unten) nach a1 (oben) ab Radius r1 nach innen, bis zum Anteil f in color
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

    private function drawMorningReady(dc as Dc, data as Dictionary) as Void {
        var rv = data.get("rv") as Array?;
        var labels = ["SCHLAF", "HRV", "PULS"];
        
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

            dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.08), Graphics.COLOR_TRANSPARENT);
            dc.fillRoundedRectangle(x, y, w, h, px(K_R));
            
            dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.45), Graphics.COLOR_TRANSPARENT);
            dc.setPenWidth(px(2) > 1 ? px(2) : 1);
            dc.drawRoundedRectangle(x, y, w, h, px(K_R));

            var subColor = blend(color, FG, 0.5);
            drawItems(dc, mid, y + px(20), [["t", labels[k], subColor, _tag]], 0);

            var text = v == null ? "--" : (k == 0 ? hours((v * 60).toNumber()) : v.toNumber().toString());
            drawItems(dc, mid, y + px(52), [["t", text, v == null ? TER : color, _val]], 0);

            if (v != null && band != null && band.size() == 4) {
                drawCompactBand(dc, x + px(12), x + w - px(12), y + px(62), v, band, color);
            }

            if (base != null) {
                var baseText = "Ø " + (k == 0 ? hours((base * 60).toNumber()) : base.toNumber().toString());
                drawItems(dc, mid, y + px(86), [["t", baseText, SEC, _tag]], 0);
            }
        }
    }

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
        
        var dx = x0 + (w * frac(v - p10, p90 - p10)).toNumber();
        dc.setColor(blend(Graphics.COLOR_BLACK, color, 0.3), Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(dx, y + h / 2, px(5));
        dc.setColor(color, Graphics.COLOR_TRANSPARENT);
        dc.fillCircle(dx, y + h / 2, px(3));
    }

    // ---- Tagsüber: Woche gegen Plan ---------------------------------------------------------------
    // Ausdauerzeit der Woche (wm[0], sonst aus wv aufsummiert) von ihrem Ziel (wm[1], das Zeitziel des Coaches), dahinter der
    // Abstand zur geraden Soll-Linie von heute (Ziel mal vergangene Tage / 6). Darunter ein Balken über das Wochenziel,
    // farbig nach Sportart (rz), die weiße Marke ist das Soll von heute. Seitlich Kalorien und Eiweiß. Ein Urteil ist das nicht.

    private function drawWeek(dc as Dc, data as Dictionary, rz as Array?) as Void {
        var wv = data.get("wv") as Array?;
        var wm = data.get("wm") as Array?;
        var cur = wv != null && wv.size() > 0 && wv[0] instanceof Array ? cumulate(wv[0] as Array) : [] as Array<Number>;
        var d = cur.size() - 1;
        d = d < 0 ? 0 : (d > 6 ? 6 : d);
        var now = cur.size() > 0 ? cur[cur.size() - 1] as Number : 0;
        var goal = 0;
        if (wm != null && wm.size() == 2) {
            var w0 = numOrNull(wm[0]);
            var w1 = numOrNull(wm[1]);
            now = w0 != null ? w0.toNumber() : now;
            goal = w1 != null ? w1.toNumber() : 0;
        }
        if (goal <= 0) {
            goal = goalSum(rz);
        }

        drawSideValues(dc, data);
        var items = [["t", hours(now), ACCENT, _big]] as Array<Array>;
        if (goal > 0) {
            var diff = now - goal * d / 6;
            items.add(["t", " von " + hours(goal) + "   ", SEC, _cap]);
            items.add(["t", (diff < 0 ? "−" : "+") + hours(diff < 0 ? -diff : diff), FG, _cap]);
            items.add(["t", " SOLL", SEC, _tag]);
        }
        drawItems(dc, _cx, px(WEEK_Y), items, 0);
        if (goal > 0) {
            drawWeekBar(dc, rz, goal, d);
        }
    }

    // Was bis zum Tagesziel noch fehlt: links die Kalorien, die heute noch übrig sind, rechts das Eiweiß, das noch fehlt (ohne
    // Ziel keine Zahl, über dem Ziel 0)
    private function drawSideValues(dc as Dc, data as Dictionary) as Void {
        var kcal = data.get("kcal");
        var kLeft = kcal instanceof Dictionary ? remaining(kcal) : null;
        if (kLeft != null) {
            drawItems(dc, px(56), px(SIDE_Y), [["t", kLeft.toString(), ACCENT, _tag], ["t", " kcal übrig", SEC, _tag]], -1);
        }
        var pr = data.get("pr");
        var pLeft = pr instanceof Dictionary ? remaining(pr) : null;
        if (pLeft != null) {
            drawItems(dc, px(398), px(SIDE_Y), [["t", pLeft.toString() + " g", PROTEIN, _tag], ["t", " noch", SEC, _tag]], 1);
        }
    }

    // Ziel minus Stand eines {v, g}-Eintrags, mindestens 0; ohne Ziel null
    private function remaining(e as Dictionary) as Number? {
        var v = numOrNull(e.get("v"));
        var g = numOrNull(e.get("g"));
        if (g == null || g <= 0) { return null; }
        var left = g.toNumber() - (v != null ? v.toNumber() : 0);
        return left < 0 ? 0 : left;
    }

    // Summe der Wochenziele (rz) in Minuten, falls der Coach kein Gesamtziel (wm) gesetzt hat
    private function goalSum(rz as Array?) as Number {
        var sum = 0;
        if (rz == null) { return 0; }
        for (var i = 0; i < rz.size(); i++) {
            var e = rz[i] as Array;
            var g = numOrNull(e[2]);
            sum += g != null ? g.toNumber() : 0;
        }
        return sum;
    }

    // Balken von BAR_X, Länge = Wochenziel, gefüllt mit den erledigten Minuten je Sportart (Lauf, Rad, Schwimmen von links),
    // abgerundete Enden, dünne Trennlinien zwischen den Sportarten, weiße Marke bei Tag d von 6
    private function drawWeekBar(dc as Dc, rz as Array?, goal as Number, d as Number) as Void {
        var x0 = px(BAR_X);
        var w = px(BAR_W);
        var y = px(BAR_Y);
        var h = px(BAR_H);
        var r = h / 2;
        dc.setColor(RING_BG, Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(x0, y, w, h, r);

        var sports = ["run", "bike", "swim"];
        // Anteile in Pixel: gefüllt wird von x0 bis xt, die Enden sind Halbkreise, dazwischen Rechtecke je Sportart
        var done = 0;
        for (var k = 0; k < sports.size(); k++) {
            var e = goalOf(rz, sports[k]);
            done += e != null && numOrNull(e[1]) != null ? (e[1] as Numeric).toNumber() : 0;
        }
        done = done > goal ? goal : done;
        if (done > 0) {
            var xt = x0 + w * done / goal;
            var lo = x0 + r;
            var hi = xt - r > lo ? xt - r : lo;
            var acc = 0;
            var firstColor = RING_BG;
            var lastColor = RING_BG;
            for (var k = 0; k < sports.size(); k++) {
                var e = goalOf(rz, sports[k]);
                var m = e != null && numOrNull(e[1]) != null ? (e[1] as Numeric).toNumber() : 0;
                if (m <= 0 || acc >= goal) { continue; }
                var end = acc + m > goal ? goal : acc + m;
                var xs = x0 + w * acc / goal;
                var xe = x0 + w * end / goal;
                xs = xs < lo ? lo : xs;
                xe = xe > hi ? hi : xe;
                var color = sportColor(sports[k]);
                if (firstColor == RING_BG) { firstColor = color; }
                lastColor = color;
                if (xe > xs) {
                    dc.setColor(color, Graphics.COLOR_TRANSPARENT);
                    dc.fillRectangle(xs, y, xe - xs, h);
                    if (acc > 0) {
                        dc.setColor(Graphics.COLOR_BLACK, Graphics.COLOR_TRANSPARENT);
                        dc.fillRectangle(xs - 1, y, px(2) > 1 ? px(2) : 1, h);
                    }
                }
                acc = end;
            }
            dc.setColor(firstColor, Graphics.COLOR_TRANSPARENT);
            dc.fillCircle(lo, y + r, r);
            dc.setColor(lastColor, Graphics.COLOR_TRANSPARENT);
            dc.fillCircle(hi, y + r, r);
        }

        var mx = x0 + w * d / 6;
        dc.setColor(FG, Graphics.COLOR_TRANSPARENT);
        dc.fillRoundedRectangle(mx - px(2), y - px(5), px(3), h + px(10), px(1));
    }

    private function cumulate(mins as Array?) as Array<Number> {
        var out = [] as Array<Number>;
        if (mins == null) { return out; }
        var sum = 0;
        for (var i = 0; i < mins.size() && i < 7; i++) {
            var v = numOrNull(mins[i]);
            sum += v != null ? v.toNumber() : 0;
            out.add(sum);
        }
        return out;
    }

    // ---- Untere Reihe: offene Zeit je Sportart -------------------------------------------------
    // Symbol in Sportfarbe (Laufen, Rad, Schwimmen von links) und darunter die Zeit, die diese Woche noch offen ist (Ziel des
    // Coaches minus erledigt, h:mm). Ziel erreicht: Haken, ohne Ziel "--".

    private function drawWeekRow(dc as Dc, rz as Array?) as Void {
        var sports = ["run", "bike", "swim"];
        var glyphs = ["r", "b", "s"];
        var y = px(ICON_Y);
        for (var k = 0; k < sports.size(); k++) {
            var x = _cx + px(ICON_DX * (k - 1));
            var color = sportColor(sports[k]);
            dc.setColor(color, Graphics.COLOR_TRANSPARENT);
            dc.drawText(x, y, _icons, glyphs[k], Graphics.TEXT_JUSTIFY_CENTER | Graphics.TEXT_JUSTIFY_VCENTER);
            var e = goalOf(rz, sports[k]);
            var done = e != null && numOrNull(e[1]) != null ? (e[1] as Numeric).toNumber() : 0;
            var goal = e != null && numOrNull(e[2]) != null ? (e[2] as Numeric).toNumber() : 0;
            if (e == null) {
                drawItems(dc, x, px(OPEN_Y), [["t", "--", TER, _cap]], 0);
            } else if (goal - done > 0) {
                drawItems(dc, x, px(OPEN_Y), [["t", hours(goal - done), FG, _cap]], 0);
            } else {
                drawCheck(dc, x, px(OPEN_Y) - px(9), color);
            }
        }
        dc.setColor(RING_BG, Graphics.COLOR_TRANSPARENT);
        dc.setPenWidth(px(2) > 1 ? px(2) : 1);
        for (var k = 0; k < 2; k++) {
            var lx = _cx + px(ICON_DX * (k - 1) + ICON_DX / 2);
            dc.drawLine(lx, px(ICON_Y - 22), lx, px(OPEN_Y + 12));
        }
    }

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
        if ("run".equals(sport)) { return 0xFFD60A; }        
        if ("bike".equals(sport)) { return 0x30D158; }       
        if ("swim".equals(sport)) { return 0x409CFF; }       
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
