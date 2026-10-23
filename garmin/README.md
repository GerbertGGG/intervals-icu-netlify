# Garmin-Watchface "Intervals"

Zifferblatt für runde Garmin-Uhren (Connect IQ, Monkey C), Entwurf „Modular“: schwarz, große Uhrzeit, eine Karte in der Mitte, die mit der Tageszeit wechselt. Sportfarben: Laufen gelb, Rad grün, Schwimmen blau.

Fest von oben nach unten:

- **Kopfzeile**: Datum („DO 23.“), Akku in Prozent (grau, ab 20 % orange, ab 10 % rot, beim Laden grün) und das Urteil des Coaches für heute als Wort (Feld `go` in `save_coach`): HART grün, LOCKER orange, FREI rot, COACH blau = Coach hat geschrieben, aber kein Urteil, ohne Aussage nichts. Eine eigene Ampel hat die Uhr nicht.
- **Uhrzeit**, groß, ohne führende Null.
- **Offene Wochenminuten** je Sportart bis zum Zeitziel des Coaches (Einträge der Kategorie „Target“ im Intervals-Kalender), groß in Sportfarbe mit Minutenzeichen (95'), darunter ein Balken, der sich zum Ziel füllt. Ziel erreicht: Haken, ohne Ziel „--“.

Die **Karte in der Mitte** und die Zeile ganz unten wechseln:

- **Morgens bis 12 Uhr**: Schlaf (lila) und HRV von heute mit der eigenen Basis (Ø), darunter die HRV der letzten 7 Tage als Balken, heute grün, wenn über der Basis, die Basis gestrichelt. Unten die Kopfzeile des Coaches von heute in Blau, auf 20 Zeichen gekürzt (ohne sie die nächste Einheit).
- **Bis 3 Stunden nach einer Einheit** (von heute): Sportart mit Startzeit, Strecke, Zeit (h:mm), Pace oder Watt und Puls (nur, wenn das Dashboard ihn für die Einheit zeigt). Rechts oben „LÄNGSTE SEIT N WO.“, wenn die letzte mindestens so lange Einheit derselben Sportart zwei Wochen oder länger zurückliegt. Unten die nächste Einheit.
- **Sonst (tagsüber)**: die Woche Mo bis So, je Tag die Minuten je Sportart übereinander, erledigt gefüllt, geplant als Umriss; oben rechts die Ausdauer der Woche gegen das Ziel des Coaches. Unten die nächste Einheit („Heute Lauf 45'“, „Morgen Rad 60'“).

Essen, Wetter, TSS und Kraft stehen nicht auf der Uhr (Widget und Dashboard zeigen sie). Der Worker schickt ältere Felder weiter, die Uhr nutzt `rd`, `co`, `rz`, `next`, `ti`, `sl`, `hv`, `hm`, `cl`, `wd`, `wm` und `la`.

Die Daten holt die Uhr alle 5 Minuten von `GET /api/widget?view=watch` (siehe `buildWidgetWatch` in `src/widget.js`). Der Akku kommt von der Uhr selbst.

Ist der letzte Stand älter als eine Stunde, steht in der Kopfzeile statt des Coach-Urteils ein oranges „!“ und, wenn der letzte Abruf gescheitert ist, dessen Fehlercode: `-104` keine Bluetooth-Verbindung zum Handy, `-2`/`-300` Zeitüberschreitung, `401` falsches Uhr-Token, `404` falsche Worker-Adresse, `-400`/`-402`/`-403` Antwort unlesbar oder zu groß, `5xx` Fehler im Worker.

Stand: noch nicht auf einer Uhr getestet. Der Code ist nach dem Mockup „Modular“ geschrieben (Layout mit den Bitmap-Schriften in Python nachgestellt) und wurde nicht mit dem Connect-IQ-Compiler gebaut. Beim ersten Build sind kleine Anpassungen möglich (z. B. Berechtigungen im Manifest, Typhinweise).

## 1. Worker

Ein eigenes Token, das nur die Ansicht `watch` öffnet (das Dashboard-Token bleibt privat):

```
npx wrangler secret put WATCH_TOKEN
```

Optional `WEEKLY_TSS_GOAL` für das Wochenziel (siehe `wrangler.toml`). Der Plan der Woche kommt aus dem Intervals-Kalender; die Wochenziele sind dort Einträge der Kategorie „Target“ (Weekly) je Sportart mit Zeit und Load.

## 2. Zugangsdaten für die Uhr

In `garmin/source/Config.mc` die Adresse des Workers (`https://intervals-icu-netlify.<konto>.workers.dev`) und das `WATCH_TOKEN` eintragen. Mit leeren Werten baut die App, holt aber keine Daten. Die eingetragene Datei nicht einchecken.

Sideloaded Watchfaces haben in Garmin Connect keine Einstellungen. Wird die App später im Connect-IQ-Store veröffentlicht (auch privat/Beta), überschreiben die Felder "Worker-Adresse" und "Uhr-Token" in den App-Einstellungen die Werte aus `Config.mc`.

## 3. Bauen und installieren

1. Connect IQ SDK und die VS-Code-Erweiterung "Monkey C" installieren, einen Developer Key erzeugen (`Monkey C: Generate a Developer Key`).
2. Ordner `garmin/` in VS Code öffnen, `Monkey C: Build for Device`, Gerät wählen.
3. Die erzeugte `.prg` aus `bin/` per USB nach `GARMIN/APPS/` auf der Uhr kopieren, dann das Zifferblatt auf der Uhr auswählen. Zum Ausprobieren ohne Uhr gibt es den Simulator (`Run`).

Gebaut wird für die Forerunner 265 (416 × 416 Pixel, AMOLED, rund; `manifest.xml`). Die Maße im Code sind auf 454 Pixel ausgelegt und werden auf die Displaygröße skaliert. Weitere runde Modelle lassen sich in `manifest.xml` ergänzen.

## Hinweise

- Schrift: Apples SF Pro darf nicht auf die Uhr. Alle Schriften sind Bitmap-Schriften aus **Inter Tight** (SIL Open Font License) in `resources/fonts/`: `time` (Uhrzeit, fett, nur Ziffern und gleich breit), `val` (Werte in Karte und unterer Reihe), `cap` (Kopfzeile, Zeile unten), `tag` (Beschriftungen). Die Größen sind für 416 Pixel (Forerunner 265) erzeugt (Beschriftungen 17, Kopf- und Fußzeile 21, Werte 30, Uhrzeit 100 Pixel; kleiner ist auf der Uhr schlecht lesbar), Bitmap-Schriften skalieren nicht mit. Neu erzeugen: siehe Kopf von `tools/make_font.py` (braucht Pillow und die variable Inter Tight von Google Fonts).
- Always-on-Modus: Es wird nur die Uhrzeit gezeichnet, grau in der eingebauten Schrift (AMOLED schonen).
- Ist der letzte Abruf älter als eine Stunde, steht in der Kopfzeile ein `!` statt des Coach-Urteils.
