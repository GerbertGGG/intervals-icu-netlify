# Garmin-Watchface "Intervals"

Zifferblatt für runde Garmin-Uhren (Connect IQ, Monkey C), Entwurf „Regenbogen“ (Mockup 5d): schmale Bögen am Rand, oben Essen, unten Sport, innen große Zahlen. Sportfarben: Laufen gelb, Rad grün, Schwimmen blau.

- **Obere Bögen (Essen)**: außen Kalorien (weiß) gegen das Tagesziel, der Bogen füllt sich mit dem Gegessenen; innen Eiweiß (türkis) gegen das Yazio-Ziel.
- **Untere Bögen (Sport)**: außen Lauf, dann Rad, innen Schwimmen, jeweils die Minuten der Woche gegen das Zeitziel des Coaches (Einträge der Kategorie „Target“ im Intervals-Kalender); voller Bogen = Ziel erreicht. Ohne Ziel bleibt nur der Grundbogen in gedämpfter Sportfarbe.

Innen von oben nach unten:

- **Kalorien und Eiweiß**: was heute noch übrig ist, kcal bis zum Tagesziel (darüber „+N“) mit kleinem grauem „kcal“ und Eiweiß bis zum Yazio-Ziel mit „g Eiweiß“ (erreicht: Haken). Ohne Ziel jeweils das Gegessene.
- **Datum** („MI 21. OKT“) in Grau, dahinter der **Akku** als Symbol mit Prozentzahl, weiß, ab 20 % orange, ab 10 % rot, beim Laden grün.
- **Uhrzeit**, groß. Der **Doppelpunkt** trägt das Urteil des Coaches für heute (Feld `go` in `save_coach`): grüne Kreise = hart erlaubt, orange Dreiecke = nur locker, rote Quadrate = frei, blaue Kreise = Coach hat geschrieben, aber kein Urteil, grau = noch nichts. Eine eigene Ampel hat die Uhr nicht.
- **Offene Minuten** je Sportart bis zum Zeitziel, groß in Sportfarbe, darunter klein und grau LAUF, RAD, SCHWIMM. Ziel erreicht: Haken in Sportfarbe, ohne Ziel „--“.

Wetter, TSS, Kraft, Wochenreihe, nächste Einheit und Blocker stehen nicht auf der Uhr (Widget und Dashboard zeigen sie). Der Worker schickt die Felder weiter, die Uhr nutzt davon `rz`, `kcal`, `pr`, `rd` und `co`.

Die Daten holt die Uhr alle 5 Minuten von `GET /api/widget?view=watch` (siehe `buildWidgetWatch` in `src/widget.js`). Der Akku kommt von der Uhr selbst.

Ist der letzte Stand älter als eine Stunde, wird der Doppelpunkt grau, und hinter dem Datum steht ein oranges „!“ und, wenn der letzte Abruf gescheitert ist, dessen Fehlercode: `-104` keine Bluetooth-Verbindung zum Handy, `-2`/`-300` Zeitüberschreitung, `401` falsches Uhr-Token, `404` falsche Worker-Adresse, `-400`/`-402`/`-403` Antwort unlesbar oder zu groß, `5xx` Fehler im Worker.

Stand: noch nicht auf einer Uhr getestet. Der Code ist nach dem Mockup 5d geschrieben und wurde nicht mit dem Connect-IQ-Compiler gebaut. Beim ersten Build sind kleine Anpassungen möglich (z. B. Berechtigungen im Manifest, Typhinweise).

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

- Schrift: Apples SF Pro darf nicht auf die Uhr. Alle Schriften sind Bitmap-Schriften aus **Roboto Medium** (SIL Open Font License) in `resources/fonts/`: `time` (Uhrzeit, nur Ziffern und gleich breit), `open` (offene Minuten, Kalorien, Eiweiß), `cap` (Datum, Akku), `tag` (Beschriftung unter den Minuten, „kcal“). Die Größen sind für 416 Pixel (Forerunner 265) erzeugt, Bitmap-Schriften skalieren nicht mit. Neu erzeugen: siehe Kopf von `tools/make_font.py` (braucht Pillow und Roboto von Google Fonts).
- Always-on-Modus: Es wird nur die Uhrzeit gezeichnet, grau in der eingebauten Schrift (AMOLED schonen).
- Ist der letzte Abruf älter als eine Stunde, wird der Doppelpunkt grau und das Datum bekommt ein `!`.
