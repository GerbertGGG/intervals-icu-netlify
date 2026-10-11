# Garmin-Watchface "Intervals"

Zifferblatt für runde Garmin-Uhren (Connect IQ, Monkey C). Es zeigt Uhrzeit, Rennziel, die Woche (Mo bis So) nach Sportart, den Block mit Decoupling, Kraft und ACWR, Wochen-TSS (erledigt, Plan, Ziel) mit den letzten 6 Wochen, Kalorien und Wetter.

Die Daten holt die Uhr alle 5 Minuten von `GET /api/widget?view=watch` (siehe `buildWidgetWatch` in `src/widget.js`). Wetter und Akku kommen von der Uhr selbst.

Stand: noch nicht auf einer Uhr getestet. Der Code ist nach dem Mockup geschrieben und wurde nicht mit dem Connect-IQ-Compiler gebaut. Beim ersten Build sind kleine Anpassungen möglich (z. B. Berechtigungen im Manifest, Typhinweise).

## 1. Worker

Ein eigenes Token, das nur die Ansicht `watch` öffnet (das Dashboard-Token bleibt privat):

```
npx wrangler secret put WATCH_TOKEN
```

Optional `WEEKLY_TSS_GOAL` für das Wochenziel (siehe `wrangler.toml`). Der Plan der Woche kommt aus dem Intervals-Kalender.

## 2. Zugangsdaten für die Uhr

```
cp garmin/source/Config.mc.example garmin/source/Config.mc
```

In `Config.mc` die Adresse des Workers (`https://intervals-icu-netlify.<konto>.workers.dev`) und das `WATCH_TOKEN` eintragen. Die Datei steht in `.gitignore`.

Sideloaded Watchfaces haben in Garmin Connect keine Einstellungen. Wird die App später im Connect-IQ-Store veröffentlicht (auch privat/Beta), überschreiben die Felder "Worker-Adresse" und "Uhr-Token" in den App-Einstellungen die Werte aus `Config.mc`.

## 3. Bauen und installieren

1. Connect IQ SDK und die VS-Code-Erweiterung "Monkey C" installieren, einen Developer Key erzeugen (`Monkey C: Generate a Developer Key`).
2. Ordner `garmin/` in VS Code öffnen, `Monkey C: Build for Device`, Gerät wählen.
3. Die erzeugte `.prg` aus `bin/` per USB nach `GARMIN/APPS/` auf der Uhr kopieren, dann das Zifferblatt auf der Uhr auswählen. Zum Ausprobieren ohne Uhr gibt es den Simulator (`Run`).

In `manifest.xml` stehen die Geräte (Venu 3, Venu 3S, Vivoactive 5, Forerunner 265/265S/965). Die Maße sind auf 454 Pixel ausgelegt und werden skaliert. Weitere runde Modelle lassen sich dort ergänzen.

## Bekannte Abweichungen vom Mockup

- Schrift: Das Mockup nutzt eine dicke Bubble-Schrift. Die Uhr nimmt die eingebaute Zahlenschrift (`FONT_NUMBER_HOT`). Eine eigene Bitmap-Schrift (z. B. Titan One über BMFont) lässt sich später als Ressource ergänzen.
- Kalorien- und Wettertext stehen waagerecht neben den Randbögen statt gedreht.
- Always-on-Modus: Es wird nur die Uhrzeit gezeichnet (AMOLED schonen).
- Ein `!` hinter der Statuszeile bedeutet: Der letzte Abruf ist älter als eine Stunde.
