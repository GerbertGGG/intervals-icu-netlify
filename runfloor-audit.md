# RunFloor Status-Matrix Audit

Datum: 2026-03-14

## Ergebnis (Kurzfassung)

Teilweise eingebaut.

- **Vorhanden**: 3-stufige Tagesklassifikation (**GREEN/YELLOW/RED**), `plannedDip`, `progressionTrendOK`, `allowFloorIncreaseStrict`, Soft-Dip-Zähler (`softDipCount7d`, `softDipCount14d`, `softDipStreak`) und textliche Gründe.  
- **Nicht 1:1 wie vorgeschlagen**: Bedingungen für YELLOW/RED, plannedDip-Vergabe und harte Increase-Gates weichen von der gewünschten Matrix ab.

## Detailabgleich gegen gewünschte Regeln

## 1) Status-Matrix GREEN/YELLOW/RED

- **Ist**: `floorLevel` wird aus `avg7` vs. `floorDaily` und `softDipPct` bestimmt.
  - GREEN: `avg7 >= floorDaily`
  - YELLOW: `avg7 >= floorDaily * softDipPct`
  - RED: sonst
- **Abweichung**: Die gewünschte zusätzliche Prüfung `avg21 >= floorDaily * AVG21_FLOOR_BAND` ist **nicht Teil** der YELLOW-Definition, sondern wird aktuell separat in `plannedDip` bewertet.

## 2) plannedDip „streng vergeben"

- **Ist**: `plannedDip` braucht bereits mehrere Gates (YELLOW, `avg21 >= floorDaily`, `stabilityOK`, kein `stabilityWarn`, BASE/BUILD, Eventnähe, kein Dip-Cluster, kein direkt benachbarter letzter planned dip, hohe Konfidenz).
- **Abweichung**:
  - kein explizites Gate auf **Fatigue-Override** in der Funktion,
  - Serien-/Count-Grenzen sind aktuell weicher (`softDipCount14d <= 4`, `softDipStreak <= 2` in `noDipCluster`) als gewünscht,
  - geforderte Negativ-Kombination (Kraft/Key-Spacing/Frequenz) ist hier nicht als eigener Gate-Block modelliert.

## 3) progressionTrendOK (weicher als Floor-Erhöhung)

- **Ist**: `progressionTrendOK` basiert auf `avg7TrendOK` oder soft-dip-trend mit medium/high `plannedDipConfidence`.
- **Abweichung**: Die gewünschte explizite Logik (RED=false, harte Serien-Dip-Grenzen, Fatigue-kritisch=false, `avg21` fällt=false) ist **nicht 1:1** kodiert.

## 4) allowFloorIncreaseStrict (streng)

- **Ist**: bereits streng (GREEN, `avg7TrendOK`, `stabilityOK`, kein `stabilityWarn`, `softDipCount14d <= 2`, `softDipStreak <= 1`).
- **Abweichung**:
  - nicht explizit `softDipCount7d == 0`,
  - nicht explizit `avg7 >= floorDaily` (implizit via GREEN),
  - kein explizites Fatigue-Override-Gate in dieser Funktion,
  - Deload/Event/Freeze-Gates liegen teilweise außerhalb dieses Booleans (im Erhöhungs-Block später kombiniert).

## 5) State-Felder

- **Vorhanden**: `floorLevel`, `plannedDip`, `softDipCount7d`, `softDipCount14d`, `softDipStreak`, `allowFloorIncreaseStrict`, `plannedDipConfidence`.
- **Abweichung**:
  - gewünschtes Feld `floorColor` heißt aktuell `floorLevel` (inhaltlich gleich),
  - `progressionTrendOK` wird intern genutzt, aber nicht im Rückgabeobjekt persistiert,
  - optionales `floorContext` fehlt.

## 6) Textlogik

- **Ist**: `reasons` enthält bereits Texte für planned dip / soft dip / Stabilitätswarnung.
- **Abweichung**: Die gewünschte explizite GREEN/YELLOW(planned)/YELLOW(unplanned)/RED-Textmatrix ist nur teilweise abgebildet.

## Empfehlung

Wenn du willst, kann ich als nächsten Schritt die Funktion auf die von dir gewünschte Matrix **1:1** umstellen (inkl. `floorColor`, `floorContext`, strikterer plannedDip-Logik und separatem `progressionTrendOK`-Persist).
