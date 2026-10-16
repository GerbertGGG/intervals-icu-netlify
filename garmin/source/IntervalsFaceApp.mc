import Toybox.Application;
import Toybox.Background;
import Toybox.Lang;
import Toybox.System;
import Toybox.Time;
import Toybox.WatchUi;

// Die App-Klasse wird auch im Hintergrundprozess geladen, deshalb (:background).
(:background)
class IntervalsFaceApp extends Application.AppBase {

    function initialize() {
        AppBase.initialize();
    }

    function getInitialView() as [WatchUi.Views] or [WatchUi.Views, WatchUi.InputDelegates] {
        // Frühester Abstand für Hintergrundläufe sind 5 Minuten
        Background.registerForTemporalEvent(new Time.Duration(5 * 60));
        return [ new IntervalsFaceView() ];
    }

    function getServiceDelegate() as [System.ServiceDelegate] {
        return [ new SyncDelegate() ];
    }

    // Antwort des Hintergrunddienstes: bei Fehler kommt { "err" => Code }, dann bleibt der letzte Stand stehen
    // und der Code wird neben dem "!" angezeigt
    function onBackgroundData(data as Application.PersistableType) as Void {
        if (data instanceof Lang.Dictionary && (data as Dictionary).hasKey("err")) {
            Application.Storage.setValue("e", (data as Dictionary).get("err"));
        } else if (data instanceof Lang.Dictionary) {
            data.put("at", Time.now().value());
            Application.Storage.setValue("d", data);
            Application.Storage.deleteValue("e");
        }
        WatchUi.requestUpdate();
    }

    function onSettingsChanged() as Void {
        WatchUi.requestUpdate();
    }
}
