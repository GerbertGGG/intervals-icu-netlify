import Toybox.Application;
import Toybox.Background;
import Toybox.Communications;
import Toybox.Lang;
import Toybox.System;

// Holt alle 5 Minuten /api/widget?view=watch vom Worker (siehe src/widget.js, buildWidgetWatch).
(:background)
class SyncDelegate extends System.ServiceDelegate {

    function initialize() {
        ServiceDelegate.initialize();
    }

    function onTemporalEvent() as Void {
        var base = setting("BaseUrl", Config.BASE_URL);
        var token = setting("Token", Config.TOKEN);
        if (base.length() == 0 || token.length() == 0) {
            Background.exit(null);
            return;
        }
        // Schrägstrich am Ende der Adresse abschneiden
        if (base.substring(base.length() - 1, base.length()).equals("/")) {
            base = base.substring(0, base.length() - 1);
        }
        var options = {
            :method => Communications.HTTP_REQUEST_METHOD_GET,
            :headers => { "Authorization" => "Bearer " + token },
            :responseType => Communications.HTTP_RESPONSE_CONTENT_TYPE_JSON
        };
        Communications.makeWebRequest(base + "/api/widget", { "view" => "watch" }, options, method(:onReceive));
    }

    function onReceive(code as Number, data as Dictionary or String or Null) as Void {
        if (code == 200 && data instanceof Lang.Dictionary) {
            // Zu große Antwort: Background.exit wirft, das Zifferblatt bekäme nichts und zeigte nur "!". Dann -900 melden.
            try {
                Background.exit(data);
            } catch (e instanceof Background.ExitDataSizeLimitException) {
                Background.exit({ "err" => -900 });
            }
        } else {
            // Fehlercode an die App melden (z. B. -104 = keine Verbindung zum Handy, 401 = Token, 404 = Adresse)
            Background.exit({ "err" => code });
        }
    }

    // Einstellung aus Garmin Connect, sonst der Wert aus Config.mc
    private function setting(id as String, fallback as String) as String {
        var v = Application.Properties.getValue(id);
        if (v instanceof Lang.String && v.length() > 0) {
            return v;
        }
        return fallback;
    }
}
