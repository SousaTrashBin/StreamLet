package utils.logs;

public class Logger {

    public static LogLevel CURRENT_LOG_LEVEL = LogLevel.NORMAL;

    public static void log(String message) {
        if (CURRENT_LOG_LEVEL == LogLevel.NORMAL || CURRENT_LOG_LEVEL == LogLevel.DEBUG) {
            IO.println(message);
        }
    }

    public static void debug(String message) {
        if (CURRENT_LOG_LEVEL == LogLevel.DEBUG) {
            IO.println(message);
        }
    }

}
