package utils.logs;

import utils.application.Block;
import utils.application.Message;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.logging.*;

public class AppLogger {

    private static final Logger logger = Logger.getLogger(AppLogger.class.getName());

    private static final Map<Level, String> LEVEL_COLORS = Map.of(
            Level.SEVERE, "\u001B[31m", // red
            Level.WARNING, "\u001B[33m", // yellow
            Level.INFO, "\u001B[32m", // green
            Level.FINE, "\u001B[36m" // cyan
    );

    static {
        logger.setUseParentHandlers(false);

        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                String color = LEVEL_COLORS.getOrDefault(record.getLevel(), "\u001B[0m"); // reset
                return String.format("%s[%s]%s %s%n", color, record.getLevel().getLocalizedName(), "\u001B[0m", record.getMessage());
            }
        });

        logger.addHandler(handler);
        updateLoggerLevel(LogLevel.NORMAL); // normal by default
    }

    public static void updateLoggerLevel(LogLevel logLevel) {
        Level newLevel = logLevel == LogLevel.DEBUG ? Level.FINE : Level.INFO;
        logger.setLevel(newLevel);
        for (Handler handler : logger.getHandlers()) {
            handler.setLevel(newLevel);
        }
    }

    public static void logInfo(String message) {
        logger.info(message); // green
    }

    public static void logDebug(String message) {
        logger.fine(message); // cyan
    }

    public static void logWarning(String message) {
        logger.warning(message); // yellow
    }

    public static void logError(String message, Throwable error) {
        String red = "\u001B[31m"; // red
        String reset = "\u001B[0m"; // reset

        String fullMessage = String.format("%sERROR%s: %s", red, reset, message);
        logger.severe(fullMessage);

        if (error != null) {
            StringWriter sw = new StringWriter();
            error.printStackTrace(new PrintWriter(sw));
            logger.severe(sw.toString());
        }
    }

    public static void logDeliveringMessage(Message message) {
        if (message.content() instanceof Block block) {
            String resetColor = "\u001B[0m";
            String typeColor = switch (message.type()) {
                case VOTE -> "\u001B[33m"; // yellow
                case PROPOSE -> "\u001B[32m"; // green
                default -> resetColor;
            };

            logger.info(() -> String.format(
                    "%s%s%s | Peer %d | %s",
                    typeColor, message.type(), resetColor, message.sender(), block.toStringSummary()
            ));
        }
    }
}
