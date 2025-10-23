package utils.logs;

import utils.application.Block;
import utils.application.Message;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    public static void logDeliveringMessage(Message message) {
        if (CURRENT_LOG_LEVEL == LogLevel.NORMAL || CURRENT_LOG_LEVEL == LogLevel.DEBUG) {
            if (message.content() instanceof Block currentBlock) {
                String partialParentHash = IntStream.range(0, 4)
                        .mapToObj(i -> Byte.toString(currentBlock.parentHash()[i]))
                        .collect(Collectors.joining(" "));

                IO.println("[" + message.type() + "] From Peer " + message.sender() +
                            " Block[ Epoch: " + currentBlock.epoch()                +
                            ", Length: " + currentBlock.length()                    +
                            ", PartialParentHash:" + partialParentHash       + " ]" +
                            ", " + Arrays.toString(currentBlock.transactions()));
            }
        }
    }

}
