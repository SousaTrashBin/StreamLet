package utils.communication;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record Address(String ip, int port) {
    public static Address fromString(String string) {
        String regex = "(?<ip>\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):(?<port>\\d+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(string);

        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    String.format("Invalid format found on string '%s'. Should match <ip>:<port>.", string)
            );
        }

        String ip = matcher.group("ip");
        int port = Integer.parseInt(matcher.group("port"));
        return new Address(ip, port);
    }
}
