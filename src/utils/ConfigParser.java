package utils;

import utils.communication.Address;
import utils.communication.PeerInfo;
import utils.logs.LogLevel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigParser {
    public final static String CONFIG_FILE = "config.txt";

    private static final Pattern P2P_PATTERN = Pattern.compile("^P2P\\s*=\\s*(.+)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern SERVER_PATTERN = Pattern.compile("^server\\s*=\\s*(.+)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern LOGLEVEL_PATTERN = Pattern.compile("^logLevel\\s*=\\s*(.+)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TRANSACTION_MODE_PATTERN = Pattern.compile("^transactionsMode\\s*=\\s*(.+)$", Pattern.CASE_INSENSITIVE);

    public static ConfigData parseConfig() throws IOException {
        ConfigData configData = new ConfigData();
        List<String> lines = Files.readAllLines(Paths.get(CONFIG_FILE));
        int peerIndex = 0;
        int serverIndex = 0;

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue; // skip empty lines or comments

            Matcher p2pMatcher = P2P_PATTERN.matcher(line);
            Matcher serverMatcher = SERVER_PATTERN.matcher(line);
            Matcher logLevelMatcher = LOGLEVEL_PATTERN.matcher(line);
            Matcher transactionMatcher = TRANSACTION_MODE_PATTERN.matcher(line);

            if (p2pMatcher.matches()) {
                configData.peers.add(new PeerInfo(peerIndex++, Address.fromString(p2pMatcher.group(1).trim())));
            } else if (serverMatcher.matches()) {
                configData.servers.put(serverIndex++, Address.fromString(serverMatcher.group(1).trim()));
            } else if (logLevelMatcher.matches()) {
                try {
                    configData.logLevel = LogLevel.valueOf(logLevelMatcher.group(1).trim().toUpperCase());
                } catch (IllegalArgumentException ignored) {
                    configData.logLevel = LogLevel.NORMAL;
                }
            } else if (transactionMatcher.matches()) {
                configData.isClientGeneratingTransactions = transactionMatcher.group(1).trim().equalsIgnoreCase("CLIENT");
            }
        }

        return configData;
    }

    public static class ConfigData {
        public List<PeerInfo> peers = new ArrayList<>();
        public Map<Integer, Address> servers = new HashMap<>();
        public LogLevel logLevel = LogLevel.NORMAL;
        public boolean isClientGeneratingTransactions = false;
    }
}
