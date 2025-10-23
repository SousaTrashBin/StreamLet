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

public class ConfigParser {
    public final static String CONFIG_FILE = "config.txt";

    public static List<PeerInfo> parsePeers() throws IOException {
        List<PeerInfo> peersInfoList = new ArrayList<>();
        List<String> lines = Files.readAllLines(Paths.get(CONFIG_FILE));
        int index = 0;
        for (String line: lines) {
            if (line.startsWith("P2P")) {
                String rawPeerInfo = line.split("=")[1].trim();
                peersInfoList.add(new PeerInfo(index, Address.fromString(rawPeerInfo)));
                index++;
            }
        }
        return peersInfoList;
    }

    public static LogLevel getLogLevel() throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(CONFIG_FILE));
        for (String line: lines) {
            if (line.startsWith("logLevel=")) {
                return LogLevel.valueOf(line.split("=")[1].trim().toUpperCase());
            }
        }
        return LogLevel.NORMAL;
    }

    public static Map<Integer, Address> parseServers() {
        Map<Integer, Address>  serversInfoList = new HashMap<>();
        List<String> lines = null;
        try {
            lines = Files.readAllLines(Paths.get(CONFIG_FILE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int index = 0;
        for (String line: lines) {
            if (line.startsWith("server")) {
                String rawPeerInfo = line.split("=")[1].trim();
                serversInfoList.put(index, Address.fromString(rawPeerInfo));
                index++;
            }
        }
        return serversInfoList;
    }

    public static boolean isTransactionsClientMode() {
        List<String> lines = null;
        try {
            lines = Files.readAllLines(Paths.get(CONFIG_FILE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (String line: lines) {
            if (line.startsWith("transactionsMode=")) {
                if (line.split("=")[1].trim().equalsIgnoreCase("CLIENT")) {
                    return true;
                }
            }
        }
        return false;
    }

}
