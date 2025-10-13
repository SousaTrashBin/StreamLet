package utils;

import utils.communication.Address;
import utils.communication.PeerInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.IntStream;

public class ConfigParser {
    public final static String CONFIG_FILE = "config.txt";

    public static List<PeerInfo> parsePeers() throws IOException {
        var lines = Files.readAllLines(Paths.get(CONFIG_FILE));
        return IntStream.range(0, lines.size())
                .mapToObj(index -> new PeerInfo(index, Address.fromString(lines.get(index))))
                .toList();
    }

}
