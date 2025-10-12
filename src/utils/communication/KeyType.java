package utils.communication;

import java.nio.channels.SelectionKey;

public enum KeyType {
    ACCEPT, CONNECT, READ, WRITE, UNKNOWN;

    public static KeyType fromSelectionKey(SelectionKey key) {
        return key.isAcceptable() ? KeyType.ACCEPT :
                key.isConnectable() ? KeyType.CONNECT :
                        key.isReadable() ? KeyType.READ :
                                key.isWritable() ? KeyType.WRITE :
                                        KeyType.UNKNOWN;

    }
}
