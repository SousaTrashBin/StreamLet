import utils.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

public class P2PNode implements Runnable, AutoCloseable {
    private final PeerInfo myPeerInfo;
    private final CountDownLatch connectionBarrier;
    private final Map<Integer, PeerInfo> idsToOtherPeersInfo;
    private ServerSocketChannel myServerSocketChannel;
    private final ConcurrentLinkedQueue<MessageWithReceiver> writeQueue;
    private final ConcurrentLinkedQueue<Message> readQueue;
    private final Selector selector;
    Integer counter = 0;

    private final Map<Address, SocketChannel> connections = new ConcurrentHashMap<>();

    public P2PNode(PeerInfo myPeerInfo,
                   List<PeerInfo> otherPeersInfo,
                   CountDownLatch connectionBarrier
                   //ConcurrentLinkedQueue<Message> readQueue,
    ) throws IOException {
        this(myPeerInfo, otherPeersInfo, connectionBarrier, new ConcurrentLinkedQueue<>(), new ConcurrentLinkedQueue<>(), Selector.open());
    }

    public P2PNode(PeerInfo myPeerInfo,
                   List<PeerInfo> otherPeersInfo,
                   CountDownLatch connectionBarrier,
                   ConcurrentLinkedQueue<MessageWithReceiver> writeQueue,
                   ConcurrentLinkedQueue<Message> readQueue
                   //ConcurrentLinkedQueue<Message> readQueue,
    ) throws IOException {
        this(myPeerInfo, otherPeersInfo, connectionBarrier, writeQueue, readQueue, Selector.open());
    }

    public P2PNode(PeerInfo myPeerInfo,
                   List<PeerInfo> otherPeersInfo,
                   CountDownLatch connectionBarrier,
                   ConcurrentLinkedQueue<MessageWithReceiver> writeQueue,
                   ConcurrentLinkedQueue<Message> readQueue,
                   Selector selector) throws IOException {
        this.myPeerInfo = myPeerInfo;
        idsToOtherPeersInfo = otherPeersInfo.stream().collect(Collectors.toMap(PeerInfo::id, Function.identity()));
        this.connectionBarrier = connectionBarrier;
        this.writeQueue = writeQueue;
        this.readQueue = readQueue;
        this.selector = selector;
        startConnection();
    }

    private void startConnection() throws IOException {
        myServerSocketChannel = ServerSocketChannel.open();
        myServerSocketChannel.configureBlocking(false); // 1 thread por iniciar conexão
        myServerSocketChannel.bind(new InetSocketAddress(myPeerInfo.address().port()));
        myServerSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    private void tryToConnect(PeerInfo otherPeerInfo) throws IOException {
        if (connections.containsKey(otherPeerInfo.address())) return;
        if (myPeerInfo.toString().compareTo(otherPeerInfo.toString()) < 0) return;

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(new InetSocketAddress(otherPeerInfo.address().ip(), otherPeerInfo.address().port()));
        socketChannel.register(selector, SelectionKey.OP_CONNECT, otherPeerInfo);
        System.out.println(myPeerInfo.id() + " initiating connection to: " + otherPeerInfo);
    }


    @Override
    public void run() {
        try {
            runAux();
        } catch (IOException | ClassNotFoundException e) {

        }
    }

    private void runAux() throws IOException, ClassNotFoundException {
        for (PeerInfo otherPeerInfo : idsToOtherPeersInfo.values()) {
            tryToConnect(otherPeerInfo);
        }
        while (true) {
            selector.select();
            sendPendingMessages();
            for (SelectionKey key : selector.selectedKeys()) {
                switch (KeyType.fromSelectionKey(key)) {
                    case CONNECT -> {
                        SocketChannel serverChannel = (SocketChannel) key.channel();
                        if (serverChannel.finishConnect()) {
                            Address peerAddress = new Address(
                                    serverChannel.socket().getInetAddress().getHostAddress(),
                                    serverChannel.socket().getPort()
                            );
                            connections.put(peerAddress, serverChannel);
                            connectionBarrier.countDown();
                            System.out.println(myPeerInfo.id() + " connected to server " + key.attachment());
                            serverChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        }
                    }
                    case ACCEPT -> {
                        SocketChannel clientChannel = ((ServerSocketChannel) key.channel()).accept();
                        Address peerAddress = new Address(clientChannel.socket().getInetAddress().getHostAddress(), clientChannel.socket().getPort());
                        if (!connections.containsKey(peerAddress)) {
                            connections.put(peerAddress, clientChannel);
                            clientChannel.configureBlocking(false);
                            System.out.println(myPeerInfo.id() + " connected to client: " + peerAddress);
                            connectionBarrier.countDown();
                            clientChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        }
                    }
                    case READ -> {
                        SocketChannel serverChannel = (SocketChannel) key.channel();
                        Message message = readMessage(serverChannel);
                        readQueue.add(message);
                        System.out.println(myPeerInfo.id() + " reading message: " + message);
                        counter++;
                        System.out.println(counter);
                    }
                    case WRITE -> {

                    }

                    case UNKNOWN -> {
                    }
                }
            }

            selector.selectedKeys().clear();
        }
    }

    private Message readMessage(SocketChannel channel) throws IOException, ClassNotFoundException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        while (lengthBuffer.hasRemaining()) {
            channel.read(lengthBuffer);
        }
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();

        ByteBuffer dataBuffer = ByteBuffer.allocate(length);
        while (dataBuffer.hasRemaining()) {
            channel.read(dataBuffer);
        }
        dataBuffer.flip();
        byte[] data = dataBuffer.array();
        return Message.fromBytes(data);
    }

    private void sendPendingMessages() throws IOException {
        for (MessageWithReceiver messageWithReceiver : writeQueue) {
            System.out.println(myPeerInfo.id() + " sending to " + messageWithReceiver.receiverId() + " message: " + messageWithReceiver.message());
            sendMessage(messageWithReceiver.receiverId(), messageWithReceiver.message());
            writeQueue.remove(messageWithReceiver);
        }
    }

    private void sendMessage(Integer receiverId, Message message) throws IOException {
        Address targetAddress = idsToOtherPeersInfo.get(receiverId).address();
        SocketChannel socketChannel = connections.get(targetAddress);

        if (socketChannel == null || !socketChannel.isConnected()) {
            System.out.println("Cannot send, no connection to " + receiverId);
            return;
        }

        byte[] data = message.toBytes();
        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();

        while (buffer.hasRemaining()) {
            int written = socketChannel.write(buffer);
            if (written == 0) break;
        }
    }

    @Override
    public void close() {
        try {
            this.myServerSocketChannel.close();
            for (SocketChannel value : connections.values()) {
                value.close();
            }
            this.selector.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
