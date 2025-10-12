import utils.KeyType;
import utils.Message;
import utils.MessageWithReceiver;
import utils.PeerInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private final Map<Integer, SocketChannel> connections = new ConcurrentHashMap<>();

    public P2PNode(PeerInfo myPeerInfo,
                   List<PeerInfo> otherPeersInfo,
                   CountDownLatch connectionBarrier) throws IOException {
        this(myPeerInfo, otherPeersInfo, connectionBarrier,
                new ConcurrentLinkedQueue<>(), new ConcurrentLinkedQueue<>(), Selector.open());
    }

    public P2PNode(PeerInfo myPeerInfo,
                   List<PeerInfo> otherPeersInfo,
                   CountDownLatch connectionBarrier,
                   ConcurrentLinkedQueue<MessageWithReceiver> writeQueue,
                   ConcurrentLinkedQueue<Message> readQueue) throws IOException {
        this(myPeerInfo, otherPeersInfo, connectionBarrier, writeQueue, readQueue, Selector.open());
    }

    public P2PNode(PeerInfo myPeerInfo,
                   List<PeerInfo> otherPeersInfo,
                   CountDownLatch connectionBarrier,
                   ConcurrentLinkedQueue<MessageWithReceiver> writeQueue,
                   ConcurrentLinkedQueue<Message> readQueue,
                   Selector selector) throws IOException {
        this.myPeerInfo = myPeerInfo;
        this.idsToOtherPeersInfo = otherPeersInfo.stream().collect(Collectors.toMap(PeerInfo::id, Function.identity()));
        this.connectionBarrier = connectionBarrier;
        this.writeQueue = writeQueue;
        this.readQueue = readQueue;
        this.selector = selector;
        startConnection();
    }

    private void startConnection() throws IOException {
        myServerSocketChannel = ServerSocketChannel.open();
        myServerSocketChannel.configureBlocking(false);
        myServerSocketChannel.bind(new InetSocketAddress(myPeerInfo.address().port()));
        myServerSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    private void tryToConnect(PeerInfo otherPeerInfo) throws IOException {
        if (connections.containsKey(otherPeerInfo.id())) return;
        if (myPeerInfo.id() < otherPeerInfo.id()) return;

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(new InetSocketAddress(otherPeerInfo.address().ip(), otherPeerInfo.address().port()));
        socketChannel.register(selector, SelectionKey.OP_CONNECT, otherPeerInfo);
        System.out.println(myPeerInfo.id() + " initiating connection to: " + otherPeerInfo.id());
    }

    @Override
    public void run() {
        try {
            runAux();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
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
                    case CONNECT -> handleConnect(key);
                    case ACCEPT -> handleAccept(key);
                    case READ -> handleRead(key);
                }
            }

            selector.selectedKeys().clear();
        }
    }

    private void handleConnect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        PeerInfo otherPeer = (PeerInfo) key.attachment();

        if (!channel.finishConnect()) {
            return;
        }

        ByteBuffer idBuffer = ByteBuffer.allocate(4).putInt(myPeerInfo.id());
        idBuffer.flip();
        channel.write(idBuffer);

        connections.put(otherPeer.id(), channel);
        connectionBarrier.countDown();
        System.out.println(myPeerInfo.id() + " connected to peer " + otherPeer.id());
        channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    private void handleAccept(SelectionKey key) throws IOException {
        SocketChannel clientChannel = ((ServerSocketChannel) key.channel()).accept();
        clientChannel.configureBlocking(false);

        ByteBuffer idBuffer = ByteBuffer.allocate(4);
        while (idBuffer.hasRemaining()) {
            clientChannel.read(idBuffer);
        }
        idBuffer.flip();
        int peerId = idBuffer.getInt();

        if (connections.containsKey(peerId)) {
            clientChannel.close();
            return;
        }

        connections.put(peerId, clientChannel);
        connectionBarrier.countDown();
        System.out.println(myPeerInfo.id() + " accepted connection from peer " + peerId);
        clientChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

    }

    private void handleRead(SelectionKey key) throws IOException, ClassNotFoundException {
        SocketChannel channel = (SocketChannel) key.channel();
        Message message = readMessage(channel);
        readQueue.add(message);
        System.out.println(myPeerInfo.id() + " received message: " + message);
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
        return Message.fromBytes(dataBuffer.array());
    }

    private void sendPendingMessages() throws IOException {
        MessageWithReceiver msg;
        while ((msg = writeQueue.poll()) != null) {
            sendMessage(msg.receiverId(), msg.message());
        }
    }

    private void sendMessage(Integer receiverId, Message message) throws IOException {
        if (Objects.equals(receiverId, myPeerInfo.id())) {
            System.out.println("Can't send message to self");
        }
        SocketChannel channel = connections.get(receiverId);
        if (channel == null || !channel.isConnected()) {
            System.out.println("Can't send, no connection to " + receiverId);
            return;
        }

        byte[] data = message.toBytes();
        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();

        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    @Override
    public void close() {
        try {
            myServerSocketChannel.close();
            for (SocketChannel channel : connections.values()) {
                channel.close();
            }
            selector.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
