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
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class P2PNode implements Runnable, AutoCloseable {
    private final PeerInfo localPeerInfo;
    private final CountDownLatch allPeersConnectedLatch;
    private final Map<Integer, PeerInfo> peerInfoById;
    private final ConcurrentLinkedQueue<MessageWithReceiver> outgoingMessageQueue = new ConcurrentLinkedQueue<>();
    private final BlockingQueue<Message> incomingMessageQueue = new LinkedBlockingQueue<>();
    private final Selector ioSelector = Selector.open();
    private final Map<Integer, SocketChannel> peerConnections = new ConcurrentHashMap<>();
    private ServerSocketChannel serverChannel;

    public P2PNode(PeerInfo localPeerInfo,
                   List<PeerInfo> remotePeersInfo) throws IOException {
        this.localPeerInfo = localPeerInfo;
        this.peerInfoById = remotePeersInfo.stream()
                .collect(Collectors.toMap(PeerInfo::id, Function.identity()));
        this.allPeersConnectedLatch = new CountDownLatch(peerInfoById.size());
        initializeServerSocket();
    }

    private void initializeServerSocket() throws IOException {
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(localPeerInfo.address().port()));
        serverChannel.register(ioSelector, SelectionKey.OP_ACCEPT);
    }

    private void attemptConnectionToPeer(PeerInfo remotePeer) throws IOException {
        if (peerConnections.containsKey(remotePeer.id())) return;
        if (localPeerInfo.id() < remotePeer.id()) return;

        SocketChannel clientChannel = SocketChannel.open();
        clientChannel.configureBlocking(false);
        clientChannel.connect(new InetSocketAddress(remotePeer.address().ip(), remotePeer.address().port()));
        clientChannel.register(ioSelector, SelectionKey.OP_CONNECT, remotePeer);
        System.out.println(localPeerInfo.id() + " initiating connection to: " + remotePeer.id());
    }

    @Override
    public void run() {
        try {
            runEventLoop();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void runEventLoop() throws IOException, ClassNotFoundException {
        for (PeerInfo remotePeer : peerInfoById.values()) {
            attemptConnectionToPeer(remotePeer);
        }

        while (true) {
            ioSelector.select(100);
            processOutgoingMessages();

            for (SelectionKey key : ioSelector.selectedKeys()) {
                switch (KeyType.fromSelectionKey(key)) {
                    case CONNECT -> handleConnectComplete(key);
                    case ACCEPT -> handleIncomingConnection(key);
                    case READ -> handleIncomingMessage(key);
                }
            }

            ioSelector.selectedKeys().clear();
        }
    }

    private void handleConnectComplete(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        PeerInfo remotePeer = (PeerInfo) key.attachment();

        if (!clientChannel.finishConnect()) {
            return;
        }

        ByteBuffer idBuffer = ByteBuffer.allocate(4).putInt(localPeerInfo.id());
        idBuffer.flip();
        clientChannel.write(idBuffer);

        peerConnections.put(remotePeer.id(), clientChannel);
        allPeersConnectedLatch.countDown();
        System.out.println(localPeerInfo.id() + " connected to peer " + remotePeer.id());
        clientChannel.register(ioSelector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    private void handleIncomingConnection(SelectionKey key) throws IOException {
        SocketChannel incomingChannel = ((ServerSocketChannel) key.channel()).accept();
        incomingChannel.configureBlocking(false);

        ByteBuffer idBuffer = ByteBuffer.allocate(4);
        while (idBuffer.hasRemaining()) {
            incomingChannel.read(idBuffer);
        }
        idBuffer.flip();
        int remotePeerId = idBuffer.getInt();

        if (peerConnections.containsKey(remotePeerId)) {
            incomingChannel.close();
            return;
        }

        peerConnections.put(remotePeerId, incomingChannel);
        allPeersConnectedLatch.countDown();
        System.out.println(localPeerInfo.id() + " accepted connection from peer " + remotePeerId);
        incomingChannel.register(ioSelector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    private void handleIncomingMessage(SelectionKey key) throws IOException, ClassNotFoundException {
        SocketChannel channel = (SocketChannel) key.channel();
        Message message = readMessageFromChannel(channel);
        incomingMessageQueue.add(message);
        System.out.println(localPeerInfo.id() + " received message: " + message);
    }

    private Message readMessageFromChannel(SocketChannel channel) throws IOException, ClassNotFoundException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        while (lengthBuffer.hasRemaining()) {
            channel.read(lengthBuffer);
        }
        lengthBuffer.flip();
        int messageLength = lengthBuffer.getInt();

        ByteBuffer messageBuffer = ByteBuffer.allocate(messageLength);
        while (messageBuffer.hasRemaining()) {
            channel.read(messageBuffer);
        }
        messageBuffer.flip();
        return Message.fromBytes(messageBuffer.array());
    }

    void enqueueOutgoingMessage(MessageWithReceiver messageWithReceiver) {
        outgoingMessageQueue.add(messageWithReceiver);
        ioSelector.wakeup();
    }

    public Message receiveMessage() throws InterruptedException {
        return incomingMessageQueue.take();
    }

    public void enqueueIncomingMessage(Message message) {
        incomingMessageQueue.add(message);
    }

    public void waitForAllPeersConnected() throws InterruptedException {
        allPeersConnectedLatch.await();
    }

    private void processOutgoingMessages() throws IOException {
        MessageWithReceiver messageWithReceiver;
        while ((messageWithReceiver = outgoingMessageQueue.poll()) != null) {
            sendMessageToPeer(messageWithReceiver.receiverId(), messageWithReceiver.message());
        }
    }

    private void sendMessageToPeer(Integer receiverId, Message message) throws IOException {
        if (Objects.equals(receiverId, localPeerInfo.id())) {
            System.out.println("Can't send message to self");
            return;
        }
        SocketChannel peerChannel = peerConnections.get(receiverId);
        if (peerChannel == null || !peerChannel.isConnected()) {
            System.out.println("Can't send, no connection to " + receiverId);
            return;
        }

        byte[] messageBytes = message.toBytes();
        ByteBuffer sendBuffer = ByteBuffer.allocate(4 + messageBytes.length);
        sendBuffer.putInt(messageBytes.length);
        sendBuffer.put(messageBytes);
        sendBuffer.flip();

        while (sendBuffer.hasRemaining()) {
            peerChannel.write(sendBuffer);
        }
    }

    @Override
    public void close() {
        try {
            serverChannel.close();
            for (SocketChannel channel : peerConnections.values()) {
                channel.close();
            }
            ioSelector.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}