package network;

import utils.application.Message;
import utils.communication.KeyType;
import utils.communication.MessageWithReceiver;
import utils.communication.PeerInfo;

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
import java.util.Set;
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
    private final Set<Integer> connectedPeers = ConcurrentHashMap.newKeySet();
    private ServerSocketChannel serverChannel;

    private static final long RETRY_DELAY_MS = 2500;
    private final Map<Integer, Long> peerConnectionBackoff = new ConcurrentHashMap<>();

    public P2PNode(PeerInfo localPeerInfo, List<PeerInfo> remotePeersInfo) throws IOException {
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
        peerConnections.computeIfPresent(remotePeer.id(), (_, existing) -> {
            try {
                if (existing.isConnected() || existing.isConnectionPending()) return existing;
                if (existing.isOpen()) {
                    existing.keyFor(ioSelector).cancel();
                    existing.close();
                }
            } catch (IOException ignored) {
            }
            return null;
        });

        SocketChannel clientChannel = SocketChannel.open();
        clientChannel.configureBlocking(false);
        clientChannel.connect(new InetSocketAddress(remotePeer.address().ip(), remotePeer.address().port()));
        clientChannel.register(ioSelector, SelectionKey.OP_CONNECT, remotePeer);
        peerConnections.put(remotePeer.id(), clientChannel);
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
        while (true) {
            attemptToConnectToPeers();
            ioSelector.select(1000);
            processOutgoingMessages();

            for (SelectionKey key : ioSelector.selectedKeys()) {
                if (!key.isValid()) continue;
                try {
                    switch (KeyType.fromSelectionKey(key)) {
                        case CONNECT -> handleConnectComplete(key);
                        case ACCEPT -> handleIncomingConnection(key);
                        case READ -> handleIncomingMessage(key);
                    }
                } catch (IOException e) {
                    handleConnectionFailure(key, e);
                }
            }
            ioSelector.selectedKeys().clear();
        }
    }

    private void handleConnectionFailure(SelectionKey key, IOException e) {
        if (!(key.channel() instanceof SocketChannel channel)) {
            key.cancel();
            try {
                key.channel().close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return;
        }

        Integer peerId;

        if (key.attachment() instanceof Integer) {
            peerId = (Integer) key.attachment();
        } else if (key.attachment() instanceof PeerInfo) {
            peerId = ((PeerInfo) key.attachment()).id();
        } else {
            peerId = findPeerIdByChannel(channel);
        }

        if (peerId != null) {
            peerConnections.remove(peerId, channel);
            connectedPeers.remove(peerId);
        }

        try {
            key.cancel();
            channel.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private Integer findPeerIdByChannel(SocketChannel channel) {
        return peerConnections.entrySet().stream()
                .filter(entry -> entry.getValue() == channel)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    private void attemptToConnectToPeers() {
        long now = System.currentTimeMillis();
        for (PeerInfo remotePeer : peerInfoById.values()) {
            SocketChannel channel = peerConnections.get(remotePeer.id());
            if (channel == null || !channel.isOpen()) {
                long lastAttempt = peerConnectionBackoff.getOrDefault(remotePeer.id(), 0L);
                if (now - lastAttempt > RETRY_DELAY_MS) {
                    try {
                        peerConnectionBackoff.put(remotePeer.id(), now);
                        attemptConnectionToPeer(remotePeer);
                    } catch (IOException ignored) {
                    }
                }
            } else if (!channel.isConnected() && !channel.isConnectionPending()) {
                long lastAttempt = peerConnectionBackoff.getOrDefault(remotePeer.id(), 0L);
                if (now - lastAttempt > RETRY_DELAY_MS) {
                    try {
                        peerConnectionBackoff.put(remotePeer.id(), now);
                        attemptConnectionToPeer(remotePeer);
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }

    private void handleConnectComplete(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        PeerInfo remotePeer = (PeerInfo) key.attachment();

        try {
            clientChannel.finishConnect();
        } catch (IOException e) {
            key.cancel();
            clientChannel.close();
            peerConnections.remove(remotePeer.id(), clientChannel);
            throw e;
        }

        if (localPeerInfo.id() > remotePeer.id()) { // tie break
            SocketChannel existing = peerConnections.get(remotePeer.id());
            if (existing != null && existing.isConnected() && existing != clientChannel) {
                clientChannel.close();
                key.cancel();
                peerConnections.remove(remotePeer.id(), clientChannel);
                return;
            }
        } else {
            SocketChannel existing = peerConnections.remove(remotePeer.id());
            if (existing != null && existing != clientChannel && existing.isOpen()) {
                existing.keyFor(ioSelector).cancel();
                existing.close();
            }
            peerConnections.put(remotePeer.id(), clientChannel);
        }

        ByteBuffer idBuffer = ByteBuffer.allocate(4).putInt(localPeerInfo.id());
        idBuffer.flip();
        clientChannel.write(idBuffer);

        if (connectedPeers.add(remotePeer.id())) {
            allPeersConnectedLatch.countDown();
            peerConnectionBackoff.remove(remotePeer.id());
        }
        System.out.println(localPeerInfo.id() + " connected to peer " + remotePeer.id());
        clientChannel.register(ioSelector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, remotePeer.id());
    }

    private void handleIncomingConnection(SelectionKey key) throws IOException {
        SocketChannel incomingChannel = ((ServerSocketChannel) key.channel()).accept();
        incomingChannel.configureBlocking(false);

        ByteBuffer idBuffer = ByteBuffer.allocate(4);
        while (idBuffer.hasRemaining()) {
            int read = incomingChannel.read(idBuffer);
            if (read == -1) {
                incomingChannel.close();
                return;
            }
        }
        idBuffer.flip();
        int remotePeerId = idBuffer.getInt();

        if (remotePeerId > localPeerInfo.id()) {
            SocketChannel existing = peerConnections.get(remotePeerId);
            if (existing != null && (existing.isConnectionPending() || existing.isConnected())) {
                incomingChannel.close();
                return;
            }
        } else {
            SocketChannel existing = peerConnections.remove(remotePeerId);
            if (existing != null && existing.isOpen()) {
                existing.keyFor(ioSelector).cancel();
                existing.close();
            }
            peerConnections.put(remotePeerId, incomingChannel);
        }
        if (connectedPeers.add(remotePeerId)) {
            allPeersConnectedLatch.countDown();
            peerConnectionBackoff.remove(remotePeerId);
        }
        System.out.println(localPeerInfo.id() + " accepted connection from peer " + remotePeerId);
        incomingChannel.register(ioSelector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, remotePeerId);
    }

    private void handleIncomingMessage(SelectionKey key) throws IOException, ClassNotFoundException {
        SocketChannel channel = (SocketChannel) key.channel();
        Message message = readMessageFromChannel(channel);
        if (message != null) {
            incomingMessageQueue.add(message);
        }
    }

    private Message readMessageFromChannel(SocketChannel channel) throws IOException, ClassNotFoundException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        while (lengthBuffer.hasRemaining()) {
            int read = channel.read(lengthBuffer);
            if (read == -1) {
                throw new IOException("Connection closed");
            }
        }
        lengthBuffer.flip();
        int messageLength = lengthBuffer.getInt();

        ByteBuffer messageBuffer = ByteBuffer.allocate(messageLength);
        while (messageBuffer.hasRemaining()) {
            int read = channel.read(messageBuffer);
            if (read == -1) {
                throw new IOException("Connection closed");
            }
        }
        messageBuffer.flip();
        return Message.fromBytes(messageBuffer.array());
    }

    public void enqueueOutgoingMessage(MessageWithReceiver messageWithReceiver) {
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
            return;
        }
        SocketChannel peerChannel = peerConnections.get(receiverId);
        if (peerChannel == null || !peerChannel.isConnected()) {
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