import utils.Address;
import utils.KeyType;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class P2PNode implements Runnable,AutoCloseable {
    private final Address myAddress;
    private final CountDownLatch connectionBarrier;
    private final List<Address> otherAddresses;
    private final String name;
    private ServerSocketChannel myServerSocketChannel;
    private Selector selector;

    private final Map<Address, SocketChannel> connections = new ConcurrentHashMap<>();

    public P2PNode(String name, Address myAddress, List<Address> otherAddresses, CountDownLatch connectionBarrier) throws IOException {
        this.myAddress = myAddress;
        this.name = name;
        this.otherAddresses = otherAddresses;
        this.connectionBarrier = connectionBarrier;
        startConnection();
    }

    private void startConnection() throws IOException {
        myServerSocketChannel = ServerSocketChannel.open();
        selector = Selector.open();

        myServerSocketChannel.configureBlocking(false); // 1 thread por iniciar conexão
        myServerSocketChannel.bind(new InetSocketAddress(myAddress.port()));
        myServerSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    private void tryToConnect(Address otherAddress) throws IOException {
        if (connections.containsKey(otherAddress)) return;

        if (myAddress.toString().compareTo(otherAddress.toString()) < 0) return;

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(new InetSocketAddress(otherAddress.ip(), otherAddress.port()));
        socketChannel.register(selector, SelectionKey.OP_CONNECT, otherAddress);
        System.out.println(name + " initiating connection to: " + otherAddress);
    }


    @Override
    public void run() {
        try {
            runAux();
        }catch (IOException e) {

        }
    }

    private void runAux() throws IOException {
        for (Address otherAddress : otherAddresses) {
            tryToConnect(otherAddress);
        }
        while (true) {
            selector.select();
            for (SelectionKey key : selector.selectedKeys()) {
                switch (KeyType.fromSelectionKey(key)) {
                    case CONNECT -> {
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        if (socketChannel.finishConnect()) {
                            InetAddress inetAddress = socketChannel.socket().getInetAddress();
                            String ip = inetAddress.getHostAddress();
                            int port = socketChannel.socket().getPort();
                            connections.put(new Address(ip,port), socketChannel);
                            connectionBarrier.countDown();
                            System.out.println(name + " connected to server " + key.attachment());
                            socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        }
                    }
                    case ACCEPT -> {
                        SocketChannel client = ((ServerSocketChannel) key.channel()).accept();
                        Address address = new Address(client.socket().getInetAddress().getHostAddress(), client.socket().getPort());
                        if (!connections.containsKey(address)) {
                            connections.put(address, client);
                            client.configureBlocking(false);
                            System.out.println(name + " connected to client: " + address);
                            connectionBarrier.countDown();
                            client.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        }
                    }
                    case READ -> {

                    }
                    case WRITE -> {

                    }

                    case UNKNOWN -> {}
                }
            }

            selector.selectedKeys().clear();
        }
    }

    @Override
    public void close() {
        try {
            this.myServerSocketChannel.close();
            this.selector.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
