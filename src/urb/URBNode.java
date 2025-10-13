package urb;

import network.P2PNode;
import utils.application.Message;
import utils.application.MessageType;
import utils.communication.MessageWithReceiver;
import utils.communication.PeerInfo;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class URBNode {
    private final Set<Message> deliveredMessages = new HashSet<>();
    private final List<Integer> remotePeerIds;
    private final P2PNode networkLayer;
    private final int localPeerId;
    private final URBCallback callback;

    public URBNode(PeerInfo localPeerInfo,
                   List<PeerInfo> remotePeersInfo,
                   URBCallback callback) throws IOException {
        localPeerId = localPeerInfo.id();
        remotePeerIds = remotePeersInfo.stream().map(PeerInfo::id).toList();
        networkLayer = new P2PNode(localPeerInfo, remotePeersInfo);
        new Thread(networkLayer).start();
        this.callback = callback;
    }

    public void startURBNode() throws InterruptedException {
        networkLayer.waitForAllPeersConnected();
        System.out.printf("P2PNode %d is ready\n", localPeerId);
        new Thread(this::processIncomingMessages).start();
    }

    public void broadcastToPeers(Message message) {
        remotePeerIds.stream()
                .filter(peerId -> !Objects.equals(peerId, message.sender()))
                .map(peerId -> new MessageWithReceiver(peerId, message))
                .forEach(networkLayer::enqueueOutgoingMessage);
    }

    private void processIncomingMessages() {
        while (true) {
            try {
                Message receivedMessage = networkLayer.receiveMessage();
                deliverMessage(receivedMessage);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void deliverMessage(Message message) {
        switch (message.type()) {
            case ECHO -> {
                if (message.content() instanceof Message contentMessage
                        && deliveredMessages.add(contentMessage)) {

                    deliveredMessages.add(message);
                    System.out.println(localPeerId + " delivering message " + contentMessage);
                    deliverToApplication(contentMessage);
                }
            }
            case PROPOSE, VOTE -> {
                if (!deliveredMessages.add(message)) {
                    return;
                }
                Message echoMessage = new Message(MessageType.ECHO, message, localPeerId);
                broadcastToPeers(echoMessage);
                deliverToApplication(message);
                System.out.println(localPeerId + " delivered message " + message);
            }
        }
    }


    private void deliverToApplication(Message message) {
        callback.onDelivery(message);
    }

    public void broadcastFromLocal(Message message) {
        networkLayer.enqueueIncomingMessage(message);
    }
}