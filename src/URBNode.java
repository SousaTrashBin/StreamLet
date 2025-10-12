import utils.Message;
import utils.MessageWithReceiver;
import utils.PeerInfo;

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

    public URBNode(PeerInfo localPeerInfo,
                   List<PeerInfo> remotePeersInfo) throws IOException {
        localPeerId = localPeerInfo.id();
        remotePeerIds = remotePeersInfo.stream().map(PeerInfo::id).toList();
        networkLayer = new P2PNode(localPeerInfo, remotePeersInfo);
        new Thread(networkLayer).start();
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
        if (!deliveredMessages.add(message)) return;
        broadcastToPeers(message);
        deliverToApplication(message);
    }

    private void deliverToApplication(Message message) {
        System.out.printf("Peer %d delivered message from %d: %s%n",
                localPeerId, message.sender(), message);
    }

    public void broadcastFromLocal(Message message) {
        networkLayer.enqueueIncomingMessage(message);
    }
}