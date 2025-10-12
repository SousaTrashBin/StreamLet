import utils.*;

void main() throws IOException, InterruptedException {
    PeerInfo p1 = new PeerInfo(1, new Address("127.0.0.1", 12345));
    PeerInfo p2 = new PeerInfo(2, new Address("127.0.0.1", 12346));
    PeerInfo p3 = new PeerInfo(3, new Address("127.0.0.1", 12347));

    URBNode node1 = new URBNode(p1, List.of(p2, p3));
    URBNode node2 = new URBNode(p2, List.of(p1, p3));
    URBNode node3 = new URBNode(p3, List.of(p1, p2));
    node1.startURBNode();
    node2.startURBNode();
    node3.startURBNode();

    Message dummy = new Message(MessageType.PROPOSE, new Block(null, 1, 2, null), 1);

    node1.broadcastFromLocal(dummy);
}
