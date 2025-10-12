import urb.URBNode;
import utils.ConfigParser;
import utils.application.Block;
import utils.application.Message;
import utils.application.MessageType;
import utils.communication.PeerInfo;

void main() throws IOException, InterruptedException {
    List<PeerInfo> peerInfos = ConfigParser.parsePeers();
    PeerInfo p1 = peerInfos.getFirst();
    PeerInfo p2 = peerInfos.get(1);
    PeerInfo p3 = peerInfos.get(2);

    URBNode node1 = new URBNode(p1, List.of(p2, p3));
    URBNode node2 = new URBNode(p2, List.of(p1, p3));
    URBNode node3 = new URBNode(p3, List.of(p1, p2));
    node1.startURBNode();
    node2.startURBNode();
    node3.startURBNode();

    Message dummy = new Message(MessageType.PROPOSE, new Block(null, 1, 2, null), 1);

    node1.broadcastFromLocal(dummy);
    Thread.sleep(1000);
    System.exit(0); // for now exits just to force every thread to stop
}
