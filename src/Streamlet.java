import application.StreamletNode;
import utils.ConfigParser;
import utils.communication.PeerInfo;


void main(String[] args) throws IOException, InterruptedException, NoSuchAlgorithmException {
    if (args.length != 1) {
        System.out.println("Usage: Streamlet <nodeId>");
        return;
    }

    int nodeId = Integer.parseInt(args[0]);

    List<PeerInfo> peerInfos = ConfigParser.parsePeers();
    System.out.println(peerInfos);
    PeerInfo localPeer = peerInfos.get(nodeId);

    List<PeerInfo> remotePeers = peerInfos.stream()
            .filter(p -> p.id() != nodeId)
            .toList();

    StreamletNode node = new StreamletNode(localPeer, remotePeers, 2);
}
