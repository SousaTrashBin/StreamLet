package application;

import urb.URBNode;
import utils.application.Block;
import utils.application.Message;
import utils.application.MessageType;
import utils.application.Transaction;
import utils.communication.PeerInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamletNode {

    private final URBNode urbNode;
    private final int deltaInSeconds;
    private final int numberOfDistinctNodes;
    private final TransactionPoolSimulator transactionPoolSimulator;
    private final int localId;
    private final Random random = new Random(1L);
    private final BlockchainManager blockchainManager;
    private final Map<Block, Set<Integer>> votedBlocks = new HashMap<>();

    public StreamletNode(PeerInfo localPeerInfo, List<PeerInfo> remotePeersInfo, int deltaInSeconds)
            throws IOException {
        localId = localPeerInfo.id();
        numberOfDistinctNodes = 1 + remotePeersInfo.size();
        this.deltaInSeconds = deltaInSeconds;
        transactionPoolSimulator = new TransactionPoolSimulator(numberOfDistinctNodes);
        blockchainManager = new BlockchainManager();
        urbNode = new URBNode(localPeerInfo, remotePeersInfo, this::handleMessageDelivery);
    }

    public void startProtocol() throws InterruptedException {
        urbNode.startURBNode();

        AtomicInteger currentEpoch = new AtomicInteger(1);
        long epochDurationInSeconds = 2L * deltaInSeconds;

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            int currentEpochValue = currentEpoch.addAndGet(1);
            int currentLeaderId = getLeaderId(currentEpochValue);
            System.out.printf("Epoch advanced to %d, current leader is %d%n", currentEpochValue, currentLeaderId);

                if (localId == currentLeaderId) {
                    try {
                        proposeNewBlock(currentEpochValue);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
        }, epochDurationInSeconds, epochDurationInSeconds, TimeUnit.SECONDS);
        }

    private void proposeNewBlock(int currentEpoch) throws NoSuchAlgorithmException {
        int transactionCount = random.nextInt(2, 6);
        Transaction[] transactions = new Transaction[transactionCount];
        for (int i = 0; i < transactionCount; i++) {
            transactions[i] = transactionPoolSimulator.generateNewTransaction();
        }

        Block parent = blockchainManager.getLongestNotarizedChainTip();

        Block newBlock = new Block(
                parent.hash(),
                currentEpoch,
                blockchainManager.getChainSize() + 1,
                transactions
        );

        Message proposeMessage = new Message(MessageType.PROPOSE, newBlock, localId);
        urbNode.broadcastFromLocal(proposeMessage);
    }

    private void handleMessageDelivery(Message message) {
        switch (message.type()) {
            case PROPOSE -> {
                Block proposedBlock = (Block) message.content();
                votedBlocks.putIfAbsent(proposedBlock, new HashSet<>());

                if (votedBlocks.get(proposedBlock).contains(localId) ||
                        proposedBlock.length() <= blockchainManager.getChainSize()) {
                    return;
                }

                votedBlocks.get(proposedBlock).add(localId);
                blockchainManager.addBlock(proposedBlock);

                Message voteMessage = new Message(MessageType.VOTE, proposedBlock, localId);
                urbNode.broadcastFromLocal(voteMessage);
            }

            case VOTE -> {
                Block votedBlock = (Block) message.content();
                votedBlocks.putIfAbsent(votedBlock, new HashSet<>());

                votedBlocks.get(votedBlock).add(message.sender());

                if (!blockchainManager.isNotarized(votedBlock) &&
                        votedBlocks.get(votedBlock).size() > numberOfDistinctNodes / 2) {
                    blockchainManager.notarizeBlock(votedBlock);
                    // maybe the block could be deleted once its voted
                    System.out.println("Block " + Arrays.toString(votedBlock.hash()) + " has been notarized");
                }
            }
        }
    }

    private int getLeaderId(int currentEpoch) {
        Random epochRandom = new Random(1L + currentEpoch);
        return epochRandom.nextInt(numberOfDistinctNodes);
    }
}
