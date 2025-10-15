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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

record SeenProposalKey(int leader, int epoch) {

}

public class StreamletNode {

    private final static int CONFUSION_START = 8;
    private final static int CONFUSION_DURATION = 12;
    private final URBNode urbNode;
    private final int deltaInSeconds;
    private final int numberOfDistinctNodes;
    private final TransactionPoolSimulator transactionPoolSimulator;
    private final int localId;
    private final Random random = new Random(1L);
    private final BlockchainManager blockchainManager;
    private final Map<Block, Set<Integer>> votedBlocks = new HashMap<>();
    private final BlockingQueue<Message> derivableQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger currentEpoch = new AtomicInteger(0);
    private final ExecutorService messageExecutor = Executors.newFixedThreadPool(2);
    private final Map<SeenProposalKey, Block> seenProposals = new HashMap<>();
    private int currentLeaderId = -1;

    public StreamletNode(PeerInfo localPeerInfo, List<PeerInfo> remotePeersInfo, int deltaInSeconds)
            throws IOException {
        localId = localPeerInfo.id();
        numberOfDistinctNodes = 1 + remotePeersInfo.size();
        this.deltaInSeconds = deltaInSeconds;
        transactionPoolSimulator = new TransactionPoolSimulator(numberOfDistinctNodes);
        blockchainManager = new BlockchainManager();
        urbNode = new URBNode(localPeerInfo, remotePeersInfo, derivableQueue::add, messageExecutor);
    }

    public void startProtocol() throws InterruptedException {
        launchConsumerThread();
        launchUrbThread();

        urbNode.waitForAllPeersToConnect();

        long epochDurationInSeconds = 2L * deltaInSeconds;

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            int currentEpochValue = currentEpoch.addAndGet(1);
            currentLeaderId = calculateLeaderId(currentEpochValue);
            System.out.printf("Epoch advanced to %d, current leader is %d%n", currentEpochValue, currentLeaderId);

            if (localId == currentLeaderId) {
                try {
                    proposeNewBlock(currentEpochValue);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

            if (currentEpochValue % 5 == 0) {
                blockchainManager.printLinearBlockchain();
            }
        }, epochDurationInSeconds, epochDurationInSeconds, TimeUnit.SECONDS);
    }

    private void launchUrbThread() {
        Thread urbThread = new Thread(() -> {
            try {
                urbNode.startURBNode();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        urbThread.setDaemon(true);
        urbThread.start();
    }

    private void launchConsumerThread() {
        messageExecutor.execute(() -> {
            try {
                while (true) {
                    Message message = derivableQueue.take();
                    int epoch = currentEpoch.get();

                    if (epoch >= CONFUSION_START && epoch < CONFUSION_START + CONFUSION_DURATION + 1) {
                        System.out.printf("pausing message processing during confusion %d%n", epoch);

                        while (epoch >= CONFUSION_START && epoch < CONFUSION_START + CONFUSION_DURATION + 1) {
                            Thread.sleep(deltaInSeconds * 1000L);
                            epoch = currentEpoch.get();
                        }

                        System.out.printf("resuming message processing after confusion %d%n", epoch);
                    }

                    handleMessageDelivery(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        });
    }


    private void proposeNewBlock(int currentEpoch) throws NoSuchAlgorithmException {
        int transactionCount = random.nextInt(2, 6);
        Transaction[] transactions = new Transaction[transactionCount];
        for (int i = 0; i < transactionCount; i++) {
            transactions[i] = transactionPoolSimulator.generateNewTransaction();
        }

        Block parent = blockchainManager.getNotarizedTips().getFirst();

        Block newBlock = new Block(
                parent.hash(),
                currentEpoch,
                parent.length() + 1,
                transactions
        );

        Message proposeMessage = new Message(MessageType.PROPOSE, newBlock, localId);
        urbNode.broadcastFromLocal(proposeMessage);
    }

    private void handleMessageDelivery(Message message) {
        System.out.printf("Delivering Message %s%n", message);
        switch (message.type()) {
            case PROPOSE -> handlePropose(message);
            case VOTE -> handleVote(message);
        }
    }

    private void handlePropose(Message message) {
        Block proposedBlock = (Block) message.content();

        SeenProposalKey seenProposalKey = new SeenProposalKey(message.sender(), proposedBlock.epoch());
        if (seenProposals.containsKey(seenProposalKey)
                || !blockchainManager.extendNotarizedAnyChainTip(proposedBlock)) {
            return;
        }

        seenProposals.put(seenProposalKey, proposedBlock);
        blockchainManager.addBlock(proposedBlock);

        Message voteMessage = new Message(MessageType.VOTE, proposedBlock, localId);
        urbNode.broadcastFromLocal(voteMessage);

    }


    private void handleVote(Message message) {
        Block votedBlock = (Block) message.content();

        votedBlocks.putIfAbsent(votedBlock, new HashSet<>());
        Set<Integer> votesForBlock = votedBlocks.get(votedBlock);
        votesForBlock.add(message.sender());

        if (blockchainManager.extendNotarizedAnyChainTip(votedBlock)
                && votedBlocks.get(votedBlock).size() > numberOfDistinctNodes / 2) {
            blockchainManager.notarizeBlock(votedBlock);
        }
    }

    private int calculateLeaderId(int epoch) {
        if (epoch < CONFUSION_START || epoch >= CONFUSION_START + CONFUSION_DURATION + 1) {
            random.nextInt(numberOfDistinctNodes);
        }
        return epoch % numberOfDistinctNodes;
    }


}
