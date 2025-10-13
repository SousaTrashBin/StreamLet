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

public class StreamletNode {

    private final URBNode urbNode;
    private final int deltaInSeconds;
    private final int numberOfDistinctNodes;
    private final TransactionPoolSimulator transactionPoolSimulator;
    private final int localId;
    private final Random random = new Random(1L);
    private final BlockchainManager blockchainManager;
    private final Map<Block, Set<Integer>> votedBlocks = new HashMap<>();
    private final static int CONFUSION_START = 10;
    private final static int CONFUSION_DURATION = 5;
    private final BlockingQueue<Message> derivableQueue = new LinkedBlockingQueue<>();

    public StreamletNode(PeerInfo localPeerInfo, List<PeerInfo> remotePeersInfo, int deltaInSeconds)
            throws IOException, InterruptedException {
        localId = localPeerInfo.id();
        numberOfDistinctNodes = 1 + remotePeersInfo.size();
        this.deltaInSeconds = deltaInSeconds;
        transactionPoolSimulator = new TransactionPoolSimulator(numberOfDistinctNodes);
        blockchainManager = new BlockchainManager();
        urbNode = new URBNode(localPeerInfo, remotePeersInfo, derivableQueue::add);
    }

    public void startProtocol() throws InterruptedException {
        launchConsumerThread();
        launchUrbThread();

        urbNode.waitForAllPeersToConnect();

        AtomicInteger currentEpoch = new AtomicInteger(1);
        long epochDurationInSeconds = 2L * deltaInSeconds;

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        //in order to test if it was successful maybe it will be important to add a max number of runs
        //before logging what happened (at least the whole chain that was validated)
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
        Thread consumerThread = new Thread(() -> {
            try {
                while (true) {
                    Message message = derivableQueue.take();
                    handleMessageDelivery(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
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
                blockchainManager.getChainLength() + 1,
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
                        proposedBlock.length() <= blockchainManager.getChainLength()) {
                    return;
                }

                votedBlocks.get(proposedBlock).add(localId);
                blockchainManager.addBlock(proposedBlock);

                Message voteMessage = new Message(MessageType.VOTE, proposedBlock, localId);
                urbNode.broadcastFromLocal(voteMessage);
            }

            case VOTE -> {
                Block votedBlock = (Block) message.content();

                if (votedBlock.length() < blockchainManager.getChainLength()) {
                    System.out.printf("Vote ignored, Block %s does not extend the current chain (length = %d)%n",
                            Arrays.toString(votedBlock.hash()), votedBlock.length());
                    return;
                }

                votedBlocks.putIfAbsent(votedBlock, new HashSet<>());
                votedBlocks.get(votedBlock).add(message.sender());

                if (!blockchainManager.isNotarized(votedBlock) &&
                        votedBlocks.get(votedBlock).size() > numberOfDistinctNodes / 2) {
                    blockchainManager.notarizeBlock(votedBlock);
                    System.out.println("Block " + Arrays.toString(votedBlock.hash()) + " has been notarized");
                }
            }
        }
    }

    private int getLeaderId(int currentEpoch) {
        if (currentEpoch < CONFUSION_START
                || currentEpoch >= CONFUSION_START + CONFUSION_DURATION - 1) {
            return random.nextInt(numberOfDistinctNodes);
        }
        return currentEpoch % numberOfDistinctNodes;
    }

}
