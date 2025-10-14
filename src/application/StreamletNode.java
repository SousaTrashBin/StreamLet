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

    private final static int CONFUSION_START = 30;
    private final static int CONFUSION_DURATION = 5;
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
                        System.out.printf("Pausing message processing during confusion %d%n", epoch);

                        while (epoch >= CONFUSION_START && epoch < CONFUSION_START + CONFUSION_DURATION + 1) {
                            Thread.sleep(deltaInSeconds * 1000L);
                            epoch = currentEpoch.get();
                        }

                        System.out.printf("Resuming message processing after confusion %d%n", epoch);
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

        Block parent = blockchainManager.getLongestNotarizedChainTips().stream().findFirst().get();

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
        switch (message.type()) {
            case PROPOSE -> handlePropose(message, currentEpoch.get());
            case VOTE -> handleVote(message);
        }
    }

    private void handlePropose(Message message, int epoch) {
        Block proposedBlock = (Block) message.content();

        if (message.sender() != currentLeaderId) {
            return;
        }

        if (!blockchainManager.extendsAnyLongestNotarizedTip(proposedBlock)) {
            return;
        }

        votedBlocks.putIfAbsent(proposedBlock, new HashSet<>());
        votedBlocks.get(proposedBlock).add(localId);

        Message voteMessage = new Message(MessageType.VOTE, proposedBlock, localId);
        urbNode.broadcastFromLocal(voteMessage);

        blockchainManager.addBlock(proposedBlock);
    }


    private void handleVote(Message message) {
        Block votedBlock = (Block) message.content();
        votedBlocks.putIfAbsent(votedBlock, new HashSet<>());
        Set<Integer> votesForBlock = votedBlocks.get(votedBlock);
        votesForBlock.add(message.sender());

        if (blockchainManager.extendsAnyLongestNotarizedTip(votedBlock)
                && votedBlocks.get(votedBlock).size() > numberOfDistinctNodes / 2) {
            blockchainManager.notarizeBlock(votedBlock);
        }

        blockchainManager.addBlock(votedBlock);
    }

    private int calculateLeaderId(int epoch) {
        return epoch % numberOfDistinctNodes;
    }


}
