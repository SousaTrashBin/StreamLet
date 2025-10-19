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

record SeenProposal(int leader, int epoch) {
}

public class StreamletNode {
    private static final int CONFUSION_START = 20;
    private static final int CONFUSION_DURATION = 12;

    private final int deltaInSeconds;
    private final int numberOfDistinctNodes;
    private final TransactionPoolSimulator transactionPoolSimulator;
    private final Random random = new Random(1L);
    private final AtomicInteger currentEpoch = new AtomicInteger(0);

    private final int localId;
    private final URBNode urbNode;
    private final BlockchainManager blockchainManager;
    private final Map<Block, Set<Integer>> votedBlocks = new HashMap<>();
    private final BlockingQueue<Message> derivableQueue = new LinkedBlockingQueue<>(1000);
    private final Set<SeenProposal> seenProposals = new HashSet<>();

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public StreamletNode(PeerInfo localPeerInfo, List<PeerInfo> remotePeersInfo, int deltaInSeconds)
            throws IOException {
        localId = localPeerInfo.id();
        numberOfDistinctNodes = 1 + remotePeersInfo.size();
        this.deltaInSeconds = deltaInSeconds;
        transactionPoolSimulator = new TransactionPoolSimulator(numberOfDistinctNodes);
        blockchainManager = new BlockchainManager();
        urbNode = new URBNode(localPeerInfo, remotePeersInfo, derivableQueue::add);
    }

    public void startProtocol() throws InterruptedException {
        launchThreads();
        urbNode.waitForAllPeersToConnect();

        long epochDuration = 2L * deltaInSeconds;
        scheduler.scheduleAtFixedRate(this::safeAdvanceEpoch, epochDuration, epochDuration, TimeUnit.SECONDS);
    }

    private void safeAdvanceEpoch() {
        try {
            advanceEpoch();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void advanceEpoch() {
        int epoch = currentEpoch.incrementAndGet();
        int currentLeaderId = calculateLeaderId(epoch);
        System.out.printf("Epoch %d, leader %d%n", epoch, currentLeaderId);

        if (localId == currentLeaderId) {
            try {
                proposeNewBlock(epoch);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        if (epoch % 5 == 0) blockchainManager.printBlockchainTree();
    }

    private void launchThreads() {
        executor.submit(() -> {
            try {
                urbNode.startURBNode();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        executor.submit(this::consumeMessages);
    }

    private void consumeMessages() {
        final Queue<Message> bufferedMessages = new LinkedList<>();
        try {
            while (true) {
                Message message = derivableQueue.poll(100, TimeUnit.MILLISECONDS);
                if (message == null) continue;

                if (inConfusionEpoch(currentEpoch.get())) {
                    bufferedMessages.add(message);
                    continue;
                }

                while (!bufferedMessages.isEmpty()) {
                    handleMessageDelivery(bufferedMessages.poll());
                }

                handleMessageDelivery(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void proposeNewBlock(int epoch) throws NoSuchAlgorithmException {
        Optional<Block> parentOpt = blockchainManager.getNotarizedTips().stream()
                .max(Comparator.comparingInt(Block::length).thenComparing(Block::epoch));

        if (parentOpt.isEmpty()) {
            return;
        }

        Block parent = parentOpt.get();
        Transaction[] transactions = transactionPoolSimulator.generateTransactions();

        Block newBlock = new Block(parent.getSHA1(), epoch, parent.length() + 1, transactions);
        urbNode.broadcastFromLocal(new Message(MessageType.PROPOSE, newBlock, localId));
    }

    private void handleMessageDelivery(Message message) {
        System.out.printf("Delivering Message %s%n", message);
        switch (message.type()) {
            case PROPOSE -> handlePropose(message);
            case VOTE -> handleVote(message);
        }
    }

    private void handlePropose(Message message) {
        Block block = (Block) message.content();
        SeenProposal proposal = new SeenProposal(message.sender(), block.epoch());

        if (seenProposals.contains(proposal) || !blockchainManager.extendNotarizedAnyChainTip(block))
            return;

        seenProposals.add(proposal);
        blockchainManager.addPendingBlock(block);

        Block voteBlock = new Block(block.parentHash(), block.epoch(), block.length(), new Transaction[0]);
        urbNode.broadcastFromLocal(new Message(MessageType.VOTE, voteBlock, localId));
    }


    private void handleVote(Message message) {
        Block block = (Block) message.content();
        votedBlocks.computeIfAbsent(block, _ -> new HashSet<>()).add(message.sender());

        if (blockchainManager.extendNotarizedAnyChainTip(block)
                && votedBlocks.get(block).size() > numberOfDistinctNodes / 2) {

            blockchainManager.notarizeBlock(block);
        }
    }

    private boolean inConfusionEpoch(int epoch) {
        return epoch >= CONFUSION_START && epoch < CONFUSION_START + CONFUSION_DURATION + 1;
    }

    private int calculateLeaderId(int epoch) {
        return inConfusionEpoch(epoch) ? epoch % numberOfDistinctNodes
                : random.nextInt(numberOfDistinctNodes);
    }

}
