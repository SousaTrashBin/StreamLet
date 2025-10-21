package application;

import urb.URBNode;
import utils.application.Block;
import utils.application.BlockWithStatus;
import utils.application.Message;
import utils.application.MessageType;
import utils.application.Proposal;
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
    private static final int CONFUSION_START = 0;
    private static final int CONFUSION_DURATION = 2;
    public static final int BLOCK_CHAIN_PRINT_EPOCH_FREQUENCY = 5;

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
        scheduler.scheduleAtFixedRate(this::safeAdvanceEpoch, 0, epochDuration, TimeUnit.SECONDS);
    }

    private void safeAdvanceEpoch() {
        try {
            advanceEpoch();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void advanceEpoch() {
        int epoch = currentEpoch.get();
        int currentLeaderId = calculateLeaderId(epoch);
        System.out.printf("Epoch %d, leader %d%n", epoch, currentLeaderId);

        if (localId == currentLeaderId) {
            try {
                proposeNewBlock(epoch);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        if (epoch != 0 && epoch % BLOCK_CHAIN_PRINT_EPOCH_FREQUENCY == 0) blockchainManager.printBlockchainTree();
        currentEpoch.incrementAndGet();
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
        SequencedSet<BlockWithStatus> parentChain = blockchainManager.longestNotarizedChains().getFirst();

        Block parent = parentChain.getLast().block();
        Transaction[] transactions = transactionPoolSimulator.generateTransactions();
        Block newBlock = new Block(parent.getSHA1(), epoch, parent.length() + 1, transactions);

        Proposal proposal = new Proposal(parentChain, newBlock);
        urbNode.broadcastFromLocal(new Message(MessageType.PROPOSE, proposal, localId));
    }

    private void handleMessageDelivery(Message message) {
        System.out.printf("Delivering Message %s%n", message);
        switch (message.type()) {
            case PROPOSE -> handlePropose(message);
            case VOTE -> handleVote(message);
        }
    }

    private void handlePropose(Message message) {
        Proposal proposal = (Proposal) message.content();
        Block block = proposal.proposedBlock();

        SeenProposal seenProposal = new SeenProposal(message.sender(), block.epoch());
        if (seenProposals.contains(seenProposal) || !blockchainManager.extendsFromLongestNotarizedChains(block))
            return;
        seenProposals.add(seenProposal);

        SequencedSet<BlockWithStatus> parentChain = proposal.parentChain();
        blockchainManager.addPendingBlock(block, parentChain);

        Block voteBlock = new Block(block.parentHash(), block.epoch(), block.length(), new Transaction[0]);
        urbNode.broadcastFromLocal(new Message(MessageType.VOTE, voteBlock, localId));
    }


    private void handleVote(Message message) {
        Block block = (Block) message.content();
        votedBlocks.computeIfAbsent(block, _ -> new HashSet<>()).add(message.sender());

        if (blockchainManager.extendsFromLongestNotarizedChains(block)
                && votedBlocks.get(block).size() > numberOfDistinctNodes / 2) {
            blockchainManager.notarizeBlock(block);
        }
    }

    private boolean inConfusionEpoch(int epoch) {
        return epoch >= CONFUSION_START && epoch <= CONFUSION_START + CONFUSION_DURATION - 1;
    }

    private int calculateLeaderId(int epoch) {
        return inConfusionEpoch(epoch) ? epoch % numberOfDistinctNodes
                : random.nextInt(numberOfDistinctNodes);
    }

}
