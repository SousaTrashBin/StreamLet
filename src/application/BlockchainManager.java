package application;

import utils.application.Block;
import utils.application.Transaction;

import java.util.*;
import java.util.stream.Collectors;

public class BlockchainManager {
    private static final int SHA1_LENGTH = 20;

    private static final Block GENESIS_BLOCK =
            new Block(new byte[SHA1_LENGTH], 0, 0, new Transaction[0]);

    private final Set<ChainView> seenNotarizedChains = new HashSet<>();
    private final HashMap<Block, Block> headerToBlock = new HashMap<>();
    private final Set<Block> hasNotarized = new HashSet<>();
    private LinkedList<Block> biggestNotarizedChain = new LinkedList<>();
    private LinkedList<Block> biggestFinalizedChain = new LinkedList<>();

    public BlockchainManager() {
        biggestNotarizedChain.add(GENESIS_BLOCK);
        seenNotarizedChains.add(new ChainView(biggestNotarizedChain));
    }

    public void notarizeBlock(Block headerBlock) {
        Block fullBlock = headerToBlock.get(headerBlock);
        if (fullBlock == null
                || hasNotarized.contains(fullBlock)
                || fullBlock.length() <= biggestNotarizedChain.getLast().length()) {
            return;
        }
        hasNotarized.add(fullBlock);
        biggestNotarizedChain.add(fullBlock);
        tryUpdateFinalizedChain();
    }

    private void tryUpdateFinalizedChain() {
        int size = biggestNotarizedChain.size();
        if (size < 3) return;

        Block b0 = biggestNotarizedChain.get(size - 3);
        Block b1 = biggestNotarizedChain.get(size - 2);
        Block b2 = biggestNotarizedChain.get(size - 1);

        if (b1.epoch() - b0.epoch() == 1 && b2.epoch() - b1.epoch() == 1) {
            biggestFinalizedChain = new LinkedList<>(biggestNotarizedChain.subList(0, size - 1));
        }
    }

    public boolean onPropose(Block proposedBlock, LinkedList<Block> parentChain) {
        seenNotarizedChains.add(new ChainView(parentChain));
        if (parentChain.size() > biggestNotarizedChain.size()) {
            biggestNotarizedChain = parentChain;
        }
        Block parentTip = parentChain.getLast();

        if (!Arrays.equals(proposedBlock.parentHash(), parentTip.getSHA1())) return false;

        boolean isStrictlyLonger = seenNotarizedChains.stream()
                .allMatch(chain -> proposedBlock.length() > chain.blocks().getLast().length());
        if (!isStrictlyLonger) {
            return false;
        }
        headerToBlock.put(proposedBlock, proposedBlock);
        return true;
    }

    public void printBiggestFinalizedChain() {
        final String GREEN = "\u001B[32m";
        final String RESET = "\u001B[0m";

        String header = "=== LONGEST FINALIZED CHAIN ===";
        String border = "=".repeat(header.length());

        String chainString = biggestFinalizedChain.stream()
                .map(block -> GREEN + "Block[%d-%d]".formatted(block.epoch(), block.length()) + RESET)
                .collect(Collectors.joining(" <- "));

        String output = String.format(
                "%s%n%s%n%s%n%s%n%s",
                border,
                header,
                border,
                chainString,
                border
        );

        System.out.println(output);
    }


    public LinkedList<Block> getBiggestNotarizedChain() {
        return biggestNotarizedChain;
    }
}
