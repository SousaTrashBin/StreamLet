package application;

import utils.application.Block;
import utils.application.BlockWithChain;
import utils.application.Transaction;

import java.util.*;
import java.util.stream.Collectors;

public class BlockchainManager {
    private static final int SHA1_LENGTH = 20;

    private static final Block GENESIS_BLOCK =
            new Block(new byte[SHA1_LENGTH], 0, 0, new Transaction[0]);

    private final Set<ChainView> seenNotarizedChains = new HashSet<>();
    private LinkedList<Block> biggestNotarizedChain = new LinkedList<>();
    private LinkedList<Block> biggestFinalizedChain = new LinkedList<>();
    private final HashMap<Block, BlockWithChain> pendingProposes = new HashMap<>();

    public BlockchainManager() {
        biggestNotarizedChain.add(GENESIS_BLOCK);
        seenNotarizedChains.add(new ChainView(biggestNotarizedChain));
    }

    public void notarizeBlock(Block headerBlock) {
        BlockWithChain proposal = pendingProposes.get(headerBlock);
        if (proposal == null) {
            return;
        }
        LinkedList<Block> chain = proposal.chain();
        chain.add(proposal.block());
        pendingProposes.remove(headerBlock);
        seenNotarizedChains.add(new ChainView(chain));
        if (chain.getLast().length() > biggestNotarizedChain.getLast().length()) {
            biggestNotarizedChain = chain;
            tryToUpdateFinalizedChain();
        }
    }

    private void tryToUpdateFinalizedChain() {
        int size = biggestNotarizedChain.size();
        if (size < 3) return;

        Block b0 = biggestNotarizedChain.get(size - 3);
        Block b1 = biggestNotarizedChain.get(size - 2);
        Block b2 = biggestNotarizedChain.get(size - 1);

        if (b1.epoch() - b0.epoch() == 1 && b2.epoch() - b1.epoch() == 1) {
            biggestFinalizedChain = new LinkedList<>(biggestNotarizedChain.subList(0, size - 1));
        }
    }

    public boolean onPropose(BlockWithChain proposal) {
        Block proposedBlock = proposal.block();
        LinkedList<Block> chain = proposal.chain();
        Block parentTip = chain.getLast();

        if (!Arrays.equals(proposedBlock.parentHash(), parentTip.getSHA1())) return false;

        boolean isStrictlyLonger = seenNotarizedChains.stream()
                .anyMatch(notarizedChain -> proposedBlock.length() > notarizedChain.blocks().getLast().length());
        if (!isStrictlyLonger) {
            return false;
        }
        pendingProposes.put(proposedBlock, proposal);
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
