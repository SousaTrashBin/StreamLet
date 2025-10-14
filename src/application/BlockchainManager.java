package application;

import utils.application.Block;
import utils.application.Transaction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

record HashKey(byte[] bytes) {
    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof HashKey(byte[] bytes1) && Arrays.equals(bytes, bytes1);
    }
}

public class BlockchainManager {
    private static final int SHA1_LENGTH = 20;

    private static final Block GENESIS_BLOCK =
            new Block(new byte[SHA1_LENGTH], 0, 1, new Transaction[0]);

    private final BlockNode genesis;
    private final Map<HashKey, BlockNode> hashToNode = new HashMap<>();

    public BlockchainManager() {
        genesis = new BlockNode(GENESIS_BLOCK, null);
        genesis.notarized = true;
        hashToNode.put(new HashKey(GENESIS_BLOCK.hash()), genesis);
    }

    public void notarizeBlock(Block block) {
        BlockNode node = hashToNode.get(new HashKey(block.hash()));
        if (node == null) return;

        node.notarized = true;
        tryToFinalize(node);
    }

    public void addBlock(Block block) {
        HashKey blockKey = new HashKey(block.hash());

        if (hashToNode.containsKey(blockKey)) {
            return;
        }

        HashKey parentKey = new HashKey(block.parentHash());
        BlockNode parent = hashToNode.get(parentKey);

        if (parent == null) {
            return;
        }

        BlockNode blockNode = new BlockNode(block, parent);
        parent.addChildren(blockNode);

        hashToNode.put(blockKey, blockNode);
    }

    public boolean extendsAnyLongestNotarizedTip(Block proposedBlock) {
        return getLongestNotarizedChainTips().stream()
                .anyMatch(tip -> Arrays.equals(proposedBlock.parentHash(), tip.hash()));
    }

    public List<Block> getLongestNotarizedChainTips() {
        int maxLength = hashToNode.values().stream()
                .filter(blockNode -> blockNode.notarized)
                .mapToInt(BlockNode::getLength)
                .max()
                .orElse(0);

        return hashToNode.values().stream()
                .filter(node -> node.getLength() == maxLength)
                .map(node -> node.block)
                .toList();
    }


    private void tryToFinalize(BlockNode b1) {
        BlockNode b2 = b1.parent;
        BlockNode b3 = (b2 != null) ? b2.parent : null;

        if (b2 != null && b3 != null &&
                b1.notarized && b2.notarized && b3.notarized &&
                b1.block.epoch() == b2.block.epoch() + 1 &&
                b2.block.epoch() == b3.block.epoch() + 1) {

            finalizeChain(b2);
        }
    }

    private void finalizeChain(BlockNode node) {
        while (node != null && !node.finalized) {
            node.finalized = true;
            node = node.parent;
        }
    }

    public void printLinearBlockchain() {
        System.out.println("\n=== Blockchain Tree ===");
        printSubtree(genesis, 0);
    }

    private void printSubtree(BlockNode node, int indent) {
        final String RESET = "\u001B[0m";
        final String GREEN = "\u001B[32m";
        final String YELLOW = "\u001B[33m";

        String prefix = " ".repeat(indent * 2);
        String color = node.finalized ? GREEN : (node.notarized ? YELLOW : RESET);
        System.out.printf("%s%sBlock[%d-%d]%s%n", prefix, color, node.block.epoch(), node.block.length(), RESET);

        for (BlockNode child : node.children) {
            printSubtree(child, indent + 1);
        }
    }
}
