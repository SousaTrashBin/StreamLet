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

    private final BlockNode notarizedBlockChain;
    private final Map<HashKey, BlockNode> hashToNode = new HashMap<>();

    public BlockchainManager() {
        notarizedBlockChain = new BlockNode(GENESIS_BLOCK, null);
        notarizedBlockChain.notarized = true;
        hashToNode.put(new HashKey(GENESIS_BLOCK.hash()), notarizedBlockChain);
    }

    public void notarizeBlock(Block block) {
        BlockNode parent = hashToNode.get(new HashKey(block.parentHash()));
        BlockNode node = hashToNode.computeIfAbsent(new HashKey(block.hash()), _ -> new BlockNode(block, parent));
        parent.addChildren(node);
        node.parent = parent;
        node.notarized = true;
        tryToFinalize(node);
    }

    public void addBlock(Block block) {
        HashKey blockKey = new HashKey(block.hash());
        BlockNode blockNode = new BlockNode(block, null);
        hashToNode.put(blockKey, blockNode);
    }

    public boolean extendNotarizedAnyChainTip(Block proposedBlock) {
        return getNotarizedTips().stream()
                .anyMatch(tip -> proposedBlock.length() > tip.length() &&
                        Arrays.equals(proposedBlock.parentHash(), tip.hash()));
    }

    public List<Block> getNotarizedTips() {
        return notarizedBlockChain.getNotarizedTips();
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

    private BlockNode buildWholeChain() {
        Map<HashKey, BlockNode> newNodes = new HashMap<>();
        for (BlockNode oldNode : hashToNode.values()) {
            BlockNode newNode = new BlockNode(oldNode.block, null);
            newNode.notarized = oldNode.notarized;
            newNode.finalized = oldNode.finalized;
            newNodes.put(new HashKey(newNode.block.hash()), newNode);
        }

        BlockNode root = newNodes.get(new HashKey(GENESIS_BLOCK.hash()));

        for (BlockNode node : newNodes.values()) {
            if (node.block != GENESIS_BLOCK) {
                BlockNode parent = newNodes.get(new HashKey(node.block.parentHash()));
                node.parent = parent;
                parent.addChildren(node);
            }
        }

        return root;
    }

    public void printLinearBlockchain() {
        System.out.println("\n=== Blockchain Tree ===");
        StringBuilder sb = new StringBuilder();
        printSubtree(buildWholeChain(), 0, sb);
        System.out.println(sb);
    }

    private void printSubtree(BlockNode node, int indent, StringBuilder sb) {
        final String RESET = "\u001B[0m";
        final String GREEN = "\u001B[32m";
        final String YELLOW = "\u001B[33m";

        String prefix = " ".repeat(indent * 2);
        String color = node.finalized ? GREEN : (node.notarized ? YELLOW : RESET);
        sb.append(String.format("%s%sBlock[%d-%d]%s%n", prefix, color, node.block.epoch(), node.block.length(), RESET));

        for (BlockNode child : node.children) {
            printSubtree(child, indent + 1, sb);
        }
    }
}
