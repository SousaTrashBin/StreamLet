package application;

import utils.application.Block;
import utils.application.Transaction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockchainManager {
    private static final int SHA1_LENGTH = 20;

    private static final Block GENESIS_BLOCK =
            new Block(new byte[SHA1_LENGTH], 0, 0, new Transaction[0]);

    private final BlockNode notarizedBlockChain;
    private final Map<Block, BlockNode> blockToNode = new HashMap<>();
    private final Map<HashKey, BlockNode> hashToNode = new HashMap<>();

    public BlockchainManager() {
        notarizedBlockChain = new BlockNode(GENESIS_BLOCK, null);
        notarizedBlockChain.notarized = true;
        blockToNode.put(GENESIS_BLOCK, notarizedBlockChain);
        hashToNode.put(new HashKey(GENESIS_BLOCK.getSHA1()), notarizedBlockChain);
    }

    public void notarizeBlock(Block block) {
        BlockNode node = blockToNode.get(block);
        if (node == null || node.notarized) {
            return;
        }
        BlockNode parent = hashToNode.get(new HashKey(block.parentHash()));
        if (parent == null) {
            return;
        }
        parent.addChild(node);
        node.parent = parent;
        node.notarized = true;
        tryToFinalize(node);
    }

    public void addPendingBlock(Block block) {
        BlockNode node = blockToNode.compute(block, (_, oldNode) -> {
            if (oldNode == null) return new BlockNode(block, null);
            oldNode.block = block;
            return oldNode;
        });

        hashToNode.put(new HashKey(block.getSHA1()), node);
    }

    public boolean extendNotarizedAnyChainTip(Block proposedBlock) {
        return getNotarizedTips().stream()
                .anyMatch(tip -> proposedBlock.length() > tip.length() &&
                        Arrays.equals(proposedBlock.parentHash(), tip.getSHA1()));
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

    private BlockNode buildFullChain() {
        Map<HashKey, BlockNode> newHashMap = new HashMap<>();
        Map<Block, BlockNode> newIdentityMap = new HashMap<>();

        for (BlockNode oldNode : blockToNode.values()) {
            BlockNode newNode = oldNode.copyWithoutParent();
            newHashMap.put(new HashKey(oldNode.block.getSHA1()), newNode);

            newIdentityMap.put(newNode.block, newNode);
        }

        for (BlockNode node : newIdentityMap.values()) {
            if (node.block == GENESIS_BLOCK) continue;
            BlockNode parent = newHashMap.get(new HashKey(node.block.parentHash()));
            if (parent == null) continue;
            node.parent = parent;
            parent.addChild(node);
        }

        return newIdentityMap.get(GENESIS_BLOCK);
    }

    public void printBlockchainTree() {
        BlockNode root = buildFullChain();
        root.printTree();
    }

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
}
