package application;

import utils.application.Block;
import utils.application.Transaction;

import java.nio.ByteBuffer;
import java.util.*;

public class BlockchainManager {
    private static final int SHA1_LENGTH = 20;
    private static final Block GENESIS_BLOCK = new Block(new byte[SHA1_LENGTH], 0, 0, new Transaction[0]);
    private final List<Block> blockChain = new ArrayList<>();
    private final Map<ByteBuffer, Boolean> blockNotarizationStatus = new HashMap<>();
    private final Set<Block> finalizedBlocks = new HashSet<>();

    public BlockchainManager() {
        blockChain.add(GENESIS_BLOCK);
        ByteBuffer hashBuffer = ByteBuffer.wrap(GENESIS_BLOCK.hash());
        blockNotarizationStatus.put(hashBuffer, false);
    }

    public void addBlock(Block newBlock) {
        blockChain.add(newBlock);
        ByteBuffer hashBuffer = ByteBuffer.wrap(newBlock.hash());
        blockNotarizationStatus.put(hashBuffer, false);
    }

    public void notarizeBlock(Block block) {
        ByteBuffer hashBuffer = ByteBuffer.wrap(block.hash());
        blockNotarizationStatus.put(hashBuffer, true);

        finalizeConsecutiveBlocks(block);
    }

    public boolean isNotarized(Block block) {
        return blockNotarizationStatus.getOrDefault(ByteBuffer.wrap(block.hash()), false);
    }

    public Block getLongestNotarizedChainTip() {
        for (int i = blockChain.size() - 1; i >= 0; i--) {
            if (isNotarized(blockChain.get(i))) return blockChain.get(i);
        }
        return GENESIS_BLOCK;
    }

    private void finalizeConsecutiveBlocks(Block latestNotarized) {
        int index = blockChain.indexOf(latestNotarized);
        if (index < 2) return;

        if (blockChain.subList(index - 2, index + 1).stream().allMatch(this::isNotarized)) {
            finalizedBlocks.add(blockChain.get(index - 1));
        }
    }

    public int getChainSize() {
        return blockChain.size();
    }
}
