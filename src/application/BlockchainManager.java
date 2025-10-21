package application;

import utils.application.Block;
import utils.application.BlockWithStatus;
import utils.application.Transaction;

import java.util.*;
import java.util.function.Function;

public class BlockchainManager {
    private static final int SHA1_LENGTH = 20;

    private static final Block GENESIS_BLOCK =
            new Block(new byte[SHA1_LENGTH], 0, 0, new Transaction[0]);

    private final List<SequencedSet<BlockWithStatus>> chains = new LinkedList<>();
    private final Map<Block, BlockWithStatus> blockToStatus = new HashMap<>();

    private int longestNotarizedChainLength;

    public BlockchainManager() {
        SequencedSet<BlockWithStatus> firstChain = new LinkedHashSet<>();
        firstChain.add(new BlockWithStatus(GENESIS_BLOCK));
        chains.add(firstChain);
    }

    public List<SequencedSet<BlockWithStatus>> longestNotarizedChains() {
        return chains.stream()
            .filter(chain -> chain.size() - 1 == longestNotarizedChainLength)
            .toList();
    }

    public boolean extendsFromLongestNotarizedChains(Block block) {
        return chains.stream()
            .anyMatch(chain -> chain.size() - 1 == longestNotarizedChainLength && block.length() > chain.size() - 1);
    }

    public void addPendingBlock(Block block, SequencedSet<BlockWithStatus> parentChain) {
        blockToStatus.put(block, new BlockWithStatus(block));

        boolean chainsContainAParentBlock = false;
        for (SequencedSet<BlockWithStatus> chain : chains)
            for (BlockWithStatus b : chain) {
                if (b.block().length() != 0 && parentChain.contains(b))
                    chainsContainAParentBlock = true;
            }

        if (chainsContainAParentBlock) return;
        
        chains.add(parentChain);
        int parentLength = parentChain.size() - 1;
        if (parentLength > longestNotarizedChainLength)
            longestNotarizedChainLength = parentLength;
    }

    public void notarizeBlock(Block block) {
        BlockWithStatus blockWithStatus = blockToStatus.get(block);
        if (blockWithStatus == null || blockWithStatus.isNotarized()) {
            System.out.println("I SHOULDNT BE HERE " + blockWithStatus);
            return;
        }
        blockWithStatus.notarize();
        SequencedSet<BlockWithStatus> longestChain = addBlockToLongestNotarizedChain(blockWithStatus);
        tryToFinalize(blockWithStatus, longestChain);
    }

    private SequencedSet<BlockWithStatus> addBlockToLongestNotarizedChain(BlockWithStatus block) {
        SequencedSet<BlockWithStatus> longestNotarizedChain = chains.stream()
            .max((chain1, chain2) -> chain1.size() - chain2.size()).get();
        longestNotarizedChain.add(block);
        longestNotarizedChainLength++;
        return longestNotarizedChain;
    }

    private void tryToFinalize(BlockWithStatus b1, SequencedSet<BlockWithStatus> chain) {
        chain.removeLast();
        BlockWithStatus b2 = chain.removeLast();
        if (chain.isEmpty()) {
            chain.add(b2);
            chain.add(b1);
            return;
        }
        BlockWithStatus b3 = chain.removeLast();
        chain.add(b3);
        chain.add(b2);
        chain.add(b1);

        if (b1.isNotarized() && b2.isNotarized() && b3.isNotarized() &&
            b1.block().epoch() == b2.block().epoch() + 1 &&
            b2.block().epoch() == b3.block().epoch() + 1
        ) {
            finalizeChain(chain);
        }
    }

    private void finalizeChain(SequencedSet<BlockWithStatus> chain) {
        BlockWithStatus temp = chain.removeLast();
        chain.forEach(BlockWithStatus::finalize);
        chain.add(temp);
    }

    public void printBlockchainTree() {
        final String RESET = "\u001B[0m";
        final String GREEN = "\u001B[32m";
        final String YELLOW = "\u001B[33m";
        Function<Integer, String> indent = depth -> " ".repeat(depth * 2);
        Function<BlockWithStatus, String> color = block -> {
            return block.isFinalized() ? GREEN : (block.isNotarized() ? YELLOW : RESET);
        };

        System.out.println("\n=== Blockchain Tree ===");
        StringBuilder output = new StringBuilder();
        chains.forEach(chain -> buildTreeString(chain, output, indent, color, RESET));
        System.out.println(output);
    }

    private void buildTreeString(
        SequencedSet<BlockWithStatus> chain,
        StringBuilder output,
        Function<Integer, String> indent,
        Function<BlockWithStatus, String> color,
        String RESET
    ) {
        int index = 0;
        for (BlockWithStatus block : chain) {
            if (block.equals(chain.getFirst()))
                output.append(String.format("%sGENESIS%s%n", color.apply(block), RESET));
            else
                output.append(String.format("%s%sBlock[%d-%d]%s%n",
                    indent.apply(index), color.apply(block), block.block().epoch(), block.block().length(), RESET));
            index++;
        }
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
