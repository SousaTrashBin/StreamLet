package application;

import utils.application.Block;

import java.util.*;

class BlockNode {
    Block block;
    BlockNode parent;
    final Set<BlockNode> children = new HashSet<>();
    boolean notarized = false;
    boolean finalized = false;

    BlockNode(Block block, BlockNode parent) {
        this.block = block;
        this.parent = parent;
    }

    public void addChild(BlockNode child) {
        children.add(child);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BlockNode blockNode)) return false;
        return Objects.equals(block, blockNode.block);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(block);
    }

    public List<Block> getNotarizedTips() {
        List<Block> tips = new ArrayList<>();
        getNotarizedTipsAux(tips);
        return tips;
    }

    private void getNotarizedTipsAux(List<Block> tips) {
        if (!notarized) return;
        if (children.isEmpty()) {
            tips.add(block);
            return;
        }
        for (BlockNode child : children) {
            child.getNotarizedTipsAux(tips);
        }
    }

    public void printTree() {
        System.out.println("\n=== Blockchain Tree ===");
        StringBuilder output = new StringBuilder();
        buildTreeString(0, output);
        System.out.println(output);
    }

    private void buildTreeString(int depth, StringBuilder output) {
        final String RESET = "\u001B[0m";
        final String GREEN = "\u001B[32m";
        final String YELLOW = "\u001B[33m";
        String indent = " ".repeat(depth * 2);
        String color = finalized ? GREEN : (notarized ? YELLOW : RESET);

        output.append(String.format("%s%sBlock[%d-%d]%s%n",
                indent, color, block.epoch(), block.length(), RESET));

        children.stream()
                .sorted(Comparator.comparingInt(child -> child.block.epoch()))
                .forEach(child -> child.buildTreeString(depth + 1, output));
    }

    public BlockNode copyWithoutParent() {
        BlockNode copy = new BlockNode(this.block, null);
        copy.notarized = this.notarized;
        copy.finalized = this.finalized;
        return copy;
    }
}
