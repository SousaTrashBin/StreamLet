package application;

import utils.application.Block;

import java.util.*;

class BlockNode {
    final Block block;
    BlockNode parent;
    final Set<BlockNode> children = new HashSet<>();
    boolean notarized = false;
    boolean finalized = false;

    BlockNode(Block block, BlockNode parent) {
        this.block = block;
        this.parent = parent;
    }

    public void addChildren(BlockNode blockNode) {
        children.add(blockNode);
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
        buildNotarizedTipsAux(tips);
        return tips;
    }

    private void buildNotarizedTipsAux(List<Block> tips) {
        if (!notarized) {
            return;
        }
        if (this.children.isEmpty()) {
            tips.add(block);
            return;
        }
        for (BlockNode blockNode : this.children) {
            blockNode.buildNotarizedTipsAux(tips);
        }
    }

}