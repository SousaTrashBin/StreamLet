package application;

import utils.application.Block;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

class BlockNode {
    final Block block;
    final BlockNode parent;
    final Set<BlockNode> children = new HashSet<>();
    boolean notarized = false;
    boolean finalized = false;

    BlockNode(Block block, BlockNode parent) {
        this.block = block;
        this.parent = parent;
    }

    int getLength() {
        return block.length();
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
}