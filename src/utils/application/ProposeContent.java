package utils.application;

import java.util.LinkedList;

public record ProposeContent(Block block, LinkedList<Block> parentChain) implements Content {
}
