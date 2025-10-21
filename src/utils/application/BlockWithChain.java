package utils.application;

import java.util.LinkedList;

public record BlockWithChain(Block block, LinkedList<Block> chain) implements Content {
}
