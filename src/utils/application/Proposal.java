package utils.application;

import java.util.SequencedSet;

public record Proposal(SequencedSet<BlockWithStatus> parentChain, Block proposedBlock) implements Content {
    
}
