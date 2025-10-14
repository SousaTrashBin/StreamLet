package utils.application;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public final class Block implements Content {

    private final byte[] parentHash;
    private final Integer epoch;
    private final Integer length;
    private final Transaction[] transactions;
    private final byte[] blockHash;

    public Block(byte[] parentHash, Integer epoch, Integer length, Transaction[] transactions) {
        this.parentHash = parentHash;
        this.epoch = epoch;
        this.length = length;
        this.transactions = transactions.clone();
        this.blockHash = computeSHA1();
    }

    public byte[] hash() {
        return blockHash;
    }

    private byte[] computeSHA1() {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            sha1.update(parentHash);

            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(epoch);
            buffer.putInt(length);
            sha1.update(buffer.array());
            return sha1.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 not available", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Block other)) return false;
        return Arrays.equals(this.blockHash, other.blockHash);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(blockHash);
    }

    public byte[] parentHash() {
        return parentHash;
    }

    public Integer epoch() {
        return epoch;
    }

    public Integer length() {
        return length;
    }

    public Transaction[] transactions() {
        return transactions;
    }
}
