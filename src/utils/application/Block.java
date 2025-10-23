package utils.application;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public record Block(byte[] parentHash, Integer epoch, Integer length, Transaction[] transactions) implements Content {

    public Block(byte[] parentHash, Integer epoch, Integer length, Transaction[] transactions) {
        this.parentHash = parentHash;
        this.epoch = epoch;
        this.length = length;
        this.transactions = transactions.clone();
    }

    public byte[] getSHA1() {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");

            sha1.update(parentHash);

            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(epoch);
            buffer.putInt(length);
            sha1.update(buffer.array());

            for (Transaction transaction : transactions) {
                ByteBuffer transactionBuffer = ByteBuffer.allocate(24);
                transactionBuffer.putLong(transaction.id());
                transactionBuffer.putDouble(transaction.amount());
                transactionBuffer.putInt(transaction.sender());
                transactionBuffer.putInt(transaction.receiver());
                sha1.update(transactionBuffer.array());
            }

            return sha1.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 not available", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Block block)) return false;

        return epoch.equals(block.epoch) && length.equals(block.length) && Arrays.equals(parentHash, block.parentHash);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(parentHash);
        result = 31 * result + epoch.hashCode();
        result = 31 * result + length.hashCode();
        return result;
    }
}
