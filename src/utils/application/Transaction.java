package utils.application;

import java.io.Serializable;

public record Transaction(Long id, Double amount, Integer sender, Integer receiver) implements Serializable {
    public String toStringSummary() {
        return String.format("id=%d, %d→%d: %.2f", id, sender, receiver, amount);
    }
}
