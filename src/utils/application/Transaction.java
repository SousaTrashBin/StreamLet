package utils.application;

import java.io.Serializable;

public record Transaction(Long id, Double amount, Integer sender, Integer receiver) implements Serializable {
}
