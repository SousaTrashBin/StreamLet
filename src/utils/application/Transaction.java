package utils.application;

import java.io.Serializable;

public record Transaction(Integer id, Double amount, Integer sender, Integer receiver) implements Serializable {
}
