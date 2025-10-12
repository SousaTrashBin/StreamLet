package utils.application;

public record Block(byte[] hash, Integer epoch, Integer length, Transaction[] transactions) implements Content {
}
