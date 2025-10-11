package utils;

public record Block(byte[] hash, Integer epoch, Integer length, Transaction[] transactions) implements Content {
}
