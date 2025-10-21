package utils.application;

public record BlockWithStatus(Block block) implements Content {

    private static boolean notarized = false;
    private static boolean finalized = false;

    public void notarize() {notarized = true;}

    public boolean isNotarized() {return notarized;}

    public void finalize() {finalized = true;}

    public boolean isFinalized() {return finalized;}
    
}
