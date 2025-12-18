package exception;

public class AllShardsUnavailableException extends RuntimeException {
    public AllShardsUnavailableException(String message) {
        super(message);
    }
}
