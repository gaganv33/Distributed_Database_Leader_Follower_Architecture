package exception;

public class ShardWriteFailedException extends RuntimeException {
    public ShardWriteFailedException(String message) {
        super(message);
    }
}
