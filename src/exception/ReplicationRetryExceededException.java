package exception;

public class ReplicationRetryExceededException extends RuntimeException {
    public ReplicationRetryExceededException(String message) {
        super(message);
    }
}
