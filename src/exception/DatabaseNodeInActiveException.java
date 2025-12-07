package exception;

public class DatabaseNodeInActiveException extends RuntimeException {
    public DatabaseNodeInActiveException(String message) {
        super(message);
    }
}
