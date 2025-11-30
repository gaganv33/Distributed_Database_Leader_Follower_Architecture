package data;

import config.OperationType;

public class UpsertOperation extends Operation {
    public final String value;

    public UpsertOperation(OperationType operationType, String hash, String value) {
        super(operationType, hash);
        this.value = value;
    }
}
