package data;

import config.OperationType;

public class Operation {
    public final OperationType operationType;
    public final String hash;

    public Operation(OperationType operationType, String hash) {
        this.operationType = operationType;
        this.hash = hash;
    }
}
