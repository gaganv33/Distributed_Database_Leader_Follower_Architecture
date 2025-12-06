package data.operationDetails;

import data.OperationType;

public class OperationDetails {
    private final OperationType operationType;
    private final String key;

    public OperationDetails(OperationType operationType, String key) {
        this.operationType = operationType;
        this.key = key;
    }

    public OperationType getOperationType() {
        return this.operationType;
    }

    public String getKey() {
        return this.key;
    }
}
