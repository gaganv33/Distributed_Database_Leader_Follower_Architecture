package data;

import config.OperationType;

public class UpsertOperation extends Operation {
    public final String value;

    public UpsertOperation(OperationType operationType, String value) {
        super(operationType);
        this.value = value;
    }
}
