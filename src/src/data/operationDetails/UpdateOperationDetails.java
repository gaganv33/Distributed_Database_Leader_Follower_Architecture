package data.operationDetails;

import data.OperationType;

public class UpdateOperationDetails extends OperationDetails {
    private final String value;

    public UpdateOperationDetails(OperationType operationType, String key, String value) {
        super(operationType, key);
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }
}
