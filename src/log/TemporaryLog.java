package log;

import data.OperationType;
import data.operationDetails.DeleteOperationDetails;
import data.operationDetails.OperationDetails;
import data.operationDetails.UpdateOperationDetails;

import java.math.BigInteger;
import java.util.HashMap;

public class TemporaryLog {
    private final HashMap<BigInteger, OperationDetails> log;


    public TemporaryLog() {
        log = new HashMap<>();
    }

    public void addUpdateLog(BigInteger logicalTimestamp, String key, String value) {
        log.put(logicalTimestamp, new UpdateOperationDetails(OperationType.UPDATE, key, value));
    }

    public void addDeleteLog(BigInteger logicalTimestamp, String key) {
        log.put(logicalTimestamp, new DeleteOperationDetails(OperationType.DELETE, key));
    }

    public HashMap<BigInteger, OperationDetails> getTemporaryLog() {
        return log;
    }

    public void clearTemporaryLog() {
        log.clear();
    }
}
