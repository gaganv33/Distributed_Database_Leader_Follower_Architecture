package log;

import data.OperationType;
import data.operationDetails.DeleteOperationDetails;
import data.operationDetails.OperationDetails;
import data.operationDetails.UpdateOperationDetails;

import java.math.BigInteger;
import java.util.TreeMap;

public class WriteAheadLog {
    private final TreeMap<BigInteger, OperationDetails> log;

    public WriteAheadLog() {
        log = new TreeMap<>();
    }

    public void addUpdateLog(BigInteger logicalTimestamp, String key, String value) {
        log.put(logicalTimestamp, new UpdateOperationDetails(OperationType.UPDATE, key, value));
    }

    public void addDeleteLog(BigInteger logicalTimestamp, String key) {
        log.put(logicalTimestamp, new DeleteOperationDetails(OperationType.DELETE, key));
    }

    public BigInteger maximumLogicalTimeStamp() {
        if (log.isEmpty()) {
            return new BigInteger(String.valueOf(-1));
        }
        return log.lastKey();
    }
}
