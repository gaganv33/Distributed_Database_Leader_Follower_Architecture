package log;

import data.HybridLogicalClock;
import data.OperationType;
import data.operationDetails.DeleteOperationDetails;
import data.operationDetails.OperationDetails;
import data.operationDetails.UpdateOperationDetails;

import java.util.HashMap;

public class TemporaryLog {
    private final HashMap<HybridLogicalClock, OperationDetails> log;


    public TemporaryLog() {
        log = new HashMap<>();
    }

    public void addUpdateLog(HybridLogicalClock hybridLogicalClock, String key, String value) {
        log.put(hybridLogicalClock, new UpdateOperationDetails(OperationType.UPDATE, key, value));
    }

    public void addDeleteLog(HybridLogicalClock hybridLogicalClock, String key) {
        log.put(hybridLogicalClock, new DeleteOperationDetails(OperationType.DELETE, key));
    }

    public HashMap<HybridLogicalClock, OperationDetails> getTemporaryLog() {
        return log;
    }

    public void clearTemporaryLog() {
        log.clear();
    }
}
