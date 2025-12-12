package log;

import data.HybridLogicalClock;
import data.OperationType;
import data.operationDetails.DeleteOperationDetails;
import data.operationDetails.OperationDetails;
import data.operationDetails.UpdateOperationDetails;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.TreeMap;

public class WriteAheadLog {
    private final TreeMap<HybridLogicalClock, OperationDetails> log;

    public WriteAheadLog() {
        log = new TreeMap<>((firstHybridLogicalClock, secondHybridLogicalClock) -> {
            if (!firstHybridLogicalClock.getPhysicalClock().isEqual(secondHybridLogicalClock.getPhysicalClock())) {
                return firstHybridLogicalClock.getPhysicalClock().compareTo(secondHybridLogicalClock.getPhysicalClock());
            }
            return firstHybridLogicalClock.getLogicalClock().compareTo(secondHybridLogicalClock.getLogicalClock());
        });
    }

    public void addUpdateLog(HybridLogicalClock hybridLogicalClock, String key, String value) {
        log.put(hybridLogicalClock, new UpdateOperationDetails(OperationType.UPDATE, key, value));
    }

    public void addDeleteLog(HybridLogicalClock hybridLogicalClock, String key) {
        log.put(hybridLogicalClock, new DeleteOperationDetails(OperationType.DELETE, key));
    }

    public HybridLogicalClock getMaximumHybridLogicalClock() {
        if (log.isEmpty()) {
            return null;
        }
        return log.lastKey();
    }

    public HashMap<HybridLogicalClock, OperationDetails> getLogsAfterTheGivenTimestamp(HybridLogicalClock hybridLogicalClock) {
        return new HashMap<>(log.tailMap(hybridLogicalClock));
    }
}
