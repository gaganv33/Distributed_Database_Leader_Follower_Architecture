package log;

import data.HybridLogicalClock;
import data.OperationType;
import data.operationDetails.DeleteOperationDetails;
import data.operationDetails.OperationDetails;
import data.operationDetails.UpdateOperationDetails;
import util.HashingHelper;

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

    public HashMap<HybridLogicalClock, OperationDetails> getLogsInRangeOfConsistentHashingPosition(
            int startingPositionInConsistentHashingRing, int endingPositionInConsistentHashingRing
    ) {
        HashMap<HybridLogicalClock, OperationDetails> logsInRange = new HashMap<>();
        for (var entry : log.entrySet()) {
            HybridLogicalClock hybridLogicalClock = entry.getKey();
            OperationDetails operationDetails = entry.getValue();
            String key = operationDetails.getKey();
            int positionInConsistentHashingRing = HashingHelper.hash(key);
            if (checkIfInRange(positionInConsistentHashingRing, startingPositionInConsistentHashingRing,
                    endingPositionInConsistentHashingRing)) {
                logsInRange.put(hybridLogicalClock, operationDetails);
            }
        }
        return logsInRange;
    }

    public HashMap<HybridLogicalClock, OperationDetails> getLogsInRangeOfConsistentHashingPosition(
            int startingPositonInConsistentHashingRing, int intermediateEndingPositonInConsistentHashingRing,
            int intermediateStartingPositionInConsistentHashingRing, int endingPositionInConsistentHashing
    ) {
        HashMap<HybridLogicalClock, OperationDetails> logsInRange = new HashMap<>();
        for (var entry : log.entrySet()) {
            HybridLogicalClock hybridLogicalClock = entry.getKey();
            OperationDetails operationDetails = entry.getValue();
            String key = operationDetails.getKey();
            int positionInConsistentHashingRing = HashingHelper.hash(key);
            if (checkIfInRange(positionInConsistentHashingRing, startingPositonInConsistentHashingRing,
                    intermediateEndingPositonInConsistentHashingRing) ||
            checkIfInRange(positionInConsistentHashingRing, intermediateStartingPositionInConsistentHashingRing,
                    endingPositionInConsistentHashing)) {
                logsInRange.put(hybridLogicalClock, operationDetails);
            }
        }
        return logsInRange;
    }

    public HashMap<HybridLogicalClock, OperationDetails> getLogs() {
        return new HashMap<>(log);
    }

    private boolean checkIfInRange(int positionInConsistentHashingRing, int startingPositionInConsistentHashingRing,
                                   int endingPositionInConsistentHashingRing) {
        return (positionInConsistentHashingRing >= startingPositionInConsistentHashingRing &&
                positionInConsistentHashingRing <= endingPositionInConsistentHashingRing);
    }
}
