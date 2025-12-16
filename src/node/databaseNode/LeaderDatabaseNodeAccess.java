package node.databaseNode;

import data.HybridLogicalClock;
import data.operationDetails.OperationDetails;
import exception.DatabaseNodeInActiveException;
import exception.NotLeaderException;

import java.util.HashMap;

public interface LeaderDatabaseNodeAccess extends DatabaseNodeAccess {
    void write(HybridLogicalClock hybridLogicalClock, String key, String value) throws DatabaseNodeInActiveException, NotLeaderException;
    void delete(HybridLogicalClock hybridLogicalClock, String key) throws DatabaseNodeInActiveException, NotLeaderException;
    HashMap<HybridLogicalClock, OperationDetails> getLogsInRangeOfConsistentHashingPosition(
            int startingPositionInConsistentHashingRing, int endPositionInConsistentHashingRing) throws
            DatabaseNodeInActiveException, NotLeaderException;
    HashMap<HybridLogicalClock, OperationDetails> getLogs() throws DatabaseNodeInActiveException, NotLeaderException;
    HashMap<HybridLogicalClock, OperationDetails> getLogsInRangeOfConsistentHashingPosition(
            int startingPositionInConsistentHashingRing, int intermediateEndingPositionInConsistentHashingRing,
            int intermediateStartingPositonInConsistentHashingRing, int endingPositionInConsistentHashingRing
    ) throws DatabaseNodeInActiveException, NotLeaderException;
}
