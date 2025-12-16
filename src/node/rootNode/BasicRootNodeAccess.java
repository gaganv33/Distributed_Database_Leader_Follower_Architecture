package node.rootNode;

import data.HybridLogicalClock;
import data.operationDetails.OperationDetails;
import exception.DataNotFoundException;
import exception.DatabaseNodeInActiveException;
import exception.NotLeaderException;
import exception.RootNodeDownException;

import java.util.HashMap;

public interface BasicRootNodeAccess extends RootNodeAccess {
    String get(String key) throws DatabaseNodeInActiveException, DataNotFoundException, RootNodeDownException;
    void write(HybridLogicalClock hybridLogicalClock, String key, String value) throws DatabaseNodeInActiveException,
            NotLeaderException, RootNodeDownException;
    void delete(HybridLogicalClock hybridLogicalClock, String key) throws DatabaseNodeInActiveException,
            NotLeaderException, RootNodeDownException;
    String getRootNodeName();
    HashMap<HybridLogicalClock, OperationDetails> getLogsInRangeOfConsistentHashingPosition(
            int startingPositionInConsistentHashingRing, int endingPositionInConsistentHashingRing
    ) throws DatabaseNodeInActiveException, NotLeaderException, RootNodeDownException;
    HashMap<HybridLogicalClock, OperationDetails> getLogs() throws DatabaseNodeInActiveException, NotLeaderException,
            RootNodeDownException;
    HashMap<HybridLogicalClock, OperationDetails> getLogsInRangeOfConsistentHashingPosition(
            int startingPositionInConsistentHashingRing, int intermediateEndingPositionInConsistentHashingRing,
            int intermediateStartingPositionInConsistentHashingRing, int endingPositionInConsistentHashingRing
    ) throws DatabaseNodeInActiveException, NotLeaderException, RootNodeDownException;
}
