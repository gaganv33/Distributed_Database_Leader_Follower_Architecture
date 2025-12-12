package node.databaseNode;

import data.HybridLogicalClock;
import data.operationDetails.OperationDetails;
import exception.DatabaseNodeInActiveException;

import java.math.BigInteger;
import java.util.HashMap;

public interface ElevatedDatabaseNodeAccess extends LeaderDatabaseNodeAccess, FollowerDatabaseNodeAccess {
    void elevateToLeaderDatabaseNode();
    void replicateData(HashMap<HybridLogicalClock, OperationDetails> log) throws DatabaseNodeInActiveException;
    HybridLogicalClock getMaximumHybridLogicalClock();
    HashMap<HybridLogicalClock, OperationDetails> getLogsAfterTheGivenTimestamp(HybridLogicalClock hybridLogicalClock);
}
