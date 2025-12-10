package node.databaseNode;

import data.operationDetails.OperationDetails;
import exception.DatabaseNodeInActiveException;

import java.math.BigInteger;
import java.util.HashMap;

public interface ElevatedDatabaseNodeAccess extends LeaderDatabaseNodeAccess, FollowerDatabaseNodeAccess {
    void elevateToLeaderDatabaseNode();
    void replicateData(HashMap<BigInteger, OperationDetails> log) throws DatabaseNodeInActiveException;
    BigInteger getMaximumLogicalTimestamp();
    HashMap<BigInteger, OperationDetails> getLogsAfterTheGivenTimestamp(BigInteger maximumLogicalTimestamp);
}
