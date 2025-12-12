package node.rootNode;

import data.HybridLogicalClock;
import data.operationDetails.OperationDetails;
import node.databaseNode.ElevatedDatabaseNodeAccess;

import java.util.HashMap;

public interface ElevatedRootNodeAccess extends RootNodeAccess {
    void updateHeartBeat(ElevatedDatabaseNodeAccess databaseNode);
    void replicationOfDataWithFollowerDatabaseNodes(HashMap<HybridLogicalClock, OperationDetails> temporaryLogData);
    void replicationOfDataBetweenDatabaseNodes(ElevatedDatabaseNodeAccess databaseNode);
}
