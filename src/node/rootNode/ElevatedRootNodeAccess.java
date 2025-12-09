package node.rootNode;

import data.operationDetails.OperationDetails;
import node.databaseNode.ElevatedDatabaseNodeAccess;

import java.math.BigInteger;
import java.util.HashMap;

public interface ElevatedRootNodeAccess extends RootNodeAccess {
    void updateHeartBeat(ElevatedDatabaseNodeAccess databaseNode);
    void replicationOfDataWithFollowerDatabaseNodes(HashMap<BigInteger, OperationDetails> temporaryLogData);
    void replicationOfDataBetweenDatabaseNodes(ElevatedDatabaseNodeAccess databaseNode);
}
