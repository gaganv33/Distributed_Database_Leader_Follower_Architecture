package node.rootNode;

import node.databaseNode.ElevatedDatabaseNodeAccess;

public interface ElevatedRootNodeAccess extends RootNodeAccess {
    void updateHeartBeat(ElevatedDatabaseNodeAccess databaseNode);
    void replicationOfDataBetweenDatabaseNodes(ElevatedDatabaseNodeAccess databaseNode);
}
