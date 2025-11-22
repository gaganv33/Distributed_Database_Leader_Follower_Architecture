package node;

import config.NodeType;
import exception.DataNotFoundException;
import exception.InActiveNodeException;

public interface Node extends Runnable {
    void escalateNodeFromReplicaToLeader();
    void deescalateNodeFromLeaderToReplica() throws InterruptedException;
    String getData(String key) throws DataNotFoundException, InActiveNodeException;
    void writeData(String key, String value) throws InActiveNodeException;
    NodeType getNodeType();
    String getNodeName();
}
