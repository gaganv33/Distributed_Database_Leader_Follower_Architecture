package node;

import config.NodeType;
import data.Operation;
import exception.DataNotFoundException;
import exception.InActiveNodeException;

import java.util.concurrent.ConcurrentMap;

public interface Node extends Runnable {
    String getData(String key) throws DataNotFoundException, InActiveNodeException;
    void writeData(String key, String value) throws InActiveNodeException;
    NodeType getNodeType();
    String getNodeName();
    void deleteData(String key) throws InActiveNodeException;
    ConcurrentMap<String, String> getData();
    void setDatabaseSynced();
}
