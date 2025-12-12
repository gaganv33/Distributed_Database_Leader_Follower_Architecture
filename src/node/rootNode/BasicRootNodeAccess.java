package node.rootNode;

import data.HybridLogicalClock;
import exception.DataNotFoundException;
import exception.DatabaseNodeInActiveException;
import exception.NotLeaderException;
import exception.RootNodeDownException;

public interface BasicRootNodeAccess extends RootNodeAccess {
    String get(String key) throws DatabaseNodeInActiveException, DataNotFoundException, RootNodeDownException;
    void write(HybridLogicalClock hybridLogicalClock, String key, String value) throws DatabaseNodeInActiveException,
            NotLeaderException, RootNodeDownException;
    void delete(HybridLogicalClock hybridLogicalClock, String key) throws DatabaseNodeInActiveException,
            NotLeaderException, RootNodeDownException;
}
