package node.rootNode;

import data.HybridLogicalClock;
import data.operationDetails.OperationDetails;

import java.util.HashMap;

public interface RootNodeAccess {
    void replicationOfData(HashMap<HybridLogicalClock, OperationDetails> temporaryLogData);
}
