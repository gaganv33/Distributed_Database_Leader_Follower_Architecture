package merkleTree.synchronization;

import data.Range;
import merkleTree.construction.TreeConstruction;
import merkleTree.node.TreeNode;
import node.MasterNode;

import java.util.concurrent.ConcurrentMap;

public class SynchronizationService {
    public static void synchronizeRootNode(ConcurrentMap<String, String> nextRootNodeData,
                                           ConcurrentMap<String, String> currentRootNodeData, MasterNode currentRootNode,
                                           Range range) {
        // Constructing a merkle tree based on the next root node data
        TreeNode nextRootNodeTree = TreeConstruction.constructTreeNode(nextRootNodeData);
        // Constructing a merkle tree based on the current root node data
        TreeNode currentRootNodeTree = TreeConstruction.constructTreeNode(currentRootNodeData);

        // Synchronizing both the merkle trees
        TreeSynchronization.treeSynchronization(nextRootNodeTree, currentRootNodeTree, currentRootNode, range);
    }
}
