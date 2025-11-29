package merkleTree.synchronization;

import merkleTree.node.LeafTreeNode;
import merkleTree.node.TreeNode;
import node.Node;

public class TreeSynchronization {
    /**
     * This method is responsible for updating the replica database node with the latest data, using the leader node
     * merkle tree and replica database node merkle tree.
     * @param leaderTreeNode : The leader tree node merkle tree.
     * @param replicaNode : The replica database node that has to be updated with the latest data.
     * @param replicaTreeNode : The replica database node merkle tree.
     */
    public static void treeSynchronization(TreeNode leaderTreeNode, Node replicaNode,
                                           TreeNode replicaTreeNode) {
        if (leaderTreeNode == null) {
            System.out.println("The leader node is empty, nothing to sync with replica node");
            return;
        }
        if (replicaTreeNode == null) {
            System.out.println("The replica node is empty, so syncing the leader node with replica node");
            copyLeaderDataToReplica(leaderTreeNode, replicaNode);
            return;
        }
        System.out.println("Syncing data of leader node with replica node");
        syncData(leaderTreeNode, replicaTreeNode, replicaNode);
    }

    /**
     * The method is responsible used to copy the complete data from the leader node to replica database node. Since,
     * the replica database node is empty (no records).
     * @param leaderTreeNode : The leader tree node merkle tree.
     * @param replicaNode : The replica database node that has to be updated with the latest data.
     */
    private static void copyLeaderDataToReplica(TreeNode leaderTreeNode, Node replicaNode) {
        if (leaderTreeNode.isLeaf) {
            LeafTreeNode leafNode = (LeafTreeNode) leaderTreeNode;
            writeToReplicaNode(replicaNode, leafNode.key, leafNode.value);
            return;
        }

        if (leaderTreeNode.right != null) {
            copyLeaderDataToReplica(leaderTreeNode.right, replicaNode);
        }
        if (leaderTreeNode.left != null) {
            copyLeaderDataToReplica(leaderTreeNode.left, replicaNode);
        }
    }

    /**
     * This method is responsible to update the replica database node with the latest data.
     * If the replica database node merkle tree has the same hash value as the leader merkle tree, then that subtree has
     * the updated data.
     * @param leaderTreeNode : The leader tree node merkle tree.
     * @param replicaTreeNode : The replica database node merkle tree.
     * @param replicaNode : The replica database node that has to be updated with the latest data.
     */
    private static void syncData(TreeNode leaderTreeNode, TreeNode replicaTreeNode, Node replicaNode) {
        if (leaderTreeNode.isLeaf && replicaTreeNode.isLeaf) {
            if (!leaderTreeNode.hash.equals(replicaTreeNode.hash)) {
                System.out.println("The leader node data is not matching with replica node, so syncing data");
                LeafTreeNode leafNode = (LeafTreeNode) leaderTreeNode;
                writeToReplicaNode(replicaNode, leafNode.key, leafNode.value);
            } else {
                System.out.println("The leaf node of leader node is matching with the leaf node of replica node");
            }
        } else if (replicaTreeNode.isLeaf) {
            if (leaderTreeNode.right != null) {
                syncData(leaderTreeNode.right, replicaTreeNode, replicaNode);
            }
            if (leaderTreeNode.left != null) {
                syncData(leaderTreeNode.left, replicaTreeNode, replicaNode);
            }
        } else {
            if (leaderTreeNode.hash.equals(replicaTreeNode.hash)) {
                System.out.println("The leader node subtree matches with the replica node subtree");
            } else {
                String leaderRightHash = leaderTreeNode.rightHash;
                String leaderLeftHash = leaderTreeNode.leftHash;
                String replicaRightHash = replicaTreeNode.rightHash;
                String replicaLeftHash = replicaTreeNode.leftHash;

                if (leaderRightHash.equals(replicaRightHash)) {
                    System.out.println("The leader node right subtree matches with the replica node right subtree");
                } else {
                    syncData(leaderTreeNode.right, replicaTreeNode.right, replicaNode);
                }

                if (leaderLeftHash.equals(replicaLeftHash)) {
                    System.out.println("The leader node left subtree matches with the replica node left subtree");
                } else {
                    syncData(leaderTreeNode.left, replicaTreeNode.left, replicaNode);
                }
            }
        }
    }

    /**
     * This method is responsible for writing the key value pair in the replica database node.
     * @param replicaNode : The replica database node that has to be updated with the latest data.
     * @param key : The key corresponding to the value.
     * @param value : The value corresponding to the key.
     */
    private static void writeToReplicaNode(Node replicaNode, String key, String value) {
        try {
            replicaNode.writeData(key, value);
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }
}
