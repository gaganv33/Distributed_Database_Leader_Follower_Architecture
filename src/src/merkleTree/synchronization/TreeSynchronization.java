package merkleTree.synchronization;

import config.Constants;
import data.Range;
import exception.InActiveNodeException;
import exception.RootNodeDownException;
import merkleTree.node.LeafTreeNode;
import merkleTree.node.TreeNode;
import node.MasterNode;
import node.Node;
import node.RootNode;
import util.Hash;
import util.Helper;

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

    public static void treeSynchronization(TreeNode nextRootNodeTree, TreeNode currentRootNodeTree,
                                           MasterNode currentRootNode, Range range) {
        if (nextRootNodeTree == null) {
            System.out.println("The next root node is empty, nothing to sync with the current root node.");
            return;
        }
        if (currentRootNodeTree == null) {
            System.out.println("The current root node is empty, so syncing the leader node with the current node, only in the range");
            while (true) {
                int copyToCurrentRootNodeResult = copyNextRootNodeToCurrentRootNode(nextRootNodeTree, currentRootNode, range);
                if (copyToCurrentRootNodeResult == Constants.SUCCESSFUL_WRITE) {
                    System.out.println("Successful write to the current root node");
                    break;
                } else if (copyToCurrentRootNodeResult == Constants.UNSUCCESSFUL_ROOT_NODE_DOWN) {
                    System.out.println("The current root node is down");
                    break;
                } else {
                    System.out.println("The current root node, the leader node is inactive, waiting for another node" +
                            " to be elected as a leader. Retrying after 3 seconds");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return;
        }
        System.out.println("Syncing data of next node with the current node");
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

    public static int copyNextRootNodeToCurrentRootNode(TreeNode nextRootNodeTree, MasterNode currentRootNode,
                                                         Range range) {
        if (nextRootNodeTree.isLeaf) {
            LeafTreeNode leafTreeNode = (LeafTreeNode) nextRootNodeTree;
            String input = Helper.combineKeyAndValue(leafTreeNode.key, leafTreeNode.value);
            int positionInRing = Hash.getPositionInRing(input);
            if (positionInRing >= range.start && positionInRing <= range.end) {
                return writeToRootNode(leafTreeNode.key, leafTreeNode.value, currentRootNode);
            }
            return Constants.SUCCESSFUL_WRITE;
        }

        if (nextRootNodeTree.right != null) {
            int writeResult = copyNextRootNodeToCurrentRootNode(nextRootNodeTree.right, currentRootNode, range);
            if (writeResult != Constants.SUCCESSFUL_WRITE) return writeResult;
        }
        if (nextRootNodeTree.left != null) {
            int writeResult = copyNextRootNodeToCurrentRootNode(nextRootNodeTree.left, currentRootNode, range);
            if (writeResult != Constants.SUCCESSFUL_WRITE) return writeResult;
        }
        return Constants.SUCCESSFUL_WRITE;
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

    private static int writeToRootNode(String key, String value, MasterNode currentRootNode) {
        try {
            currentRootNode.writeData(key, value);
            return Constants.SUCCESSFUL_WRITE;
        } catch (RootNodeDownException e) {
            System.out.println("Writing to root node failed, while copying data from next root node to current root node." +
                    "Due to RootNodeDownException.");
            return Constants.UNSUCCESSFUL_ROOT_NODE_DOWN;
        } catch (InActiveNodeException e) {
            System.out.println("Writing to root node failed, while copying data from next root node to current root node." +
                    "Due to InActiveNodeException.");
            return Constants.UNSUCCESSFUL_IN_ACTIVE_NODE;
        }
    }
}
