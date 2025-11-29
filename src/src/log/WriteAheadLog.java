package log;

import config.NodeConfig;
import config.OperationType;
import data.Operation;
import data.UpsertOperation;
import merkleTree.construction.TreeConstruction;
import merkleTree.node.TreeNode;
import merkleTree.synchronization.TreeSynchronization;
import node.Node;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class WriteAheadLog {
    private final ConcurrentHashMap<String, Operation> data;
    private final ConcurrentMap<String, String> dataForRecovery;

    public WriteAheadLog() {
        this.data = new ConcurrentHashMap<>();
        this.dataForRecovery = new ConcurrentSkipListMap<>();
    }

    /**
     * This method is responsible to store the value for the corresponding key, in the temporary log and merkle tree.
     * @param key : The corresponding key to the value, that has to be stored in log file for updating the replica nodes
     *            and merkle tree, which is used to syncing nodes, which become active.
     * @param value : The corresponding value to the key.
     */
    public void addData(String key, String value) {
        Thread addDataThread = new Thread(() -> {
            data.put(key, new UpsertOperation(OperationType.UPSERT, value));
            dataForRecovery.put(key, value);
        });
        addDataThread.setDaemon(true);
        addDataThread.start();
    }

    /**
     * This method is responsible to delete the key from the temporary log and merkle tree.
     * @param key : The key which has to be removed from the temporary log and merkle tree.
     */
    public void removeData(String key) {
        Thread deleteDataThread = new Thread(() -> {
            data.put(key, new Operation(OperationType.DELETE));
            dataForRecovery.remove(key);
        });
        deleteDataThread.setDaemon(true);
        deleteDataThread.start();
    }

    /**
     * This method is responsible to replicate the recent data to all the database nodes, so that the nodes are synced
     * with the leader database node. The replication of data to all the database nodes are done asynchronously.
     * This implementation follows eventually consistent for all the database nodes.
     * @param replicaNodeImpl : All the database nodes in this shard. We replicate the data to all the database nodes.
     */
    public void notifyReplicas(List<Node> replicaNodeImpl) {
        Thread notifyReplicaThread = new Thread(() -> {
            for(var entry : data.entrySet()) {
                String key = entry.getKey();
                Operation operation = entry.getValue();
                OperationType operationType = operation.operationType;
                if (operationType == OperationType.UPSERT) {
                    data.UpsertOperation upsertOperation = (UpsertOperation) operation;
                    String value = upsertOperation.value;
                    for(var replicaNode : replicaNodeImpl) {
                        System.out.printf("WriteAheadLog: notifyReplica Upsert Operation: %s, key: %s, value: %s\n", replicaNode.getNodeName(), key, value);
                        try {
                            replicaNode.writeData(key, value);
                        } catch (Exception e) {
                            System.out.println("Exception: " + e.getMessage());
                        }
                    }
                } else {
                    for(var replicaNode : replicaNodeImpl) {
                        System.out.printf("WriteAheadLog: notifyReplica Delete Operation: %s, key: %s\n", replicaNode.getNodeName(), key);
                        try {
                            replicaNode.deleteData(key);
                        } catch (Exception e) {
                            System.out.println("Exception: " + e.getMessage());
                        }
                    }
                }
            }
            clearData();
        });
        notifyReplicaThread.setDaemon(true);
        notifyReplicaThread.start();
    }

    /**
     * This method is responsible for synchronizing the latest data, since it was inactive.
     * The synchronization is done asynchronously.
     * The database node will start serving the requests only when the synchronization is done.
     * @param nodeImpl : The database node which has just come active
     */
    public void updateANodeWhichHasJustComeActive(Node nodeImpl) {
        Thread updateANodeWhichHasJustComeActiveThread = new Thread(() -> {
            // Constructing the merkle tree only when the request for synchronization is made
            TreeNode leaderTreeNode = TreeConstruction.constructTreeNode(this.dataForRecovery);
            // Constructing the merkle tree for the data stored in the replica database node
            TreeNode replicaTreeNode = TreeConstruction.constructTreeNode(nodeImpl.getData());
            // Synchronizing the data between leader node and replica node, using the merkle tree
            TreeSynchronization.treeSynchronization(leaderTreeNode, nodeImpl, replicaTreeNode);
            // Once the database node is synchronized, we will enable the node to send heart beat requests to the root node
            nodeImpl.setDatabaseSynced();
        });
        updateANodeWhichHasJustComeActiveThread.setDaemon(true);
        updateANodeWhichHasJustComeActiveThread.start();
    }

    /**
     * This method is used to clear the hash map, which contains the latest data that has to be replicated to all the
     * database nodes.
     * This method is called once the data is replicated with all the database nodes.
     */
    private void clearData() {
        this.data.clear();
    }
}
