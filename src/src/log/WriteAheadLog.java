package log;

import config.OperationType;
import data.Operation;
import data.UpsertOperation;
import hash.Hash;
import merkleTree.construction.TreeConstruction;
import merkleTree.node.TreeNode;
import merkleTree.synchronization.TreeSynchronization;
import node.Node;
import util.Hashing;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

public class WriteAheadLog {
    private final ConcurrentHashMap<String, Operation> data;
    private final ConcurrentMap<String, String> dataForRecovery;
    private final BlockingQueue<Runnable> walQueue;
    private final Object lock = new Object();

    public WriteAheadLog() {
        this.data = new ConcurrentHashMap<>();
        this.dataForRecovery = new ConcurrentSkipListMap<>();
        this.walQueue = new LinkedBlockingDeque<>();

        // Starting a WriteAheadLog Thread, so that it can execute the operations such as writing records, deleting records,
        // and clearing the records after the replication is done in the replica nodes.
        Thread WriteAheadLogWorker = new Thread(() -> {
            System.out.println("Starting a worker thread in WriteAheadLog");
            while (true) {
                try {
                    Runnable task = walQueue.take();
                    task.run();
                } catch (Exception e) {
                    System.out.println("[WriteAheadLogWorker]: Exception " + e.getMessage());
                }
            }
        });
        WriteAheadLogWorker.start();
    }

    /**
     * This method is responsible to store the value for the corresponding key, in the temporary log and merkle tree.
     * @param key : The corresponding key to the value, that has to be stored in log file for updating the replica nodes
     *            and merkle tree, which is used to syncing nodes, which become active.
     * @param value : The corresponding value to the key.
     */
    public void addData(String key, String value) {
        walQueue.add(() -> {
            synchronized (lock) {
                String hash = Hash.getHashForString(String.format("%s - %s", OperationType.UPSERT.toString(), value));
                data.put(key, new UpsertOperation(OperationType.UPSERT, hash, value));
                dataForRecovery.put(key, value);
            }
        });
    }

    /**
     * This method is responsible to delete the key from the temporary log and merkle tree.
     * @param key : The key which has to be removed from the temporary log and merkle tree.
     */
    public void removeData(String key) {
        walQueue.add(() -> {
            synchronized (lock) {
                String hash = Hash.getHashForString(String.format("%s", OperationType.DELETE.toString()));
                data.put(key, new Operation(OperationType.DELETE, hash));
                dataForRecovery.remove(key);
            }
        });
    }

    /**
     * This method is responsible to replicate the recent data to all the database nodes, so that the nodes are synced
     * with the leader database node. The replication of data to all the database nodes are done asynchronously.
     * This implementation follows eventually consistent for all the database nodes.
     * @param replicaNodeImpl : All the database nodes in this shard. We replicate the data to all the database nodes.
     */
    public void notifyReplicas(List<Node> replicaNodeImpl) {
        Thread notifyReplicaThread = new Thread(() -> {
            // Maintaining a snapshot to prevent clearing the complete write ahead log. After the replication to all the
            // database nodes.
            ConcurrentHashMap<String, Operation> dataSnapshot;
            synchronized (lock) {
                dataSnapshot = new ConcurrentHashMap<>(data);
            }
            boolean isSuccess = true;
            for(var entry : dataSnapshot.entrySet()) {
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
                            isSuccess = false;
                            System.out.println("Exception: " + e.getMessage());
                        }
                    }
                } else {
                    for(var replicaNode : replicaNodeImpl) {
                        System.out.printf("WriteAheadLog: notifyReplica Delete Operation: %s, key: %s\n", replicaNode.getNodeName(), key);
                        try {
                            replicaNode.deleteData(key);
                        } catch (Exception e) {
                            isSuccess = false;
                            System.out.println("Exception: " + e.getMessage());
                        }
                    }
                }
            }
            if (!isSuccess) {
                System.out.println("Replication to all the database node did not succeed. Thus not clearing the log");
                return;
            }
            clearData(dataSnapshot);
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
     * Remove only the records that have been replicated to all the database nodes. So if there is a new record in the
     * log, then are not removing them. Maintaining a snapshot of the data before replicating it to all the nodes.
     * @param dataSnapshot : The records that have to be removed from the log file.
     */
    private void clearData(ConcurrentHashMap<String, Operation> dataSnapshot) {
        walQueue.add(() -> {
            synchronized (lock) {
                for (var entry : dataSnapshot.entrySet()) {
                    String key = entry.getKey();
                    String snapshotHash = entry.getValue().hash;
                    String dataHash = data.get(key).hash;

                    // We can check if the data value is same as the snapshot  value by comparing the hash value.
                    if (dataHash.equals(snapshotHash)) {
                        data.remove(key);
                    }
                }
            }
        });
    }
}
