package node.impl;

import config.DatabaseNodeConfig;
import data.DatabaseNodeType;
import data.OperationType;
import data.Value;
import data.operationDetails.OperationDetails;
import data.operationDetails.UpdateOperationDetails;
import exception.DataNotFoundException;
import exception.DatabaseNodeInActiveException;
import exception.NotLeaderException;
import log.TemporaryLog;
import log.WriteAheadLog;
import node.databaseNode.ElevatedDatabaseNodeAccess;
import node.rootNode.ElevatedRootNodeAccess;
import util.RandomHelper;

import java.math.BigInteger;
import java.util.HashMap;

public class DatabaseNode implements ElevatedDatabaseNodeAccess {
    private final String databaseNodeName;
    private final HashMap<String, Value> data;
    private final WriteAheadLog writeAheadLog;
    private DatabaseNodeType databaseNodeType;
    private TemporaryLog temporaryLog;
    private final Object lock = new Object();
    private boolean isActive;
    private Thread updatingFollowerDatabaseNodesThread;
    private final ElevatedRootNodeAccess rootNode;
    private Thread updatingHeartBeatThread;
    private Thread replicationOFDataUsingNeighbourDatabaseNodes;

    public DatabaseNode(int rootNodeId, int databaseNodeId, DatabaseNodeType databaseNodeType,
                        ElevatedRootNodeAccess rootNode) {
        this.databaseNodeName = String.format("Database Node-%d.%d", rootNodeId, databaseNodeId);
        this.isActive = true;
        this.data = new HashMap<>();
        this.writeAheadLog = new WriteAheadLog();
        this.databaseNodeType = databaseNodeType;
        this.rootNode = rootNode;

        updatingFollowerDatabaseNodesThread = new Thread(() -> {
            System.out.printf("[%s]: Starting a thread to update the follower database nodes with the latest data\n",
                    this.databaseNodeName);
            while (true) {
                try {
                    Thread.sleep(DatabaseNodeConfig.cooldownTimeForUpdatingDataInFollowerDatabaseNodes);
                } catch (InterruptedException e) {
                    System.out.printf("[%s]: Updating the follower database node thread is stopped\n", this.databaseNodeName);
                    Thread.currentThread().interrupt();
                    return;
                }
                HashMap<BigInteger, OperationDetails> temporaryLogData = new HashMap<>(temporaryLog.getTemporaryLog());
                temporaryLog.clearTemporaryLog();
                this.rootNode.replicationOfDataWithFollowerDatabaseNodes(temporaryLogData);
            }
        });

        updatingHeartBeatThread = new Thread(() -> {
            System.out.printf("[%s]: Starting a thread to send heart beat requests to the root node\n", this.databaseNodeName);
            while (true) {
                try {
                    Thread.sleep(DatabaseNodeConfig.cooldownTimeForUpdatingHeartBeat);
                } catch (InterruptedException e) {
                    System.out.printf("[%s]: Updating the heart beat thread is stopped\n", this.databaseNodeName);
                    Thread.currentThread().interrupt();
                    return;
                }
                rootNode.updateHeartBeat(this);
            }
        });

        replicationOFDataUsingNeighbourDatabaseNodes = new Thread(() -> {
            System.out.printf("[%s]: Starting a thread for periodically requesting the root node for replication of data " +
                    "using the neighbour database nodes\n", this.databaseNodeName);
            while (true) {
                try {
                    Thread.sleep(DatabaseNodeConfig.cooldownTimeForReplicationOfDataUsingNeighbourDatabaseNodes);
                } catch (InterruptedException e) {
                    System.out.printf("[%s]: Replication of data using the neighbour database nodes is stopped\n",
                            this.databaseNodeName);
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    System.out.printf("[%s]: Starting replication of data using the neighbour database nodes\n",
                            this.databaseNodeName);
                    rootNode.replicationOfDataBetweenDatabaseNodes(this);
                } catch (Exception e) {
                    System.out.printf("[%s]: Exception thrown in replicaOFDataUsingNeighbourDatabaseNodes thread\n",
                            this.databaseNodeName);
                }
            }
        });

        if (databaseNodeType == DatabaseNodeType.LEADER) {
            this.temporaryLog = new TemporaryLog();
            // start the updating follower database nodes, only if this database node is a LEADER
            updatingFollowerDatabaseNodesThread.start();
        }
        // Starting the thread to send heart beat to the root node frequently
        updatingHeartBeatThread.start();
        // Starting the thread to periodically send request to the root node, for getting the most recent data from the
        // neighbour database nodes
        replicationOFDataUsingNeighbourDatabaseNodes.start();
    }

    @Override
    public void write(BigInteger logicalTimestamp, String key, String value) throws DatabaseNodeInActiveException, NotLeaderException {
        synchronized (lock) {
            if (!isActive) {
                throw new DatabaseNodeInActiveException(String.format("[%s]: The database node is inactive.", this.databaseNodeName));
            }
            if (databaseNodeType != DatabaseNodeType.LEADER) {
                throw new NotLeaderException(String.format("[%s]: The database node is not a leader node.", this.databaseNodeName));
            }
            if (data.containsKey(key)) {
                Value valueObject = data.get(key);
                if (logicalTimestamp.compareTo(valueObject.getLogicalClock()) > 0) {
                    updateCommit(logicalTimestamp, key, value);
                }
            } else {
                updateCommit(logicalTimestamp, key, value);
            }
        }
    }

    @Override
    public void delete(BigInteger logicalTimestamp, String key) throws DatabaseNodeInActiveException, NotLeaderException {
        synchronized (lock) {
            if (!isActive) {
                throw new DatabaseNodeInActiveException(String.format("[%s]: The database node is inactive.", this.databaseNodeName));
            }
            if (databaseNodeType != DatabaseNodeType.LEADER) {
                throw new NotLeaderException(String.format("[%s]: The database node is not a leader node.", this.databaseNodeName));
            }
            if (data.containsKey(key)) {
                Value valueObject = data.get(key);
                if (logicalTimestamp.compareTo(valueObject.getLogicalClock()) > 0) {
                    deleteCommit(logicalTimestamp, key);
                }
            }
        }
    }

    @Override
    public String get(String key) throws DatabaseNodeInActiveException, DataNotFoundException {
        synchronized (lock) {
            if (!isActive) {
                throw new DatabaseNodeInActiveException(String.format("[%s]: The database node is inactive.", this.databaseNodeName));
            }
            if (!data.containsKey(key)) {
                throw new DataNotFoundException(String.format("[%s]: Data not found in database node.", this.databaseNodeName));
            }
            return data.get(key).getValue();
        }
    }

    @Override
    public void elevateToLeaderDatabaseNode() {
        synchronized (lock) {
            this.databaseNodeType = DatabaseNodeType.LEADER;
            this.temporaryLog = new TemporaryLog();
            this.updatingFollowerDatabaseNodesThread = new Thread(this.updatingFollowerDatabaseNodesThread);
            this.updatingFollowerDatabaseNodesThread.start();
        }
    }

    @Override
    public boolean getIsActive() {
        return this.isActive;
    }

    @Override
    public String getDatabaseNodeName() {
        return this.databaseNodeName;
    }

    @Override
    public int getDataSize() {
        return data.size();
    }

    @Override
    public void replicateData(HashMap<BigInteger, OperationDetails> log) throws DatabaseNodeInActiveException {
        System.out.printf("[%s]: Starting the replication of data\n", this.databaseNodeName);
        for (var entry : log.entrySet()) {
            BigInteger logicalTimestamp = entry.getKey();
            OperationDetails operationDetails = entry.getValue();
            OperationType operationType = operationDetails.getOperationType();
            String key = operationDetails.getKey();

            if (operationType == OperationType.UPDATE) {
                String value = ((UpdateOperationDetails) operationDetails).getValue();
                try {
                    this.replicaWrite(logicalTimestamp, key, value);
                } catch (DatabaseNodeInActiveException e) {
                    throw new DatabaseNodeInActiveException(String.format("[%s]: The database node is not active, " +
                                    "so replication of data is unsuccessful\n", this.databaseNodeName));
                }
            } else {
                try {
                    replicaDelete(logicalTimestamp, key);
                } catch (DatabaseNodeInActiveException e) {
                    throw new DatabaseNodeInActiveException(String.format("[%s]: The database node is not active, " +
                            "so replication of data is unsuccessful\n", this.databaseNodeName));
                }
            }
        }
    }

    @Override
    public BigInteger getMaximumLogicalTimestamp() {
        return writeAheadLog.getMaximumLogicalTimestamp();
    }

    @Override
    public HashMap<BigInteger, OperationDetails> getLogsAfterTheGivenTimestamp(BigInteger maximumLogicalTimestamp) {
        synchronized (lock) {
            return writeAheadLog.getLogsAfterTheGivenTimestamp(maximumLogicalTimestamp);
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(10000L * RandomHelper.getRandomIntegerInRange(5, 11));
                if (isActive) {
                    cleaningUpBeforeScalingDown();
                } else {
                    startingUpAfterScalingUp();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(String.format("[%s]: Interrupted exception from scaling up and down thread.",
                        this.databaseNodeName));
            }
        }
    }

    private void replicaWrite(BigInteger logicalTimestamp, String key, String value) throws DatabaseNodeInActiveException {
        synchronized (lock) {
            if (!isActive) {
                throw new DatabaseNodeInActiveException(String.format("[%s]: The database node is inactive.", this.databaseNodeName));
            }
            if (data.containsKey(key)) {
                Value valueObject = data.get(key);
                if (logicalTimestamp.compareTo(valueObject.getLogicalClock()) > 0) {
                    updateReplicaCommit(logicalTimestamp, key, value);
                }
            } else {
                updateReplicaCommit(logicalTimestamp, key, value);
            }
        }
    }

    private void replicaDelete(BigInteger logicalTimestamp, String key) throws DatabaseNodeInActiveException {
        synchronized (lock) {
            if (!isActive) {
                throw new DatabaseNodeInActiveException(String.format("[%s]: The database node is inactive.", this.databaseNodeName));
            }
            if (data.containsKey(key)) {
                Value valueObject = data.get(key);
                if (logicalTimestamp.compareTo(valueObject.getLogicalClock()) > 0) {
                    deleteReplicaCommit(logicalTimestamp, key);
                }
            }
        }
    }

    private void startingUpAfterScalingUp() {
        synchronized (lock) {
            if (isActive) {
                return;
            }
            System.out.printf("[%s]: Starting up after scaling up this database node\n", this.databaseNodeName);
            scalingUp();
            this.updatingHeartBeatThread = new Thread(this.updatingHeartBeatThread);
            this.updatingHeartBeatThread.start();
            this.replicationOFDataUsingNeighbourDatabaseNodes = new Thread(this.replicationOFDataUsingNeighbourDatabaseNodes);
            this.replicationOFDataUsingNeighbourDatabaseNodes.start();
        }
    }

    private void cleaningUpBeforeScalingDown() {
        synchronized (lock) {
            if (!isActive) {
                return;
            }
            System.out.printf("[%s]: Cleaning up before scaling down this database node\n", this.databaseNodeName);
            scalingDown();
            if (databaseNodeType == DatabaseNodeType.LEADER) {
                updatingFollowerDatabaseNodesThread.interrupt();
                temporaryLog = null;
                databaseNodeType = DatabaseNodeType.FOLLOWER;
            }
            updatingHeartBeatThread.interrupt();
            replicationOFDataUsingNeighbourDatabaseNodes.interrupt();
        }
    }

    private void updateCommit(BigInteger logicalTimestamp, String key, String value) {
        synchronized (lock) {
            temporaryLog.addUpdateLog(logicalTimestamp, key, value);
            writeAheadLog.addUpdateLog(logicalTimestamp, key, value);
            if (data.containsKey(key)) {
                Value valueObject = data.get(key);
                valueObject.setValue(value);
                valueObject.setLogicalClock(logicalTimestamp);
            } else {
                data.put(key, new Value(logicalTimestamp, value));
            }
        }
    }

    private void updateReplicaCommit(BigInteger logicalTimestamp, String key, String value) {
        synchronized (lock) {
            writeAheadLog.addUpdateLog(logicalTimestamp, key, value);
            if (data.containsKey(key)) {
                Value valueObject = data.get(key);
                valueObject.setValue(value);
                valueObject.setLogicalClock(logicalTimestamp);
            } else {
                data.put(key, new Value(logicalTimestamp, value));
            }
        }
    }

    private void deleteCommit(BigInteger logicalTimestamp, String key) {
        synchronized (lock) {
            temporaryLog.addDeleteLog(logicalTimestamp, key);
            writeAheadLog.addDeleteLog(logicalTimestamp, key);
            data.remove(key);
        }
    }

    private void deleteReplicaCommit(BigInteger logicalTimestamp, String key) {
        synchronized (lock) {
            writeAheadLog.addDeleteLog(logicalTimestamp, key);
            data.remove(key);
        }
    }

    private void scalingDown() {
        synchronized (lock) {
            this.isActive = false;
        }
    }

    private void scalingUp() {
        synchronized (lock) {
            this.isActive = true;
        }
    }
}
