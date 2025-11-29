package node.impl;

import config.NodeConfig;
import config.NodeType;
import exception.DataNotFoundException;
import exception.InActiveNodeException;
import node.EscalatingNode;
import node.RootNode;
import util.RandomInteger;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class NodeImpl implements EscalatingNode {
    private final String nodeName;
    private NodeType nodeType;
    private final RootNode rootNodeImpl;
    private final ConcurrentMap<String, String> data;
    private boolean isActive;
    private final Object lock = new Object();
    private boolean isSynced;

    public NodeImpl(String nodeName, NodeType nodeType, RootNode rootNodeImpl) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.rootNodeImpl = rootNodeImpl;
        this.data = new ConcurrentSkipListMap<>();
        this.isActive = true;
        this.isSynced = true;
    }

    /**
     * This method is responsible to change the status of the database node to LEADER if this node is selected as the
     * leader through the leader election algorithm.
     */
    @Override
    public void escalateNodeFromReplicaToLeader() {
        System.out.printf("[%s]: escalating replica node to leader node\n", this.nodeName);
        this.nodeType = NodeType.LEADER;
    }

    /**
     * This method is responsible to change the status of the database node to REPLICA. When this node is scaled down, and
     * it is a leader node, then we de-escalate the status of this node from LEADER to REPLICA.
     */
    @Override
    public void deescalateNodeFromLeaderToReplica() {
        System.out.printf("[%s]: De escalate leader node to replica node\n", this.nodeName);
        this.nodeType = NodeType.REPLICA;
    }

    /**
     * This method is responsible to get the value for the given key.
     * @param key : The key whose associated value is to be returned.
     * @return String : The corresponding value to the key.
     * @throws DataNotFoundException : This exception is thrown when the data is not present in the database.
     * @throws InActiveNodeException : This exception is thrown when this node is inactive and not able to respond to
     *                                 the request.
     */
    @Override
    public String getData(String key) throws DataNotFoundException, InActiveNodeException {
        synchronized (lock) {
            if(!this.isActive) {
                throw new InActiveNodeException("The node is inactive");
            }
            if(!data.containsKey(key)) {
                throw new DataNotFoundException("Data not found for the key: " + key);
            }
            return data.get(key);
        }
    }

    /**
     * This method is responsible to store the value for the corresponding key.
     * @param key : The corresponding key to the value.
     * @param value : The corresponding value to the key.
     * @throws InActiveNodeException : This exception is thrown when this node is inactive and not able to respond to
     *                                 the request.
     */
    @Override
    public void writeData(String key, String value) throws InActiveNodeException {
        synchronized (lock) {
            if(!this.isActive) {
                throw new InActiveNodeException("The node is inactive");
            }
            data.put(key, value);
            // Maintaining a write ahead log to store the records which has to be replicated in the replica nodes, the
            // replication is done asynchronously using a daemon thread running in the RootNode.
            // A WriteAheadLog is maintained so that only the recent updates are replicated in the replica node.
            this.rootNodeImpl.updateLog(key, value);
        }
    }

    /**
     * This method is responsible to remove the value for the corresponding key.
     * @param key : The corresponding key to the value, that has to be removed from the database.
     * @throws InActiveNodeException : This exception is thrown whn this node is inactive and not able to respond to
     *                                 the request.
     */
    @Override
    public void deleteData(String key) throws InActiveNodeException {
        synchronized (lock) {
            if(!this.isActive) {
                throw new InActiveNodeException("The node is inactive");
            }
            data.remove(key);
            // Maintaining a write ahead log to store the records which has to be replicated in the replica nodes, the
            // replication id done asynchronously using a daemon thread running in the RootNode.
            // A WriteAheadLog is maintained so that only the recent updates are replicated in the replica node.
            this.rootNodeImpl.updateLog(key);
        }
    }

    /**
     * This method is responsible for starting a daemon thread, and it will send a heart beat request frequently to
     * the root node. The heart beat indicates that this database node is active and can respond to the requests.
     * It also runs a while loop which does not end, for scaling up and down the node.
     * After scaling the database up, we will sync this database node with the latest data from the WriteAheadLog which
     * is maintained by the root node.
     */
    @Override
    public void run() {
        Thread heartBeatThread = new Thread(() -> {
            System.out.printf("[%s]: Starting a daemon thread to send a heart beat to the root data to indicate the node" +
                    "is active\n", this.nodeName);
            while (true) {
                while(!this.isActive) {
                    try {
                        Thread.sleep(NodeConfig.waitingTimeIfNodeInactive);
                    } catch (InterruptedException e) {
                        System.out.printf("[%s]: Exception heartBeatThread: %s\n", this.nodeName, e.getMessage());
                    }
                    System.out.printf("[%s]: The node is waiting to be active\n", this.nodeName);
                }
                // Waiting until the complete sync is done with the leader done.
                while (!isSynced) {
                    try {
                        Thread.sleep(NodeConfig.waitingTimeWhileSyncing);
                    } catch (InterruptedException e) {
                        System.out.printf("[%s]: Exception heartBeatThread: %s\n", this.nodeName, e.getMessage());
                    }
                    System.out.printf("[%s]: Database node syncing with the latest data in progress\n", this.nodeName);
                }
                try {
                    Thread.sleep(NodeConfig.heartBeatWaitingTime);
                    System.out.printf("[%s]: The total number of data: %d\n", this.nodeName, data.size());
                    System.out.printf("[%s]: Sending heart beat message to the root node\n", this.nodeName);
                    rootNodeImpl.updateHeartBeat(this);
                } catch (InterruptedException e) {
                    System.out.printf("[%s]: Exception heartBeatThread: %s\n", this.nodeName, e.getMessage());
                }
            }
        });
        heartBeatThread.setDaemon(true);
        heartBeatThread.start();
        while(true) {
            try {
                Thread.sleep(10000L * RandomInteger.getRandomInteger(2, 10));
                System.out.printf("[%s]: Mocking the scale up and down of the database node\n", this.nodeName);
                synchronized (lock) {
                    if(this.isActive) {
                        scalingDown();
                        setDatabaseIsNotSynced();
                    } else {
                        scalingUp();
                        syncNodeWithTheLatestData();
                    }
                }
            } catch (InterruptedException e) {
                System.out.printf("[%s]: Exception: %s\n", this.nodeName, e.getMessage());
            }
        }
    }

    /**
     * This method is responsible for setting the isSynced flag to true, this specifies that the database node is synced
     * with the leader database node.
     */
    @Override
    public void setDatabaseSynced() {
        System.out.printf("[%s]: Setting the database node as synced with latest data\n", this.nodeName);
        this.isSynced = true;
    }

    /**
     * This method is responsible for setting the isSynced flag to false, this specifies that the database node is not
     * synced with latest data.
     */
    private void setDatabaseIsNotSynced() {
        System.out.printf("[%s]: Setting the database node as not synced with latest data\n", this.nodeName);
        this.isSynced = false;
    }

    /**
     * The method is responsible to sync the node with the latest data after it becomes active.
     */
    private void syncNodeWithTheLatestData() {
        System.out.printf("[%s]: Syncing this database node with the latest data\n", this.nodeName);
        rootNodeImpl.updateANodeWhichHasJustComeActive(this);
    }

    /**
     * This method is responsible for scaling down the database node.
     */
    private void scalingDown() {
        System.out.printf("[%s]: Scaling down the database node\n", this.nodeName);
        this.isActive = false;
    }

    /**
     * This method is responsible for scaling up the database node.
     */
    private void scalingUp() {
        System.out.printf("[%s]: Scaling up the database node\n", this.nodeName);
        this.isActive = true;
    }

    /**
     * This method is responsible to return the current node type of this database node (LEADER OR REPLICA).
     * @return NodeType
     */
    @Override
    public NodeType getNodeType() {
        return this.nodeType;
    }

    /**
     * This method is responsible to return the database node name.
     * @return String
     */
    @Override
    public String getNodeName() {
        return this.nodeName;
    }

    /**
     * This method is responsible for returning the data, stored in this database node.
     * @return : Returns the data
     */
    @Override
    public ConcurrentMap<String, String> getData() {
        return this.data;
    }
}
