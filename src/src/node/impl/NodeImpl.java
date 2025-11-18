package node.impl;

import config.NodeType;
import exception.DataNotFoundException;
import exception.InActiveNodeException;
import node.Node;
import node.RootNode;
import util.RandomInteger;

import java.util.concurrent.ConcurrentHashMap;

public class NodeImpl implements Runnable, Node {
    private final String nodeName;
    private NodeType nodeType;
    private final RootNode rootNodeImpl;
    private final ConcurrentHashMap<String, String> data;
    private boolean isActive;
    private final Object lock = new Object();

    public NodeImpl(String nodeName, NodeType nodeType, RootNode rootNodeImpl) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.rootNodeImpl = rootNodeImpl;
        this.data = new ConcurrentHashMap<>();
        this.isActive = true;
    }

    @Override
    public void escalateNodeFromReplicaToLeader() {
        synchronized (lock) {
            System.out.printf("[%s]: escalating to leader node\n", this.nodeName);
            this.nodeType = NodeType.LEADER;
            this.isActive = true;
        }
    }

    @Override
    public void deescalateNodeFromLeaderToReplica() throws InterruptedException {
        synchronized (lock) {
            this.rootNodeImpl.notifyReplica();
            System.out.printf("[%s]: De escalate to replica node\n", this.nodeName);
            this.nodeType = NodeType.REPLICA;
            this.isActive = false;
        }
    }

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

    @Override
    public void writeData(String key, String value) throws InActiveNodeException {
        synchronized (lock) {
            if(!this.isActive) {
                throw new InActiveNodeException("The node is inactive");
            }
            data.put(key, value);
            // Maintaining a write ahead log to store the records which has to be replicated in the replica nodes, the
            // replication is done asynchronously using a daemon thread running in the RootNode.
            this.rootNodeImpl.updateLog(key, value);
        }
    }

    @Override
    public void run() {
        Thread heartBeatThread = new Thread(() -> {
            System.out.printf("[%s]: Starting a daemon thread to send a heart beat to the root data to indicate the node" +
                    "is active\n", this.nodeName);
            while (true) {
                while (!this.isActive) {}
                try {
                    Thread.sleep(1000);
                    rootNodeImpl.updateHeartBeat(this);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        heartBeatThread.setDaemon(true);
        heartBeatThread.start();
        while(true) {
            try {
                Thread.sleep(10000L * RandomInteger.getRandomInteger(2, 10));
                synchronized (lock) {
                    if(this.isActive) {
                        scalingDown();
                    } else {
                        scalingUp();
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Mocking as if the node is up and active, once it is scaled down
     */
    private void scalingDown() {
        this.isActive = false;
    }

    /**
     * Mocking as if the node is down and inactive
     */
    private void scalingUp() {
        this.isActive = true;
    }

    /**
     * Using this method we can get the NodeType of this node.
     * @return NodeType
     */
    @Override
    public NodeType getNodeType() {
        return this.nodeType;
    }

    @Override
    public String getNodeName() {
        return this.nodeName;
    }
}
