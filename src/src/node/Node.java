package node;

import config.NodeType;
import exception.DataNotFoundException;
import exception.InActiveNodeException;
import util.RandomInteger;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class Node implements Runnable {
    private final String nodeName;
    private NodeType nodeType;
    private final RootNode rootNode;
    private final ConcurrentHashMap<String, String> data;
    private final Thread updateWriteLogsToReplicaThread;
    private boolean isActive;

    public Node(String nodeName, NodeType nodeType, RootNode rootNode) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.rootNode = rootNode;
        this.data = new ConcurrentHashMap<>();
        this.isActive = true;
        this.updateWriteLogsToReplicaThread = new Thread(() -> {
            while(isActive) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        updateWriteLogsToReplicaThread.setDaemon(true);
    }

    public void escalateNodeFromReplicaToLeader() {
        System.out.printf("[%s]: escalating to leader node\n", this.nodeName);
        this.nodeType = NodeType.LEADER;
        this.isActive = true;
        updateWriteLogsToReplicaThread.start();
    }

    public void deescalateNodeFromLeaderToReplica() throws InterruptedException {
        System.out.printf("[%s]: De escalate to replica node\n", this.nodeName);
        this.nodeType = NodeType.REPLICA;
        this.isActive = false;
        updateWriteLogsToReplicaThread.join();
    }

    public String getData(String key) throws DataNotFoundException, InActiveNodeException {
        if(!this.isActive) {
            throw new InActiveNodeException("The node is inactive");
        }
        if(!data.containsKey(key)) {
            throw new DataNotFoundException("Data not found for the key: " + key);
        }
        return data.get(key);
    }

    public void writeData(String key, String value) throws InActiveNodeException {
        if(!this.isActive) {
            throw new InActiveNodeException("The node is inactive");
        }
        data.put(key, value);
    }

    @Override
    public void run() {
        Thread heartBeatThread = new Thread(() -> {
            while (this.isActive) {
                try {
                    Thread.sleep(1000);
                    rootNode.updateHeartBeat(this);
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
                if(this.isActive) {
                    scalingDown();
                } else {
                    scalingUp();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void scalingDown() {
        this.isActive = false;
    }

    private void scalingUp() {
        this.isActive = true;
    }

    /**
     * Using this method we can get the NodeType of this node.
     * @return NodeType
     */
    public NodeType getNodeType() {
        return this.nodeType;
    }

    /**
     * Using this method we can set what type of node it is.
     * @param nodeType : NodeType of the database (either LEADER or REPLICA)
     */
    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    public String getNodeName() {
        return this.nodeName;
    }
}
