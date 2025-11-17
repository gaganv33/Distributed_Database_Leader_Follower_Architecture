package node;

import config.NodeConfig;
import config.NodeType;
import exception.DataNotFoundException;
import exception.InActiveNodeException;
import util.RandomInteger;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RootNode implements Runnable {
    private final String rootName;
    private final ConcurrentHashMap<Node, LocalDateTime> heartBeat;
    private final ConcurrentHashMap<Node, Boolean> activeNodes;
    private Node leaderNode;
    private final List<Node> replicaNodes;
    private final AtomicInteger index;

    public RootNode(String rootName, int numberOfNodes) {
        System.out.printf("[%s]: Constructor\n", rootName);
        this.rootName = rootName;
        heartBeat = new ConcurrentHashMap<>();
        replicaNodes = Collections.synchronizedList(new ArrayList<>());
        activeNodes = new ConcurrentHashMap<>();
        index = new AtomicInteger(0);

        this.leaderNode = new Node("Node - 1", NodeType.LEADER, this);
        heartBeat.put(leaderNode, LocalDateTime.now());
        activeNodes.put(leaderNode, true);
        for (int i = 0; i < numberOfNodes - 1; i++) {
            Node node = new Node("Node - " + Integer.toString(i + 2), NodeType.REPLICA, this);
            heartBeat.put(node,  LocalDateTime.now());
            activeNodes.put(node, true);
            replicaNodes.add(node);
        }
    }

    @Override
    public void run() {
        Thread checkIfAllNodesAreActive = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(2000);
                    System.out.printf("[%s]: Checking if all the nodes are active\n", rootName);
                    for (var x : heartBeat.entrySet()) {
                        Node node = x.getKey();
                        LocalDateTime time = x.getValue();
                        long secondsDifference = LocalDateTime.now().until(time, ChronoUnit.SECONDS);
                        if (secondsDifference > NodeConfig.heartBeatLimitTime) {
                            System.out.printf("[%s]: Node is inactive, %s\n", rootName, node.getNodeName());
                            activeNodes.put(node, false);
                            if (node.getNodeType() == NodeType.LEADER) {
                                this.leaderNode.deescalateNodeFromLeaderToReplica();
                                this.leaderNode = null;
                                leaderElection();
                            } else {
                                replicaNodes.remove(node);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        checkIfAllNodesAreActive.setDaemon(true);
        checkIfAllNodesAreActive.start();
    }

    public void leaderElection() {
        int position = RandomInteger.getRandomInteger(0, replicaNodes.size());
        this.leaderNode = replicaNodes.get(position);
        this.leaderNode.escalateNodeFromReplicaToLeader();
    }

    /**
     * We will write the data to a log file first and then perform the write update to the database every 5 seconds.
     * We maintain a log file since, when the leader node is down, then the leader election process starts. During this
     * process there might be update requests, so in order to provide availability we use a log file to provide high
     * availability. But the data is not propagated to the replica node immediately. But the data will be eventually
     * modified in the replica node.
     * @param key : Using this key we can access the value
     * @param value : the value associated to the key
     */
    public void writeData(String key, String value) {

    }

    /**
     * Root node acts like a proxy for both read and write operations, and read operations load is balanced using
     * round-robin load balancing method.
     * @param key : We use this key to get the corresponding value
     * @return String
     * @throws DataNotFoundException : When data is not found then we throw this exception.
     */
    public String getData(String key) throws DataNotFoundException, InActiveNodeException {
        int position = index.getAndUpdate(x -> (x + 1) % replicaNodes.size());
        return replicaNodes.get(position).getData(key);
    }

    /**
     * Updating the heart beat for the given node and setting the node to active state.
     * @param node : Updating the heart beat for the given node
     */
    public void updateHeartBeat(Node node) {
        heartBeat.put(node, LocalDateTime.now());
        if(!activeNodes.get(node)) {
            activeNodes.put(node, true);
            replicaNodes.add(node);
        }
    }
}
