package node.impl;

import config.NodeConfig;
import config.NodeType;
import exception.DataNotFoundException;
import exception.InActiveNodeException;
import log.WriteAheadLog;
import node.MasterNode;
import node.Node;
import node.RootNode;
import util.RandomInteger;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RootNodeImpl implements Runnable, RootNode, MasterNode {
    private final String rootName;
    private final ConcurrentHashMap<Node, LocalDateTime> heartBeat;
    private final ConcurrentHashMap<Node, Boolean> activeNodes;
    private Node leaderNodeImpl;
    private final List<Node> replicaNodeImpls;
    private final AtomicInteger index;
    private final WriteAheadLog writeAheadLog;

    public RootNodeImpl(String rootName, int numberOfNodes) {
        System.out.printf("[%s]: Constructor\n", rootName);
        this.rootName = rootName;
        this.heartBeat = new ConcurrentHashMap<>();
        this.replicaNodeImpls = Collections.synchronizedList(new ArrayList<>());
        this.activeNodes = new ConcurrentHashMap<>();
        this.index = new AtomicInteger(0);
        this.writeAheadLog = new WriteAheadLog();

        this.leaderNodeImpl = new NodeImpl("NodeImpl - 1", NodeType.LEADER, this);
        this.heartBeat.put(leaderNodeImpl, LocalDateTime.now());
        this.activeNodes.put(leaderNodeImpl, true);
        for (int i = 0; i < numberOfNodes - 1; i++) {
            Node nodeImpl = new NodeImpl("NodeImpl - " + Integer.toString(i + 2), NodeType.REPLICA, this);
            this.heartBeat.put(nodeImpl,  LocalDateTime.now());
            this.activeNodes.put(nodeImpl, true);
            this.replicaNodeImpls.add(nodeImpl);
        }
    }

    @Override
    public void run() {
        Thread checkIfAllNodesAreActive = new Thread(() -> {
            System.out.printf("[%s]: Starting a daemon thread to check every 2 seconds if any node has become inactive\n",
                    this.rootName);
            while (true) {
                try {
                    Thread.sleep(2000);
                    System.out.printf("[%s]: Checking if all the nodes are active\n", rootName);
                    for (var x : heartBeat.entrySet()) {
                        Node nodeImpl = x.getKey();
                        LocalDateTime time = x.getValue();
                        long secondsDifference = LocalDateTime.now().until(time, ChronoUnit.SECONDS);
                        if (secondsDifference > NodeConfig.heartBeatLimitTime) {
                            System.out.printf("[%s]: NodeImpl is inactive, %s\n", rootName, nodeImpl.getNodeName());
                            activeNodes.put(nodeImpl, false);
                            if (nodeImpl.getNodeType() == NodeType.LEADER) {
                                this.leaderNodeImpl.deescalateNodeFromLeaderToReplica();
                                this.leaderNodeImpl = null;
                                leaderElection();
                            } else {
                                replicaNodeImpls.remove(nodeImpl);
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
        Thread updateDataInReplica = new Thread(() -> {
            System.out.printf("[%s]: Starting a daemon thread to update data in replica every 2 seconds," +
                    " using the write ahead log file\n", this.rootName);
            while (true) {
                try {
                    Thread.sleep(2000);
                    notifyReplica();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        updateDataInReplica.setDaemon(true);
        updateDataInReplica.start();
    }

    private void leaderElection() {
        int position = RandomInteger.getRandomInteger(0, replicaNodeImpls.size());
        this.leaderNodeImpl = replicaNodeImpls.get(position);
        this.leaderNodeImpl.escalateNodeFromReplicaToLeader();
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
    @Override
    public void writeData(String key, String value) {
        this.leaderNodeImpl.writeData(key, value);
    }

    /**
     * Root node acts like a proxy for both read and write operations, and read operations load is balanced using
     * round-robin load balancing method.
     * @param key : We use this key to get the corresponding value
     * @return String
     * @throws DataNotFoundException : When data is not found then we throw this exception.
     */
    @Override
    public String getData(String key) throws DataNotFoundException, InActiveNodeException {
        int position = index.getAndUpdate(x -> (x + 1) % replicaNodeImpls.size());
        return replicaNodeImpls.get(position).getData(key);
    }

    /**
     * Updating the heart beat for the given nodeImpl and setting the nodeImpl to active state.
     * @param nodeImpl : Updating the heart beat for the given nodeImpl
     */
    @Override
    public void updateHeartBeat(Node nodeImpl) {
        heartBeat.put(nodeImpl, LocalDateTime.now());
        if(!activeNodes.get(nodeImpl)) {
            activeNodes.put(nodeImpl, true);
            replicaNodeImpls.add(nodeImpl);
        }
    }

    @Override
    public void updateLog(String key, String value) {
        this.writeAheadLog.addData(key, value);
    }

    @Override
    public void notifyReplica() {
        this.writeAheadLog.notifyReplicas(replicaNodeImpls);
    }
}
