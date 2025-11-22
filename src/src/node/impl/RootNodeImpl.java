package node.impl;

import config.NodeConfig;
import config.NodeType;
import exception.DataNotFoundException;
import exception.InActiveNodeException;
import exception.RootNodeDownException;
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
    private final Object leaderLock = new Object();
    private final Object readReplicaImplsLock = new Object();
    private boolean isActive;

    private WriteAheadLog startWriteAheadLogThread(int numberOfNodes) {
        WriteAheadLog writeAheadLog = new WriteAheadLog(numberOfNodes);
        Thread writeAheadLogthread = new Thread(writeAheadLog);
        writeAheadLogthread.start();
        return writeAheadLog;
    }

    public RootNodeImpl(String rootName, int numberOfNodes) {
        this.rootName = rootName;
        this.heartBeat = new ConcurrentHashMap<>();
        this.replicaNodeImpls = Collections.synchronizedList(new ArrayList<>());
        this.activeNodes = new ConcurrentHashMap<>();
        this.index = new AtomicInteger(0);
        this.isActive = true;
        this.writeAheadLog = new WriteAheadLog(numberOfNodes);
        Thread writeAheadLogThread = new Thread(this.writeAheadLog);
        writeAheadLogThread.start();

        this.leaderNodeImpl = new NodeImpl("NodeImpl - 1", NodeType.LEADER, this);
        Thread leaderNodeImplThread = new Thread(this.leaderNodeImpl);
        leaderNodeImplThread.start();
        this.heartBeat.put(this.leaderNodeImpl, LocalDateTime.now());
        this.activeNodes.put(this.leaderNodeImpl, true);
        this.replicaNodeImpls.add(this.leaderNodeImpl);
        for (int i = 0; i < numberOfNodes - 1; i++) {
            Node nodeImpl = new NodeImpl("NodeImpl - " + Integer.toString(i + 2), NodeType.REPLICA, this);
            Thread nodeImplThread = new Thread(nodeImpl);
            nodeImplThread.start();
            this.heartBeat.put(nodeImpl,  LocalDateTime.now());
            this.activeNodes.put(nodeImpl, true);
            this.replicaNodeImpls.add(nodeImpl);
        }
    }

    /**
     * This method is responsible to start 2 daemon threads.
     * The first daemon thread is used to check if all the nodes are active. We use the heart beat map (which consists
     * of the latest time, when a heart beat request was sent from the database nodes) to check if the node is active or
     * inactive. If a node is inactive we remove the database node from the replica nodes. If the leader node does down
     * then we de-escalate the node from LEADER to REPLICA. And then start the leader election process.
     * The second daemon thread is used to replicate the latest data to all the database nodes.
     */
    @Override
    public void run() {
        Thread checkIfAllNodesAreActive = new Thread(() -> {
            System.out.printf("[%s]: Starting a daemon thread to check every 2 seconds if any node has become inactive\n",
                    this.rootName);
            while (true) {
                while(!this.isActive) {
                    try {
                        Thread.sleep(NodeConfig.waitingTimeIfNodeInactive);
                    } catch (Exception e) {
                        System.out.println("Exception: " + e.getMessage());
                    }
                    System.out.printf("[%s]: checkIfAllNodesAreActive, Waiting for at least one database node to be active\n", this.rootName);
                }
                try {
                    Thread.sleep(NodeConfig.checkingNodesAreActiveWaitingTime);
                    System.out.printf("[%s]: Checking if all the nodes are active\n", rootName);
                    for (var x : heartBeat.entrySet()) {
                        Node nodeImpl = x.getKey();
                        LocalDateTime time = x.getValue();
                        if(!activeNodes.get(nodeImpl)) {
                            continue;
                        }
                        long secondsDifference = time.until(LocalDateTime.now(), ChronoUnit.SECONDS);
                        System.out.printf("[%s]: %s -> %d %d\n", this.rootName, nodeImpl.getNodeName(), secondsDifference, NodeConfig.heartBeatLimitTime);
                        if (secondsDifference > NodeConfig.heartBeatLimitTime) {
                            System.out.printf("[%s]: NodeImpl is inactive, %s\n", rootName, nodeImpl.getNodeName());
                            synchronized (readReplicaImplsLock) {
                                activeNodes.put(nodeImpl, false);
                                replicaNodeImpls.remove(nodeImpl);
                                if (!isReadReplicaNodeImplsEmpty()) {
                                    break;
                                }
                            }
                            if (nodeImpl.getNodeType() == NodeType.LEADER) {
                                synchronized (leaderLock) {
                                    this.leaderNodeImpl.deescalateNodeFromLeaderToReplica();
                                    this.leaderNodeImpl = null;
                                    leaderElection();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Exception: " + e.getMessage());
                }
            }
        });
        checkIfAllNodesAreActive.setDaemon(true);
        checkIfAllNodesAreActive.start();
        Thread updateDataInReplica = new Thread(() -> {
            System.out.printf("[%s]: Starting a daemon thread to update data in replica every 2 seconds," +
                    " using the write ahead log file\n", this.rootName);
            while (true) {
                while(!this.isActive) {
                    try {
                        Thread.sleep(NodeConfig.waitingTimeIfNodeInactive);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.printf("[%s]: updateHeartBeatThread, Waiting for at least one database node to be active\n", this.rootName);
                }
                try {
                    Thread.sleep(NodeConfig.updateReplicaNodesTime);
                    System.out.printf("[%s]: Notifying the database replica node with the latest data\n", this.rootName);
                    notifyReplica();
                } catch (Exception e) {
                    System.out.println("Exception: " + e.getMessage());
                }
            }
        });
        updateDataInReplica.setDaemon(true);
        updateDataInReplica.start();
    }

    /**
     * This method is responsible to elect a leader node from the remaining database nodes. Randomized leader election
     * algorithm is implemented to elect the next leader.
     * @throws RootNodeDownException : If all the database nodes are down, then we throw this exception.
     */
    private void leaderElection() throws RootNodeDownException {
        synchronized (readReplicaImplsLock) {
            if (!isReadReplicaNodeImplsEmpty()) {
                throw new RootNodeDownException("The root node: " + this.rootName + " is down");
            }
            System.out.printf("[%s]: Starting leader election process\n", this.rootName);
            int position = RandomInteger.getRandomInteger(0, replicaNodeImpls.size());
            this.leaderNodeImpl = replicaNodeImpls.get(position);
            this.leaderNodeImpl.escalateNodeFromReplicaToLeader();
        }
    }

    /**
     * This method is used to store the key, value pair to the database.
     * @param key : Using this key we can access the value
     * @param value : the value associated to the key
     */
    @Override
    public void writeData(String key, String value) throws RootNodeDownException {
        synchronized (leaderLock) {
            synchronized (readReplicaImplsLock) {
                if (!isActive || !isReadReplicaNodeImplsEmpty()) {
                    throw new RootNodeDownException("The root node: " + this.rootName + " is down");
                }
            }
            System.out.printf("[%s]: Write data request\n", this.rootName);
            this.leaderNodeImpl.writeData(key, value);
        }
    }

    /**
     * This method is responsible to return the value for the given key.
     * @param key : We use this key to get the corresponding value
     * @return String : Return the corresponding value for the given key.
     * @throws DataNotFoundException : When data is not found then we throw this exception.
     */
    @Override
    public String getData(String key) throws DataNotFoundException, InActiveNodeException, RootNodeDownException {
        synchronized (readReplicaImplsLock) {
            if (!isActive || !isReadReplicaNodeImplsEmpty()) {
                throw new RootNodeDownException("The root node: " + this.rootName + " is down");
            }
            System.out.printf("[%s]: Get data request\n", this.rootName);
            int position = index.getAndUpdate(x -> (x + 1) % replicaNodeImpls.size());
            return replicaNodeImpls.get(position).getData(key);
        }
    }

    /**
     * This method is responsible to mark the database node as active and the time that the request is sent.
     * @param nodeImpl : Updating the heart beat for the given nodeImpl
     */
    @Override
    public void updateHeartBeat(Node nodeImpl) {
        synchronized (readReplicaImplsLock) {
            System.out.printf("[%s]: Update heart beat from the database node: %s\n", this.rootName, nodeImpl.getNodeName());
            heartBeat.put(nodeImpl, LocalDateTime.now());
            if(!activeNodes.get(nodeImpl)) {
                activeNodes.put(nodeImpl, true);
                replicaNodeImpls.add(nodeImpl);
            }
            if (!isActive) {
                isActive = true;
                leaderElection();
            }
        }
    }

    /**
     * This method is used to add key and value pair to the WriteAheadLog, so that it can be replicated to the database
     * nodes asynchronously.
     * @param key : Corresponding key to the value.
     * @param value : Corresponding value to the key.
     */
    @Override
    public void updateLog(String key, String value) {
        this.writeAheadLog.addData(key, value);
    }

    /**
     * This method is responsible to replicate the latest data present in the WriteAheadLog to the all the database nodes.
     */
    @Override
    public void notifyReplica() {
        synchronized (readReplicaImplsLock) {
            this.writeAheadLog.notifyReplicas(replicaNodeImpls);
        }
    }

    /**
     * This method is responsible to update the database nodes, with the latest data (or) sync with the leader node.
     * @param nodeImpl : The node which has just come back online, in order to sync the database nodes with the latest
     *                 data.
     */
    @Override
    public void updateANodeWhichHasJustComeActive(Node nodeImpl) {
        synchronized (readReplicaImplsLock) {
            if (!isActive) {
                System.out.printf("[%s]: Cannot update the database node with the latest data, since it is down\n", this.rootName);
                return;
            }
            writeAheadLog.updateTheReplicaNode(nodeImpl);
        }
    }

    /**
     * this method returns whether there is any database node which is active to take process any read or write requests.
     * If there are no active database nodes, then the root node is set to inactive. The status of the root node is
     * set to active only when there is at-least one active database node.
     * @return boolean : Returns true if there are any database active nodes, else false.
     */
    private boolean isReadReplicaNodeImplsEmpty() {
        if(replicaNodeImpls.isEmpty()) {
            System.out.printf("[%s]: All the database nodes are inactive. So the root node is down until at least one" +
                    "database node is active\n", this.rootName);
            this.isActive = false;
            return false;
        }
        return true;
    }
}
