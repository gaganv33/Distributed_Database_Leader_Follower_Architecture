package server.impl;

import config.LogsConfig;
import config.ProxyServerConfig;
import data.HybridLogicalClock;
import exception.*;
import node.impl.RootNode;
import node.rootNode.BasicRootNodeAccess;
import server.ElevatedProxyServer;
import server.RequestProxy;
import service.AsyncReplicationFromNeighbouringNode;
import service.AsyncReplicationOfUpdates;
import util.HashingHelper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

public class ProxyServer implements RequestProxy, ElevatedProxyServer {
    private final HashMap<BasicRootNodeAccess, LocalDateTime> rootNodesHeartBeat;
    private final HashMap<BasicRootNodeAccess, Boolean> rootNodesStatus;
    private final TreeMap<Integer, BasicRootNodeAccess> consistentHashingRootNode;
    private final HashMap<BasicRootNodeAccess, Integer> rootNodesPositionInConsistentHashingRing;
    private final AsyncReplicationFromNeighbouringNode asyncReplicationFromNeighbouringNode;
    private final AsyncReplicationOfUpdates asyncReplicationOfUpdates;
    private boolean isActive;
    private Thread cleaningInactiveRootNode;
    private final Object lock = new Object();

    public ProxyServer(int numberOfShards) {
        rootNodesHeartBeat = new HashMap<>();
        rootNodesStatus = new HashMap<>();
        consistentHashingRootNode = new TreeMap<>();
        rootNodesPositionInConsistentHashingRing = new HashMap<>();
        this.isActive = true;
        // Creating a thread for updating the current root node with the latest data
        this.asyncReplicationFromNeighbouringNode = new AsyncReplicationFromNeighbouringNode();
        Thread asyncReplicationFromNeighbouringNodeThread = new Thread(asyncReplicationFromNeighbouringNode);
        asyncReplicationFromNeighbouringNodeThread.start();
        // Creating a thread for replication the updates across specified number of replication factor
        this.asyncReplicationOfUpdates = new AsyncReplicationOfUpdates();
        Thread asyncReplicationOfUpdatesThread = new Thread(asyncReplicationOfUpdates);
        asyncReplicationOfUpdatesThread.start();

        // Creating and initializing the shards. And finding the position in the consistent hashing ring, using the root node name.
        initiateShard(numberOfShards);

        // Printing the shard details
        printShardDetails();

        this.cleaningInactiveRootNode = new Thread(() -> {
            System.out.println("[ProxyServer]: Starting a thread to check if any root node is inactive, using the last heart beat response");
            while (true) {
                try {
                    Thread.sleep(ProxyServerConfig.cooldownTimeForCheckingHeartBeat);
                } catch (InterruptedException e) {
                    System.out.println("[ProxyServer]: Cleaning Inactive root node thread is stopped");
                    Thread.currentThread().interrupt();
                    return;
                }

                System.out.println("[Proxy Server]: Checking if any root node is inactive, using the last heart beat");
                // Checking heart beat for all the root nodes
                List<BasicRootNodeAccess> rootNodesToRemove = new ArrayList<>();
                // Creating a consistent view of the root nodes
                Set<BasicRootNodeAccess> rootNodesInConsistentHashingRingCopy = rootNodesPositionInConsistentHashingRing.keySet();

                for (var rootNode : rootNodesInConsistentHashingRingCopy) {
                    long timeDifference = Duration.between(rootNodesHeartBeat.get(rootNode), LocalDateTime.now()).getSeconds();
                    System.out.printf("[ProxyServer]: %s %d %d\n", rootNode.getRootNodeName(), timeDifference, ProxyServerConfig.heartBeatTimeoutSeconds);
                    if (timeDifference >= ProxyServerConfig.heartBeatTimeoutSeconds) {
                        rootNodesToRemove.add(rootNode);
                    }
                }

                for (var rootNode : rootNodesToRemove) {
                    removeRootNode(rootNode);
                }
            }
        });

        this.cleaningInactiveRootNode.start();
    }

    @Override
    public void write(LocalDateTime physicalTimestamp, String key, String value) throws AllShardsUnavailableException,
            ShardWriteFailedException, RootNodeDownException {
        TreeMap<Integer, BasicRootNodeAccess> consistentHashingRootNodeCopy = new TreeMap<> (consistentHashingRootNode);
        if (consistentHashingRootNodeCopy.isEmpty()) {
            // If there are no root nodes that are active, then in this case we are stopping the proxy server
            stoppingTheProxyServer();
            throw new AllShardsUnavailableException("[ProxyServer]: All the shards are unavailable. Try again after some time.");
        }
        HybridLogicalClock hybridLogicalClock = new HybridLogicalClock(physicalTimestamp);
        int positionInConsistentHashingRing = HashingHelper.hash(key);
        Map.Entry<Integer, BasicRootNodeAccess> nextRootNodeEntry = getNextRootNode(consistentHashingRootNodeCopy, positionInConsistentHashingRing);
        BasicRootNodeAccess rootNode = extractNextRootNode(consistentHashingRootNodeCopy, nextRootNodeEntry);
        try {
            rootNode.write(hybridLogicalClock, key, value);
            // Replicating the write operation in the next root nodes only if the main root node succeed
            replicateWriteOperations(positionInConsistentHashingRing, hybridLogicalClock, key, value);
        } catch (DatabaseNodeInActiveException | NotLeaderException e) {
            throw new ShardWriteFailedException("[ProxyServer]: There was an unexpected error while performing the " +
                    "write operation. Try again after some time");
        } catch (RootNodeDownException e) {
            // If the root node is inactive, then in this case we are removing this root node
            removeRootNode(rootNode);
            throw new RootNodeDownException(String.format("[ProxyServer]: All the nodes in the shard are down: %s",
                    e.getMessage()));
        }
    }

    @Override
    public void delete(LocalDateTime physicalTimestamp, String key) throws AllShardsUnavailableException,
            ShardWriteFailedException ,RootNodeDownException {
        TreeMap<Integer, BasicRootNodeAccess> consistentHashingRootNodeCopy = new TreeMap<> (consistentHashingRootNode);
        if (consistentHashingRootNodeCopy.isEmpty()) {
            // If there are no root nodes that are active, then in this case we are stopping the proxy server
            stoppingTheProxyServer();
            throw new AllShardsUnavailableException("[ProxyServer]: All the shards are unavailable. Try again after some time.");
        }
        HybridLogicalClock hybridLogicalClock = new HybridLogicalClock(physicalTimestamp);
        int positionInConsistentHashingRing = HashingHelper.hash(key);
        Map.Entry<Integer, BasicRootNodeAccess> nextRootNodeEntry = getNextRootNode(consistentHashingRootNodeCopy, positionInConsistentHashingRing);
        BasicRootNodeAccess rootNode = extractNextRootNode(consistentHashingRootNodeCopy, nextRootNodeEntry);
        try {
            rootNode.delete(hybridLogicalClock, key);
            // Replicating the delete operation in the next root nodes only if the main root node succeed
            replicateDeleteOperation(positionInConsistentHashingRing, hybridLogicalClock, key);
        } catch (DatabaseNodeInActiveException | NotLeaderException e) {
            throw new ShardWriteFailedException("[ProxyServer]: There was an unexpected error while performing the " +
                    "delete operation. Try again after some time");
        } catch (RootNodeDownException e) {
            // If the root node is inactive, then in this case we are removing this root node
            removeRootNode(rootNode);
            throw new RootNodeDownException(String.format("[ProxyServer]: All the nodes in the shard are down: %s",
                    e.getMessage()));
        }
    }

    @Override
    public String get(String key) throws AllShardsUnavailableException, DataNotFoundException, RootNodeDownException {
        TreeMap<Integer, BasicRootNodeAccess> consistentHashingRootNodeCopy = new TreeMap<> (consistentHashingRootNode);
        if (consistentHashingRootNodeCopy.isEmpty()) {
            // If there are no root nodes that are active, then in this case we are stopping the proxy server
            stoppingTheProxyServer();
            throw new AllShardsUnavailableException("[ProxyServer]: All the shards are unavailable. Try again after some time.");
        }
        int positionInConsistentHashingRing = HashingHelper.hash(key);
        Map.Entry<Integer, BasicRootNodeAccess> nextRootNodeEntry = getNextRootNode(consistentHashingRootNodeCopy, positionInConsistentHashingRing);
        BasicRootNodeAccess rootNode = extractNextRootNode(consistentHashingRootNodeCopy, nextRootNodeEntry);
        try {
            return rootNode.get(key);
        } catch (RootNodeDownException e) {
            // If the root node is inactive, then in this case we are removing this root node
            removeRootNode(rootNode);
            throw new RootNodeDownException(String.format("[ProxyServer]: The root node is down, try again after some time: %s",
                    e.getMessage()));
        } catch (DataNotFoundException e) {
            throw new DataNotFoundException(String.format("[ProxyServer]: Data not found: %s", e.getMessage()));
        }
    }

    @Override
    public void updateHeartBeat(BasicRootNodeAccess rootNode) {
        if (LogsConfig.isUpdatedHeartBeatLoggingEnabled) {
            System.out.printf("[ProxyServer]: Updating heart beat for the root node: %s\n", rootNode.getRootNodeName());
        }
        rootNodesHeartBeat.put(rootNode, LocalDateTime.now());
        if (!rootNodesStatus.get(rootNode)) {
            if (LogsConfig.inactiveToActiveLoggingEnabled) {
                System.out.printf("[ProxyServer]: Root node: %s changed status from inactive to active\n",
                        rootNode.getRootNodeName());
            }
            rootNodesStatus.put(rootNode, true);
            // we need to add this node to the consistent hashing and get the data from the next node in the consistent
            // hashing ring, to update the current root node with the latest data when this root node was inactive
            initiatingRootNode(rootNode);
            if (!isActive) {
                startingTheProxyServer();
            }
        }
    }

    private void startingTheProxyServer() {
        synchronized (lock) {
            if (isActive) {
                return;
            }
            System.out.println("[ProxyServer]: Starting the proxy server");
            isActive = true;
            cleaningInactiveRootNode = new Thread(cleaningInactiveRootNode);
            cleaningInactiveRootNode.start();
        }
    }

    private void stoppingTheProxyServer() {
        synchronized (lock) {
            if (!isActive) {
                return;
            }
            System.out.println("[ProxyServer]: All the root nodes are down. So the ProxyServer is inactive");
            isActive = false;
            cleaningInactiveRootNode.interrupt();
        }
    }

    private void replicateWriteOperations(int positionInConsistentHashingRing, HybridLogicalClock hybridLogicalClock,
                                          String key, String value) {
        List<BasicRootNodeAccess> rootNodes = getReplicationFactorNumberOfRootNode(positionInConsistentHashingRing);
        // Replicating the write operations
        for (var rootNode : rootNodes) {
            asyncReplicationOfUpdates.replicateWriteOperations(rootNode, hybridLogicalClock, key, value);
        }
    }

    private void replicateDeleteOperation(int positionInConsistentHashingRing, HybridLogicalClock hybridLogicalClock,
                                          String key) {
        List<BasicRootNodeAccess> rootNodes = getReplicationFactorNumberOfRootNode(positionInConsistentHashingRing);
        // Replicating the delete operations
        for (var rootNode : rootNodes) {
            asyncReplicationOfUpdates.replicateDeleteOperations(rootNode, hybridLogicalClock, key);
        }
    }

    private List<BasicRootNodeAccess> getReplicationFactorNumberOfRootNode(int positionInConsistentHashingRing) {
        TreeMap<Integer, BasicRootNodeAccess> consistentHashingRootNodeCopy = new TreeMap<>(consistentHashingRootNode);
        List<BasicRootNodeAccess> rootNodes = new ArrayList<>(consistentHashingRootNodeCopy
                .tailMap(positionInConsistentHashingRing, false).values().stream()
                .limit(ProxyServerConfig.replicationFactor).toList());
        int remainingNumberOfRootNode = ProxyServerConfig.replicationFactor - rootNodes.size();
        for (var rootNode : consistentHashingRootNodeCopy.values()) {
            if (remainingNumberOfRootNode <= 0) break;
            rootNodes.add(rootNode);
            remainingNumberOfRootNode--;
        }
        return rootNodes;
    }

    private void initiatingRootNode(BasicRootNodeAccess rootNode) {
        System.out.printf("[ProxyServer]: Initiating the root node: %s\n", rootNode.getRootNodeName());
        TreeMap<Integer, BasicRootNodeAccess> consistentHashingRootNodeCopy = new TreeMap<> (consistentHashingRootNode);
        int positionInConsistentHashingRing = getPositionInConsistentHashingRing(rootNode.getRootNodeName());
        // If the consistent hashing ring is empty, then we add this root node and return
        if (consistentHashingRootNodeCopy.isEmpty()) {
            System.out.printf("[ProxyServer]: The consistent hashing node is empty, so adding the root node and returning " +
                    "for the root node: %s\n", rootNode.getRootNodeName());
            consistentHashingRootNode.put(positionInConsistentHashingRing, rootNode);
            rootNodesPositionInConsistentHashingRing.put(rootNode, positionInConsistentHashingRing);
            return;
        }
        System.out.println("[ProxyServer]: Starting the replication of data from the next and previous root node");
        printShardDetails();
        BasicRootNodeAccess nextRootNode = extractNextRootNode(consistentHashingRootNodeCopy,
                getNextRootNode(consistentHashingRootNodeCopy, positionInConsistentHashingRing));
        BasicRootNodeAccess previousRootNode = extractPreviousRootNode(consistentHashingRootNodeCopy,
                getPreviousRootNode(consistentHashingRootNodeCopy, positionInConsistentHashingRing));

        // Getting the range of the current node using the previous root node.
        // And getting the data from the next root node.
        int startingPositionInConsistentHashingRing = rootNodesPositionInConsistentHashingRing.get(previousRootNode) + 1;
        int endingPositionInConsistentHashingRing = positionInConsistentHashingRing;

        System.out.printf("[ProxyServer]: Adding new root node: %s at position: %d\n", rootNode.getRootNodeName(),
                positionInConsistentHashingRing);

        if (endingPositionInConsistentHashingRing < startingPositionInConsistentHashingRing) {
            System.out.printf("[ProxyServer]: Starting replication of data from the next root node to current root node in " +
                    "range: %d %d and %d %d\n", startingPositionInConsistentHashingRing,
                    ProxyServerConfig.positionsInTheConsistentHashingRing, 0, endingPositionInConsistentHashingRing);
            asyncReplicationFromNeighbouringNode.createTaskForReplicationFromNextRootNodeAndEnqueue(
                    rootNode, nextRootNode, startingPositionInConsistentHashingRing,
                    ProxyServerConfig.positionsInTheConsistentHashingRing, 0,
                    endingPositionInConsistentHashingRing
            );
        } else {
            System.out.printf("[ProxyServer]: Starting replication of data from next root node to current root node in range: %d, %d\n",
                    startingPositionInConsistentHashingRing, endingPositionInConsistentHashingRing);
            asyncReplicationFromNeighbouringNode.createTaskForReplicationFromNextRootNodeAndEnqueue(rootNode, nextRootNode,
                    startingPositionInConsistentHashingRing, endingPositionInConsistentHashingRing);
        }

        // Replicating data present in the previous root node to the current root node. So that we can serve the data
        // even if the previous root node goes down.
        asyncReplicationFromNeighbouringNode.createTaskForReplicationFromPreviousRootNodeAndEnqueue(rootNode, previousRootNode);
        // After starting the replication of data from the next and previous root node to get the latest data, we add
        // the root node to the consistent hashing.
        consistentHashingRootNode.put(positionInConsistentHashingRing, rootNode);
        rootNodesPositionInConsistentHashingRing.put(rootNode, positionInConsistentHashingRing);
    }

    private Map.Entry<Integer, BasicRootNodeAccess> getNextRootNode(
            TreeMap<Integer, BasicRootNodeAccess> consistentHashingRootNodeCopy, int positionInConsistentHashingRing) {
        return consistentHashingRootNodeCopy.higherEntry(positionInConsistentHashingRing);
    }

    private Map.Entry<Integer, BasicRootNodeAccess> getPreviousRootNode(
            TreeMap<Integer, BasicRootNodeAccess> consistentHashingRootNodeCopy, int positionInConsistentHashingRing
    ) {
        return consistentHashingRootNodeCopy.lowerEntry(positionInConsistentHashingRing);
    }

    private BasicRootNodeAccess extractNextRootNode(
            TreeMap<Integer, BasicRootNodeAccess> consistentHashingRootNodeCopy,
            Map.Entry<Integer, BasicRootNodeAccess> nextRootNodeEntry
    ) {
        if (nextRootNodeEntry == null) {
            return consistentHashingRootNodeCopy.firstEntry().getValue();
        }
        return nextRootNodeEntry.getValue();
    }

    private BasicRootNodeAccess extractPreviousRootNode(
            TreeMap<Integer, BasicRootNodeAccess> consistentHashingRootNodeCopy,
            Map.Entry<Integer, BasicRootNodeAccess> previousRootNodeEntry
    ) {
        if (previousRootNodeEntry == null) {
            return consistentHashingRootNodeCopy.lastEntry().getValue();
        } else {
            return previousRootNodeEntry.getValue();
        }
    }

    private void removeRootNode(BasicRootNodeAccess rootNode) {
        int positionInConsistentHashingRing = rootNodesPositionInConsistentHashingRing.get(rootNode);
        rootNodesPositionInConsistentHashingRing.remove(rootNode);
        consistentHashingRootNode.remove(positionInConsistentHashingRing);
        rootNodesStatus.put(rootNode, false);

        if (checkIfAllRootNodesAreDown()) {
            stoppingTheProxyServer();
        }
    }

    private boolean checkIfAllRootNodesAreDown() {
        return consistentHashingRootNode.isEmpty();
    }

    private void printShardDetails() {
        System.out.println("Shard Details");
        for (var entry : consistentHashingRootNode.entrySet()) {
            int positionInconsistentHashingRing = entry.getKey();
            BasicRootNodeAccess rootNode = entry.getValue();
            System.out.println(rootNode.getRootNodeName() + " " + positionInconsistentHashingRing);
        }
    }

    private void initiateShard(int numberOfShards) {
        for (int i = 0; i < numberOfShards; i++) {
            BasicRootNodeAccess rootNode = new RootNode(i + 1, 3, this);
            rootNodesHeartBeat.put(rootNode, LocalDateTime.now());
            rootNodesStatus.put(rootNode, true);
            String currentRootNodeName = rootNode.getRootNodeName();
            int positionInConsistentHashingRing = getPositionInConsistentHashingRing(currentRootNodeName);
            consistentHashingRootNode.put(positionInConsistentHashingRing, rootNode);
            rootNodesPositionInConsistentHashingRing.put(rootNode, positionInConsistentHashingRing);
        }
    }

    private int getPositionInConsistentHashingRing(String rootNodeName) {
        int attempt = 1;
        int finalPositionInConsistentHashingRing;
        while (true) {
            int positionInConsistentHashingRing = HashingHelper.hash(rootNodeName);
            if (!consistentHashingRootNode.containsKey(positionInConsistentHashingRing)) {
                finalPositionInConsistentHashingRing = positionInConsistentHashingRing;
                break;
            }
            rootNodeName = rootNodeName + String.valueOf(attempt++);
        }
        return finalPositionInConsistentHashingRing;
    }
}
