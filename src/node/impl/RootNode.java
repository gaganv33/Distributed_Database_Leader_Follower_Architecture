package node.impl;

import config.RootNodeConfig;
import data.DatabaseNodeType;
import data.operationDetails.OperationDetails;
import exception.*;
import node.databaseNode.ElevatedDatabaseNodeAccess;
import node.databaseNode.FollowerDatabaseNodeAccess;
import node.databaseNode.LeaderDatabaseNodeAccess;
import node.rootNode.BasicRootNodeAccess;
import node.rootNode.ElevatedRootNodeAccess;
import service.AsyncReplicationService;
import util.RandomHelper;

import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RootNode implements ElevatedRootNodeAccess, BasicRootNodeAccess {
    private final int rootNodeId;
    private final String rootNodeName;
    private final HashMap<ElevatedDatabaseNodeAccess, LocalDateTime> databaseNodesHeartBeat;
    private final HashMap<ElevatedDatabaseNodeAccess, Boolean> databaseNodesStatus;
    private final HashMap<ElevatedDatabaseNodeAccess, BigInteger> maximumLogicalTimestampOfDatabaseNodes;
    private LeaderDatabaseNodeAccess leaderDatabaseNode;
    private final List<FollowerDatabaseNodeAccess> followerDatabaseNodes;
    private boolean isActive;
    private BigInteger logicalTimestamp;
    private Thread cleaningInactiveDatabaseNodes;
    private final Object lock = new Object();
    private final AtomicInteger index = new AtomicInteger(0);
    private final AsyncReplicationService asyncReplicationService;

    public RootNode(int rootNodeId, int numberOfDatabaseNodes) {
        this.rootNodeId = rootNodeId;
        this.rootNodeName = String.format("Root Node-%d", rootNodeId);
        this.databaseNodesHeartBeat = new HashMap<>();
        this.databaseNodesStatus = new HashMap<>();
        this.maximumLogicalTimestampOfDatabaseNodes = new HashMap<>();
        this.followerDatabaseNodes = new ArrayList<>();
        this.isActive = true;
        logicalTimestamp = new BigInteger(String.valueOf(1));
        this.asyncReplicationService = new AsyncReplicationService();
        Thread asyncReplicationServiceThread = new Thread(this.asyncReplicationService);
        asyncReplicationServiceThread.start();

        this.leaderDatabaseNode = getAndStartLeaderDatabaseNode();
        startFollowerDatabaseNode(numberOfDatabaseNodes - 1);

        this.cleaningInactiveDatabaseNodes = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(RootNodeConfig.cooldownTimeForCheckingHeartBeat);
                } catch (InterruptedException e) {
                    System.out.printf("[%s]: Cleaning Inactive database node thread is stopped\n", this.rootNodeName);
                    Thread.currentThread().interrupt();
                    return;
                }

                System.out.printf("[%s]: Checking if any database is inactive, using the last heart beat\n", this.rootNodeName);
                // Checking heart beat for all the follower database nodes
                List<FollowerDatabaseNodeAccess> followerDatabaseNodesToRemove = new ArrayList<>();
                for (var databaseNode : followerDatabaseNodes) {
                    long timeDifference = Duration.between(databaseNodesHeartBeat.get((ElevatedDatabaseNodeAccess) databaseNode),
                            LocalDateTime.now()).getSeconds();
                    System.out.printf("[%s]: %s %d %d ; %d\n", this.rootNodeName, databaseNode.getDatabaseNodeName(),
                            timeDifference, RootNodeConfig.heartBeatTimeoutSeconds, databaseNode.getDataSize());
                    if (timeDifference > RootNodeConfig.heartBeatTimeoutSeconds) {
                        followerDatabaseNodesToRemove.add(databaseNode);
                        databaseNodesStatus.put((ElevatedDatabaseNodeAccess) databaseNode, false);
                    }
                }
                followerDatabaseNodes.removeAll(followerDatabaseNodesToRemove);

                // Checking heart beat for the leader database node
                long timeDifference = Duration.between(databaseNodesHeartBeat.get((ElevatedDatabaseNodeAccess) leaderDatabaseNode),
                        LocalDateTime.now()).getSeconds();
                System.out.printf("[%s]: %s %d %d\n", this.rootNodeName, leaderDatabaseNode.getDatabaseNodeName(),
                        timeDifference, RootNodeConfig.heartBeatTimeoutSeconds);
                if (timeDifference > RootNodeConfig.heartBeatTimeoutSeconds) {
                    databaseNodesStatus.put((ElevatedDatabaseNodeAccess) leaderDatabaseNode, false);
                    leaderElection();
                }
            }
        });
        this.cleaningInactiveDatabaseNodes.start();
    }

    public String get(String key) throws DatabaseNodeInActiveException, DataNotFoundException, RootNodeDownException {
        System.out.printf("[%s]: Get request for key: %s\n", this.rootNodeName, key);
        List<FollowerDatabaseNodeAccess> followerDatabaseNodeCopy = new ArrayList<>(followerDatabaseNodes);

        if (followerDatabaseNodeCopy.isEmpty()) {
            allDatabaseNodesAreDown();
            throw new RootNodeDownException(String.format("[%s]: Root node is down", this.rootNodeName));
        }
        int numberOfFollowerDatabaseNodes = followerDatabaseNodeCopy.size();
        int totalNumberOfFollowerDatabaseNodes = followerDatabaseNodeCopy.size();

        while (numberOfFollowerDatabaseNodes-- > 0) {
            int followerDatabaseNodeIndex = index.getAndIncrement() % totalNumberOfFollowerDatabaseNodes;
            FollowerDatabaseNodeAccess followerDatabaseNode = followerDatabaseNodeCopy.get(followerDatabaseNodeIndex);
            try {
                return followerDatabaseNode.get(key);
            } catch (DatabaseNodeInActiveException e) {
                System.out.printf("[%s]: The follower database node is down, retrying with other follower database node\n",
                        this.rootNodeName);
                removeDatabaseNode(followerDatabaseNode);
            } catch (DataNotFoundException e) {
                throw new DataNotFoundException(String.format("[%s]: Data not found in the database for the given key: %s",
                        this.rootNodeName, key));
            }
        }
        System.out.printf("[%s]: Did not get the data after a number of retries\n", this.rootNodeName);
        throw new DatabaseNodeInActiveException(String.format("[%s]: The follower database nodes are down, " +
                "try again after some time", this.rootNodeName));
    }

    public void write(String key, String value) throws DatabaseNodeInActiveException, NotLeaderException,
            RootNodeDownException {
        System.out.printf("[%s]: Write request for key: %s, value : %s\n", this.rootNodeName, key, value);
        try {
            leaderDatabaseNode.write(logicalTimestamp, key, value);
            logicalTimestamp = logicalTimestamp.add(BigInteger.ONE);
        } catch (DatabaseNodeInActiveException e) {
            // start leader election
            leaderElection();
            throw new DatabaseNodeInActiveException(String.format("[%s]: The leader node is inactive. Try again after some time.", this.rootNodeName));
        } catch (NotLeaderException e) {
            throw new NotLeaderException(String.format("[%s]: There is some error while writing to the leader node, " +
                    "try again after some time.", this.rootNodeName));
        } catch (NullPointerException e) {
            throw new RootNodeDownException(String.format("[%s]: Root Node down exception", this.rootNodeName));
        }
    }

    public void delete(String key) throws DatabaseNodeInActiveException, NotLeaderException, RootNodeDownException {
        System.out.printf("[%s]: Delete request for key: %s\n", this.rootNodeName, key);
        try {
            leaderDatabaseNode.delete(logicalTimestamp, key);
            logicalTimestamp = logicalTimestamp.add(BigInteger.ONE);
        } catch (DatabaseNodeInActiveException e) {
            // start leader election
            leaderElection();
            throw new DatabaseNodeInActiveException(String.format("[%s]: The leader node is inactive. Try again after some time.", this.rootNodeName));
        } catch (NotLeaderException e) {
            throw new NotLeaderException(String.format("[%s]: There is some error while writing to the leader node, " +
                    "try again after some time.", this.rootNodeName));
        } catch (NullPointerException e) {
            throw new RootNodeDownException(String.format("[%s]: Root Node down exception", this.rootNodeName));
        }
    }

    @Override
    public void updateHeartBeat(ElevatedDatabaseNodeAccess databaseNode) {
        System.out.printf("[%s]: Updating heart beat for the database node: %s\n", this.rootNodeName, databaseNode.getDatabaseNodeName());
        databaseNodesHeartBeat.put(databaseNode, LocalDateTime.now());
        if (!databaseNodesStatus.get(databaseNode)) {
            databaseNodesStatus.put(databaseNode, true);
            followerDatabaseNodes.add(databaseNode);
            if (!isActive) {
                startingRootNode();
            }
        }
    }

    @Override
    public void replicationOfDataWithFollowerDatabaseNodes(HashMap<BigInteger, OperationDetails> temporaryLogData) {
        System.out.printf("[%s]: Replicating data across all the database nodes\n", this.rootNodeName);
        for (var followerDatabaseNode : followerDatabaseNodes) {
            try {
                ((ElevatedDatabaseNodeAccess) followerDatabaseNode).replicateData(temporaryLogData);
            } catch (DatabaseNodeInActiveException e) {
                System.out.printf("[%s]: Exception while performing replication -> %s", this.rootNodeName, e.getMessage());
            }
        }
    }

    @Override
    public void replicationOfDataBetweenDatabaseNodes(ElevatedDatabaseNodeAccess databaseNode) {
        Runnable runnable = () -> {
            if (followerDatabaseNodes.isEmpty()) {
                throw new RootNodeDownException(String.format("[%s]: The root node is down\n", this.rootNodeName));
            }
            System.out.printf("[%s]: %s -> Starting a asynchronous replication of data using the neighbour database nodes\n",
                    this.rootNodeName, databaseNode.getDatabaseNodeName());
            BigInteger maximumLogicalTimestampBeforeLeaderElection = maximumLogicalTimestampOfDatabaseNodes.get(databaseNode);
            BigInteger currentMaximumLogicalTimestamp = databaseNode.getMaximumLogicalTimestamp();
            BigInteger maximumLogicalTimestamp;
            if (maximumLogicalTimestampBeforeLeaderElection.equals(new BigInteger(String.valueOf(-1)))) {
                maximumLogicalTimestamp = currentMaximumLogicalTimestamp;
            } else {
                maximumLogicalTimestamp = maximumLogicalTimestampBeforeLeaderElection.min(currentMaximumLogicalTimestamp);
            }
            List<FollowerDatabaseNodeAccess> followerDatabaseNodesCopy = new ArrayList<>(followerDatabaseNodes);

            int counter = 0;
            int totalFailedAttempts = 0;

            while (counter < RootNodeConfig.retryCountForFindDatabaseNodeForReplication) {
                int index = RandomHelper.getRandomIntegerInRange(0, followerDatabaseNodesCopy.size());
                FollowerDatabaseNodeAccess replicaDatabaseNode = followerDatabaseNodesCopy.get(index);

                if (!replicaDatabaseNode.getIsActive()) {
                    // removing the database node, if it is inactive
                    removeDatabaseNode(replicaDatabaseNode);
                    System.out.printf("[%s]: Replica database node is down, retry with another database node\n", this.rootNodeName);
                    totalFailedAttempts++;
                } else {
                    if (!replicaDatabaseNode.getDatabaseNodeName().equals(databaseNode.getDatabaseNodeName())) {
                        HashMap<BigInteger, OperationDetails> logsAfterTheGivenTimestamp =
                                ((ElevatedDatabaseNodeAccess) replicaDatabaseNode).getLogsAfterTheGivenTimestamp(maximumLogicalTimestamp);
                        try {
                            databaseNode.replicateData(logsAfterTheGivenTimestamp);
                            System.out.printf("[%s]: %s -> Completed replicating data from the neighbour database node: %s\n",
                                    this.rootNodeName, databaseNode.getDatabaseNodeName(), replicaDatabaseNode.getDatabaseNodeName());
                            counter++;
                        } catch (DatabaseNodeInActiveException e) {
                            totalFailedAttempts++;
                            System.out.printf("[%s]: Exception replication of data between database nodes -> %s\n",
                                    this.rootNodeName, e.getMessage());
                        }
                    }
                }

                if (totalFailedAttempts > RootNodeConfig.retryLimitForFindingDatabaseNodeForReplication) {
                    break;
                }
            }
            throw new ReplicationRetryExceededException(String.format("[%s]: Replication retry exceeded", this.rootNodeName));
        };
        asyncReplicationService.addTask(runnable);
    }

    private void startingRootNode() {
        synchronized (lock) {
            if (isActive) {
                return;
            }
            System.out.printf("[%s]: Starting the root node, since at-least one database node has become active\n", this.rootNodeName);
            isActive = true;
            cleaningInactiveDatabaseNodes = new Thread(cleaningInactiveDatabaseNodes);
            cleaningInactiveDatabaseNodes.start();
            leaderElection();
        }
    }

    private void leaderElection() {
        synchronized (lock) {
            if (leaderDatabaseNode != null && leaderDatabaseNode.getIsActive()) {
                return;
            }
            if (checkIfAllTheFollowerDatabaseNodesAreDown()) {
                allDatabaseNodesAreDown();
                return;
            }
            List<FollowerDatabaseNodeAccess> followerDatabaseNodesCopy = new ArrayList<>(followerDatabaseNodes);
            System.out.printf("[%s]: Starting leader election process\n", this.rootNodeName);
            for (int i = 0; i < RootNodeConfig.retryCountForLeaderElection; i++) {
                int index = RandomHelper.getRandomIntegerInRange(0, followerDatabaseNodesCopy.size());
                ElevatedDatabaseNodeAccess newLeaderDatabaseNode = (ElevatedDatabaseNodeAccess) followerDatabaseNodesCopy.get(index);
                if (!newLeaderDatabaseNode.getIsActive()) {
                    System.out.printf("[%s]: %s database node is down, so cannot elect this database node as the next" +
                            "leader node. Retry for leader election.\n",
                            this.rootNodeName, newLeaderDatabaseNode.getDatabaseNodeName());
                    removeDatabaseNode(newLeaderDatabaseNode);
                    continue;
                }
                System.out.printf("[%s]: Notifying all the database node, to store the logical timestamp\n", this.rootNodeName);
                // update the current logical timestamp, so that we can start the replication of data from this,
                // logical timestamp. To prevent loss of data.
                updateTheLogicalTimestampBeforeNewLeaderElection();
                System.out.printf("[%s]: Found a leader, database node name: %s\n", this.rootNodeName,
                        newLeaderDatabaseNode.getDatabaseNodeName());
                newLeaderDatabaseNode.elevateToLeaderDatabaseNode();
                leaderDatabaseNode = newLeaderDatabaseNode;
                return;
            }
        }
    }

    private void updateTheLogicalTimestampBeforeNewLeaderElection() {
        for (var databaseNode : databaseNodesStatus.keySet()) {
            maximumLogicalTimestampOfDatabaseNodes.put(databaseNode, databaseNode.getMaximumLogicalTimestamp());
        }
    }

    private void allDatabaseNodesAreDown() {
        synchronized (lock) {
            if (!isActive) {
                return;
            }
            System.out.printf("[%s]: All the database nodes are down. So the RootNode is inactive\n", this.rootNodeName);
            isActive = false;
            leaderDatabaseNode = null;
            cleaningInactiveDatabaseNodes.interrupt();
        }
    }

    private boolean checkIfAllTheFollowerDatabaseNodesAreDown() {
        return followerDatabaseNodes.isEmpty();
    }

    private void removeDatabaseNode(FollowerDatabaseNodeAccess databaseNode) {
        databaseNodesStatus.put((ElevatedDatabaseNodeAccess) databaseNode, false);
        followerDatabaseNodes.remove(databaseNode);
    }

    private LeaderDatabaseNodeAccess getAndStartLeaderDatabaseNode() {
        LeaderDatabaseNodeAccess leaderDatabaseNode = new DatabaseNode(rootNodeId, 1,
                DatabaseNodeType.LEADER, this);
        Thread leaderDatabaseNodeThread = new Thread(leaderDatabaseNode);
        leaderDatabaseNodeThread.start();
        databaseNodesHeartBeat.put((ElevatedDatabaseNodeAccess) leaderDatabaseNode, LocalDateTime.now());
        databaseNodesStatus.put((ElevatedDatabaseNodeAccess) leaderDatabaseNode, true);
        maximumLogicalTimestampOfDatabaseNodes.put((ElevatedDatabaseNodeAccess) leaderDatabaseNode,
                new BigInteger(String.valueOf(-1)));
        followerDatabaseNodes.add((FollowerDatabaseNodeAccess) leaderDatabaseNode);
        return leaderDatabaseNode;
    }

    private void startFollowerDatabaseNode(int numberOfFollowerNodes) {
        for (int i = 0; i < numberOfFollowerNodes; i++) {
            FollowerDatabaseNodeAccess followerDatabaseNode = new DatabaseNode(rootNodeId, i + 2,
                    DatabaseNodeType.FOLLOWER, this);
            Thread followerDatabaseNodeThread = new Thread(followerDatabaseNode);
            followerDatabaseNodeThread.start();
            databaseNodesHeartBeat.put((ElevatedDatabaseNodeAccess) followerDatabaseNode, LocalDateTime.now());
            databaseNodesStatus.put((ElevatedDatabaseNodeAccess) followerDatabaseNode, true);
            maximumLogicalTimestampOfDatabaseNodes.put((ElevatedDatabaseNodeAccess) followerDatabaseNode,
                    new BigInteger(String.valueOf(-1)));
            followerDatabaseNodes.add(followerDatabaseNode);
        }
    }
}
