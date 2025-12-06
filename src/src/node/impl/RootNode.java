package node.impl;

import data.DatabaseNodeType;
import node.databaseNode.ElevatedDatabaseNodeAccess;
import node.databaseNode.FollowerDatabaseNodeAccess;
import node.databaseNode.LeaderDatabaseNodeAccess;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RootNode {
    private final String rootNodeName;
    private final HashMap<ElevatedDatabaseNodeAccess, LocalDateTime> databaseNodesHeartBeat;
    private final LeaderDatabaseNodeAccess leaderDatabaseNode;
    private final List<FollowerDatabaseNodeAccess> followerDatabaseNodes;
    private boolean isActive;

    public RootNode(String rootNodeName, int numberOfDatabaseNodes) {
        this.rootNodeName = rootNodeName;
        this.databaseNodesHeartBeat = new HashMap<>();
        followerDatabaseNodes = new ArrayList<>();

        this.leaderDatabaseNode = getAndStartLeaderDatabaseNode();
        startFollowerDatabaseNode(numberOfDatabaseNodes - 1);
    }

    private LeaderDatabaseNodeAccess getAndStartLeaderDatabaseNode() {
        LeaderDatabaseNodeAccess leaderDatabaseNode = new DatabaseNode("Database Node - 1", DatabaseNodeType.LEADER);
        Thread leaderDatabaseNodeThread = new Thread(leaderDatabaseNode);
        leaderDatabaseNodeThread.start();
        databaseNodesHeartBeat.put((ElevatedDatabaseNodeAccess) leaderDatabaseNode, LocalDateTime.now());
        followerDatabaseNodes.add((FollowerDatabaseNodeAccess) leaderDatabaseNode);
        return leaderDatabaseNode;
    }

    private void startFollowerDatabaseNode(int numberOfFollowerNodes) {
        for (int i = 0; i < numberOfFollowerNodes; i++) {
            FollowerDatabaseNodeAccess followerDatabaseNode = new DatabaseNode(String.format("Database Node - %d", i + 2), DatabaseNodeType.FOLLOWER);
            Thread followerDatabaseNodeThread = new Thread(followerDatabaseNode);
            followerDatabaseNodeThread.start();
            databaseNodesHeartBeat.put((ElevatedDatabaseNodeAccess) followerDatabaseNodeThread, LocalDateTime.now());
            followerDatabaseNodes.add(followerDatabaseNode);
        }
    }
}
