package node.databaseNode;

public interface ElevatedDatabaseNodeAccess extends LeaderDatabaseNodeAccess, FollowerDatabaseNodeAccess {
    void elevateToLeaderDatabaseNode();
}
