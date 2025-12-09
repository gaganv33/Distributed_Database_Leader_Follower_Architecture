package config;

public interface RootNodeConfig {
    long heartBeatTimeoutSeconds = 5;
    long cooldownTimeForCheckingHeartBeat = 5000;
    int retryCountForLeaderElection = 3;
    int retryCountForFindDatabaseNodeForReplication = 3;
    int retryLimitForFindingDatabaseNodeForReplication = 3;
}
