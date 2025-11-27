package config;

public interface NodeConfig {
    long heartBeatLimitTime = 5;
    long heartBeatWaitingTime = 2000;
    long checkingNodesAreActiveWaitingTime = 3000;
    long waitingTimeIfNodeInactive = 3000;
    long cleaningTheLogFileTime = 10000;
    long updateReplicaNodesTime = 2000;
    long lastPositionInRing = 1024;
}
