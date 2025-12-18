package config;

public interface ProxyServerConfig {
    long cooldownTimeForCheckingHeartBeat = 5000;
    int positionsInTheConsistentHashingRing = 1024;
    long heartBeatTimeoutSeconds = 5;
    int replicationFactor = 3;
}
