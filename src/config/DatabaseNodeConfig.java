package config;

public interface DatabaseNodeConfig {
    long cooldownTimeForUpdatingHeartBeat = 2000;
    long cooldownTimeForUpdatingDataInFollowerDatabaseNodes = 2000;
    long cooldownTimeForReplicationOfDataUsingNeighbourDatabaseNodes = 30000;
}
