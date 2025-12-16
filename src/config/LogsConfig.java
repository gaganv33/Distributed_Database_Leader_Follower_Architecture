package config;

public interface LogsConfig {
    boolean isExtraLoggingEnabled = false;
    boolean isUpdatedHeartBeatLoggingEnabled = false;
    boolean isCleaningInactiveDatabaseNodesLoggingEnabled = false;
    boolean inactiveToActiveLoggingEnabled = false;
    boolean isLeaderRootNodeCleaningInactiveDatabaseNodesLoggingEnabled = true;
}
