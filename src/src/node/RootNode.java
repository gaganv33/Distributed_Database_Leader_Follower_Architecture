package node;

public interface RootNode {
    void updateHeartBeat(Node nodeImpl);
    void updateLog(String key, String value);
    void notifyReplica();
}
