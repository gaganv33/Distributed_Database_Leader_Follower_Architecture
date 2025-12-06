package node;

import java.util.concurrent.ConcurrentMap;

public interface RootNode extends MasterNode {
    void updateHeartBeat(EscalatingNode nodeImpl);
    void updateLog(String key, String value);
    void updateLog(String key);
    void notifyReplica();
    void updateANodeWhichHasJustComeActive(Node nodeImpl);
    ConcurrentMap<String, String> getDataFromLeader();
}
