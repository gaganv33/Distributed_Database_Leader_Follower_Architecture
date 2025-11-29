package node;

public interface EscalatingNode extends Node {
    void escalateNodeFromReplicaToLeader();
    void deescalateNodeFromLeaderToReplica();
}
