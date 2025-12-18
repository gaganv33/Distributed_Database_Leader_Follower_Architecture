package data;

import node.rootNode.BasicRootNodeAccess;

public class ClusterNodeMetaData {
    private final int positionInConsistentHashingRing;
    private final BasicRootNodeAccess rootNode;

    public ClusterNodeMetaData(int positionInConsistentHashingRing, BasicRootNodeAccess rootNode) {
        this.positionInConsistentHashingRing = positionInConsistentHashingRing;
        this.rootNode = rootNode;
    }

    public int getPositionInConsistentHashingRing() {
        return positionInConsistentHashingRing;
    }

    public BasicRootNodeAccess getRootNode() {
        return rootNode;
    }
}
