package server;

import node.MasterNode;

public interface ProxyServer {
    void updateHeartBeat(MasterNode rootNode);
    void addRootNode(String rootNodeName, int numberOfNodesInShard);
}
