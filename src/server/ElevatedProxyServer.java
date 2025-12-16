package server;

import node.rootNode.BasicRootNodeAccess;

public interface ElevatedProxyServer {
    void updateHeartBeat(BasicRootNodeAccess rootNode);
}
