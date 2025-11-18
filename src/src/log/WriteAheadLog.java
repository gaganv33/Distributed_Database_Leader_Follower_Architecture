package log;

import node.Node;
import node.impl.NodeImpl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class WriteAheadLog {
    private final ConcurrentHashMap<String, String> data;

    public WriteAheadLog() {
        this.data = new ConcurrentHashMap<>();
    }

    public void addData(String key, String value) {
        data.put(key, value);
    }

    public void notifyReplicas(List<Node> replicaNodeImpl) {
        for(var entry : data.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            for(var replicaNode : replicaNodeImpl) {
                replicaNode.writeData(key, value);
            }
        }
        clearData();
    }

    private void clearData() {
        this.data.clear();
    }
}
