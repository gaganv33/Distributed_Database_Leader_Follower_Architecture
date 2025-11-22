package data;

import java.util.HashSet;
import java.util.List;

public class Value {
    private String value;
    private final HashSet<String> replicaNodeNames;

    public Value(String value) {
        this.value = value;
        this.replicaNodeNames = new HashSet<>();
    }

    public void addReplicaNodeName(String replicaNodeName) {
        this.replicaNodeNames.add(replicaNodeName);
    }

    public String getValue() {
        return value;
    }

    public HashSet<String> getReplicaNodeNames() {
        return replicaNodeNames;
    }

    public boolean checkIfReplicaNodeNameExists(String nodeName) {
        return replicaNodeNames.contains(nodeName);
    }

    public int getSizeOfTheReplicaNodes() {
        return replicaNodeNames.size();
    }

    public void setValue(String  value) {
        this.value = value;
    }
}
