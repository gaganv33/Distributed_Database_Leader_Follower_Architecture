package node;

import data.Message;
import exception.DataNotFoundException;

import java.util.HashMap;
import java.util.Queue;

public class ReplicaNode {
    private final String replicaName;
    private final HashMap<String, String> data;

    public ReplicaNode(String replicaName) {
        this.replicaName = replicaName;
        data = new HashMap<>();
        System.out.printf("[%s]: Constructor", this.replicaName);
    }

    public void updateData(Queue<Message> logs) {
        System.out.printf("[%s]: updateData", this.replicaName);
        for (var log : logs) {
            String key = log.key();
            String value = log.value();
            data.put(key, value);
        }
    }

    public String getValue(String key) throws DataNotFoundException {
        System.out.printf("[%s]: getValue for key - %s", this.replicaName, key);
        if(data.containsKey(key)) {
            return data.get(key);
        } else {
            throw new DataNotFoundException("Data for key value: " + key + ", not found.");
        }
    }
}
