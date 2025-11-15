package queue;

import data.Message;
import node.ReplicaNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class MessageQueue {
    private final String messageQueueName;
    private final List<ReplicaNode> replicaNodes;

    public MessageQueue(String messageQueueName) {
        this.messageQueueName = messageQueueName;
        this.replicaNodes = new ArrayList<>();
        System.out.printf("[%s]: Constructor", this.messageQueueName);
    }

    public void subscribe(ReplicaNode replicaNode) {
        this.replicaNodes.add(replicaNode);
    }

    public void notify(Queue<Message> logs) {
        System.out.printf("[%s]: notify the replica nodes", messageQueueName);
        for (var replicaNode : replicaNodes) {
            replicaNode.updateData(logs);
        }
    }
}
