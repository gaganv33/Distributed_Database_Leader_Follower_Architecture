package loadBalancer;

import exception.DataNotFoundException;
import node.ReplicaNode;
import queue.MessageQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadBalancer {
    /**
     * LoadBalancer class is responsible for balancing the read requests with all the read replicas.
     * The load balancing algorithm used in this implementation is Round Robin.
     */
    private final String loadBalancerName;
    private final List<ReplicaNode> replicaNodes;
    private final AtomicInteger index;
    private final int numberOfReplicaNodes;

    public LoadBalancer(String loadBalancerName, int numberOfReplicaNodes, MessageQueue messageQueue) {
        this.loadBalancerName = loadBalancerName;
        System.out.printf("[%s]: Constructor", this.loadBalancerName);
        replicaNodes = new ArrayList<>();
        for (int i = 0; i < numberOfReplicaNodes; i++) {
            replicaNodes.add(new ReplicaNode("Replica Node " + Integer.toString(i)));
        }
        this.index = new AtomicInteger(0);
        this.numberOfReplicaNodes = numberOfReplicaNodes;
    }

    public String getValue(String key) throws DataNotFoundException {
        int i = index.getAndUpdate(x -> (x + 1) % numberOfReplicaNodes);
        return replicaNodes.get(i).getValue(key);
    }
}
