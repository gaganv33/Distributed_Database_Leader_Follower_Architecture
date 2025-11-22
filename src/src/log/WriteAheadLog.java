package log;

import config.NodeConfig;
import data.Value;
import node.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class WriteAheadLog implements Runnable {
    private final int totalNumberOfNodes;
    private final HashMap<String, String> data;
    private final HashMap<String, Value> dataForBackup;

    public WriteAheadLog(int totalNumberOfNodes) {
        this.totalNumberOfNodes = totalNumberOfNodes;
        this.data = new HashMap<>();
        dataForBackup = new HashMap<>();
    }

    /**
     * This method is responsible to store the value for the corresponding key.
     * @param key : The corresponding key to the value.
     * @param value : The corresponding value to the key.
     */
    public void addData(String key, String value) {
        data.put(key, value);
    }

    /**
     * This method is responsible to replicate the recent data to all the database nodes, so that the nodes are synced
     * with the leader database node. The replication of data to all the database nodes are done asynchronously.
     * This implementation follows eventually consistent for all the database nodes.
     * We also store the data in a separate hash map so that when a node is inactive now and when it comes back online,
     * in order to sync up with the latest data, it can get the data from the back-up hash map.
     * The back-up hash map we maintain the key and the value of object contains key and the database node names which have
     * this data.
     * @param replicaNodeImpl : All the database nodes in this shard. We replicate the data to all the database nodes.
     */
    public void notifyReplicas(List<Node> replicaNodeImpl) {
        for(var entry : data.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            Value valueObject = new Value(value);
            if (dataForBackup.containsKey(key)) {
                valueObject = dataForBackup.get(key);
            }
            valueObject.setValue(value);
            for(var replicaNode : replicaNodeImpl) {
                System.out.printf("WriteAheadLog: notifyReplica: %s, key: %s, value: %s\n", replicaNode.getNodeName(), key, value);
                replicaNode.writeData(key, value);
                valueObject.addReplicaNodeName(replicaNode.getNodeName());
            }
            if (valueObject.getSizeOfTheReplicaNodes() < totalNumberOfNodes) {
                dataForBackup.put(key, valueObject);
            }
        }
        clearData();
    }

    /**
     * This method is responsible to update the database node with the latest data. Since, when this node is down, it does
     * not have the latest data.
     * @param nodeImpl : The node which has just come online. The node will call this method so that it can sync with
     *                 the latest data.
     */
    public void updateTheReplicaNode(Node nodeImpl) {
        System.out.printf("WriteAheadLog: updating the read replica: %s\n", nodeImpl.getNodeName());
        String nodeName = nodeImpl.getNodeName();
        for (var entry : dataForBackup.entrySet()) {
            String key = entry.getKey();
            Value valueObject = entry.getValue();
            if (!valueObject.checkIfReplicaNodeNameExists(nodeName)) {
                System.out.printf("WriteAheadLog: %s updating key: %s, value: %s\n", nodeImpl.getNodeName(), key, valueObject.getValue());
                nodeImpl.writeData(key, valueObject.getValue());
                valueObject.addReplicaNodeName(nodeName);
            }
        }
    }

    /**
     * The method runs a loop which is used to remove the keys from the back-up hash map, which has been replicated with
     * all the database nodes.
     */
    @Override
    public void run() {
        while(true) {
            try {
                Thread.sleep(NodeConfig.cleaningTheLogFileTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            List<String> keysToRemove = getKeysToRemove();
            removeKeys(keysToRemove);
        }
    }

    /**
     * The method is used to check which keys in the back-up hash map, has been replicated to all the database nodes.
     * @return : List<String>
     */
    private List<String> getKeysToRemove() {
        List<String> keysToRemove = new ArrayList<>();
        for (var entry : dataForBackup.entrySet()) {
            int noOfNodes = entry.getValue().getSizeOfTheReplicaNodes();
            if(noOfNodes == totalNumberOfNodes) {
                keysToRemove.add(entry.getKey());
            }
        }
        return keysToRemove;
    }

    /**
     * This method is responsible to remove the keys, if all the nodes have this value associated to this key.
     * @param keysToRemove : The list of keys that can be removed from the back-up hash map.
     */
    private void removeKeys(List<String> keysToRemove) {
        for (var key : keysToRemove) {
            dataForBackup.remove(key);
        }
    }

    /**
     * This method is used to clear the hash map, which contains the latest data that has to be replicated to all the
     * database nodes.
     * This method is called once the data is replicated with all the database nodes.
     */
    private void clearData() {
        this.data.clear();
    }
}
