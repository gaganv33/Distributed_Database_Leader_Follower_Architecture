package node;

import data.Message;
import log.WriteAheadLog;
import queue.MessageQueue;

import java.util.HashMap;

public class LeaderNode implements Runnable {
    /**
     * LeaderNode is the node which only responsible for accepting write, update and delete requests for this shard.
     * All the updates done to the data are written to WriteAheadLog class to keep a check of updates done.
     * And periodically a daemon thread will run every 5 seconds to update the records done in the leader node,
     * so that the replica node have the latest records.
     * The update done to the replica node is asynchronously.
     * This implementation is eventually consistent. Since the updates done to the leader node is not immediately
     * transferred to the replica node.
     */
    private final String leaderNodeName;
    private final HashMap<String, String> data;
    private final MessageQueue messageQueue;
    private final WriteAheadLog writeAheadLog;

    public LeaderNode(String leaderNodeName, MessageQueue messageQueue) {
        this.leaderNodeName = leaderNodeName;
        this.messageQueue = messageQueue;
        this.writeAheadLog = new WriteAheadLog();
        data = new HashMap<>();
        System.out.printf("[%s]: Constructor", this.leaderNodeName);
    }

    public void writeData(String key, String value) {
        System.out.printf("[%s]: writeData", this.leaderNodeName);
        data.put(key, value);
        writeAheadLog.addLog(new Message(key, value));
    }

    @Override
    public void run() {
        Thread logsUpdationThread = new Thread (() -> {
            while(true) {
                try {
                    Thread.sleep(5000);
                    writeAheadLog.notify(messageQueue);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        logsUpdationThread.setDaemon(true);
        logsUpdationThread.start();
    }
}
