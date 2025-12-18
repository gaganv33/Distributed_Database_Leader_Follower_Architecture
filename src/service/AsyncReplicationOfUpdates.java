package service;

import config.LogsConfig;
import data.HybridLogicalClock;
import exception.DatabaseNodeInActiveException;
import exception.NotLeaderException;
import exception.RootNodeDownException;
import node.rootNode.BasicRootNodeAccess;

import java.util.concurrent.LinkedBlockingQueue;

public class AsyncReplicationOfUpdates implements Runnable {
    private final LinkedBlockingQueue<Runnable> queue;

    public AsyncReplicationOfUpdates() {
        queue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        while (true) {
            Runnable task = null;
            try {
                task = queue.take();
                task.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("The service for replication of updated is stopped");
            } catch (DatabaseNodeInActiveException | NotLeaderException e) {
                System.out.printf("[AsyncReplicationOfUpdates]: Error while performing the replication, " +
                        "trying the replication after some time: %s\n", e.getMessage());
                enqueueTask(task);
            } catch (RootNodeDownException e) {
                System.out.printf("[AsyncReplicationOfUpdates]: %s\n", e.getMessage());
            }
        }
    }

    public void replicateWriteOperations(BasicRootNodeAccess rootNode, HybridLogicalClock hybridLogicalClock,
                                         String key, String value) {
        Runnable task = () -> {
            if (LogsConfig.isAsyncReplicationOfUpdatesLoggingEnabled) {
                System.out.printf("[AsyncReplicationOfUpdates]: replicating write operation to the root node: %s\n",
                        rootNode.getRootNodeName());
            }
            rootNode.write(hybridLogicalClock, key, value);
        };
        enqueueTask(task);
    }

    public void replicateDeleteOperations(BasicRootNodeAccess rootNode, HybridLogicalClock hybridLogicalClock, String key) {
        Runnable task = () -> {
            if (LogsConfig.isAsyncReplicationOfUpdatesLoggingEnabled) {
                System.out.printf("[AsyncReplicationOfUpdates]: Replicating delete operation to the root node: %s\n",
                        rootNode.getRootNodeName());
            }
            rootNode.delete(hybridLogicalClock, key);
        };
        enqueueTask(task);
    }

    private void enqueueTask(Runnable task) {
        queue.offer(task);
    }
}
