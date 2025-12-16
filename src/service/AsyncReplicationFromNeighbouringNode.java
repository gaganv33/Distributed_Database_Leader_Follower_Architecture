package service;

import data.HybridLogicalClock;
import data.operationDetails.OperationDetails;
import exception.DatabaseNodeInActiveException;
import exception.NotLeaderException;
import exception.RootNodeDownException;
import node.rootNode.BasicRootNodeAccess;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class AsyncReplicationFromNeighbouringNode implements Runnable {
    private final LinkedBlockingQueue<Runnable> queue;

    public AsyncReplicationFromNeighbouringNode() {
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
                System.out.println("The service for replication of data from the next node is stopped");
            } catch (DatabaseNodeInActiveException | NotLeaderException e) {
                System.out.printf("[AsyncReplicationFromNeighbouringNode]: Error while performing the replication, " +
                        "trying the replication after some time: %s\n", e.getMessage());
                enqueueTaskToTheQueue(task);
            } catch (RootNodeDownException e) {
                System.out.printf("[AsyncReplicationFromNeighbouringNode]: %s\n", e.getMessage());
            }
        }
    }

    public void createTaskForReplicationFromNextRootNodeAndEnqueue(BasicRootNodeAccess currentRootNode,
                                                                   BasicRootNodeAccess nextRootNode,
                                                                   int startingPositionInConsistentHashingRing,
                                                                   int endingPositionInConsistentHashingRing) {
        Runnable task = () -> {
            System.out.println("[AsyncReplicationFromNeighbouringNode]: Starting a task to replicate data from the " +
                    "next root node to the current root node");
            HashMap<HybridLogicalClock, OperationDetails> logsInRangeOfConsistentHashingPosition =
                    nextRootNode.getLogsInRangeOfConsistentHashingPosition(startingPositionInConsistentHashingRing,
                            endingPositionInConsistentHashingRing);
            currentRootNode.replicationOfData(logsInRangeOfConsistentHashingPosition);
        };

        enqueueTaskToTheQueue(task);
    }

    public void createTaskForReplicationFromNextRootNodeAndEnqueue(BasicRootNodeAccess currentRootNode,
                                                                           BasicRootNodeAccess nextRootNode,
                                                                           int startingPositionInConsistentHashingRing,
                                                                           int intermediateEndingPositionInConsistentHashingRing,
                                                                           int intermediateStartingPositonInConsistentHashingRing,
                                                                           int endingPositonInConsistentHashingRing) {
        Runnable task = () -> {
            System.out.println("[AsyncReplicationFromNeighbouringNode]: Starting a task to replicate data from the " +
                    "next root node to the current root node");
            HashMap<HybridLogicalClock, OperationDetails> logsInRangeOfConsistentHashingPosition =
                    nextRootNode.getLogsInRangeOfConsistentHashingPosition(
                            startingPositionInConsistentHashingRing,
                            intermediateEndingPositionInConsistentHashingRing,
                            intermediateStartingPositonInConsistentHashingRing,
                            endingPositonInConsistentHashingRing
                    );
            currentRootNode.replicationOfData(logsInRangeOfConsistentHashingPosition);
        };

        enqueueTaskToTheQueue(task);
    }

    public void createTaskForReplicationFromPreviousRootNodeAndEnqueue(BasicRootNodeAccess currentRootNode,
                                                                       BasicRootNodeAccess previousRootNode) {
        Runnable task = () -> {
            System.out.println("[AsyncReplicationFromNeighbouringNode]: Starting a task to replicate data from the " +
                    "previous root node to the current root node");
            HashMap<HybridLogicalClock, OperationDetails> logs = previousRootNode.getLogs();
            currentRootNode.replicationOfData(logs);
        };

        enqueueTaskToTheQueue(task);
    }

    private void enqueueTaskToTheQueue(Runnable task) {
        queue.offer(task);
    }
}
