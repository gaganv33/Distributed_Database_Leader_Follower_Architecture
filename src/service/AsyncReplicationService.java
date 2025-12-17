package service;

import exception.ReplicationRetryExceededException;
import exception.RootNodeDownException;

import java.util.concurrent.LinkedBlockingQueue;

public class AsyncReplicationService implements Runnable {
    private final LinkedBlockingQueue<Runnable> queue;

    public AsyncReplicationService() {
        queue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        while (true) {
            Runnable task;
            try {
                task = queue.take();
                task.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("[AsyncReplicationService]: The asynchronous replication of data within the shard is stopped");
            } catch (ReplicationRetryExceededException e) {
                System.out.println("[AsyncReplicationService]: Replicate data has exceeded the try limit for replication of data");
            } catch (RootNodeDownException e) {
                System.out.println("[AsyncReplicationService]: Root node is down");
            }
        }
    }

    public void addTask(Runnable task) {
        queue.offer(task);
    }
}
