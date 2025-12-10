package service;

import java.util.LinkedList;
import java.util.Queue;

public class AsyncReplicationService implements Runnable {
    private final Queue<Runnable> queue;
    private final Object lock;

    public AsyncReplicationService() {
        this.queue = new LinkedList<>();
        this.lock = new Object();
    }

    @Override
    public void run() {
        while (true) {
            Runnable runnable = null;
            try {
                synchronized (lock) {
                    while (queue.isEmpty()) {
                        lock.wait();
                    }
                    runnable = queue.poll();
                }
                if (runnable != null) runnable.run();
            } catch (Exception e) {
                System.out.println("The replication task was interrupted: " + e.getMessage());
            }
        }
    }

    public void addTask(Runnable runnable) {
        synchronized (lock) {
            queue.offer(runnable);
            lock.notifyAll();
        }
    }
}
