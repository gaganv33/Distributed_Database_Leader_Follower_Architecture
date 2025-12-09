package service;

import java.util.concurrent.LinkedBlockingQueue;

public class AsyncReplicationService implements Runnable {
    private final LinkedBlockingQueue<Runnable> linkedBlockingQueue;

    public AsyncReplicationService() {
        this.linkedBlockingQueue = new LinkedBlockingQueue<> ();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Runnable runnable = linkedBlockingQueue.take();
                runnable.run();
            } catch (Exception e) {
                System.out.println("The replication task was interrupted: " + e.getMessage());
            }
        }
    }

    public void addTask(Runnable runnable) {
        linkedBlockingQueue.offer(runnable);
    }
}
