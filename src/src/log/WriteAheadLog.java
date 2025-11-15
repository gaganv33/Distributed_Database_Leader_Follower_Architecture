package log;

import data.Message;
import queue.MessageQueue;

import java.util.LinkedList;
import java.util.Queue;

public class WriteAheadLog {
    private final Queue<Message> logs;

    public WriteAheadLog() {
        this.logs = new LinkedList<>();
    }

    public void addLog(Message message) {
        this.logs.add(message);
    }

    public void addLogsToMessageQueue(Message message) {
        logs.add(message);
    }

    public void notify(MessageQueue messageQueue) {
        messageQueue.notify(logs);
        clearLogs();
    }

    private void clearLogs() {
        this.logs.clear();
    }
}
