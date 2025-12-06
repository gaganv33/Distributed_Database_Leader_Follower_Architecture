package node.impl;

import data.DatabaseNodeType;
import data.OperationType;
import data.Value;
import data.operationDetails.UpdateOperationDetails;
import exception.DataNotFoundException;
import exception.DatabaseNodeInActiveException;
import exception.NotLeaderException;
import log.TemporaryLog;
import log.WriteAheadLog;
import node.databaseNode.ElevatedDatabaseNodeAccess;
import util.RandomHelper;

import java.math.BigInteger;
import java.util.HashMap;

public class DatabaseNode implements ElevatedDatabaseNodeAccess {
    private final String databaseNodeName;
    private final HashMap<String, Value> data;
    private final WriteAheadLog writeAheadLog;
    private DatabaseNodeType databaseNodeType;
    private TemporaryLog temporaryLog;
    private final Object lock = new Object();
    private boolean isActive;

    public DatabaseNode(String databaseNodeName, DatabaseNodeType databaseNodeType) {
        this.databaseNodeName = databaseNodeName;
        this.isActive = true;
        this.data = new HashMap<>();
        this.writeAheadLog = new WriteAheadLog();
        this.databaseNodeType = databaseNodeType;
        if (databaseNodeType == DatabaseNodeType.LEADER) {
            this.temporaryLog = new TemporaryLog();
        }
    }

    @Override
    public void write(BigInteger logicalTimestamp, String key, String value) throws DatabaseNodeInActiveException, NotLeaderException {
        synchronized (lock) {
            if (!isActive) {
                throw new DatabaseNodeInActiveException(String.format("[%s]: The database node is inactive.", this.databaseNodeName));
            }
            if (databaseNodeType != DatabaseNodeType.LEADER) {
                throw new NotLeaderException(String.format("[%s]: The database node is not a leader node.", this.databaseNodeName));
            }
            if (data.containsKey(key)) {
                Value valueObject = data.get(key);
                if (logicalTimestamp.compareTo(valueObject.getLogicalClock()) > 0) {
                    updateCommit(logicalTimestamp, key, value);
                }
            } else {
                updateCommit(logicalTimestamp, key, value);
            }
        }
    }

    @Override
    public void delete(BigInteger logicalTimestamp, String key) throws DatabaseNodeInActiveException, NotLeaderException {
        synchronized (lock) {
            if (!isActive) {
                throw new DatabaseNodeInActiveException(String.format("[%s]: The database node is inactive.", this.databaseNodeName));
            }
            if (databaseNodeType != DatabaseNodeType.LEADER) {
                throw new NotLeaderException(String.format("[%s]: The database node is not a leader node.", this.databaseNodeName));
            }
            if (data.containsKey(key)) {
                Value valueObject = data.get(key);
                if (logicalTimestamp.compareTo(valueObject.getLogicalClock()) > 0) {
                    deleteCommit(logicalTimestamp, key);
                }
            }
        }
    }

    @Override
    public String get(String key) throws DatabaseNodeInActiveException, DataNotFoundException {
        synchronized (lock) {
            if (!isActive) {
                throw new DatabaseNodeInActiveException(String.format("[%s]: The database node is inactive.", this.databaseNodeName));
            }
            if (!data.containsKey(key)) {
                throw new DataNotFoundException(String.format("[%s]: Data not found in database node.", this.databaseNodeName));
            }
            return data.get(key).getValue();
        }
    }

    @Override
    public void elevateToLeaderDatabaseNode() {
        synchronized (lock) {
            this.databaseNodeType = DatabaseNodeType.LEADER;
            this.temporaryLog = new TemporaryLog();
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(10000L * RandomHelper.getRandomIntegerInRange(5, 11));
                if (isActive) {
                    scalingDown();
                } else {
                    scalingUp();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(String.format("[%s]: Interrupted exception from scaling up and down thread.", this.databaseNodeName));
            }
        }
    }

    private void updateCommit(BigInteger logicalTimestamp, String key, String value) {
        synchronized (lock) {
            temporaryLog.addUpdateLog(logicalTimestamp, key, value);
            writeAheadLog.addUpdateLog(logicalTimestamp, key, value);
            if (data.containsKey(key)) {
                Value valueObject = data.get(key);
                valueObject.setValue(value);
                valueObject.setLogicalClock(logicalTimestamp);
            } else {
                data.put(key, new Value(logicalTimestamp, value));
            }
        }
    }

    private void deleteCommit(BigInteger logicalTimestamp, String key) {
        synchronized (lock) {
            temporaryLog.addDeleteLog(logicalTimestamp, key);
            writeAheadLog.addDeleteLog(logicalTimestamp, key);
            data.remove(key);
        }
    }

    private void scalingDown() {
        synchronized (lock) {
            this.isActive = false;
        }
    }

    private void scalingUp() {
        synchronized (lock) {
            this.isActive = true;
        }
    }
}
