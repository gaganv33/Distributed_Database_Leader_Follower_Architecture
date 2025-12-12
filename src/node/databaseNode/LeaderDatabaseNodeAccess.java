package node.databaseNode;

import data.HybridLogicalClock;
import exception.DatabaseNodeInActiveException;
import exception.NotLeaderException;

import java.math.BigInteger;

public interface LeaderDatabaseNodeAccess extends DatabaseNodeAccess {
    void write(HybridLogicalClock hybridLogicalClock, String key, String value) throws DatabaseNodeInActiveException, NotLeaderException;
    void delete(HybridLogicalClock hybridLogicalClock, String key) throws DatabaseNodeInActiveException, NotLeaderException;
}
