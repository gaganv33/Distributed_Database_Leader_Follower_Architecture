package node.databaseNode;

import exception.DatabaseNodeInActiveException;
import exception.NotLeaderException;

import java.math.BigInteger;

public interface LeaderDatabaseNodeAccess extends DatabaseNodeAccess {
    void write(BigInteger logicalTimestamp, String key, String value) throws DatabaseNodeInActiveException, NotLeaderException;
    void delete(BigInteger logicalTimestamp, String key) throws DatabaseNodeInActiveException, NotLeaderException;
}
