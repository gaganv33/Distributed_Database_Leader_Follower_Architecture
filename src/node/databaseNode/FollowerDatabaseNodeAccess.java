package node.databaseNode;

import exception.DataNotFoundException;
import exception.DatabaseNodeInActiveException;

public interface FollowerDatabaseNodeAccess extends DatabaseNodeAccess {
    String get(String key) throws DatabaseNodeInActiveException, DataNotFoundException;
}
