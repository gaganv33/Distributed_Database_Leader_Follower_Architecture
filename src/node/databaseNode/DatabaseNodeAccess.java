package node.databaseNode;

public interface DatabaseNodeAccess extends Runnable {
    boolean getIsActive();
    String getDatabaseNodeName();
    int getDataSize();
}
