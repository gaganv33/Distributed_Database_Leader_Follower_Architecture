package node;

import exception.DataNotFoundException;
import exception.InActiveNodeException;
import exception.RootNodeDownException;

public interface MasterNode extends Runnable {
    void writeData(String key, String value) throws RootNodeDownException;
    String getData(String key) throws DataNotFoundException, InActiveNodeException, RootNodeDownException;
    void deleteData(String key) throws RootNodeDownException;
    String getRootNodeName();
}
