package node;

import exception.DataNotFoundException;
import exception.InActiveNodeException;

public interface MasterNode {
    void writeData(String key, String value);
    String getData(String key) throws DataNotFoundException, InActiveNodeException;
}
