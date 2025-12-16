package server;

import exception.*;

import java.time.LocalDateTime;

public interface RequestProxy {
    void write(LocalDateTime physicalTimestamp, String key, String value) throws AllShardsUnavailableException,
            DatabaseNodeInActiveException, NotLeaderException, RootNodeDownException;
    void delete(LocalDateTime physicalTimestamp, String key) throws AllShardsUnavailableException,
            DatabaseNodeInActiveException, RootNodeDownException, NotLeaderException;
    String get(String key) throws AllShardsUnavailableException, DataNotFoundException, RootNodeDownException;
}
