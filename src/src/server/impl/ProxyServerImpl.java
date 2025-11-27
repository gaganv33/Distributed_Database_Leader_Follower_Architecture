package server.impl;

import config.NodeConfig;
import data.Pair;
import data.Range;
import node.MasterNode;
import node.impl.RootNodeImpl;
import server.ProxyServer;
import util.Hashing;
import util.RandomInteger;

import javax.imageio.spi.ImageInputStreamSpi;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ProxyServerImpl implements ProxyServer, Runnable {
    private final ConcurrentHashMap<MasterNode, LocalDateTime> heartBeat;
    private final ConcurrentHashMap<MasterNode, Boolean> activeNodes;
    private final List<Pair> rootNodesData;
    private final HashMap<MasterNode, Pair> rootNodesLookUp;
    private boolean isActive;
    private final Object lock = new Object();

    public ProxyServerImpl(int numberOfNodes) {
        heartBeat = new ConcurrentHashMap<>();
        activeNodes = new ConcurrentHashMap<>();
        rootNodesData = Collections.synchronizedList(new ArrayList<>());
        rootNodesLookUp = new HashMap<>();
        isActive = true;

        for (int i = 0; i < numberOfNodes; i++) {
            addRootNode("Root Node - " + Integer.toString(i));
        }
        consistentHashing();
    }

    @Override
    public void updateHeartBeat(MasterNode rootNode) {
        synchronized (lock) {
            System.out.printf("[ProxyServerImpl]: Update heart beat request from root node: %s\n", rootNode.getRootNodeName());
            heartBeat.put(rootNode, LocalDateTime.now());
            if (!activeNodes.get(rootNode)) {
                activeNodes.put(rootNode, true);
            }
            if (!isActive) {
                System.out.println("[ProxyServerImpl]: A root node is active. Thus proxy server is back up");
                isActive = true;
            }
            addRootNodeConsistentHashing(rootNode);
        }
    }

    @Override
    public void addRootNode(String rootNodeName, int numberOfNodesInShard) {
        synchronized (lock) {
            MasterNode rootNode = startRootNode(rootNodeName, numberOfNodesInShard);
            heartBeat.put(rootNode, LocalDateTime.now());
            activeNodes.put(rootNode, true);
            addRootNodeConsistentHashing(rootNode);
        }
    }

    @Override
    public void run() {
        Thread checkIfNodesAreActive = new Thread(() -> {
            System.out.println("[ProxyServerImpl]: Starting a daemon thread to check if any nodes has become inactive");
            while (true) {
                while (!isActive) {
                    try {
                        Thread.sleep(NodeConfig.waitingTimeIfNodeInactive);
                    } catch (InterruptedException e) {
                        System.out.println("Exception: " + e.getMessage());
                    }
                    System.out.println("[ProxyServerImpl]: checkIfAllNodesAreActive, Waiting for at least one root node " +
                            "shard to become active");
                }
                try {
                    Thread.sleep(NodeConfig.checkingNodesAreActiveWaitingTime);
                    System.out.println("[ProxyServerImpl]: checking if all the root node shard is active");
                    for (var entry : heartBeat.entrySet()) {
                        MasterNode node = entry.getKey();
                        LocalDateTime time = entry.getValue();

                        synchronized (lock) {
                            if (!isActive) {
                                continue;
                            }
                        }
                        long secondsDifference = time.until(LocalDateTime.now(), ChronoUnit.SECONDS);
                        System.out.printf("[ProxyServerImpl]: %s -> %d %d\n", node.getRootNodeName(), secondsDifference, NodeConfig.heartBeatLimitTime);
                        if (secondsDifference > NodeConfig.heartBeatLimitTime) {
                            System.out.printf("[ProxyServerImpl]: %s root node is inactive\n", node.getRootNodeName());
                            synchronized (lock) {
                                activeNodes.put(node, false);
                                removeRootNodeConsistentHashing(node);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println("Exception: " + e.getMessage());
                }
            }
        });
        checkIfNodesAreActive.setDaemon(true);
        checkIfNodesAreActive.start();
    }

    private void consistentHashing() {
        synchronized (lock) {
            for (var x : heartBeat.keySet()) {
                int positionInRing = Hashing.getPositionInRing(x.getRootNodeName());
                Pair pair = new Pair(positionInRing, x);
                rootNodesData.add(pair);
                rootNodesLookUp.put(x, pair);
            }
            rootNodesData.sort(Comparator.comparingInt(p -> p.positionInRing));
            for (var x : rootNodesData) {
                System.out.println(x.node.getRootNodeName() + " " + x.positionInRing);
            }
        }
    }

    private void addRootNodeConsistentHashing(MasterNode rootNode) {
        synchronized (lock) {
            int positionInRing;
            Pair pair;
            if (rootNodesLookUp.containsKey(rootNode)) {
                positionInRing = rootNodesLookUp.get(rootNode).positionInRing;
                pair = rootNodesLookUp.get(rootNode);
            } else {
                String rootNodeName = rootNode.getRootNodeName();
                positionInRing = Hashing.getPositionInRing(rootNodeName);
                pair = new Pair(positionInRing, rootNode);
                rootNodesLookUp.put(rootNode, pair);
            }
            if (rootNodesData.isEmpty()) {
                rootNodesData.add(pair);
            } else {
                int index = getIndex(positionInRing);
                System.out.println(rootNode.getRootNodeName() + " " + positionInRing + " " + index);
                if(index == -1) {
                    rootNodesData.add(pair);
                    index = rootNodesData.size() - 1;
                } else {
                    rootNodesData.add(index, pair);
                }
                Range range = getRange(index);
                System.out.println("Range: " + range.start + " " + range.end);
            }

            System.out.println("After adding a new node");
            for (var x : rootNodesData) {
                System.out.println(x.node.getRootNodeName() + " " + x.positionInRing);
            }
        }
    }

    private Range getRange(int index) {
        synchronized (lock) {
            if (index == 0) {
                return new Range(rootNodesData.getFirst().positionInRing, rootNodesData.getLast().positionInRing + 1);
            } else {
                return new Range(rootNodesData.get(index - 1).positionInRing + 1, rootNodesData.get(index).positionInRing);
            }
        }
    }

    private int getIndex(int positionInRing) {
        synchronized (lock) {
            int start = 0;
            int end = rootNodesData.size() - 1;
            int index = -1;
            int diff = Integer.MAX_VALUE;

            while(start <= end) {
                int mid = (end - start) / 2 + start;
                int current = rootNodesData.get(mid).positionInRing;

                if(current < positionInRing) {
                    start = mid + 1;
                } else {
                    int currentDiff = current - positionInRing;
                    if (currentDiff < diff) {
                        diff = currentDiff;
                        index = mid;
                    }
                    end = mid - 1;
                }
            }
            return index;
        }
    }

    private void removeRootNodeConsistentHashing(MasterNode rootNode) {
        synchronized (lock) {
            Pair pair = rootNodesLookUp.get(rootNode);
            rootNodesData.remove(pair);
        }
    }

    private void addRootNode(String rootNodeName) {
        MasterNode rootNode = startRootNode(rootNodeName, RandomInteger.getRandomInteger(1, 5));
        heartBeat.put(rootNode, LocalDateTime.now());
        activeNodes.put(rootNode, true);
    }

    private MasterNode startRootNode(String rootNodeName, int numberOfNodesInShard) {
        RootNodeImpl rootNodeImpl = new RootNodeImpl(rootNodeName, numberOfNodesInShard, this);
        Thread rootNodeImplThread = new Thread(rootNodeImpl);
//        rootNodeImplThread.start();
        return rootNodeImpl;
    }
}
