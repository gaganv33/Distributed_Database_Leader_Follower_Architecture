package data;

import node.MasterNode;

public class Pair {
    public final int positionInRing;
    public final MasterNode node;

    public Pair(int positionInRing, MasterNode node) {
        this.positionInRing = positionInRing;
        this.node = node;
    }
}
