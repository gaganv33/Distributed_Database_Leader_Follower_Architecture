package data;

import java.math.BigInteger;

public class Value {
    private HybridLogicalClock hybridLogicalClock;
    private String value;

    public Value(HybridLogicalClock hybridLogicalClock, String value) {
        this.hybridLogicalClock = hybridLogicalClock;
        this.value = value;
    }

    public HybridLogicalClock getHybridLogicalClock() {
        return hybridLogicalClock;
    }

    public String getValue() {
        return value;
    }

    public void setHybridLogicalClock(HybridLogicalClock hybridLogicalClock) {
        this.hybridLogicalClock = hybridLogicalClock;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
