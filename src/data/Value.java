package data;

import java.math.BigInteger;

public class Value {
    private BigInteger logicalClock;
    private String value;

    public Value(BigInteger logicalClock, String value) {
        this.logicalClock = logicalClock;
        this.value = value;
    }

    public BigInteger getLogicalClock() {
        return logicalClock;
    }

    public String getValue() {
        return value;
    }

    public void setLogicalClock(BigInteger logicalClock) {
        this.logicalClock = logicalClock;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
