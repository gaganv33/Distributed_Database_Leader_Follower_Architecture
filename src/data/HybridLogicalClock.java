package data;

import java.math.BigInteger;
import java.time.LocalDateTime;

public class HybridLogicalClock {
    private final LocalDateTime physicalClock;
    private BigInteger logicalClock;

    public HybridLogicalClock(LocalDateTime physicalClock, BigInteger logicalClock) {
        this.physicalClock = physicalClock;
        this.logicalClock = logicalClock;
    }

    public LocalDateTime getPhysicalClock() {
        return physicalClock;
    }

    public BigInteger getLogicalClock() {
        return logicalClock;
    }

    public void incrementLogicalClockByOne() {
        logicalClock = logicalClock.add(BigInteger.ONE);
    }

    public void setLogicalClock(BigInteger logicalClock) {
        this.logicalClock = logicalClock;
    }
}
