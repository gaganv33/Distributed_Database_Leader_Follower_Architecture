package util;

import data.HybridLogicalClock;

import java.util.Comparator;

public class HybridLogicalClockComparator {
    public static Comparator<HybridLogicalClock> getHybridLogicalClock() {
        return new Comparator<HybridLogicalClock>() {
            @Override
            public int compare(HybridLogicalClock o1, HybridLogicalClock o2) {
                if (o1.getPhysicalClock().isBefore(o2.getPhysicalClock())) {
                    return -1;
                } else if (o1.getPhysicalClock().isAfter(o2.getPhysicalClock())) {
                    return 1;
                }

                if (o1.getLogicalClock().compareTo(o2.getLogicalClock()) < 0) {
                    return -1;
                } else if (o1.getLogicalClock().compareTo(o2.getLogicalClock()) > 0) {
                    return 1;
                }

                return 0;
            }
        };
    }
}
