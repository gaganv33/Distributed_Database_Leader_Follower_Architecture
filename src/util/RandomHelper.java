package util;

import java.util.Random;

public class RandomHelper {
    private final static Random random = new Random();

    public static int getRandomIntegerInRange(int start, int end) {
        return random.nextInt(start, end);
    }
}
