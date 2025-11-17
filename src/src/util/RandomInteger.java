package util;

import java.util.Random;

public class RandomInteger {
    public static int getRandomInteger(int start, int end) {
        Random random = new Random();
        return random.nextInt(start, end);
    }
}
