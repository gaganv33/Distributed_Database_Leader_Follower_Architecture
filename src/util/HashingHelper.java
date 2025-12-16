package util;

import config.ProxyServerConfig;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashingHelper {
    public static int hash(String input) {
        try {
            MessageDigest hashingAlgorithm = MessageDigest.getInstance("SHA256");
            byte[] hash = hashingAlgorithm.digest(input.getBytes());
            BigInteger index = new BigInteger(1, hash);
            return index.mod(BigInteger.valueOf(ProxyServerConfig.positionsInTheConsistentHashingRing)).intValue();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
