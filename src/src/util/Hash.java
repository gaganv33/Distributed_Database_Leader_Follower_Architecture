package util;

import config.NodeConfig;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {
    public static String getHashForKeyAndValue(String key, String value) {
        try {
            String input = Helper.combineKeyAndValue(key, value);
            return getHashForString(input);
        } catch (Exception e) {
            throw new RuntimeException("Error hashing string", e);
        }
    }

    public static String getHashForString(String input) {
        try {
            byte[] hashBytes = getByteFormat(input);
            return getHash(hashBytes);
        } catch (Exception e) {
            throw new RuntimeException("Error hashing string", e);
        }
    }

    public static String getHashForRightAndLeftSubtreeHash(String rightHash, String leftHash) {
        try {
            byte[] rightHashBytes = getByteFormat(rightHash);
            byte[] leftHashBytes = getByteFormat(leftHash);
            byte[] hashBytes = appendHashBytes(rightHashBytes, leftHashBytes);
            return getHash(hashBytes);
        } catch (Exception e) {
            throw new RuntimeException("Error hashing string", e);
        }
    }

    private static byte[] getByteFormat(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return md.digest(input.getBytes());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getHash(byte[] hashBytes) {
        StringBuilder hex = new StringBuilder(2 * hashBytes.length);
        for (byte b : hashBytes) {
            hex.append(String.format("%02x", b));
        }

        return hex.toString();
    }

    private static byte[] appendHashBytes(byte[] rightHashBytes, byte[] leftHashBytes) {
        int n = rightHashBytes.length;
        int m = leftHashBytes.length;

        byte[] hashBytes = new byte[n + m];
        int index = 0;
        for (byte rightHashByte : rightHashBytes) {
            hashBytes[index++] = rightHashByte;
        }
        for (byte leftHashByte : leftHashBytes) {
            hashBytes[index++] = leftHashByte;
        }
        return hashBytes;
    }

    public static int getPositionInRing(String rootNodeName) {
        byte[] digest = getByteFormat(rootNodeName);
        BigInteger bigInt = new BigInteger(1, digest);
        return bigInt.mod(BigInteger.valueOf(NodeConfig.lastPositionInRing)).intValue();
    }
}
