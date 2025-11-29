package hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {
    public static String getHashForKeyAndValue(String key, String value) {
        try {
            String input = String.format("%s : %s", key, value);
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

    private static byte[] getByteFormat(String input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return md.digest(input.getBytes());
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
}
