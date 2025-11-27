package util;

import config.NodeConfig;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hashing {
    public static int getPositionInRing(String rootNodeName) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(rootNodeName.getBytes());
            BigInteger bigInt = new BigInteger(1, digest);
            return bigInt.mod(BigInteger.valueOf(NodeConfig.lastPositionInRing)).intValue();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
