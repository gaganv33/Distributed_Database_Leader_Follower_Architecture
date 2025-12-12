import data.HybridLogicalClock;
import node.impl.RootNode;
import node.rootNode.BasicRootNodeAccess;
import util.RandomHelper;

import java.math.BigInteger;
import java.time.LocalDateTime;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        BasicRootNodeAccess rootNode = new RootNode(1, 3);

        while (true) {
            try {
                write("Key-" + RandomHelper.getRandomIntegerInRange(1, 500), "Value-" + RandomHelper.getRandomIntegerInRange(1, 5), rootNode);
                write("Key-" + RandomHelper.getRandomIntegerInRange(1, 500), "Value-" + RandomHelper.getRandomIntegerInRange(1, 5), rootNode);
                write("Key-" + RandomHelper.getRandomIntegerInRange(1, 500), "Value-" + RandomHelper.getRandomIntegerInRange(1, 5), rootNode);
                Thread.sleep(2000);

                System.out.println(get("Key-" + RandomHelper.getRandomIntegerInRange(1, 500), rootNode));
                System.out.println(get("Key-" + RandomHelper.getRandomIntegerInRange(1, 500), rootNode));
                System.out.println(get("Key-" + RandomHelper.getRandomIntegerInRange(1, 500), rootNode));
                Thread.sleep(2000);

                delete("Key-" + RandomHelper.getRandomIntegerInRange(1, 500), rootNode);
                delete("Key-" + RandomHelper.getRandomIntegerInRange(1, 500), rootNode);
                delete("Key-" + RandomHelper.getRandomIntegerInRange(1, 500), rootNode);
                Thread.sleep(2000);
            } catch (Exception e) {
                Thread.sleep(3000);
                System.out.println("Exception in client side, making a request after 3 seconds -> " + e.getMessage());
            }
        }
    }

    private static void write(String key, String value, BasicRootNodeAccess rootNode) {
        rootNode.write(new HybridLogicalClock(LocalDateTime.now(), new BigInteger(String.valueOf(0))), key, value);
    }

    private static String get(String key, BasicRootNodeAccess rootNode) {
        return rootNode.get(key);
    }

    private static void delete(String key, BasicRootNodeAccess rootNode) {
        rootNode.delete(new HybridLogicalClock(LocalDateTime.now(), new BigInteger(String.valueOf(0))), key);
    }
}
