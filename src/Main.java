import node.impl.RootNode;
import util.RandomHelper;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        RootNode rootNode = new RootNode("Root Node-1", 3);

        while (true) {
            try {
                write("Key-" + RandomHelper.getRandomIntegerInRange(1, 50), "Value-" + RandomHelper.getRandomIntegerInRange(1, 5), rootNode);
                write("Key-" + RandomHelper.getRandomIntegerInRange(1, 50), "Value-" + RandomHelper.getRandomIntegerInRange(1, 5), rootNode);
                write("Key-" + RandomHelper.getRandomIntegerInRange(1, 50), "Value-" + RandomHelper.getRandomIntegerInRange(1, 5), rootNode);
                Thread.sleep(2000);

                System.out.println(get("Key-" + RandomHelper.getRandomIntegerInRange(1, 50), rootNode));
                System.out.println(get("Key-" + RandomHelper.getRandomIntegerInRange(1, 50), rootNode));
                System.out.println(get("Key-" + RandomHelper.getRandomIntegerInRange(1, 50), rootNode));
                Thread.sleep(2000);
            } catch (Exception e) {
                Thread.sleep(3000);
                System.out.println("Exception in client side, making a request after 3 seconds -> " + e.getMessage());
            }
        }
    }

    private static void write(String key, String value, RootNode rootNode) throws InterruptedException {
        rootNode.write(key, value);
    }

    private static String get(String key, RootNode rootNode) {
        return rootNode.get(key);
    }
}
