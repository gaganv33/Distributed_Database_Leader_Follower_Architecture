import node.MasterNode;
import node.RootNode;
import node.impl.RootNodeImpl;
import util.RandomInteger;

import java.util.Random;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        MasterNode rootNodeImpl = startRootNode();

        while(true) {
            Thread.sleep(3000);
            writeData("Key-" + RandomInteger.getRandomInteger(1, 2000), "Value-" + RandomInteger.getRandomInteger(1, 10), rootNodeImpl);
            writeData("Key-" + RandomInteger.getRandomInteger(1, 2000), "Value-" + RandomInteger.getRandomInteger(1, 10), rootNodeImpl);
            writeData("Key-" + RandomInteger.getRandomInteger(1, 2000), "Value-" + RandomInteger.getRandomInteger(1, 10), rootNodeImpl);

            Thread.sleep(3000);
            getData("Key-" + RandomInteger.getRandomInteger(1, 2000), rootNodeImpl);
            getData("Key-" + RandomInteger.getRandomInteger(1, 2000), rootNodeImpl);
            getData("Key-" + RandomInteger.getRandomInteger(1, 2000), rootNodeImpl);
            getData("Key-" + RandomInteger.getRandomInteger(1, 2000), rootNodeImpl);
        }
    }

    public static MasterNode startRootNode() {
        RootNodeImpl rootNodeImpl = new RootNodeImpl("RootNode - 1", 3);
        Thread rootNodeImplThread = new Thread(rootNodeImpl);
        rootNodeImplThread.start();
        return rootNodeImpl;
    }

    private static void getData(String key, MasterNode rootNodeImpl) {
        try {
            System.out.println(rootNodeImpl.getData(key));
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }

    private static void writeData(String key, String value, MasterNode rootNodeImpl) {
        try {
            rootNodeImpl.writeData(key, value);
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }
}
