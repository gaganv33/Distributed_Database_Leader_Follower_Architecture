import node.MasterNode;
import node.impl.RootNodeImpl;
import util.RandomInteger;

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

            Thread.sleep(3000);
            deleteData("Key-" + RandomInteger.getRandomInteger(1, 2000), rootNodeImpl);
            deleteData("Key-" + RandomInteger.getRandomInteger(1, 2000), rootNodeImpl);
            deleteData("Key-" + RandomInteger.getRandomInteger(1, 2000), rootNodeImpl);
            deleteData("Key-" + RandomInteger.getRandomInteger(1, 2000), rootNodeImpl);
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
            System.out.println("Exception getData: " + e.getMessage());
        }
    }

    private static void writeData(String key, String value, MasterNode rootNodeImpl) {
        try {
            rootNodeImpl.writeData(key, value);
        } catch (Exception e) {
            System.out.println("Exception writeData: " + e.getMessage());
        }
    }

    private static void deleteData(String key, MasterNode rootNodeImpl) {
        try {
            rootNodeImpl.deleteData(key);
        } catch (Exception e) {
            System.out.println("Exception deleteData: " + e.getMessage());
        }
    }
}
