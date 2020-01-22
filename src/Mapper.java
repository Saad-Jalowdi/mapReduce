import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * each instance of a subclass from {@code Mapper} represents a node
 * in the mapping phase of the mapreduce framework.
 * the user should create a class that extends @{@code Mapper} and write
 * the implementation of the method map() user should also provide a main
 * method in his class creating an instance of his class and call the
 * start(); method.
 *
 * @param <K> output key of Mapper
 * @param <V> output value of Mapper
 * @author Sa'ad Al Jalowdi.
 */
public abstract class Mapper<K extends Comparable, V> {

    protected Context<K, V> context = new Context<>();
    protected LinkedList<String> data;
    private Configuration config;
    private PerformanceLogger performanceLogger = PerformanceLogger.getLogger(this.getClass().getName());

    //read data from splitter.
    private void readData() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.SPLITTER_MAPPER_PORT);
            Socket splitter = serverSocket.accept();
            log("connected with" + splitter.getInetAddress());
            ObjectInputStream objectInputStream = new ObjectInputStream(splitter.getInputStream());
            data = (LinkedList<String>) objectInputStream.readObject();
            config = (Configuration) objectInputStream.readObject();
            log("data read from splitter with size : " + data.size());
            objectInputStream.close();
            splitter.close();
            serverSocket.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public abstract void map();


    private void sendContextToShuffler() {
        try {
            Socket shuffler = new Socket(config.getShufflerIp(), Ports.MAPPER_SHUFFLER_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(shuffler.getOutputStream());
            objectOutputStream.writeObject(context);
            log("context with size " + context.getMap().size() + " is sent to shuffler.");
            objectOutputStream.close();
            shuffler.close();
        } catch (IOException e) {
            log(e.toString());
            e.printStackTrace();
        }
    }

    public void start() throws InterruptedException {
        performanceLogger.start();
        readData();
        map();
        log("done mapping");
        sendContextToShuffler();
        log("sent to shuffler");
        performanceLogger.stop();
        performanceLogger.log();
        TimeUnit.SECONDS.sleep(5);
    }

    private void log(String msg) {
        try {
            File file = new File("/map_reduce/log_mapper.txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file, true);
            fileWriter.write(msg + "\n");
            fileWriter.flush();
            fileWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}