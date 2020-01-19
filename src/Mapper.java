import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

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

    private void readData() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.SPLITTER_MAPPER_PORT);
            Socket splitter = serverSocket.accept();
            log("connected with" + splitter.getInetAddress());
            ObjectInputStream objectInputStream = new ObjectInputStream(splitter.getInputStream());
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(splitter.getOutputStream());
            data = (LinkedList<String>) objectInputStream.readObject();
            config = (Configuration) objectInputStream.readObject();
            objectOutputStream.writeInt(1);//ACK
            objectOutputStream.close();
            objectInputStream.close();
            splitter.close();
            serverSocket.close();
            log("read data finished");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public abstract void map();


    private void sendToShuffler() {
        try {
            log(config.getShufflerIp());
            Socket shuffler = new Socket(config.getShufflerIp(), Ports.MAPPER_SHUFFLER_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(shuffler.getOutputStream());
            ObjectInputStream objectInputStream = new ObjectInputStream(shuffler.getInputStream());
            objectOutputStream.writeObject(context);
            objectInputStream.readInt(); // wait until ACK from shuffler
            log("data sent to shuffler with size " + context.getMap().size());
            objectInputStream.close();
            objectOutputStream.close();
            shuffler.close();
        } catch (IOException e) {
            for (StackTraceElement element : e.getStackTrace())log(element.toString());
            e.printStackTrace();
        }
    }

    public void start() throws InterruptedException {
        performanceLogger.start();
        log("mapper started");
        readData();
        log(data.toString());
        map();
        log("done mapping");
        sendToShuffler();
        log("sent to shuffler");
        performanceLogger.stop();
        performanceLogger.log();
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
