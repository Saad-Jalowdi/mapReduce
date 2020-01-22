import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * each instance of a subclass from {@code Reducer} represents a node
 * in the reducing phase of the mapreduce framework.
 * the user should create a class that extends @{@code Reducer} and write
 * the implementation of the method reduce() user should also provide a main
 * method in his class creating an instance of his class and call the
 * start(); method.
 *
 * @param <K> output key of Reducer
 * @param <V> output value of Reducer
 * @author Sa'ad Al Jalowdi.
 */
public abstract class Reducer<K extends Comparable, V> {

    protected Context<K, V> context = new Context<>();
    private Context<K, V> mapperContext;
    private Iterable<K> keys;
    private String resultIp;
    private PerformanceLogger performanceLogger = PerformanceLogger.getLogger(this.getClass().getName());

    //read context from shuffler
    private void readContextFromShuffler() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.SHUFFLER_REDUCER_PORT);
            Socket shuffler = serverSocket.accept();
            log("connected with" + shuffler.getInetAddress());
            ObjectInputStream objectInputStream = new ObjectInputStream(shuffler.getInputStream());
            mapperContext = (Context) objectInputStream.readObject();
            resultIp = objectInputStream.readUTF();
            keys = mapperContext.getMap().keySet();
            log("context read with size : " + mapperContext.getMap().size() + " from shuffler.");
            objectInputStream.close();
            shuffler.close();
            serverSocket.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    protected abstract void reduce();

    /**
     * this method returns an Iterable of the keys that have been written on mappers.
     *
     * @return Iterable<K>
     */
    public Iterable<K> getKeys() {
        return keys;
    }

    /**
     * this method returns an Iterable of the values for a specified key.
     *
     * @param key that desired to get values for.
     * @return Iterable<V>
     */
    protected Iterable<V> getValuesFor(K key) {
        return mapperContext.getMap().get(key);
    }

    //sending context to final result.
    private void sendContextToResult() {
        try {
            Socket result = new Socket(resultIp, Ports.REDUCER_RESULT_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(result.getOutputStream());
            objectOutputStream.writeObject(context);
            log("sending context with size : " + context.getMap().size() + " to result");
            objectOutputStream.close();
            result.close();
        } catch (IOException e) {
            log(e.toString());
            e.printStackTrace();
        }
    }

    protected void start() {
        try {
            performanceLogger.start();
            readContextFromShuffler();
            log("read context size : " + mapperContext.getMap().size());
            reduce();
            log("finished reducing reduced");
            sendContextToResult();
            performanceLogger.stop();
            performanceLogger.log();
        } catch (Exception e) {
            log(e.toString());
        }

    }

    private void log(String msg) {
        try {
            File file = new File("/map_reduce/log_reducer.txt");
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