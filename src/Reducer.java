import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

public abstract class Reducer<K, V> {

    protected Context<K, V> context = new Context<>();
    private Context<K, V> mapperContext;
    private Iterable<K> keys;
    private String resultIp;


    private void readContext() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.SHUFFLER_REDUCER_PORT);
            Socket shuffler = serverSocket.accept();
            print("connected with" + shuffler.getInetAddress());
            ObjectInputStream objectInputStream = new ObjectInputStream(shuffler.getInputStream());
            mapperContext = (Context) objectInputStream.readObject();
            resultIp = objectInputStream.readUTF();
            keys = mapperContext.getMap().keySet();
            print(mapperContext.getMap().toString());
            objectInputStream.close();
            shuffler.close();
            serverSocket.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    protected abstract void reduce();

    public Iterable<K> getKeys() {
        return keys;
    }

    protected Iterable<V> getValuesFor(K key) throws Exception {
        if (mapperContext == null)
            throw new Exception("context has not been initialized"); //TODO context has not been initialized exception
        return mapperContext.getMap().get(key);
    }

    private void sendToResult() {
        try {
            Socket result = new Socket(resultIp, Ports.REDUCER_RESULT_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(result.getOutputStream());
            objectOutputStream.writeObject(context);
            print(context.getMap().toString());
            objectOutputStream.close();
            result.close();
        } catch (IOException e) {
            print(e.toString());
            e.printStackTrace();
        }
    }

    public void start() {
        try {
            print("hello ");
            readContext();
            print("mapperContext: " + mapperContext.getMap().toString());
            reduce();
            print(" reduced");
            TimeUnit.SECONDS.sleep(5);
            sendToResult();
            TimeUnit.SECONDS.sleep(5);
            print("send to result");
        } catch (Exception e) {
            print(e.toString());
            System.exit(1);
        }

    }

    protected void print(String msg) {
        try {
            FileWriter fileWriter = new FileWriter(new File("/map_reduce/log_reducer.txt"), true);
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
