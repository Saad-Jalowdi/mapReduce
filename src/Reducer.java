import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public abstract class Reducer<K, V> {

    protected Context<K, V> context;
    private Context<K, V> mapperContext;
    private Iterable<K> keys;
    private String resultIp;


    private void readContext() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.SHUFFLER_REDUCER_PORT);
            Socket shuffler = serverSocket.accept();
            ObjectInputStream objectInputStream = new ObjectInputStream(shuffler.getInputStream());
            mapperContext = (Context) objectInputStream.readObject();
            resultIp = objectInputStream.readUTF();
            keys = mapperContext.getMap().keySet();
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
        if (context == null)
            throw new Exception("context has not been initialized"); //TODO context has not been initialized exception
        return context.getMap().get(key);
    }

    private void sendToResult() {
        try {
            Socket result = new Socket(resultIp, Ports.REDUCER_RESULT_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(result.getOutputStream());
            objectOutputStream.writeObject(context.getMap());
            objectOutputStream.close();
            result.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void start() {
        readContext();
        reduce();
        sendToResult();
    }

}
