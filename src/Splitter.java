import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.LinkedList;

public class Splitter extends Thread{
    private Socket mapper;
    private LinkedList<String> chunk;
    private Configuration config;

    public Splitter(Socket socket, LinkedList<String> chunk, Configuration config) {
        this.mapper = socket;
        this.chunk=chunk;
        this.config = config;
    }

    @Override
    public void run() {
        try(final ObjectOutputStream objectOutputStream = new ObjectOutputStream(mapper.getOutputStream())){
            objectOutputStream.writeObject(chunk);
            objectOutputStream.writeObject(config);
            mapper.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
