import java.io.*;
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
            log(chunk.toString());
            objectOutputStream.writeObject(config);
            mapper.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void log(String msg) {
        try {
            FileWriter fileWriter = new FileWriter(new File("/map_reduce/log_splitter.txt"),true);
            fileWriter.write(msg+"\n");
            fileWriter.flush();
            fileWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
