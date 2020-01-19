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
        try(final ObjectOutputStream objectOutputStream = new ObjectOutputStream(mapper.getOutputStream())
            ;final ObjectInputStream objectInputStream = new ObjectInputStream(mapper.getInputStream())){
            objectOutputStream.writeObject(chunk);
            objectOutputStream.writeObject(config);
            objectInputStream.readInt();//wait until ack from mapper
            mapper.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void log(String msg) {
        try {
            File file = new File("/map_reduce/log_splitter.txt");
            if(!file.exists()){
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file,true);
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
