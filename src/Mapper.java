import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

public abstract class Mapper<K extends Comparable, V> {

    protected Context<K, V> context = new Context<>();
    protected LinkedList<String> data;
    private Configuration config;

    private void readData() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.SPLITTER_MAPPER_PORT);
            Socket splitter = serverSocket.accept();
            ObjectInputStream objectInputStream = new ObjectInputStream(splitter.getInputStream());
            data = (LinkedList<String>) objectInputStream.readObject();
            config = (Configuration) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public abstract void map();


    private void sendToShuffler() {
        try {
            Socket shuffler = new Socket(config.getShufflerIp(), Ports.MAPPER_SHUFFLER_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(shuffler.getOutputStream());
            objectOutputStream.writeObject(context);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        mapperStarted("mapper started");
        readData();
        mapperStarted(data.toString());
        map();
        mapperStarted("done mapping");
        sendToShuffler();
    }

    protected void mapperStarted(String msg){
        try {
            PrintStream printStream = new PrintStream(new FileOutputStream(new File("/map_reduce/msgFromMapper.txt")));
            printStream.append(msg);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

}
