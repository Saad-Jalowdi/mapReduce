import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public abstract class Mapper<K extends Comparable, V> {

    protected Context<K, V> context = new Context<>();
    protected LinkedList<String> data;
    private Configuration config;

    private void readData() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.SPLITTER_MAPPER_PORT);
            Socket splitter = serverSocket.accept();
            print("connected with" + splitter.getInetAddress());
            ObjectInputStream objectInputStream = new ObjectInputStream(splitter.getInputStream());
            data = (LinkedList<String>) objectInputStream.readObject();
            config = (Configuration) objectInputStream.readObject();
            objectInputStream.close();
            splitter.close();
            serverSocket.close();
            print("read data finished");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public abstract void map();


    private void sendToShuffler() {
        try {
            print(config.getShufflerIp());
            Socket shuffler = new Socket(config.getShufflerIp(), Ports.MAPPER_SHUFFLER_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(shuffler.getOutputStream());
            objectOutputStream.writeObject(context);
            print(context.getMap().toString());
            objectOutputStream.close();
            shuffler.close();
        } catch (IOException e) {
            print(e.toString());
            e.printStackTrace();
        }
    }

    public void start()  {
        print("mapper started");
        readData();
        print(data.toString());
        map();
        print("done mapping");
        sendToShuffler();
        print("sent to shuffler");

    }

    protected void print(String msg) {
        try {
            FileWriter fileWriter = new FileWriter(new File("/map_reduce/msgFromMapper.txt"),true);
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
