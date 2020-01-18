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
            log("connected with" + splitter.getInetAddress());
            ObjectInputStream objectInputStream = new ObjectInputStream(splitter.getInputStream());
            data = (LinkedList<String>) objectInputStream.readObject();
            config = (Configuration) objectInputStream.readObject();
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
            objectOutputStream.writeObject(context);
            log(context.getMap().toString());
            objectOutputStream.close();
            shuffler.close();
        } catch (IOException e) {
            log(e.toString());
            e.printStackTrace();
        }
    }

    public void start() throws InterruptedException {
        log("mapper started");
        readData();
        log(data.toString());
        map();
        log("done mapping");
        sendToShuffler();
        log("sent to shuffler");
        TimeUnit.SECONDS.sleep(5);
    }

    protected void log(String msg) {
        try {
            FileWriter fileWriter = new FileWriter(new File("/map_reduce/log_mapper.txt"),true);
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
