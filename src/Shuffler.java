import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class Shuffler {
    private Vector<Context> contexts = new Vector<>();
    private TreeMap map;
    private Configuration config;

    public Shuffler() {

    }

    private void readConfig() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.INPUT_SHUFFLER_PORT);
            Socket input = serverSocket.accept();
            ObjectInputStream objectInputStream = new ObjectInputStream(input.getInputStream());
            this.config = (Configuration) objectInputStream.readObject();
            objectInputStream.close();
            input.close();
            serverSocket.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    private void readFromMappers() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.MAPPER_SHUFFLER_PORT);
            while (true) {
                if (contexts.size() == config.getMapperNodes()) break;
                print("waiting for mappers");
                Socket mapper = serverSocket.accept();
                print("connected with " + mapper.getInetAddress());
                new Thread(() -> {
                    try {
                        ObjectInputStream objectInputStream = new ObjectInputStream(mapper.getInputStream());
                        Context context = (Context) objectInputStream.readObject();
                        print(context.getMap().toString());
                        contexts.add(context);
                        if (contexts.size() == config.getMapperNodes()) {
                            objectInputStream.close();
                            mapper.close();
                            serverSocket.close();
                            //TODO LOG this ...
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sort() {
        map = new TreeMap();

        for (Context context : contexts) {
            context.getMap().forEach((k, v) -> {
                if (map.containsKey(k)) {
                    LinkedList currentVal = (LinkedList) map.get(k);
                    LinkedList addToCurrent = (LinkedList) v;
                    currentVal.addAll(addToCurrent);
                    map.put(k, currentVal);
                } else {
                    map.put(k, v);
                }
            });
        }

    }

    private void sendToReducers() {
        print("creating chunks");
        Vector<Context> chunks = createChunks();
        print("chunks created");
        Iterator iterator = config.getReducerIpAddresses().iterator();
        for (Context chunk : chunks) {
            print("chunk : " + chunk.getMap().toString());
            if (iterator.hasNext()) {
                Iterator finalIterator = iterator;
                new Thread(() -> {
                    try {
                        String ip = (String) finalIterator.next();
                        Socket reducer = new Socket(ip, Ports.SHUFFLER_REDUCER_PORT);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(reducer.getOutputStream());
                        print(chunk.getMap().toString());
                        objectOutputStream.writeObject(chunk);
                        objectOutputStream.writeUTF(config.getResultIp());
                        objectOutputStream.close();
                        reducer.close();
                    } catch (IOException e) {
                        print(e.toString());
                        e.printStackTrace();
                    }
                }).start();
            } else {
                iterator = config.getReducerIpAddresses().iterator();
            }
        }

        /*Iterator iterator = chunks.iterator();

        for (String ip : config.getReducerIpAddresses()) {

            new Thread(() -> {
                try {
                    Context chunk = (Context) iterator.next();
                    Socket reducer = new Socket(ip, Ports.SHUFFLER_REDUCER_PORT);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(reducer.getOutputStream());
                    print(chunk.getMap().toString());
                    objectOutputStream.writeObject(chunk);
                    objectOutputStream.writeUTF(config.getResultIp());
                    objectOutputStream.close();
                    reducer.close();
                } catch (IOException e) {
                    print(e.toString());
                    e.printStackTrace();
                }
            }).start();
        }*/
    }

    private Vector<Context> createChunks() {
        int numOfChunks = config.getReducerNodes();
        int sizeOfChunk = map.size() / numOfChunks;
        Vector<Context> chunks = new Vector<>();
        TreeMap tmp;
        for (int i = 0; i < map.size(); i += sizeOfChunk) {
            if (i + sizeOfChunk >= map.size()) {
                tmp = (TreeMap) map.tailMap(map.keySet().toArray()[i]);
                chunks.add(new Context(tmp));
                print(tmp.toString());
            } else {
                tmp = (TreeMap) map.subMap(map.keySet().toArray()[i], map.keySet().toArray()[i + sizeOfChunk]);
                chunks.add(new Context(tmp));
                print(tmp.toString());
            }
        }
        return chunks;
    }


    public void start() {
        print("hello");
        readConfig();
        print("read config");
        readFromMappers();
        print("read from mappers");
        sort();
        sendToReducers();
        print("sent to reducers");
    }

    protected void print(String msg) {
        try {
            FileWriter fileWriter = new FileWriter(new File("/map_reduce/msgFromShuffler.txt"), true);
            fileWriter.write(msg + "\n");
            fileWriter.flush();
            fileWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new Shuffler().start();
        TimeUnit.MINUTES.sleep(1);

    }

}
