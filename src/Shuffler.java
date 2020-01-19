import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Shuffler {
    private Vector<Context> contexts = new Vector<>();
    private TreeMap map;
    private Configuration config;
    private boolean finished = false;

    public Shuffler() {

    }

    private void readConfig() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.INPUT_SHUFFLER_PORT);
            Socket input = serverSocket.accept();
            ObjectInputStream objectInputStream = new ObjectInputStream(input.getInputStream());
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(input.getOutputStream());
            this.config = (Configuration) objectInputStream.readObject();
            objectOutputStream.writeInt(1);
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
                if (contexts.size() == config.getMapperNodes()) {
                    finished = true;
                    break;
                }
                log("waiting for mappers");
                Socket mapper = serverSocket.accept();
                log("connected with " + mapper.getInetAddress());
                new Thread(() -> {
                    try {
                        log("entered thread");
                        ObjectInputStream objectInputStream = new ObjectInputStream(mapper.getInputStream());
                        log("object input stream created");
                        Context context = (Context) objectInputStream.readObject();
                        log("context read");
                        log(context.getMap().toString());
                        contexts.add(context);
                        if (contexts.size() == config.getMapperNodes()) {
                            finished = true;
                            objectInputStream.close();
                            mapper.close();
                            serverSocket.close();
                            //TODO LOG this ...
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                        log(e.getStackTrace().toString());
                    }
                }).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
            log(e.getStackTrace().toString());
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
        log("creating chunks");
        Vector<Context> chunks = createChunks();
        log("chunks created");
        Iterator iterator = config.getReducerIpAddresses().iterator();
        for (Context chunk : chunks) {
            log("chunk : " + chunk.getMap().toString());
            if (iterator.hasNext()) {
                Iterator finalIterator = iterator;
                new Thread(() -> {
                    try {
                        String ip = (String) finalIterator.next();
                        Socket reducer = new Socket(ip, Ports.SHUFFLER_REDUCER_PORT);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(reducer.getOutputStream());
                        log("sending " + chunk.getMap().toString() + " to " + reducer.getInetAddress());
                        objectOutputStream.writeObject(chunk);
                        objectOutputStream.writeUTF(config.getResultIp());
                        objectOutputStream.close();
                        reducer.close();
                    } catch (IOException e) {
                        log(e.toString());
                        e.printStackTrace();
                    }
                }).start();
            } else {
                iterator = config.getReducerIpAddresses().iterator();
            }
        }

    }

    private Vector<Context> createChunks() {
        try {
            int numOfChunks = config.getReducerNodes();
            if (map.size() < numOfChunks)
                numOfChunks = map.size();//throw new Exception();//too many reducer for such an input you need map.size() reducers or less ...
            int sizeOfChunk = map.size() / numOfChunks;
            Vector<Context> chunks = new Vector<>();
            Map tmp;
            for (int i = 0; i < map.size(); i += sizeOfChunk) {
                if (i + sizeOfChunk * 2 >= map.size()) {
                    tmp = map.tailMap(map.keySet().toArray()[i]);
                } else {
                    tmp = map.subMap(map.keySet().toArray()[i], map.keySet().toArray()[i + sizeOfChunk]);
                }
                chunks.add(new Context((SortedMap) tmp));
                log(tmp.toString());
            }
            for (int i = map.size(); i < config.getReducerNodes(); i++) {
                chunks.add(new Context());
            }
            return chunks;
        } catch (Exception e) {
            log(e.getStackTrace().toString());
            System.exit(1);
        }
        return null;

    }


    public void start() {
        try {
            log("hello");
            readConfig();
            log("read config");
            readFromMappers();
            while (!finished) ;
            log("read from mappers");
            sort();
            sendToReducers();
            log("sent to reducers");
        } catch (Exception e) {
            log(e.getStackTrace().toString());
        }
    }

    protected void log(String msg) {
        try {
            File file = new File("/map_reduce/log_shuffler.txt");
            if (!file.exists()){
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

    public static void main(String[] args) throws InterruptedException {
        new Shuffler().start();
        TimeUnit.SECONDS.sleep(5);
    }

}
