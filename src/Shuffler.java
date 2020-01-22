import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * {@code Shuffler} represents the Shuffling phase of the
 * mapreduce framework.
 * it reads the output of each mapper in parallel and sort it
 * and resend it to reducers as evenly as possible.
 *
 * @author Sa'ad Al Jalowdi.
 */
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
            this.config = (Configuration) objectInputStream.readObject();
            objectInputStream.close();
            input.close();
            serverSocket.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    /**
     * this method reads contexts from mappers and adds them to Vector<Context>
     * to be sorted later.
     */
    private void readContextFromMappers() {
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
                        ObjectInputStream objectInputStream = new ObjectInputStream(mapper.getInputStream());
                        Context context = (Context) objectInputStream.readObject();
                        log("context with size " + context.getMap().size() + " received from mapper : " + mapper.getInetAddress());
                        contexts.add(context);
                        if (contexts.size() == config.getMapperNodes()) {
                            finished = true;
                            objectInputStream.close();
                            mapper.close();
                            serverSocket.close();
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
            log(e.getStackTrace().toString());
        }
    }


    /**
     * this method sorts the gathered contexts by simply adding them to
     * a TreeMap since TreeMap stores Keys in order once they added to it.
     */
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

    /**
     * this method sends the data to the reducers
     * in parallel and as evenly as possible.
     */
    private void sendContextToReducers() {
        Vector<Context> chunks = createChunks();
        Iterator iterator = config.getReducerIpAddresses().iterator();
        for (Context chunk : chunks) {
            if (iterator.hasNext()) {
                Iterator finalIterator = iterator;
                new Thread(() -> {
                    try {
                        String ip = (String) finalIterator.next();
                        Socket reducer = new Socket(ip, Ports.SHUFFLER_REDUCER_PORT);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(reducer.getOutputStream());
                        log("sending a chunk with size " + chunk.getMap().size() + " to reducer : " + reducer.getInetAddress());
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

    /**
     * this method splits the sorted data into chunks that are equal to
     * the number of reducers as evenly as possible.
     *
     * @return Vector<Context> each context in this vector is a chunk.
     */
    private Vector<Context> createChunks() {
        try {
            int numOfChunks;
            if (map.size() < config.getReducerNodes())
                numOfChunks = map.size();
            else {
                numOfChunks = config.getReducerNodes();
            }

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
            }
            for (int i = map.size(); i < config.getReducerNodes(); i++) {
                chunks.add(new Context());
            }
            return chunks;
        } catch (Exception e) {
            log(e.getStackTrace().toString());
        }
        return null;

    }


    public void start() {
        try {
            readConfig();
            log("config's");
            readContextFromMappers();
            while (!finished) ;
            log("finished reading from mappers");
            sort();
            sendContextToReducers();
            log("sent to reducers");
        } catch (Exception e) {
            log(e.toString());
        }
    }

    protected void log(String msg) {
        try {
            File file = new File("/map_reduce/log_shuffler.txt");
            if (!file.exists()) {
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
