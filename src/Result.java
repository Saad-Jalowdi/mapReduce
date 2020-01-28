import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.Vector;

/**
 * this class collects data from reducers in parallel
 * the same way shuffler collects data from mappers
 * and it keeps order.
 */
public class Result {

    private Vector<ReducerContext> contexts = new Vector<>();
    private Configuration config;
    private TreeMap map;

    private void readConfig() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.INPUT_RESULT_PORT);
            Socket input = serverSocket.accept();
            ObjectInputStream objectInputStream = new ObjectInputStream(input.getInputStream());
            this.config = (Configuration) objectInputStream.readObject();
            objectInputStream.close();
            input.close();
        } catch (IOException |
                ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    private void readContext() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.REDUCER_RESULT_PORT);
            while (true) {
                log("contexts size = : " + contexts.size());
                if (contexts.size() == config.getReducerNodes()) break;
                log("waiting for reducers");
                Socket reducer = serverSocket.accept();
                log(" connection established with : " + reducer.getInetAddress());
                new Thread(() -> {
                    log("entered thread");
                    try {
                        ObjectInputStream objectInputStream = new ObjectInputStream(reducer.getInputStream());
                        log("before reading object");
                        ReducerContext context = (ReducerContext) objectInputStream.readObject();
                        log("context received with size : " + context.getMap().size() + " from : " + reducer.getInetAddress());
                        contexts.add(context);
                        if (contexts.size() == config.getReducerNodes()) {
                            objectInputStream.close();
                            reducer.close();
                            serverSocket.close();
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        log(e.toString());
                        e.printStackTrace();
                    }
                }).start();
            }
        } catch (IOException e) {
            log(e.toString());
            e.printStackTrace();
        }
    }

    private void sort() {
        map = new TreeMap();

        for (ReducerContext context : contexts) {
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

    private void writeFinalResult() {
        try {
            File file = config.getOutputFile();
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            PrintStream printStream = new PrintStream(fileOutputStream);
            map.forEach((k, v) -> printStream.println(k + "," + v));
            printStream.flush();
            printStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    private final void start() {
        try {
            readConfig();
            log("config read");
            readContext();
            log("context read");
            sort();
            log("done sorting");
            writeFinalResult();
            log("finished writing result");
        } catch (Exception e) {
            log(e.toString());
        }
    }

    protected void log(String msg) {
        try {
            File file = new File("/map_reduce/log_result.txt");
            if(!file.exists()){
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

    public static void main(String[] args)  {
        new Result().start();
    }
}