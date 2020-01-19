import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.Vector;

public class Result {

    private Vector<Context> contexts = new Vector<>();
    private Configuration config;
    private TreeMap map;

    private void readConfig() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.INPUT_RESULT_PORT);
            Socket input = serverSocket.accept();
            ObjectInputStream objectInputStream = new ObjectInputStream(input.getInputStream());
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(input.getOutputStream());
            this.config = (Configuration) objectInputStream.readObject();
            objectOutputStream.writeInt(1);//ACK
            objectInputStream.close();
            objectOutputStream.close();
            input.close();
            serverSocket.close();
        } catch (IOException |
                ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    private void readContext() {
        try {
            ServerSocket serverSocket = new ServerSocket(Ports.REDUCER_RESULT_PORT);
            while (true) {
                print("contexts size = : " + contexts.size());
                if (contexts.size() == config.getReducerNodes()) break;
                print("waiting for reducers");
                Socket reducer = serverSocket.accept();
                print(" connection established with : " + reducer.getInetAddress());
                new Thread(() -> {
                    print("entered thread");
                    try {
                        ObjectInputStream objectInputStream = new ObjectInputStream(reducer.getInputStream());
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(reducer.getOutputStream());
                        print("before reading object");
                        Context context = (Context) objectInputStream.readObject();
                        objectOutputStream.writeInt(1);//ACK
                        print("context received : " + context.getMap().toString() + " from : " + reducer.getInetAddress());
                        contexts.add(context);
                        if (contexts.size() == config.getReducerNodes()) {
                            serverSocket.close();
                            //TODO LOG this ...
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        print(e.toString());
                        e.printStackTrace();
                    }
                }).start();
            }
        } catch (IOException e) {
            print(e.toString());
            e.printStackTrace();
        }
    }

    private void merge() {
        print("merging...");
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
        print(map.toString());
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


    private void start() {
        try {
            print("hello");
            readConfig();
            print("config read");
            readContext();
            print("context read");
            merge();
            print("done merging");
            writeFinalResult();
            print("actually it finished");
        } catch (Exception e) {
            print(e.toString());
        }
    }

    protected void print(String msg) {
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
