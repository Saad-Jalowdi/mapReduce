import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Scanner;

/**
 * this class represents the input phase in the mapreduce framework.
 * it reads the input file
 *
 * @author Sa'ad Al Jalowdi.
 */
public class Input {
    private File inputFile;
    private Configuration config;
    private LinkedList<String> listOfStrings = new LinkedList<>();
    private ArrayList<String> mapperIpAddresses;
    private PerformanceLogger performanceLogger = PerformanceLogger.getLogger(this.getClass().getName());

    public Input(Configuration config) throws Exception {
        this.config = config;
        this.inputFile = this.config.getInputFile();
        readFile();
        this.mapperIpAddresses = config.getMapperIpAddresses();
    }


    private void readFile() throws Exception {
        if (inputFile == null) throw new Exception("input file should be initialized"); //TODO no input file exception
        try {
            Scanner scanner = new Scanner(inputFile);
            while (scanner.hasNext()) {
                listOfStrings.add(scanner.next());
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * splits the input file into splits that are equal to the number of mappers
     * and send them to the mappers in parallel.
     * if the read data are less than the number of mappers an empty LinkedList will be
     * sent to the remaining mappers as evenly as possible.
     *
     * @throws IOException
     */
    private void split() {
        int splitsToBeFilled;

        if (listOfStrings.size() < config.getMapperNodes()) {
            splitsToBeFilled = listOfStrings.size();
        } else {
            splitsToBeFilled = config.getMapperNodes();
        }

        LinkedList<LinkedList<String>> chunks = new LinkedList<>();
        LinkedList<String> tmp = new LinkedList<>();
        int size = listOfStrings.size();
        int sizePerSplit = size / splitsToBeFilled;
        int counter = 0;
        int filledSplits = 0;
        for (String s : listOfStrings) {
            if (counter == sizePerSplit && filledSplits < splitsToBeFilled) {
                chunks.add((LinkedList<String>) tmp.clone());
                tmp.clear();
                counter = 0;
                filledSplits++;
            }
            tmp.add(s);
            counter++;
        }

        if (sizePerSplit * splitsToBeFilled <= listOfStrings.size() && chunks.size() != splitsToBeFilled) {
            chunks.add((LinkedList<String>) tmp.clone());
        } else {
            chunks.getLast().addAll((LinkedList<String>) tmp.clone());
        }

        try {
            Iterator iterator = mapperIpAddresses.iterator();
            for (LinkedList<String> chunk : chunks) {
                new Splitter(new Socket((String) iterator.next(), Ports.SPLITTER_MAPPER_PORT), chunk, config).start();
            }

            for (int i = splitsToBeFilled; i < config.getMapperNodes(); i++) {
                new Splitter(new Socket((String) iterator.next(), Ports.SPLITTER_MAPPER_PORT), new LinkedList<>(), config).start();
            }
        } catch (IOException e) {
            log(e.toString());
        }
    }

    private void sendConfigToShuffler() {
        try {
            Socket shuffler = new Socket(config.getShufflerIp(), Ports.INPUT_SHUFFLER_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(shuffler.getOutputStream());
            ObjectInputStream objectInputStream = new ObjectInputStream(shuffler.getInputStream());
            objectOutputStream.writeObject(config);
            while (objectInputStream.readInt() != 1) ; //wait until ACK from shuffler important mapper could fail
            objectOutputStream.close();
            objectInputStream.close();
            shuffler.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendConfigToResult() {
        try {
            Socket result = new Socket(config.getResultIp(), Ports.INPUT_RESULT_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(result.getOutputStream());
            ObjectInputStream objectInputStream = new ObjectInputStream(result.getInputStream());
            objectOutputStream.writeObject(config);
            while (objectInputStream.readInt() != 1) ;//ACK
            objectInputStream.close();
            objectOutputStream.close();
            result.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        try {
            performanceLogger.start();
            sendConfigToShuffler();
            sendConfigToResult();
            split();
            performanceLogger.stop();
        } catch (Exception e) {
            log(e.toString());
        }
    }

    protected void log(String msg) {
        try {
            FileWriter fileWriter = new FileWriter(new File("/map_reduce/log_input.txt"), true);
            fileWriter.write(msg + "\n");
            fileWriter.flush();
            fileWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
