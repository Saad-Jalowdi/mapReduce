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
    private LinkedList<String> wordsInFile = new LinkedList<>();
    private ArrayList<String> mapperIpAddresses;
    private PerformanceLogger performanceLogger = PerformanceLogger.getLogger(this.getClass().getName());

    public Input(Configuration config) throws NoInputFileException {
        this.config = config;
        this.inputFile = this.config.getInputFile();
        readInputFile();
        this.mapperIpAddresses = config.getMapperIpAddresses();
    }


    private void readInputFile() throws NoInputFileException {
        if (inputFile == null) throw new NoInputFileException("use Configuration with args.");
        try {
            Scanner scanner = new Scanner(inputFile);
            while (scanner.hasNext()) {
                wordsInFile.add(scanner.next());
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

        if (wordsInFile.size() < config.getMapperNodes()) {
            splitsToBeFilled = wordsInFile.size();
        } else {
            splitsToBeFilled = config.getMapperNodes();
        }

        LinkedList<LinkedList<String>> chunks = new LinkedList<>();
        LinkedList<String> tmp = new LinkedList<>();
        int size = wordsInFile.size();
        int sizePerSplit = size / splitsToBeFilled;
        int counter = 0;
        int filledSplits = 0;
        for (String s : wordsInFile) {
            if (counter == sizePerSplit && filledSplits < splitsToBeFilled) {
                chunks.add((LinkedList<String>) tmp.clone());
                tmp.clear();
                counter = 0;
                filledSplits++;
            }
            tmp.add(s);
            counter++;
        }

        if (sizePerSplit * splitsToBeFilled <= wordsInFile.size() && chunks.size() < splitsToBeFilled) {
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
            objectOutputStream.writeObject(config);
            objectOutputStream.close();
            shuffler.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendConfigToResult() {
        try {
            Socket result = new Socket(config.getResultIp(), Ports.INPUT_RESULT_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(result.getOutputStream());
            objectOutputStream.writeObject(config);
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
            log("config sent to shuffler");
            sendConfigToResult();
            log("config sent to result");
            split();
            log("data splitted");
            performanceLogger.stop();
            performanceLogger.log();
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