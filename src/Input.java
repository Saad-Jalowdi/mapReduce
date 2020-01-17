import java.io.*;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.logging.Logger;

public class Input {
    private final transient Logger logger = Logger.getLogger(Input.class.getName());
    private File inputFile;
    private Configuration config;
    private LinkedList<String> listOfStrings = new LinkedList<>();
    private LinkedList<String> mapperIpAddresses;

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

    private void split() throws IOException {
        try {
            int splits = config.getMapperNodes();
            if (listOfStrings.size() < splits) {splits = listOfStrings.size();}
            LinkedList<LinkedList<String>> chunks = new LinkedList<>();
            LinkedList<String> tmp = new LinkedList<>();
            int size = listOfStrings.size();
            int sizeForEachSplit = size / splits;
            int counter = 0;
            for (String s : listOfStrings) {
                if (counter == sizeForEachSplit) {
                    chunks.add((LinkedList<String>) tmp.clone());
                    tmp.clear();
                    counter = 0;
                }
                tmp.add(s);
                counter++;
            }
            if (sizeForEachSplit * splits <= listOfStrings.size() && chunks.size() != splits) {
                chunks.add((LinkedList<String>) tmp.clone());
            } else {
                chunks.getLast().addAll((LinkedList<String>) tmp.clone());
            }
            log("chunks : ");
            for (LinkedList chunk : chunks) log(chunk.toString());
            for (int i = 0; i < splits; i++) {
                log(mapperIpAddresses.get(i));
                log(chunks.get(i).toString());
                new Splitter(new Socket(mapperIpAddresses.get(i), Ports.SPLITTER_MAPPER_PORT), chunks.get(i), config).start();
            }
            for (int i = splits ; i < config.getReducerNodes()-splits ; i++){
                new Splitter(new Socket(mapperIpAddresses.get(i), Ports.SPLITTER_MAPPER_PORT), new LinkedList<>(), config).start();
            }
        } catch (Exception e) {
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
            log(listOfStrings.toString());
            sendConfigToShuffler();
            sendConfigToResult();
            split();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void log(String msg) {
        try {
            FileWriter fileWriter = new FileWriter(new File("/map_reduce/msgFromInput.txt"), true);
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
